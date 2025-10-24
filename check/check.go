package check

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"slices"
	"strings"
	"time"

	"github.com/Chaintable/consistency-checker/metrics"

	"log"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/segmentio/kafka-go"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/Chaintable/consistency-checker/config"
	"github.com/Chaintable/consistency-checker/db"
	"github.com/Chaintable/consistency-checker/nodes"
	"github.com/Chaintable/pipeline/types"
	"github.com/Chaintable/pipeline/util"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Checker struct {
	innerNewBlockReader                *kafka.Reader
	outerNewBlockWriter                *kafka.Writer
	outerS3Reader                      *s3.Client
	etcdClient                         *clientv3.Client
	confg                              *config.Config
	latestWriteEtcd                    time.Time
	latestOuterBlockChangeNotification *types.OuterBlockChangeNotification
	latestMsgOffset                    int64
	// 副本80%高度
	ReplicaLatestBlockNumber uint64
	quit                     chan struct{}
}

func NewChecker(config *config.Config) (*Checker, error) {
	err := db.OpenConsistencyDB(config.ConsistencyDBPath)
	if err != nil {
		log.Printf("open db error %+v", err)
		return nil, err
	}

	innerS3Reader, err := util.NewS3Client(config.OuterS3Region)
	if err != nil {
		log.Printf("create s3 reader error %+v", err)
		return nil, err
	}

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:       config.EtcdEndpoints,
		DialTimeout:     5 * time.Second,
		MaxUnaryRetries: 10,
	})
	if err != nil {
		log.Printf("create etcd client error %+v", err)
		return nil, err
	}

	err = nodes.InitFromEtcd(config.ChainID, etcdClient)
	if err != nil {
		log.Printf("init from etcd error %+v", err)
		return nil, err
	}

	outerReader := util.NewKafkaReader(config.OuterBrokers, config.OuterNewBlockTopic, "")
	latestOuterBlockChangeNotification, err := GetLastOuterBlockNotice(outerReader)
	if err != nil {
		log.Printf("get last outer block notice error %+v", err)
		return nil, err
	}
	log.Printf("latestOuterBlockChangeNotification %+v", latestOuterBlockChangeNotification)

	innerNewBlockReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        config.InnerBrokers,
		Topic:          config.InnerNewBlockTopic,
		GroupID:        config.InnerNewBlockGroupID,
		CommitInterval: time.Duration(config.CommitInterval * int(time.Second)),
	})

	return &Checker{
		innerNewBlockReader:                innerNewBlockReader,
		outerS3Reader:                      innerS3Reader,
		outerNewBlockWriter:                util.NewKafkaWriter(config.OuterBrokers, config.OuterNewBlockTopic),
		etcdClient:                         etcdClient,
		confg:                              config,
		latestOuterBlockChangeNotification: latestOuterBlockChangeNotification,
		quit:                               make(chan struct{}),
	}, nil
}

func (c *Checker) Close() {
	close(c.quit)
	time.Sleep(1 * time.Second)
	db.DB.Close()
}

func (c *Checker) getValidationHash(blockCtx *types.BlockContext) (int64, error) {
	s3Key := fmt.Sprintf("%d/%d/%s", c.confg.ChainID, blockCtx.BlockNumber, blockCtx.Hash.String())
	obj, err := c.outerS3Reader.GetObject(
		context.Background(),
		&s3.GetObjectInput{
			Bucket: &c.confg.OuterS3Bucket,
			Key:    &s3Key,
		},
	)
	if err != nil {
		return 0, err
	}
	defer obj.Body.Close()
	buf := new(bytes.Buffer)
	buf.ReadFrom(obj.Body)
	validation := types.BlockValidation{}
	err = util.DecodeFromGzipJson(buf.Bytes(), &validation)
	if err != nil {
		return 0, err
	}
	return validation.ValidationHash, nil
}

func (c *Checker) getValidationHashWithReTry(blockCtx *types.BlockContext) (int64, error) {
	for i := 0; i < 3; i++ {
		validationHash, err := c.getValidationHash(blockCtx)
		if err != nil {
			log.Printf("get validation hash error %+v", err)
		} else {
			return validationHash, nil
		}
		time.Sleep(1 * time.Second)
	}
	return 0, fmt.Errorf("get validation hash many times but not ready")
}

func (c *Checker) getValidationHashMany(newBlocks []types.BlockContext) ([]int64, error) {
	validationHashes := make([]int64, len(newBlocks))
	var err error
	for i, block := range newBlocks {
		validationHashes[i], err = c.getValidationHashWithReTry(&block)
		if err != nil {
			return nil, err
		}
	}
	return validationHashes, nil
}

func (c *Checker) rewriteBlock(blockCtx *types.BlockContext, blockValidation int64) error {
	s3Key := fmt.Sprintf("%d/%d/%s", c.confg.ChainID, blockCtx.BlockNumber, blockCtx.Hash.String())
	validation := types.BlockValidation{
		ValidationHash: blockValidation,
		IsFork:         true,
	}
	data, err := util.EncodeToJsonGzip(&validation)
	if err != nil {
		return nil
	}
	params := &s3.PutObjectInput{
		Bucket: &c.confg.OuterS3Bucket,
		Key:    &s3Key,
		Body:   bytes.NewReader(data),
	}
	_, err = c.outerS3Reader.PutObject(context.Background(), params)
	if err != nil {
		return err
	}
	return nil
}

func (c *Checker) rewriteDropBlocks(dropBlocks []types.BlockContext) error {
	for _, block := range dropBlocks {
		blockValidation, err := c.getValidationHashWithReTry(&block)
		if err != nil {
			return err
		}
		log.Printf("rewrite block %d", block.BlockNumber)
		err = c.rewriteBlock(&block, blockValidation)
		if err != nil {
			return err
		}
	}
	return nil
}

// rewriteForkBlocksAtSameHeight 检查S3中相同高度但不同hash的区块，将其标记为fork
func (c *Checker) rewriteForkBlocksAtSameHeight(newBlocks []types.BlockContext) {
	for _, block := range newBlocks {
		// 列出该高度下的所有区块
		prefix := fmt.Sprintf("%d/%d/", c.confg.ChainID, block.BlockNumber)
		listParams := &s3.ListObjectsV2Input{
			Bucket: &c.confg.OuterS3Bucket,
			Prefix: &prefix,
		}

		resp, err := c.outerS3Reader.ListObjectsV2(context.Background(), listParams)
		if err != nil {
			log.Printf("list objects at height %d error: %+v", block.BlockNumber, err)
			continue // 继续处理其他区块
		}

		// 遍历该高度下的所有区块
		for _, obj := range resp.Contents {
			if obj.Key == nil {
				continue
			}

			// 从key中提取hash
			// key格式: {chainID}/{blockNumber}/{hash}
			keyParts := bytes.Split([]byte(*obj.Key), []byte("/"))
			if len(keyParts) != 3 {
				continue
			}

			existingHashStr := string(keyParts[2])

			// 如果hash不同，说明是fork，需要重写
			if !strings.EqualFold(existingHashStr, block.Hash.String()) {
				log.Printf("found fork block at height %d: existing hash %s, new canonical hash %s",
					block.BlockNumber, existingHashStr, block.Hash.String())

				// 获取原有的validation hash
				forkBlockCtx := types.BlockContext{
					BlockNumber: block.BlockNumber,
					Hash:        common.HexToHash(existingHashStr),
				}

				forkValidationHash, err := c.getValidationHash(&forkBlockCtx)
				if err != nil {
					log.Printf("get fork block validation hash error: %+v", err)
					continue
				}

				// 重写为fork
				err = c.rewriteBlock(&forkBlockCtx, forkValidationHash)
				if err != nil {
					log.Printf("rewrite fork block %s at height %d error: %+v",
						existingHashStr, block.BlockNumber, err)
				} else {
					log.Printf("successfully marked block %s at height %d as fork",
						existingHashStr, block.BlockNumber)
				}
			}
		}
	}

}

type ReplicaStateChangeNotification struct {
	LatestBlockNumber *hexutil.Big
	ReplicaStates     []nodes.NodeWithHeight
}

func (c *Checker) check(kafkaLatestBlockNumber uint64) (*ReplicaStateChangeNotification, error) {
	nodeStates := nodes.NodeMap.CheckAll(kafkaLatestBlockNumber)
	if len(nodeStates) == 0 {
		return nil, fmt.Errorf("no node")
	}
	readyNodes := 0
	for _, nodeState := range nodeStates {
		if nodeState.StateType == 1 {
			readyNodes++
		}
	}
	// latestBlockNumber 是所有ready节点中，高度最低的节点的高度
	latestBlockNumber := math.MaxInt64
	if float64(readyNodes)/float64(len(nodeStates)) >= c.confg.ReadyRatio {
		for _, nodeState := range nodeStates {
			if nodeState.StateType == 1 {
				if latestBlockNumber > int(nodeState.LatestBlockNumber) {
					latestBlockNumber = int(nodeState.LatestBlockNumber)
				}
			}
		}
		return &ReplicaStateChangeNotification{
			LatestBlockNumber: (*hexutil.Big)(big.NewInt(int64(latestBlockNumber))),
			ReplicaStates:     nodeStates,
		}, nil
	}
	return nil, nil
}

func (c *Checker) checkWithReTry(kafkaLatestBlockNumber uint64) (*ReplicaStateChangeNotification, error) {
	var err error
	var replicaStateChange *ReplicaStateChangeNotification
	for i := 0; i < c.confg.CheckNum; i++ {
		replicaStateChange, err = c.check(kafkaLatestBlockNumber)
		if err != nil {
			log.Printf("check error %+v", err)
		}
		if replicaStateChange != nil {
			return replicaStateChange, nil
		}
		time.Sleep(time.Duration(c.confg.CheckInterval) * time.Millisecond)
	}
	return nil, fmt.Errorf("check many times but not ready: %v", err)
}

func (c *Checker) checkAndNotify(kafkaLatestBlockNumber uint64) bool {
	// 如果副本高度大于kafka最新高度，直接返回(一致性节点后上线)
	if c.ReplicaLatestBlockNumber > kafkaLatestBlockNumber && time.Since(c.latestWriteEtcd) < 1*time.Second {
		return true
	}

	replicaStateChange, err := c.checkWithReTry(kafkaLatestBlockNumber)
	if err != nil {
		log.Printf("check error %+v", err)
		return false
	}
	if replicaStateChange.LatestBlockNumber != nil {
		c.ReplicaLatestBlockNumber = uint64(replicaStateChange.LatestBlockNumber.ToInt().Int64())
		log.Printf("ReplicaLatestBlockNumber %d", c.ReplicaLatestBlockNumber)
	}
	if replicaStateChange != nil {
		err = c.WriteReplicaStateChangeToEtcd(c.etcdClient, replicaStateChange)
		if err != nil {
			log.Printf("write replica state change error %+v", err)
			return false
		}
	}
	return true
}

func (c *Checker) WriteReplicaStateChangeToEtcd(writer *clientv3.Client, replicaStateChange *ReplicaStateChangeNotification) error {
	ops := make([]clientv3.Op, 0)

	type LastBlockBumber struct {
		LatestBlockNumber *hexutil.Big `json:"latestBlockNumber"`
	}

	lastHeight := LastBlockBumber{
		LatestBlockNumber: replicaStateChange.LatestBlockNumber,
	}

	lastHeightstr, err := json.Marshal(&lastHeight)
	if err != nil {
		return err
	}

	ops = append(ops, clientv3.OpPut(fmt.Sprintf("%d/lastBlockNumber", c.confg.ChainID), string(lastHeightstr)))

	for _, change := range replicaStateChange.ReplicaStates {
		if change.ShouldWrite {
			nodestr, err := json.Marshal(&change.Node)
			if err != nil {
				return err
			}
			if change.Node.Lease == 0 {
				return fmt.Errorf(change.Address + " lease is 0")
			}
			ops = append(ops, clientv3.OpPut(fmt.Sprintf("%d/nodes/%s_%d", c.confg.ChainID, change.Address, change.Port), string(nodestr), clientv3.WithLease(clientv3.LeaseID(change.Node.Lease))))
		}
	}

	timeout := time.Duration(c.confg.EtcdWriteTimeout) * time.Millisecond
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	log.Printf("start write to etcd ops %+v", ops)

	_, err = c.etcdClient.Txn(ctx).
		Then(ops...).
		Commit()

	log.Printf("end write to etcd ops %+v", ops)

	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			log.Printf("etcd write timeout: %v", err)
		}
		return err
	}
	c.latestWriteEtcd = time.Now()
	log.Printf("write replica state change to etcd %+v", replicaStateChange)
	return nil
}

func (c *Checker) reWriteForkBlock(dropBlocks []types.BlockContext) bool {
	// 对于fork block，重写is_fork=true
	err := c.rewriteDropBlocks(dropBlocks)
	if err != nil {
		log.Printf("remote drop blocks error %+v", err)
		return false
	}

	return true
}

func (c *Checker) writeBlockInfoToDB(newBlocks []types.BlockContext) bool {
	validationHashes, err := c.getValidationHashMany(newBlocks)
	if err != nil {
		log.Printf("get validation hash error %+v\n", err)
		return false
	}

	err = db.DB.WriteBlockInfos(newBlocks, validationHashes)
	if err != nil {
		log.Printf("write block info error %+v", err)
		return false
	}
	return true
}

func (c *Checker) msgCheck(blockNotice *types.BlockChangeNotification) bool {
	if c.latestOuterBlockChangeNotification != nil {
		if len(blockNotice.DropBlocks) > 0 {
			if blockNotice.DropBlocks[len(blockNotice.DropBlocks)-1].Hash != c.latestOuterBlockChangeNotification.Hash {
				log.Printf("drop block hash is not equal to latest block hash")
				return false
			}
		} else {
			if blockNotice.NewBlocks[0].ParentHash != c.latestOuterBlockChangeNotification.Hash {
				log.Printf("new block parentHash is not equal to latest block hash, blockNotice.NewBlocks %+v, c.latestOuterBlockChangeNotification %+v", blockNotice.NewBlocks, c.latestOuterBlockChangeNotification)
				return false
			}
		}
	}
	return true
}

func (c *Checker) Process(blockNotice *types.BlockChangeNotification) bool {
	// 0. 消息校验
	if !c.msgCheck(blockNotice) {
		log.Printf("msg check error")
		return false
	}

	// kafka最新高度
	kafkaLatestBlockNumber := blockNotice.NewBlocks[len(blockNotice.NewBlocks)-1].BlockNumber

	// 1. 一致性check，返回已就绪节点的最低高度
	if !c.checkAndNotify(kafkaLatestBlockNumber) {
		log.Printf("check and notify error")
		return false
	}

	dropBlocks := blockNotice.DropBlocks
	slices.Reverse(dropBlocks)

	newBlocks := blockNotice.NewBlocks

	// 2. 重写fork block
	if !c.reWriteForkBlock(dropBlocks) {
		log.Printf("rewrite fork block error")
		return false
	}

	// 3. 写入db
	if !c.writeBlockInfoToDB(blockNotice.NewBlocks) {
		log.Printf("write block info to db error")
		return false
	}

	// 4. 检查并重写S3中相同高度但不同hash的BlockValidation，标记IsFork=true
	c.rewriteForkBlocksAtSameHeight(newBlocks)

	// 5. 发送drop block通知
	if !c.WriteDropBlockNotice(dropBlocks) {
		log.Printf("write drop block notice error")
		return false
	}

	// 6. 发送新块通知
	if !c.WriteNewBlockNotice(newBlocks) {
		log.Printf("write new block notice error")
		return false
	}
	return true
}

func (c *Checker) WriteDropBlockNotice(dropBlocks []types.BlockContext) bool {
	for _, block := range dropBlocks {
		b := &types.OuterBlockChangeNotification{
			BlockNumber: block.BlockNumber,
			Hash:        block.Hash,
			ChainID:     c.confg.ChainID,
			Timestamp:   block.Timestamp,
			IsFork:      true,
		}
		err := util.WriteOuterBlockNotice(c.outerNewBlockWriter, b)
		if err != nil {
			log.Printf("write drop block notice error %+v", err)
			return false
		}
	}
	return true
}

func (c *Checker) WriteNewBlockNotice(newBlocks []types.BlockContext) bool {
	for _, block := range newBlocks {
		metrics.LatestPushedBlockNumber.Set(float64(block.BlockNumber))
		metrics.LatestPushedBlockTime.Set(float64(block.Timestamp))
		b := &types.OuterBlockChangeNotification{
			BlockNumber: block.BlockNumber,
			Hash:        block.Hash,
			ChainID:     c.confg.ChainID,
			Timestamp:   block.Timestamp,
			IsFork:      false,
		}
		err := util.WriteOuterBlockNotice(c.outerNewBlockWriter, b)
		if err != nil {
			log.Printf("write new block notice error %+v", err)
			return false
		}
		c.latestOuterBlockChangeNotification = b
	}
	return true
}

func (c *Checker) Run() {
	for {
		select {
		case <-c.quit:
			goto shutdown
		default:
			msg, err := c.innerNewBlockReader.FetchMessage(context.Background())
			if err != nil {
				log.Printf("fetch message error %+v", err)
				time.Sleep(1 * time.Second)
				continue
			}
			blockNotice := &types.BlockChangeNotification{}
			err = util.DecodeFromGzipJson(msg.Value, blockNotice)
			if err != nil {
				log.Printf("decode message error %+v", err)
				time.Sleep(1 * time.Second)
				continue
			}
			for {
				if c.latestMsgOffset != 0 && msg.Offset <= c.latestMsgOffset {
					log.Printf("msg offset %d is same as last offset %d", msg.Offset, c.latestMsgOffset)
					break
				}
				if !c.Process(blockNotice) {
					log.Printf("process error %+v", err)
					time.Sleep(1 * time.Second)
					continue
				} else {
					break
				}
			}
			for {
				err = c.innerNewBlockReader.CommitMessages(context.Background(), msg)
				if err != nil {
					log.Printf("CommitMessages message error %+v", err)
					time.Sleep(1 * time.Second)
					continue
				} else {
					c.latestMsgOffset = msg.Offset
					if msg.Offset%100 == 0 {
						log.Printf("CommitMessages last offset %d, blockNotice %v", msg.Offset, blockNotice)
					}
					break
				}
			}
		}
	}

shutdown:
	c.innerNewBlockReader.Close()
	c.outerNewBlockWriter.Close()
	nodes.NodeMap.StopWatch()
	c.etcdClient.Close()
}

// 获取最后一个OuterBlockChangeNotification
func GetLastOuterBlockNotice(reader *kafka.Reader) (*types.OuterBlockChangeNotification, error) {
	// 获取 reader 配置信息
	config := reader.Config()
	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("no brokers configured")
	}

	// 连接到 topic 的 leader broker
	conn, err := kafka.DialLeader(context.Background(), "tcp", config.Brokers[0], config.Topic, 0)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// 获取低水位
	firstOffset, err := conn.ReadFirstOffset()
	if err != nil {
		return nil, err
	}

	// 获取高水位
	lastOffset, err := conn.ReadLastOffset()
	if err != nil {
		return nil, err
	}

	// 如果高水位和低水位相同，说明 topic 中没有消息
	if firstOffset == lastOffset {
		return nil, nil
	}

	reader.SetOffset(0)
	lag, err := reader.ReadLag(context.Background())
	if err != nil {
		return nil, err
	}
	if lag == 0 {
		return nil, nil
	}

	err = reader.SetOffset(lag - 1)
	if err != nil {
		return nil, err
	}

	msg, err := reader.ReadMessage(context.Background())
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(msg.Key, []byte("NewBlock")) {
		return nil, fmt.Errorf("last message is not NewBlock")
	}

	blockNotice := &types.OuterBlockChangeNotification{}
	err = util.DecodeFromGzipJson(msg.Value, blockNotice)
	if err != nil {
		return nil, err
	}

	return blockNotice, nil
}
