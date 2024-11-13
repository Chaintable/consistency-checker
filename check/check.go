package check

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"log"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/segmentio/kafka-go"

	"github.com/Chaintable/consistency_checker/config"
	"github.com/Chaintable/consistency_checker/db"
	"github.com/Chaintable/consistency_checker/nodes"
	"github.com/Chaintable/pipeline/types"
	"github.com/Chaintable/pipeline/util"

	s3config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Checker struct {
	innerNewBlockReader           *kafka.Reader
	innerReplicaStateChangeWriter *kafka.Writer
	outerNewBlockWriter           *kafka.Writer
	outerS3Reader                 *s3.Client
	confg                         *config.Config
	// 副本80%高度
	ReplicaLatestBlockNumber uint64
	quit                     chan struct{}
}

func NewKafkaReader(brokers []string, topic string, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	})
}

func NewKafkaWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,
		BatchSize:    1,
	}
}

func NewS3Reader(region string) (*s3.Client, error) {
	cfg, err := s3config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(cfg)
	return client, nil
}

func NewChecker(config *config.Config) (*Checker, error) {
	err := db.OpenConsistencyDB(config.ConsistencyDBPath)
	if err != nil {
		log.Printf("open db error %+v", err)
		return nil, err
	}

	innerS3Reader, err := NewS3Reader(config.OuterS3Region)
	if err != nil {
		log.Printf("create s3 reader error %+v", err)
		return nil, err
	}

	return &Checker{
		innerNewBlockReader:           NewKafkaReader(config.InnerBrokers, config.InnerReplicaStateChangeTopic, config.InnerNewBlockGroupID),
		innerReplicaStateChangeWriter: NewKafkaWriter(config.InnerBrokers, config.InnerReplicaStateChangeTopic),
		outerS3Reader:                 innerS3Reader,
		outerNewBlockWriter:           NewKafkaWriter(config.OuterBrokers, config.OuterNewBlockTopic),
		confg:                         config,
		quit:                          make(chan struct{}),
	}, nil
}

func (c *Checker) Close() {
	close(c.quit)
	time.Sleep(1 * time.Second)
	db.DB.Close()
}

func (c *Checker) getValidationHash(blockCtx *types.BlockContext) (int64, error) {
	chainID := (*hexutil.Big)(big.NewInt(int64(c.confg.ChainID)))
	s3Key := fmt.Sprintf("%s/%s/", chainID.String(), blockCtx.Hash.String())
	params := &s3.ListObjectsV2Input{
		Bucket: &c.confg.OuterS3Bucket,
		Prefix: &s3Key,
	}
	res, err := c.outerS3Reader.ListObjectsV2(context.Background(), params, nil)
	if err != nil {
		return 0, err
	}
	if len(res.Contents) == 0 {
		return 0, nil
	}
	for _, obj := range res.Contents {
		after, found := strings.CutPrefix(*obj.Key, s3Key)
		if !found {
			continue
		}
		after = strings.TrimSuffix(after, "/")
		// parse to int
		validationHash, err := strconv.ParseInt(after, 10, 64)
		if err != nil {
			return 0, err
		}
		return validationHash, nil

	}
	return 0, nil
}

func (c *Checker) getValidationHashWithReTry(blockCtx *types.BlockContext) (int64, error) {
	for i := 0; i < 3; i++ {
		validationHash, err := c.getValidationHash(blockCtx)
		if err != nil {
			return 0, err
		}
		if validationHash != 0 {
			return validationHash, nil
		}
		time.Sleep(1 * time.Second)
	}
	return 0, fmt.Errorf("get validation hash many times but not ready")
}

func (c *Checker) getValidationHashMany(blockNotice *types.BlockChangeNotification) ([]int64, error) {
	blockIDs := make([]common.Hash, len(blockNotice.NewBlocks))
	validationHashes := make([]int64, len(blockIDs))
	var err error
	for i, block := range blockNotice.NewBlocks {
		validationHashes[i], err = c.getValidationHashWithReTry(&block)
		if err != nil {
			return nil, nil
		}
	}
	return validationHashes, nil
}

func (c *Checker) remoteBlock(blockCtx *types.BlockContext) error {
	chainID := (*hexutil.Big)(big.NewInt(int64(c.confg.ChainID)))
	s3Key := fmt.Sprintf("%s/%d/%s", chainID.String(), blockCtx.BlockNumber, blockCtx.Hash.String())
	params := &s3.DeleteObjectInput{
		Bucket: &c.confg.OuterS3Bucket,
		Key:    &s3Key,
	}
	_, err := c.outerS3Reader.DeleteObject(context.Background(), params)
	if err != nil {
		return err
	}
	return nil
}

func (c *Checker) remote_drop_blocks(blockNotice *types.BlockChangeNotification) error {
	for _, block := range blockNotice.DropBlocks {
		err := c.remoteBlock(&block)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Checker) check(kafkaLatestBlockNumber uint64) (*types.ReplicaStateChangeNotification, error) {
	nodeStates := nodes.NodeMap.CheckAll(kafkaLatestBlockNumber)
	readyNodes := 0
	for _, nodeState := range nodeStates {
		if nodeState.StateType == 1 {
			readyNodes++
		}
	}
	if float64(readyNodes)/float64(len(nodeStates)) >= c.confg.ReadyRatio {
		for _, nodeState := range nodeStates {
			if nodeState.StateType == 1 {
				if c.ReplicaLatestBlockNumber < nodeState.LatestBlockNumber.ToInt().Uint64() {
					c.ReplicaLatestBlockNumber = nodeState.LatestBlockNumber.ToInt().Uint64()
				}
			}
		}
		return &types.ReplicaStateChangeNotification{
			LatestBlockNumber: (*hexutil.Big)(big.NewInt(int64(c.ReplicaLatestBlockNumber))),
		}, nil
	}
	return nil, nil
}

func (c *Checker) checkWithReTry(kafkaLatestBlockNumber uint64) (*types.ReplicaStateChangeNotification, error) {
	if c.ReplicaLatestBlockNumber > kafkaLatestBlockNumber {
		return nil, nil
	}
	for i := 0; i < 3; i++ {
		replicaStateChange, err := c.check(kafkaLatestBlockNumber)
		if err != nil {
			return nil, err
		}
		if replicaStateChange != nil {
			return replicaStateChange, nil
		}
		time.Sleep(time.Duration(c.confg.CheckInterval) * time.Millisecond)
	}
	return nil, fmt.Errorf("check many times but not ready")
}

func (c *Checker) Process(blockNotice *types.BlockChangeNotification) bool {
	latestBlockNumber := blockNotice.NewBlocks[len(blockNotice.NewBlocks)-1].BlockNumber
	replicaStateChange, err := c.checkWithReTry(latestBlockNumber)
	if err != nil {
		log.Printf("check error %+v", err)
		return false
	}
	if replicaStateChange != nil {
		err = WriteReplicaStateChange(c.innerReplicaStateChangeWriter, replicaStateChange)
		if err != nil {
			return false
		}
	}

	err = c.remote_drop_blocks(blockNotice)
	if err != nil {
		log.Printf("remote drop blocks error %+v", err)
		return false
	}

	validationHashes, err := c.getValidationHashMany(blockNotice)
	if err != nil {
		log.Printf("get validation hash error %+v", err)
		return false
	}

	err = db.DB.WriteBlockInfos(blockNotice, validationHashes)
	if err != nil {
		log.Printf("write block info error %+v", err)
		return false
	}

	err = WriteBlockNotice(c.outerNewBlockWriter, blockNotice)
	return err == nil
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
					break
				}
			}
		}
	}

shutdown:
	c.innerNewBlockReader.Close()
	c.outerNewBlockWriter.Close()
}

func WriteReplicaStateChange(writer *kafka.Writer, replicaStateChange *types.ReplicaStateChangeNotification) error {
	value, err := util.EncodeToJsonGzip(replicaStateChange)
	if err != nil {
		return err
	}
	err = writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte("ReplicaStateChange"),
		Value: value,
	})
	if err != nil {
		return err
	}
	return nil
}

func WriteBlockNotice(writer *kafka.Writer, blockNotice *types.BlockChangeNotification) error {
	value, err := util.EncodeToJsonGzip(blockNotice)
	if err != nil {
		return err
	}
	err = writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte("NewBlock"),
		Value: value,
	})
	if err != nil {
		return err
	}
	return nil
}
