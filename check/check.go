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
	"sync"
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
	sync.Mutex
	innerNewBlockReader                *kafka.Reader
	outerVersionNewBlockWriter         *kafka.Writer
	latestOuterBlockChangeNotification *types.OuterBlockChangeNotification
	outerSingletonNewBlockWriter       *kafka.Writer
	etcdLock                           *EtcdLock
	outerS3Reader                      *s3.Client
	etcdClient                         *clientv3.Client
	config                             *config.Config
	latestWriteEtcd                    time.Time
	latestMsgOffset                    int64
	isOuterSingletonAlign              bool
	// 缓存的outer singleton block，避免重复创建kafka reader
	cachedOuterSingleBlockChangeNotification *types.OuterBlockChangeNotification
	// 副本80%高度
	ReplicaLatestBlockNumber uint64
	// 上次写入etcd的LatestBlockNumber
	lastWrittenBlockNumber uint64
	quit                   chan struct{}
}

func NewChecker(config *config.Config) (*Checker, error) {
	err := db.OpenConsistencyDB(config.ConsistencyDBPath)
	if err != nil {
		log.Printf("open db error %+v", err)
		return nil, err
	}

	outerS3Reader, err := util.NewS3Client(config.OuterS3Region)
	if err != nil {
		log.Printf("create s3 reader error %+v", err)
		return nil, err
	}

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   config.EtcdEndpoints,
		DialTimeout: 1 * time.Second,
	})
	if err != nil {
		log.Printf("create etcd client error %+v", err)
		return nil, err
	}

	err = nodes.InitFromEtcd(config.ChainID, config.Version, etcdClient)
	if err != nil {
		log.Printf("init from etcd error %+v", err)
		return nil, err
	}

	innerNewBlockReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        config.InnerBrokers,
		Topic:          config.InnerNewBlockTopic,
		GroupID:        config.InnerNewBlockGroupID,
		CommitInterval: time.Duration(config.CommitInterval * int(time.Second)),
	})

	c := &Checker{
		innerNewBlockReader:          innerNewBlockReader,
		outerS3Reader:                outerS3Reader,
		outerSingletonNewBlockWriter: util.NewKafkaWriter(config.OuterBrokers, config.OuterNewBlockTopic),
		etcdClient:                   etcdClient,
		config:                       config,
		quit:                         make(chan struct{}),
	}

	// 版本模式：初始化版本相关的组件
	if config.IsVersionMode() {
		log.Printf("version mode enabled: version=%s, topic=%s", config.Version, config.OuterVersionNewBlockTopic)
		c.outerVersionNewBlockWriter = util.NewKafkaWriter(config.OuterBrokers, config.OuterVersionNewBlockTopic)

		latestOuterVersionBlockChangeNotification, err := GetLastOuterBlockNotice(util.NewKafkaReader(config.OuterBrokers, config.OuterVersionNewBlockTopic, ""))
		if err != nil {
			log.Printf("get last outer block notice error %+v", err)
			return nil, err
		}
		log.Printf("latestOuterVersionBlockChangeNotification %+v", latestOuterVersionBlockChangeNotification)
		c.latestOuterBlockChangeNotification = latestOuterVersionBlockChangeNotification

		err = c.InitLeaderFromEtcd()
		if err != nil {
			return nil, err
		}
	} else {
		// 非版本模式：直接获取 OuterNewBlockTopic 的最新消息
		log.Printf("legacy mode enabled: topic=%s", config.OuterNewBlockTopic)
		latestOuterBlockChangeNotification, err := GetLastOuterBlockNotice(util.NewKafkaReader(config.OuterBrokers, config.OuterNewBlockTopic, ""))
		if err != nil {
			log.Printf("get last outer block notice error %+v", err)
			return nil, err
		}
		log.Printf("latestOuterBlockChangeNotification %+v", latestOuterBlockChangeNotification)
		c.latestOuterBlockChangeNotification = latestOuterBlockChangeNotification
	}

	return c, nil
}

func (c *Checker) ChangeToChainLeader() error {
	c.Lock()
	defer c.Unlock()
	if c.etcdLock != nil {
		return nil
	}
	etcdLock := NewLock(
		fmt.Sprintf("%d/outer_block_notice", c.config.ChainID),
		fmt.Sprintf("%d/version", c.config.ChainID),
		c.config.Version,
		c.etcdClient,
		int64(c.config.EtcdLockTTL),
		c.RevokeChainLeader,
	)
	acquired := etcdLock.Acquire()
	if !acquired {
		log.Printf("acquire etcd lock failed")
		return fmt.Errorf("acquire etcd lock failed")
	}
	log.Printf("acquire etcd lock success")
	c.etcdLock = etcdLock
	return nil
}

func (c *Checker) RevokeChainLeader() {
	c.Lock()
	defer c.Unlock()
	if c.etcdLock != nil {
		c.etcdLock.Release()
		log.Printf("release etcd lock success")
		c.etcdLock = nil
		c.isOuterSingletonAlign = false
	}
}

func (c *Checker) InitLeaderFromEtcd() error {
	outerVersionKey := fmt.Sprintf("%d/version", c.config.ChainID)

	// 初始检查当前版本
	resp, err := c.etcdClient.Get(context.Background(), outerVersionKey)
	if err != nil {
		log.Printf("get outer version from etcd error %+v", err)
		return err
	}

	if len(resp.Kvs) > 0 {
		etcdVersion := string(resp.Kvs[0].Value)
		if etcdVersion == c.config.Version {
			log.Printf("version matches, becoming leader: %s", c.config.Version)
			err := c.ChangeToChainLeader()
			if err != nil {
				return err
			}
		} else {
			log.Printf("version %s, leader version %s, not leader", c.config.Version, etcdVersion)
		}
	} else {
		log.Printf("version key does not exist in etcd: %s", outerVersionKey)
	}

	// 启动 goroutine 周期性检查 version key
	go func() {
		interval := time.Duration(c.config.VersionCheckInterval) * time.Second
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		log.Printf("Starting periodic check for version key: %s (interval: %v)", outerVersionKey, interval)

		for {
			select {
			case <-ticker.C:
				resp, err := c.etcdClient.Get(context.Background(), outerVersionKey)
				if err != nil {
					log.Printf("Failed to get version key: %v", err)
					continue
				}

				if len(resp.Kvs) > 0 {
					currentVersion := string(resp.Kvs[0].Value)
					if currentVersion == c.config.Version {
						// 版本匹配，尝试成为 leader
						if c.etcdLock == nil {
							log.Printf("Version matches, attempting to become leader: %s", c.config.Version)
							if err := c.ChangeToChainLeader(); err != nil {
								log.Printf("Failed to become chain leader: %v", err)
							}
						}
					} else {
						// 版本不匹配，放弃 leader 身份
						if c.etcdLock != nil {
							log.Printf("Version mismatch (expected: %s, got: %s), revoking leader",
								c.config.Version, currentVersion)
							c.RevokeChainLeader()
						}
					}
				} else {
					// version key 不存在，放弃 leader 身份
					if c.etcdLock != nil {
						log.Printf("Version key does not exist, revoking leader")
						c.RevokeChainLeader()
					}
				}

			case <-c.quit:
				log.Printf("Stopping periodic check for version key: %s", outerVersionKey)
				return
			}
		}
	}()

	return nil
}

func (c *Checker) Close() {
	close(c.quit)
	time.Sleep(1 * time.Second)
	db.DB.Close()
}

func (c *Checker) getVersionBlockByHash(hash common.Hash) (*types.BlockContext, error) {
	s3Key := fmt.Sprintf("%d/%s/%s/block", c.config.ChainID, c.config.Version, hash.String())
	obj, err := c.outerS3Reader.GetObject(
		context.Background(),
		&s3.GetObjectInput{
			Bucket: &c.config.OuterS3Bucket,
			Key:    &s3Key,
		},
	)
	if err != nil {
		return nil, err
	}
	defer obj.Body.Close()
	buf := new(bytes.Buffer)
	buf.ReadFrom(obj.Body)
	header := types.Header{}
	err = util.DecodeFromGzipJson(buf.Bytes(), &header)
	if err != nil {
		return nil, err
	}
	blockCtx := &types.BlockContext{
		BlockNumber: header.Number.ToInt().Uint64(),
		Hash:        hash,
		ParentHash:  header.ParentHash,
		Timestamp:   uint64(header.Timestamp),
	}
	return blockCtx, nil
}

// GetCommonAncestor 查找两个区块的共同祖先
// 返回: (共同祖先, 本地祖先列表, 远程祖先列表, 错误)
func (c *Checker) GetCommonAncestor(localBlock, remoteBlock *types.BlockContext) (*types.BlockContext, []*types.BlockContext, []*types.BlockContext, error) {
	var localAncestors []*types.BlockContext
	var remoteAncestors []*types.BlockContext

	// 如果 local_block.hash == remote_block.parent_hash，说明 remote 是 local 的直接子节点
	if localBlock.Hash == remoteBlock.ParentHash {
		remoteAncestors = append(remoteAncestors, remoteBlock)
		return localBlock, localAncestors, remoteAncestors, nil
	}

	// 创建副本以避免修改原始数据
	currentLocal := *localBlock
	currentRemote := *remoteBlock

	// 如果 remote_block 的高度大于 local_block，先让 remote_block 往回走，直到两者高度相同
	for currentRemote.BlockNumber > currentLocal.BlockNumber {
		remoteAncestors = append(remoteAncestors, &types.BlockContext{
			BlockNumber: currentRemote.BlockNumber,
			Hash:        currentRemote.Hash,
			ParentHash:  currentRemote.ParentHash,
			Timestamp:   currentRemote.Timestamp,
		})

		parentBlock, err := c.getVersionBlockByHash(currentRemote.ParentHash)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to get remote parent block %s: %w", currentRemote.ParentHash.String(), err)
		}
		currentRemote = *parentBlock
	}

	// 两者同时往回走，直到找到相同的 hash（共同祖先）
	for currentLocal.Hash != currentRemote.Hash {
		localAncestors = append(localAncestors, &types.BlockContext{
			BlockNumber: currentLocal.BlockNumber,
			Hash:        currentLocal.Hash,
			ParentHash:  currentLocal.ParentHash,
			Timestamp:   currentLocal.Timestamp,
		})

		localParent, err := c.getVersionBlockByHash(currentLocal.ParentHash)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to get local parent block %s: %w", currentLocal.ParentHash.String(), err)
		}
		currentLocal = *localParent

		remoteAncestors = append(remoteAncestors, &types.BlockContext{
			BlockNumber: currentRemote.BlockNumber,
			Hash:        currentRemote.Hash,
			ParentHash:  currentRemote.ParentHash,
			Timestamp:   currentRemote.Timestamp,
		})

		remoteParent, err := c.getVersionBlockByHash(currentRemote.ParentHash)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to get remote parent block %s: %w", currentRemote.ParentHash.String(), err)
		}
		currentRemote = *remoteParent
	}

	// 共同祖先
	commonAncestor := &types.BlockContext{
		BlockNumber: currentLocal.BlockNumber,
		Hash:        currentLocal.Hash,
		ParentHash:  currentLocal.ParentHash,
		Timestamp:   currentLocal.Timestamp,
	}

	// 反转祖先列表，使其按照从祖先到当前的顺序排列
	slices.Reverse(localAncestors)
	slices.Reverse(remoteAncestors)

	return commonAncestor, localAncestors, remoteAncestors, nil
}

func (c *Checker) getRawValidation(blockCtx *types.BlockContext) (*types.BlockValidation, error) {
	var s3Key string
	if c.config.IsVersionMode() {
		s3Key = fmt.Sprintf("%d/%s/%d/%s", c.config.ChainID, c.config.Version, blockCtx.BlockNumber, blockCtx.Hash.String())
	} else {
		s3Key = fmt.Sprintf("%d/%d/%s", c.config.ChainID, blockCtx.BlockNumber, blockCtx.Hash.String())
	}
	obj, err := c.outerS3Reader.GetObject(
		context.Background(),
		&s3.GetObjectInput{
			Bucket: &c.config.OuterS3Bucket,
			Key:    &s3Key,
		},
	)
	if err != nil {
		return nil, err
	}
	defer obj.Body.Close()
	buf := new(bytes.Buffer)
	buf.ReadFrom(obj.Body)
	validation := types.BlockValidation{}
	err = util.DecodeFromGzipJson(buf.Bytes(), &validation)
	if err != nil {
		return nil, err
	}
	return &validation, nil
}

func (c *Checker) getValidationHash(blockCtx *types.BlockContext) (int64, error) {
	validation, err := c.getRawValidation(blockCtx)
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

func (c *Checker) getRawValidationWithReTry(blockCtx *types.BlockContext) (*types.BlockValidation, error) {
	for i := 0; i < 3; i++ {
		validation, err := c.getRawValidation(blockCtx)
		if err != nil {
			log.Printf("get raw validation error %+v", err)
		} else {
			return validation, nil
		}
		time.Sleep(1 * time.Second)
	}
	return nil, fmt.Errorf("get raw validation many times but not ready")
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

func (c *Checker) rewriteBlock(blockCtx *types.BlockContext, validation *types.BlockValidation) error {
	var s3Key string
	if c.config.IsVersionMode() {
		s3Key = fmt.Sprintf("%d/%s/%d/%s", c.config.ChainID, c.config.Version, blockCtx.BlockNumber, blockCtx.Hash.String())
	} else {
		s3Key = fmt.Sprintf("%d/%d/%s", c.config.ChainID, blockCtx.BlockNumber, blockCtx.Hash.String())
	}
	data, err := util.EncodeToJsonGzip(validation)
	if err != nil {
		return err
	}
	params := &s3.PutObjectInput{
		Bucket: &c.config.OuterS3Bucket,
		Key:    &s3Key,
		Body:   bytes.NewReader(data),
	}
	_, err = c.outerS3Reader.PutObject(context.Background(), params)
	if err != nil {
		return err
	}
	return nil
}

func (c *Checker) rewriteDropBlocks(dropBlocks []types.BlockContext) {
	for _, block := range dropBlocks {
		blockValidation, err := c.getRawValidation(&block)
		if err != nil {
			continue
		}
		blockValidation.IsFork = true
		log.Printf("rewrite block %d", block.BlockNumber)
		err = c.rewriteBlock(&block, blockValidation)
		if err != nil {
			continue
		}
	}
}

// rewriteForkBlocksAtSameHeight 检查S3中相同高度但不同hash的区块，将其标记为fork, 严格
func (c *Checker) rewriteForkBlocksAtSameHeight(newBlocks []types.BlockContext) error {
	for _, block := range newBlocks {
		// 列出该高度下的所有区块
		var prefix string
		if c.config.IsVersionMode() {
			prefix = fmt.Sprintf("%d/%s/%d/", c.config.ChainID, c.config.Version, block.BlockNumber)
		} else {
			prefix = fmt.Sprintf("%d/%d/", c.config.ChainID, block.BlockNumber)
		}
		listParams := &s3.ListObjectsV2Input{
			Bucket: &c.config.OuterS3Bucket,
			Prefix: &prefix,
		}

		var resp *s3.ListObjectsV2Output
		var err error
		for i := 0; i < 3; i++ {
			resp, err = c.outerS3Reader.ListObjectsV2(context.Background(), listParams)
			if err == nil {
				break
			}
			log.Printf("list objects at height %d error: %+v", block.BlockNumber, err)
			time.Sleep(1 * time.Second)
		}
		if err != nil {
			return fmt.Errorf("list objects at height %d error: %w", block.BlockNumber, err)
		}

		// 遍历该高度下的所有区块
		for _, obj := range resp.Contents {
			if obj.Key == nil {
				continue
			}

			// 从key中提取hash
			// 版本模式 key格式: {chainID}/{version}/{blockNumber}/{hash}
			// 非版本模式 key格式: {chainID}/{blockNumber}/{hash}
			keyParts := bytes.Split([]byte(*obj.Key), []byte("/"))
			var existingHashStr string
			if c.config.IsVersionMode() {
				if len(keyParts) != 4 {
					continue
				}
				existingHashStr = string(keyParts[3])
			} else {
				if len(keyParts) != 3 {
					continue
				}
				existingHashStr = string(keyParts[2])
			}

			// 如果hash不同，说明是fork，需要重写
			if !strings.EqualFold(existingHashStr, block.Hash.String()) {
				log.Printf("found fork block at height %d: existing hash %s, new canonical hash %s",
					block.BlockNumber, existingHashStr, block.Hash.String())

				// 读取原有validation，并仅更新IsFork，避免覆盖其他字段
				forkBlockCtx := types.BlockContext{
					BlockNumber: block.BlockNumber,
					Hash:        common.HexToHash(existingHashStr),
				}

				forkValidation, err := c.getRawValidationWithReTry(&forkBlockCtx)
				if err != nil {
					return fmt.Errorf("get fork block validation error at height %d, hash %s: %w",
						block.BlockNumber, existingHashStr, err)
				}
				if forkValidation.IsFork {
					continue
				}
				forkValidation.IsFork = true

				// 重写为fork
				for i := 0; i < 3; i++ {
					err = c.rewriteBlock(&forkBlockCtx, forkValidation)
					if err == nil {
						break
					}
					log.Printf("rewrite fork block %s at height %d error: %+v",
						existingHashStr, block.BlockNumber, err)
					time.Sleep(1 * time.Second)
				}
				if err != nil {
					return fmt.Errorf("rewrite fork block %s at height %d error: %w",
						existingHashStr, block.BlockNumber, err)
				}
				log.Printf("successfully marked block %s at height %d as fork",
					existingHashStr, block.BlockNumber)
			} else {
				validation, err := c.getRawValidationWithReTry(&block)
				if err != nil {
					return fmt.Errorf("get canonical block validation hash error at height %d, hash %s: %w",
						block.BlockNumber, block.Hash.String(), err)
				}
				if validation.IsFork {
					validation.IsFork = false
					data, err := util.EncodeToJsonGzip(validation)
					if err != nil {
						return fmt.Errorf("encode canonical block validation error at height %d, hash %s: %w",
							block.BlockNumber, block.Hash.String(), err)
					}
					for i := 0; i < 3; i++ {
						params := &s3.PutObjectInput{
							Bucket: &c.config.OuterS3Bucket,
							Key:    obj.Key,
							Body:   bytes.NewReader(data),
						}
						_, err = c.outerS3Reader.PutObject(context.Background(), params)
						if err == nil {
							break
						}
						log.Printf("rewrite canonical block %s at height %d error: %+v",
							block.Hash.String(), block.BlockNumber, err)
						time.Sleep(1 * time.Second)
					}
					if err != nil {
						return fmt.Errorf("rewrite canonical block %s at height %d error: %w",
							block.Hash.String(), block.BlockNumber, err)
					}
				}
			}
		}
	}
	return nil
}

type ReplicaStateChangeNotification struct {
	LatestBlockNumber *hexutil.Big
	ReplicaStates     []nodes.NodeWithHeight
}

func (c *Checker) check(kafkaLatestBlockNumber uint64) (*ReplicaStateChangeNotification, error) {
	nodeStates := nodes.NodeMap.CheckAll(kafkaLatestBlockNumber, time.Duration(c.config.RpcNodeTimeout)*time.Millisecond)
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
	if float64(readyNodes)/float64(len(nodeStates)) >= c.config.ReadyRatio {
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
	} else {
		log.Printf("ready nodes ratio %.2f below threshold %.2f", float64(readyNodes)/float64(len(nodeStates)), c.config.ReadyRatio)
		return &ReplicaStateChangeNotification{
			LatestBlockNumber: nil,
			ReplicaStates:     nodeStates,
		}, nil
	}
}

func (c *Checker) checkWithReTry(kafkaLatestBlockNumber uint64) (*ReplicaStateChangeNotification, error) {
	var err error
	var replicaStateChange *ReplicaStateChangeNotification
	for i := 0; i < c.config.CheckNum; i++ {
		replicaStateChange, err = c.check(kafkaLatestBlockNumber)
		if err != nil {
			log.Printf("check error %+v", err)
		}
		if replicaStateChange != nil {
			if replicaStateChange.LatestBlockNumber != nil {
				return replicaStateChange, nil
			}
		}
		time.Sleep(time.Duration(c.config.CheckInterval) * time.Millisecond)
	}
	return replicaStateChange, fmt.Errorf("check many times but not ready: %v", err)
}

func (c *Checker) CheckAndNotifyEtcd() bool {
	c.Lock()
	defer c.Unlock()
	return c.checkAndNotify(c.ReplicaLatestBlockNumber)
}

func (c *Checker) checkAndNotify(kafkaLatestBlockNumber uint64) bool {
	// 如果副本高度大于kafka最新高度，直接返回(一致性节点后上线)
	if c.ReplicaLatestBlockNumber > kafkaLatestBlockNumber && time.Since(c.latestWriteEtcd) < 1*time.Second {
		return true
	}

	replicaStateChange, err := c.checkWithReTry(kafkaLatestBlockNumber)
	if err != nil {
		log.Printf("check error %+v", err)
		if replicaStateChange != nil {
			c.RemoveOfflineNodesFromEtcd(c.etcdClient, replicaStateChange)
		}
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

func (c *Checker) RemoveOfflineNodesFromEtcd(writer *clientv3.Client, replicaStateChange *ReplicaStateChangeNotification) {
	var err error
	ops := make([]clientv3.Op, 0)
	for _, change := range replicaStateChange.ReplicaStates {
		if change.ChangeType == nodes.DelNode && change.Node.Lease == 0 {
			var nodeKey string
			if c.config.IsVersionMode() {
				nodeKey = fmt.Sprintf("%d/%s/nodes/%s_%d", c.config.ChainID, c.config.Version, change.Address, change.Port)
			} else {
				nodeKey = fmt.Sprintf("%d/nodes/%s_%d", c.config.ChainID, change.Address, change.Port)
			}
			ops = append(ops, clientv3.OpDelete(nodeKey))
			log.Printf("remove offline node from etcd: %s", nodeKey)
		}
	}
	// 如果ops为空，无需提交事务
	if len(ops) == 0 {
		return
	}

	timeout := time.Duration(c.config.EtcdWriteTimeout) * time.Millisecond
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	_, err = c.etcdClient.Txn(ctx).
		Then(ops...).
		Commit()

	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			log.Printf("etcd write timeout: %v", err)
		} else {
			log.Printf("etcd delete error: %v", err)
		}
	}
}

func (c *Checker) WriteReplicaStateChangeToEtcd(writer *clientv3.Client, replicaStateChange *ReplicaStateChangeNotification) error {
	var err error
	ops := make([]clientv3.Op, 0)

	// 只在LatestBlockNumber变化时写入etcd
	if replicaStateChange.LatestBlockNumber != nil {
		currentBlockNumber := uint64(replicaStateChange.LatestBlockNumber.ToInt().Int64())
		if currentBlockNumber != c.lastWrittenBlockNumber {
			type LastBlockBumber struct {
				LatestBlockNumber *hexutil.Big `json:"latestBlockNumber"`
			}

			lastHeight := LastBlockBumber{
				LatestBlockNumber: replicaStateChange.LatestBlockNumber,
			}

			var lastHeightstr []byte
			lastHeightstr, err = json.Marshal(&lastHeight)
			if err != nil {
				return err
			}

			var lastBlockNumberKey string
			if c.config.IsVersionMode() {
				lastBlockNumberKey = fmt.Sprintf("%d/%s/lastBlockNumber", c.config.ChainID, c.config.Version)
			} else {
				lastBlockNumberKey = fmt.Sprintf("%d/lastBlockNumber", c.config.ChainID)
			}
			ops = append(ops, clientv3.OpPut(lastBlockNumberKey, string(lastHeightstr)))
			c.lastWrittenBlockNumber = currentBlockNumber
		}
	}

	for _, change := range replicaStateChange.ReplicaStates {
		if change.ChangeType != nodes.NoChange {
			nodestr, err := json.Marshal(&change.Node)
			if err != nil {
				return err
			}
			var nodeKey string
			if c.config.IsVersionMode() {
				nodeKey = fmt.Sprintf("%d/%s/nodes/%s_%d", c.config.ChainID, c.config.Version, change.Address, change.Port)
			} else {
				nodeKey = fmt.Sprintf("%d/nodes/%s_%d", c.config.ChainID, change.Address, change.Port)
			}
			if change.Node.Lease == 0 {
				if change.ChangeType == nodes.DelNode {
					ops = append(ops, clientv3.OpDelete(nodeKey))
				} else {
					ops = append(ops, clientv3.OpPut(nodeKey, string(nodestr)))
				}
			} else {
				ops = append(ops, clientv3.OpPut(nodeKey, string(nodestr), clientv3.WithLease(clientv3.LeaseID(change.Node.Lease))))
			}
		}
	}

	// 如果ops为空，无需提交事务
	if len(ops) == 0 {
		return nil
	}

	timeout := time.Duration(c.config.EtcdWriteTimeout) * time.Millisecond
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	_, err = c.etcdClient.Txn(ctx).
		Then(ops...).
		Commit()

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

func (c *Checker) isDuplicateBlockNotification(blockNotice *types.BlockChangeNotification) bool {
	if c.latestOuterBlockChangeNotification != nil {
		if len(blockNotice.DropBlocks) == 0 && len(blockNotice.NewBlocks) == 1 {
			if blockNotice.NewBlocks[0].Hash == c.latestOuterBlockChangeNotification.Hash {
				log.Printf("skip duplicate block notification: hash=%s", blockNotice.NewBlocks[0].Hash)
				return true
			}
		}
	}
	return false
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
	c.Lock()
	defer c.Unlock()
	// 0. 消息校验
	if c.isDuplicateBlockNotification(blockNotice) {
		return true
	}
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

	dropBlocks := append([]types.BlockContext(nil), blockNotice.DropBlocks...)
	slices.Reverse(dropBlocks)

	newBlocks := blockNotice.NewBlocks

	// 2. 重写fork block
	c.rewriteDropBlocks(dropBlocks)

	// 3. 写入db
	if !c.writeBlockInfoToDB(blockNotice.NewBlocks) {
		log.Printf("write block info to db error")
		return false
	}

	// 4. 检查并重写S3中相同高度但不同hash的BlockValidation，标记IsFork=true
	if err := c.rewriteForkBlocksAtSameHeight(newBlocks); err != nil {
		log.Printf("rewrite fork blocks at same height error %+v", err)
		return false
	}

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

	// 7. 对于从切换成leader的情况，进行topic align
	if !c.AlignOuterSingleton() {
		log.Printf("align outer singleton error")
		return false
	}

	return true
}

// fetchLatestOuterSingleBlockNotice 获取最新的 outer singleton block 通知
func (c *Checker) fetchLatestOuterSingleBlockNotice() (*types.OuterBlockChangeNotification, error) {
	reader := util.NewKafkaReader(c.config.OuterBrokers, c.config.OuterNewBlockTopic, "")
	defer reader.Close()
	return GetLastOuterBlockNotice(reader)
}

func (c *Checker) AlignOuterSingleton() bool {
	// 无需outer singleton对齐
	if c.etcdLock == nil {
		return true
	}
	// 已经对齐
	if c.isOuterSingletonAlign {
		return true
	}
	// 还没有outer消息
	if c.latestOuterBlockChangeNotification == nil {
		return true
	}

	// 使用缓存的 singleton block，避免重复创建 kafka reader
	if c.cachedOuterSingleBlockChangeNotification == nil {
		latestOuterSingleBlockChangeNotification, err := c.fetchLatestOuterSingleBlockNotice()
		if err != nil {
			log.Printf("get last outer singleton block notice error %+v", err)
			return false
		}
		if latestOuterSingleBlockChangeNotification == nil {
			c.isOuterSingletonAlign = true
			return true
		}
		c.cachedOuterSingleBlockChangeNotification = latestOuterSingleBlockChangeNotification
	}

	if c.latestOuterBlockChangeNotification.BlockNumber < c.cachedOuterSingleBlockChangeNotification.BlockNumber {
		return true
	}

	// align 前重新获取最新值
	latestOuterSingleBlockChangeNotification, err := c.fetchLatestOuterSingleBlockNotice()
	if err != nil {
		log.Printf("get last outer singleton block notice error %+v", err)
		return false
	}
	c.cachedOuterSingleBlockChangeNotification = latestOuterSingleBlockChangeNotification
	err = c.align(c.latestOuterBlockChangeNotification, c.cachedOuterSingleBlockChangeNotification)
	if err != nil {
		log.Printf("align error %+v", err)
		return false
	}
	c.isOuterSingletonAlign = true
	c.cachedOuterSingleBlockChangeNotification = nil // 对齐成功后清除缓存
	log.Printf("align success")
	return true
}

func (c *Checker) align(latestOuterVersionBlockChangeNotification, latestOuterSingleBlockChangeNotification *types.OuterBlockChangeNotification) error {
	if latestOuterVersionBlockChangeNotification.Hash == latestOuterSingleBlockChangeNotification.Hash {
		return nil
	}
	block, err := db.DB.GetBlockInfoByNum(big.NewInt(int64(latestOuterSingleBlockChangeNotification.BlockNumber)))
	if err != nil {
		return fmt.Errorf("failed to get singleton block at height %d: %w", latestOuterSingleBlockChangeNotification.BlockNumber, err)
	}
	if block.ID == latestOuterSingleBlockChangeNotification.Hash {
		// 顺序对齐：singleton 落后，追赶到 version 高度
		for i := latestOuterSingleBlockChangeNotification.BlockNumber + 1; i <= latestOuterVersionBlockChangeNotification.BlockNumber; i++ {
			block, err := db.DB.GetBlockInfoByNum(big.NewInt(int64(i)))
			if err != nil {
				return fmt.Errorf("failed to get block at height %d during alignment: %w", i, err)
			}
			b := &types.OuterBlockChangeNotification{
				BlockNumber: block.Height,
				Hash:        block.ID,
				ChainID:     c.config.ChainID,
				Timestamp:   uint64(time.Now().Unix()),
				IsFork:      block.IsFork,
			}
			err = util.WriteOuterBlockNotice(c.outerSingletonNewBlockWriter, b)
			if err != nil {
				return fmt.Errorf("failed to write block notice for height %d (hash: %s): %w", block.Height, block.ID.String(), err)
			}
		}
		log.Printf("aligned singleton by fast forward from height %d to %d",
			latestOuterSingleBlockChangeNotification.BlockNumber,
			latestOuterVersionBlockChangeNotification.BlockNumber)
	} else {
		// fork - 需要找到共同祖先并重新发送区块
		blockA, err := c.getVersionBlockByHash(latestOuterVersionBlockChangeNotification.Hash)
		if err != nil {
			return fmt.Errorf("failed to get version block by hash %s: %w", latestOuterVersionBlockChangeNotification.Hash.String(), err)
		}
		blockB, err := c.getVersionBlockByHash(latestOuterSingleBlockChangeNotification.Hash)
		if err != nil {
			return fmt.Errorf("failed to get singleton block by hash %s: %w", latestOuterSingleBlockChangeNotification.Hash.String(), err)
		}

		// 找到共同祖先
		_, dropBlocks, newBlocks, err := c.GetCommonAncestor(blockB, blockA)
		if err != nil {
			return fmt.Errorf("failed to find common ancestor between singleton %s and version %s: %w",
				latestOuterSingleBlockChangeNotification.Hash.String(),
				latestOuterVersionBlockChangeNotification.Hash.String(),
				err)
		}

		// write drop blocks
		for i, block := range dropBlocks {
			b := &types.OuterBlockChangeNotification{
				BlockNumber: block.BlockNumber,
				Hash:        block.Hash,
				ChainID:     c.config.ChainID,
				Timestamp:   block.Timestamp,
				IsFork:      true,
			}
			err := util.WriteOuterBlockNotice(c.outerSingletonNewBlockWriter, b)
			if err != nil {
				return fmt.Errorf("failed to write drop block notice %d/%d (height: %d, hash: %s): %w",
					i+1, len(dropBlocks), block.BlockNumber, block.Hash.String(), err)
			}
		}

		// write new blocks
		for i, block := range newBlocks {
			b := &types.OuterBlockChangeNotification{
				BlockNumber: block.BlockNumber,
				Hash:        block.Hash,
				ChainID:     c.config.ChainID,
				Timestamp:   block.Timestamp,
				IsFork:      false,
			}
			err := util.WriteOuterBlockNotice(c.outerSingletonNewBlockWriter, b)
			if err != nil {
				return fmt.Errorf("failed to write new block notice %d/%d (height: %d, hash: %s): %w",
					i+1, len(newBlocks), block.BlockNumber, block.Hash.String(), err)
			}
		}
	}
	return nil
}

func (c *Checker) WriteDropBlockNotice(dropBlocks []types.BlockContext) bool {
	for _, block := range dropBlocks {
		b := &types.OuterBlockChangeNotification{
			BlockNumber: block.BlockNumber,
			Hash:        block.Hash,
			ChainID:     c.config.ChainID,
			Timestamp:   block.Timestamp,
			IsFork:      true,
		}
		// 版本模式：写入 version topic，并在 leader 时同步写入 singleton topic
		if c.config.IsVersionMode() {
			err := util.WriteOuterBlockNotice(c.outerVersionNewBlockWriter, b)
			if err != nil {
				log.Printf("write drop block notice error %+v", err)
				return false
			}
			if c.etcdLock != nil && c.isOuterSingletonAlign {
				err = util.WriteOuterBlockNotice(c.outerSingletonNewBlockWriter, b)
				if err != nil {
					log.Printf("write drop block notice to singleton error %+v", err)
					return false
				}
			}
		} else {
			// 非版本模式：直接写入 singleton topic
			err := util.WriteOuterBlockNotice(c.outerSingletonNewBlockWriter, b)
			if err != nil {
				log.Printf("write drop block notice error %+v", err)
				return false
			}
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
			ChainID:     c.config.ChainID,
			Timestamp:   block.Timestamp,
			IsFork:      false,
		}
		// 版本模式：写入 version topic，并在 leader 时同步写入 singleton topic
		if c.config.IsVersionMode() {
			err := util.WriteOuterBlockNotice(c.outerVersionNewBlockWriter, b)
			if err != nil {
				log.Printf("write new block notice error %+v", err)
				return false
			}
			if c.etcdLock != nil && c.isOuterSingletonAlign {
				err = util.WriteOuterBlockNotice(c.outerSingletonNewBlockWriter, b)
				if err != nil {
					log.Printf("write new block notice to singleton error %+v", err)
					return false
				}
			}
		} else {
			// 非版本模式：直接写入 singleton topic
			err := util.WriteOuterBlockNotice(c.outerSingletonNewBlockWriter, b)
			if err != nil {
				log.Printf("write new block notice error %+v", err)
				return false
			}
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
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.config.MsgWaitTimeout)*time.Millisecond)
			msg, err := c.innerNewBlockReader.FetchMessage(ctx)
			cancel()

			if err != nil {
				if err == context.DeadlineExceeded {
					// 超时，执行定期节点状态检查
					c.CheckAndNotifyEtcd()
					continue
				}
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
					log.Printf("process error, retrying msg offset %d", msg.Offset)
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
	if c.outerVersionNewBlockWriter != nil {
		c.outerVersionNewBlockWriter.Close()
	}
	c.outerSingletonNewBlockWriter.Close()
	nodes.NodeMap.StopWatch()
	c.etcdClient.Close()
	if c.etcdLock != nil {
		c.etcdLock.Release()
	}
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
