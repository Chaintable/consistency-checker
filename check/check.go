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
	// 当前正在处理的 inner 通知，以及它已成功投递到各 outer topic 的通知集合：
	// 整条 Process 重试时据此跳过已写成功的目的地，既不重复投递也不漏投递
	currentNotice noticeID
	delivered     map[string]map[outerNoticeKey]struct{}
	quit          chan struct{}
	done          chan struct{}

	// 限制预取 S3 请求的并发数，Checker 级共享，重试的 Process 不会各自再开一组
	prefetchSem chan struct{}

	// 以下两个函数字段默认指向真实实现，测试中可替换
	checkReplicas func(kafkaLatestBlockNumber uint64) (*ReplicaStateChangeNotification, error)
	writeOuter    func(writer *kafka.Writer, notice *types.OuterBlockChangeNotification) error
}

// s3RetryMaxWait 是单次 S3 读写累计重试等待的上限；退避从 50ms 起步、每次翻倍、封顶 1s。
// writer 的 S3 上传是异步的，对象"还没到"通常只差几十毫秒，固定睡 1s 会白白拖慢整条链路。
const s3RetryMaxWait = 3 * time.Second

const (
	outerVersionDestination   = "version"
	outerSingletonDestination = "singleton"
)

// outerNoticeKey 唯一标识一条 outer 通知：同一个 hash 在 reorg 来回时会先后以 drop(IsFork=true)
// 和 new(IsFork=false) 两种身份出现，所以要连同 IsFork 一起比较。
type outerNoticeKey struct {
	hash   common.Hash
	isFork bool
}

// noticeID 标识一条 inner 通知；同一条消息的多次重试内容相同，不同消息的 new 链尾不同。
type noticeID struct {
	tail      common.Hash
	newCount  int
	dropCount int
}

// beginNotice 在开始处理一条 inner 通知时调用：换了新通知就清空"已投递"记录。
func (c *Checker) beginNotice(n *types.BlockChangeNotification) {
	id := noticeID{tail: n.NewBlocks[len(n.NewBlocks)-1].Hash, newCount: len(n.NewBlocks), dropCount: len(n.DropBlocks)}
	if id != c.currentNotice {
		c.currentNotice = id
		c.delivered = nil
	}
}

func (c *Checker) isDelivered(destination string, key outerNoticeKey) bool {
	_, ok := c.delivered[destination][key]
	return ok
}

func (c *Checker) markDelivered(destination string, key outerNoticeKey) {
	if c.delivered == nil {
		c.delivered = make(map[string]map[outerNoticeKey]struct{})
	}
	if c.delivered[destination] == nil {
		c.delivered[destination] = make(map[outerNoticeKey]struct{})
	}
	c.delivered[destination][key] = struct{}{}
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
		done:                         make(chan struct{}),
		writeOuter:                   util.WriteOuterBlockNotice,
		prefetchSem:                  make(chan struct{}, prefetchConcurrency),
	}
	c.checkReplicas = c.check

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

// Close requests shutdown and waits for Run to release all resources.
func (c *Checker) Close(ctx context.Context) error {
	close(c.quit)

	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Checker) getVersionBlockByHash(hash common.Hash) (*types.BlockContext, error) {
	// 从 outer S3 的 blockfile（key 为 {chainID}/{version}/{hash}）读区块头。
	// 注意：{hash}/block 这个独立 header 对象只存在于 inner bucket，checker 只配了
	// outer bucket，故改用 outer 已有的 blockfile，其 block 段含 height/parent_id/timestamp。
	s3Key := fmt.Sprintf("%d/%s/%s", c.config.ChainID, c.config.Version, hash.String())
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
	blockFile := types.BlockFile{}
	err = util.DecodeFromGzipJson(buf.Bytes(), &blockFile)
	if err != nil {
		return nil, err
	}
	blockCtx := &types.BlockContext{
		BlockNumber: blockFile.Block.Height.Uint64(),
		Hash:        hash,
		ParentHash:  common.HexToHash(blockFile.Block.ParentID),
		Timestamp:   blockFile.Block.Timestamp,
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

func (c *Checker) getRawValidationByKeyCtx(ctx context.Context, s3Key string) (*types.BlockValidation, error) {
	obj, err := c.outerS3Reader.GetObject(
		ctx,
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

// retryWithBackoff 反复执行 fn 直到成功；失败时以 50ms 起步、每次翻倍、封顶 1s 的间隔重试，
// 累计等待达到 maxWait 后返回最后一次错误。收到 quit 或 ctx 取消时立即返回当前错误。
func (c *Checker) retryWithBackoff(ctx context.Context, what string, maxWait time.Duration, fn func() error) error {
	backoff := 50 * time.Millisecond
	var waited time.Duration
	for {
		err := fn()
		if err == nil {
			return nil
		}
		if waited >= maxWait {
			return fmt.Errorf("%s: retries exhausted after %v: %w", what, waited, err)
		}
		if ctx.Err() != nil {
			return err
		}
		log.Printf("%s error, retrying in %v: %+v", what, backoff, err)
		select {
		case <-c.quit:
			return err
		case <-ctx.Done():
			return err
		case <-time.After(backoff):
		}
		waited += backoff
		if backoff < time.Second {
			backoff *= 2
			if backoff > time.Second {
				backoff = time.Second
			}
		}
	}
}

func (c *Checker) getRawValidationByKeyWithReTry(s3Key string) (*types.BlockValidation, error) {
	return c.getRawValidationByKeyWithReTryCtx(context.Background(), s3Key)
}

func (c *Checker) getRawValidationByKeyWithReTryCtx(ctx context.Context, s3Key string) (*types.BlockValidation, error) {
	var validation *types.BlockValidation
	err := c.retryWithBackoff(ctx, "get raw validation "+s3Key, s3RetryMaxWait, func() error {
		var err error
		validation, err = c.getRawValidationByKeyCtx(ctx, s3Key)
		return err
	})
	if err != nil {
		return nil, err
	}
	return validation, nil
}

func (c *Checker) rewriteValidationAtKey(s3Key string, validation *types.BlockValidation) error {
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

func (c *Checker) blockValidationPrefix(blockNumber uint64) string {
	if c.config.IsVersionMode() {
		return fmt.Sprintf("%d/%s/%d/", c.config.ChainID, c.config.Version, blockNumber)
	}
	return fmt.Sprintf("%d/%d/", c.config.ChainID, blockNumber)
}

func (c *Checker) blockValidationKey(blockCtx *types.BlockContext) string {
	if c.config.IsVersionMode() {
		return fmt.Sprintf("%d/%s/%d/%s", c.config.ChainID, c.config.Version, blockCtx.BlockNumber, blockCtx.Hash.String())
	}
	return fmt.Sprintf("%d/%d/%s", c.config.ChainID, blockCtx.BlockNumber, blockCtx.Hash.String())
}

func (c *Checker) blockValidationHashFromKey(key string) (string, bool) {
	keyParts := strings.Split(key, "/")
	if c.config.IsVersionMode() {
		if len(keyParts) != 4 {
			return "", false
		}
		return keyParts[3], true
	}
	if len(keyParts) != 3 {
		return "", false
	}
	return keyParts[2], true
}

func (c *Checker) listBlockValidationKeys(blockNumber uint64) ([]string, error) {
	return c.listBlockValidationKeysCtx(context.Background(), blockNumber)
}

func (c *Checker) listBlockValidationKeysCtx(ctx context.Context, blockNumber uint64) ([]string, error) {
	prefix := c.blockValidationPrefix(blockNumber)
	var keys []string
	var continuationToken *string

	for {
		// ListObjectsV2 returns at most 1000 keys per page.
		listParams := &s3.ListObjectsV2Input{
			Bucket:            &c.config.OuterS3Bucket,
			Prefix:            &prefix,
			ContinuationToken: continuationToken,
		}

		var resp *s3.ListObjectsV2Output
		err := c.retryWithBackoff(ctx, fmt.Sprintf("list objects at height %d", blockNumber), s3RetryMaxWait, func() error {
			var err error
			resp, err = c.outerS3Reader.ListObjectsV2(ctx, listParams)
			return err
		})
		if err != nil {
			return nil, err
		}

		for _, obj := range resp.Contents {
			if obj.Key != nil {
				keys = append(keys, *obj.Key)
			}
		}

		if resp.IsTruncated == nil || !*resp.IsTruncated {
			return keys, nil
		}
		if resp.NextContinuationToken == nil {
			return nil, fmt.Errorf("list objects at height %d truncated without continuation token", blockNumber)
		}
		continuationToken = resp.NextContinuationToken
	}
}

func (c *Checker) rewriteBlockWithRetry(blockCtx *types.BlockContext, validation *types.BlockValidation) error {
	return c.rewriteValidationAtKeyWithRetry(
		c.blockValidationKey(blockCtx),
		blockCtx.BlockNumber,
		blockCtx.Hash.String(),
		validation,
	)
}

func (c *Checker) rewriteValidationAtKeyWithRetry(s3Key string, blockNumber uint64, hash string, validation *types.BlockValidation) error {
	return c.retryWithBackoff(context.Background(), fmt.Sprintf("rewrite block %s at height %d", hash, blockNumber), s3RetryMaxWait, func() error {
		return c.rewriteValidationAtKey(s3Key, validation)
	})
}

// rewriteDropBlocks 将被drop的块的validation标记为fork。
// 失败不阻塞Process：同高度的标记会被rewriteForkBlocksAtSameHeight和定期巡检兜底，
// 这里重试后仍失败只计数告警。
func (c *Checker) rewriteDropBlocks(dropBlocks []types.BlockContext) {
	for _, block := range dropBlocks {
		blockValidation, err := c.getRawValidationByKeyWithReTry(c.blockValidationKey(&block))
		if err != nil {
			log.Printf("rewrite drop block %s at height %d: get validation error %+v",
				block.Hash.String(), block.BlockNumber, err)
			metrics.DropBlockRewriteFailures.Inc()
			continue
		}
		if blockValidation.IsFork {
			continue
		}
		blockValidation.IsFork = true
		log.Printf("rewrite block %d", block.BlockNumber)
		err = c.rewriteBlockWithRetry(&block, blockValidation)
		if err != nil {
			log.Printf("rewrite drop block %s at height %d error %+v",
				block.Hash.String(), block.BlockNumber, err)
			metrics.DropBlockRewriteFailures.Inc()
			continue
		}
	}
}

// rewriteForkBlocksAtSameHeight 检查S3中相同高度但不同hash的区块，将其标记为fork, 严格。
// prefetched 是 Process 开头在等副本期间预取的同高度 key 列表与 canonical validation，
// 第一轮直接复用以省掉关键路径上的 S3 往返；若第一轮有改写，后续轮次重新 LIST 确认收敛。
func (c *Checker) rewriteForkBlocksAtSameHeight(newBlocks []types.BlockContext, prefetched []*blockPrefetch) error {
	for i, block := range newBlocks {
		var hint *blockPrefetch
		if i < len(prefetched) {
			hint = prefetched[i]
		}
		for attempt := 0; attempt < 3; attempt++ {
			done, err := c.rewriteForkBlocksAtHeight(block, hint)
			if err != nil {
				return err
			}
			if done {
				break
			}
			if attempt == 2 {
				return fmt.Errorf("rewrite fork blocks at height %d did not converge, canonical hash %s",
					block.BlockNumber, block.Hash.String())
			}
			hint = nil
			time.Sleep(100 * time.Millisecond)
		}
	}
	return nil
}

// forkRewrite 是一次待执行的 validation 改写：把 key 对应对象的 IsFork 改成 validation.IsFork。
type forkRewrite struct {
	key        string
	hash       string
	validation *types.BlockValidation
}

// planForkRewrites 只读地计算某高度上需要改写的 fork 标记：非 canonical 的对象应为 IsFork=true，
// canonical 应为 IsFork=false。不持有 c.Lock，可在巡检里与消息处理并发执行。
// hint 提供预取的 key 列表与 canonical validation：keys 为空时重新 LIST；预取列表里没有 canonical
// （对象可能刚上传）时也会重新 LIST 一次再判定。
func (c *Checker) planForkRewrites(block types.BlockContext, hint *blockPrefetch) ([]forkRewrite, error) {
	canonicalHash := block.Hash.String()
	var keys []string
	var canonicalValidation *types.BlockValidation
	if hint != nil && hint.keysErr == nil {
		keys = hint.keys
		if hint.validationErr == nil {
			canonicalValidation = hint.validation
		}
	}
	listed := false
	if keys == nil {
		var err error
		if keys, err = c.listBlockValidationKeys(block.BlockNumber); err != nil {
			return nil, err
		}
		listed = true
	}
	hasCanonical := func(keys []string) bool {
		for _, key := range keys {
			if h, ok := c.blockValidationHashFromKey(key); ok && strings.EqualFold(h, canonicalHash) {
				return true
			}
		}
		return false
	}
	if !hasCanonical(keys) && !listed {
		var err error
		if keys, err = c.listBlockValidationKeys(block.BlockNumber); err != nil {
			return nil, err
		}
	}

	var plan []forkRewrite
	canonicalFound := false
	for _, key := range keys {
		existingHashStr, ok := c.blockValidationHashFromKey(key)
		if !ok {
			continue
		}
		isCanonical := strings.EqualFold(existingHashStr, canonicalHash)
		var validation *types.BlockValidation
		if isCanonical {
			canonicalFound = true
			validation = canonicalValidation
		}
		if validation == nil {
			var err error
			validation, err = c.getRawValidationByKeyWithReTry(key)
			if err != nil {
				return nil, fmt.Errorf("get block validation error at height %d, hash %s: %w",
					block.BlockNumber, existingHashStr, err)
			}
		}
		shouldFork := !isCanonical
		if validation.IsFork == shouldFork {
			continue
		}
		rewritten := *validation
		rewritten.IsFork = shouldFork
		plan = append(plan, forkRewrite{key: key, hash: existingHashStr, validation: &rewritten})
	}
	if !canonicalFound {
		return nil, fmt.Errorf("canonical block %s at height %d not found in S3",
			canonicalHash, block.BlockNumber)
	}
	return plan, nil
}

// applyForkRewrites 执行 planForkRewrites 得到的改写。
func (c *Checker) applyForkRewrites(block types.BlockContext, plan []forkRewrite) error {
	canonicalHash := block.Hash.String()
	for _, rw := range plan {
		if err := c.rewriteValidationAtKeyWithRetry(rw.key, block.BlockNumber, rw.hash, rw.validation); err != nil {
			return fmt.Errorf("rewrite block %s at height %d error: %w",
				rw.hash, block.BlockNumber, err)
		}
		if rw.validation.IsFork {
			log.Printf("found fork block at height %d: existing hash %s, new canonical hash %s",
				block.BlockNumber, rw.hash, canonicalHash)
			log.Printf("successfully marked block %s at height %d as fork",
				rw.hash, block.BlockNumber)
		} else {
			log.Printf("successfully marked block %s at height %d as canonical",
				rw.hash, block.BlockNumber)
		}
	}
	return nil
}

// rewriteForkBlocksAtHeight 检查并改写某高度的 fork 标记，返回 done=true 表示无需改写（已收敛）。
func (c *Checker) rewriteForkBlocksAtHeight(block types.BlockContext, hint *blockPrefetch) (bool, error) {
	plan, err := c.planForkRewrites(block, hint)
	if err != nil {
		return false, err
	}
	if len(plan) == 0 {
		return true, nil
	}
	return false, c.applyForkRewrites(block, plan)
}

// runForkScan 周期性巡检最近若干高度的fork标记。
// rewriteForkBlocksAtSameHeight只在通知到达的瞬间检查同高度对象，
// 之后被外部覆盖的is_fork标记（如上传方重启回放时的重复上传）没有任何机制能发现，
// 巡检把一次性窗口变成持续收敛。
func (c *Checker) runForkScan() {
	interval := c.config.ForkScanInterval
	if interval <= 0 || c.config.ForkScanLookback == 0 {
		log.Printf("fork scan disabled")
		return
	}
	log.Printf("fork scan enabled: interval %ds, lookback %d heights", interval, c.config.ForkScanLookback)
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.quit:
			return
		case <-ticker.C:
			c.scanRecentForkBlocks()
		}
	}
}

func (c *Checker) scanRecentForkBlocks() {
	c.Lock()
	latest := c.latestOuterBlockChangeNotification
	c.Unlock()
	if latest == nil {
		return
	}
	tip := latest.BlockNumber
	start := uint64(0)
	if lookback := c.config.ForkScanLookback; tip >= lookback {
		start = tip - lookback + 1
	}
	for h := start; h <= tip; h++ {
		c.scanForkBlocksAtHeight(h)
	}
}

// scanForkBlocksAtHeight 巡检单个高度。S3 的 LIST/GET 不持 c.Lock 执行，避免每分钟一轮巡检
// 挤占消息处理；只有真的需要改写时才持锁，并在持锁后复核 canonical 未被并发的 reorg 改变。
func (c *Checker) scanForkBlocksAtHeight(height uint64) {
	canonicalAt := func() (common.Hash, bool) {
		// 巡检期间可能reorg到更短链，tip之上的高度没有canonical，跳过
		if c.latestOuterBlockChangeNotification == nil || height > c.latestOuterBlockChangeNotification.BlockNumber {
			return common.Hash{}, false
		}
		hash, ok, err := db.DB.GetCanonicalHashByNum(height)
		if err != nil {
			log.Printf("fork scan: get canonical hash at height %d error %+v", height, err)
			metrics.ForkScanErrors.Inc()
			return common.Hash{}, false
		}
		if !ok {
			// db中没有该高度的记录（冷启动/丢盘），没有可信基准，跳过
			metrics.ForkScanSkips.Inc()
			return common.Hash{}, false
		}
		return hash, true
	}

	c.Lock()
	hash, ok := canonicalAt()
	c.Unlock()
	if !ok {
		return
	}
	plan, err := c.planForkRewrites(types.BlockContext{BlockNumber: height, Hash: hash}, nil)
	if err != nil {
		log.Printf("fork scan: plan at height %d error %+v", height, err)
		metrics.ForkScanErrors.Inc()
		return
	}
	if len(plan) == 0 {
		return
	}

	c.Lock()
	defer c.Unlock()
	if current, ok := canonicalAt(); !ok || current != hash {
		// 计算期间该高度已被 reorg 改写，放弃这份过期计划，下一轮巡检重算
		return
	}
	if err := c.applyForkRewrites(types.BlockContext{BlockNumber: height, Hash: hash}, plan); err != nil {
		log.Printf("fork scan: rewrite at height %d error %+v", height, err)
		metrics.ForkScanErrors.Inc()
		return
	}
	log.Printf("fork scan: rewrote fork marks at height %d", height)
	metrics.ForkScanRewrites.Inc()
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

// waitReplicasReady 每隔 check_interval_ms 轮询一次副本，直到 ready_ratio 比例的副本追上
// kafkaLatestBlockNumber，或累计等待超过 timeout。timeout<=0 时只查一次。
// 副本（leafage）从收到同一条 inner 通知到应用完成通常只要几十毫秒，但尾部会超过固定次数的窗口；
// 与其失败后整条 Process 睡 1s 重来，不如在这里多等几十毫秒。
// 注意 timeout 约束的是轮询循环，每一轮 CheckAll 会等所有节点返回、受 rpc_node_timeout_ms 约束：
// 某个副本 RPC 卡死时一轮仍可能耗到 rpc_node_timeout_ms（与改动前一致）。
func (c *Checker) waitReplicasReady(kafkaLatestBlockNumber uint64, timeout time.Duration) (*ReplicaStateChangeNotification, error) {
	start := time.Now()
	deadline := start.Add(timeout)
	interval := time.Duration(c.config.CheckInterval) * time.Millisecond
	var lastErr error
	var replicaStateChange *ReplicaStateChangeNotification
	for {
		var err error
		replicaStateChange, err = c.checkReplicas(kafkaLatestBlockNumber)
		if err != nil {
			log.Printf("check error %+v", err)
			lastErr = err
		}
		if replicaStateChange != nil && replicaStateChange.LatestBlockNumber != nil {
			return replicaStateChange, nil
		}
		// 每轮 RPC 本身受 rpc_node_timeout_ms 约束（不在这里缩短：把慢节点误判 offline 会连带
		// 从 etcd 摘除），所以只保证不会在过了 deadline 之后再发起新一轮
		if !time.Now().Add(interval).Before(deadline) {
			return replicaStateChange, fmt.Errorf("replicas not ready for block %d after %v: %v",
				kafkaLatestBlockNumber, time.Since(start).Round(time.Millisecond), lastErr)
		}
		select {
		case <-c.quit:
			return replicaStateChange, fmt.Errorf("checker quitting")
		case <-time.After(interval):
		}
	}
}

func (c *Checker) replicaWaitTimeout() time.Duration {
	return time.Duration(c.config.CheckTimeout) * time.Millisecond
}

// CheckAndNotifyEtcd 是空闲时的周期性节点状态刷新。副本通常早已到达该高度，第一次采样就通过；
// 只有节点真的不响应时才会用满窗口——节点要在整个 check_timeout_ms 内持续失败才会被判 offline
// 并从 etcd 摘除，避免一次瞬时抖动就删掉注册。
func (c *Checker) CheckAndNotifyEtcd() bool {
	c.Lock()
	defer c.Unlock()
	return c.checkAndNotify(c.ReplicaLatestBlockNumber, c.replicaWaitTimeout(), false)
}

// checkAndNotify 等副本就绪并把节点状态写入 etcd；record 为 true 时记录副本等待指标（消息路径）。
func (c *Checker) checkAndNotify(kafkaLatestBlockNumber uint64, timeout time.Duration, record bool) bool {
	// 如果副本高度大于kafka最新高度，直接返回(一致性节点后上线)
	if c.ReplicaLatestBlockNumber > kafkaLatestBlockNumber && time.Since(c.latestWriteEtcd) < 1*time.Second {
		return true
	}

	waitStart := time.Now()
	replicaStateChange, err := c.waitReplicasReady(kafkaLatestBlockNumber, timeout)
	if record {
		metrics.ReplicaReadyWait.Observe(time.Since(waitStart).Seconds())
	}
	if err != nil {
		log.Printf("check error %+v", err)
		if record {
			metrics.ReplicaReadyTimeouts.Inc()
		}
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

// blockPrefetch 是单个新块在等副本期间预取到的 S3 数据。
type blockPrefetch struct {
	validation    *types.BlockValidation
	validationErr error
	keys          []string
	keysErr       error
}

type prefetchResult struct {
	blocks []*blockPrefetch
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// wait 等全部预取完成（成功路径）
func (p *prefetchResult) wait() {
	p.wg.Wait()
}

// abort 取消仍在进行的预取（失败路径）：不阻塞等待，goroutine 收到取消后自行退出，
// 避免每次 Process 重试都叠加一组新的 S3 请求
func (p *prefetchResult) abort() {
	p.cancel()
}

// prefetchConcurrency 限制并发预取的 S3 请求数（追块时一条通知可能带很多块）
const prefetchConcurrency = 16

// prefetchNewBlocks 并发预取每个新块的 validation 与同高度 key 列表。这两样只依赖消息内容、
// 不依赖副本状态，放在等副本的窗口里并行拉，让关键路径上不再有串行的 S3 往返。
func (c *Checker) prefetchNewBlocks(newBlocks []types.BlockContext) *prefetchResult {
	ctx, cancel := context.WithCancel(context.Background())
	res := &prefetchResult{blocks: make([]*blockPrefetch, len(newBlocks)), cancel: cancel}
	if c.prefetchSem == nil {
		c.prefetchSem = make(chan struct{}, prefetchConcurrency)
	}
	sem := c.prefetchSem
	acquire := func() bool {
		select {
		case sem <- struct{}{}:
			return true
		case <-ctx.Done():
			return false
		}
	}
	for i := range newBlocks {
		pf := &blockPrefetch{}
		res.blocks[i] = pf
		block := newBlocks[i]
		key := c.blockValidationKey(&block)
		res.wg.Add(2)
		go func() {
			defer res.wg.Done()
			if !acquire() {
				pf.validationErr = ctx.Err()
				return
			}
			defer func() { <-sem }()
			pf.validation, pf.validationErr = c.getRawValidationByKeyWithReTryCtx(ctx, key)
		}()
		go func() {
			defer res.wg.Done()
			if !acquire() {
				pf.keysErr = ctx.Err()
				return
			}
			defer func() { <-sem }()
			pf.keys, pf.keysErr = c.listBlockValidationKeysCtx(ctx, block.BlockNumber)
		}()
	}
	return res
}

func (c *Checker) writeBlockInfoToDB(newBlocks []types.BlockContext, prefetched []*blockPrefetch) bool {
	validationHashes := make([]int64, len(newBlocks))
	for i, block := range newBlocks {
		if prefetched[i].validationErr != nil {
			log.Printf("get validation hash for block %d %s error %+v", block.BlockNumber, block.Hash, prefetched[i].validationErr)
			return false
		}
		validationHashes[i] = prefetched[i].validation.ValidationHash
	}

	err := db.DB.WriteBlockInfos(newBlocks, validationHashes)
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

// isAlreadyProcessed 判断这条消息是否已被处理过：若本通知的 new 链尾已等于当前
// latest，说明上一次已处理到 WriteNewBlockNotice（outer 已转发、latest 已推进），
// 只是整条 Process 未提交。用于避免 reorg 消息失败重试时重入 msgCheck 死锁。
func (c *Checker) isAlreadyProcessed(blockNotice *types.BlockChangeNotification) bool {
	if c.latestOuterBlockChangeNotification == nil || len(blockNotice.NewBlocks) == 0 {
		return false
	}
	return blockNotice.NewBlocks[len(blockNotice.NewBlocks)-1].Hash == c.latestOuterBlockChangeNotification.Hash
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

func (c *Checker) Process(blockNotice *types.BlockChangeNotification, blockTimings map[common.Hash]time.Time) bool {
	c.Lock()
	defer c.Unlock()
	// 0. 消息校验。两条短路路径不跳过 singleton 对齐：上一轮可能在通知已发出之后、
	//    对齐失败时返回 false，重试时若直接提交，singleton 要等下一条消息才会补齐。
	if c.isDuplicateBlockNotification(blockNotice) {
		return c.AlignOuterSingleton()
	}
	// 幂等短路：本消息 new 链尾已等于 latest，说明上次已处理到 WriteNewBlockNotice
	// 只是整条 Process 未提交（如 reorg 消息后续步骤失败）。对齐后提交前进，避免死锁重试。
	if c.isAlreadyProcessed(blockNotice) {
		log.Printf("msg already processed (latest == newBlocks tail), align and advance")
		return c.AlignOuterSingleton()
	}
	if !c.msgCheck(blockNotice) {
		log.Printf("msg check error")
		return false
	}
	c.beginNotice(blockNotice)

	start := time.Now()

	// kafka最新高度
	kafkaLatestBlockNumber := blockNotice.NewBlocks[len(blockNotice.NewBlocks)-1].BlockNumber

	dropBlocks := append([]types.BlockContext(nil), blockNotice.DropBlocks...)
	slices.Reverse(dropBlocks)

	newBlocks := blockNotice.NewBlocks

	// 1. 预取 validation 与同高度 key 列表，与下面等副本的过程并行；
	//    失败路径不等它们结束（goroutine 自行收尾，重试时会重新预取）
	prefetched := c.prefetchNewBlocks(newBlocks)

	// 2. 一致性check，等 ready_ratio 比例的副本追上高度
	if !c.checkAndNotify(kafkaLatestBlockNumber, c.replicaWaitTimeout(), true) {
		log.Printf("check and notify error")
		prefetched.abort()
		return false
	}
	prefetched.wait()

	// 3. 重写fork block
	c.rewriteDropBlocks(dropBlocks)

	// 4. 写入db
	if !c.writeBlockInfoToDB(newBlocks, prefetched.blocks) {
		log.Printf("write block info to db error")
		return false
	}

	// 5. 检查并重写S3中相同高度但不同hash的BlockValidation，标记IsFork=true
	if err := c.rewriteForkBlocksAtSameHeight(newBlocks, prefetched.blocks); err != nil {
		log.Printf("rewrite fork blocks at same height error %+v", err)
		return false
	}

	// 6. 发送drop block通知
	if !c.WriteDropBlockNotice(dropBlocks) {
		log.Printf("write drop block notice error")
		return false
	}

	// 7. 发送新块通知
	if !c.WriteNewBlockNotice(newBlocks, blockTimings) {
		log.Printf("write new block notice error")
		return false
	}

	// 8. 对于从切换成leader的情况，进行topic align
	if !c.AlignOuterSingleton() {
		log.Printf("align outer singleton error")
		return false
	}

	metrics.ProcessPublishDuration.Observe(time.Since(start).Seconds())

	// 9. 通知已发出，再用一次新鲜的 LIST 复核同高度的 fork 标记：预取的列表是 Process 开头的
	//    快照，等副本期间刚上传的同高度对象不在其中。这一步不在发布的关键路径上，
	//    失败只记录，周期巡检兜底。
	//    复用预取到的 canonical validation、但强制重新 LIST（hint 里 keys 置空即会重新列举）。
	fresh := make([]*blockPrefetch, len(prefetched.blocks))
	for i, pf := range prefetched.blocks {
		fresh[i] = &blockPrefetch{validation: pf.validation, validationErr: pf.validationErr}
	}
	if err := c.rewriteForkBlocksAtSameHeight(newBlocks, fresh); err != nil {
		log.Printf("post-publish fork recheck error %+v", err)
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
			err = c.writeOuter(c.outerSingletonNewBlockWriter, b)
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
			err := c.writeOuter(c.outerSingletonNewBlockWriter, b)
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
			err := c.writeOuter(c.outerSingletonNewBlockWriter, b)
			if err != nil {
				return fmt.Errorf("failed to write new block notice %d/%d (height: %d, hash: %s): %w",
					i+1, len(newBlocks), block.BlockNumber, block.Hash.String(), err)
			}
		}
	}
	return nil
}

// publishOuter 把一条 outer 通知写到该去的 topic：传统模式只有 singleton；版本模式写 version，
// leader 且已对齐时同时写 singleton。目的地互不依赖，并发写以省掉一次串行的 Kafka 往返。
// 任一目的地写失败返回 false，由调用方整条 Process 重试；已写成功的目的地记入 delivered，
// 重试时跳过，因此重试既不会重复投递、也不会漏投递。
//
// observe 在某个目的地写成功时回调，at 是该目的地自己的完成时刻，用于端到端延迟打点。
func (c *Checker) publishOuter(b *types.OuterBlockChangeNotification, observe func(destination string, at time.Time)) bool {
	type target struct {
		destination string
		writer      *kafka.Writer
	}
	var targets []target
	if c.config.IsVersionMode() {
		targets = append(targets, target{outerVersionDestination, c.outerVersionNewBlockWriter})
		if c.etcdLock != nil && c.isOuterSingletonAlign {
			targets = append(targets, target{outerSingletonDestination, c.outerSingletonNewBlockWriter})
		}
	} else {
		targets = append(targets, target{outerSingletonDestination, c.outerSingletonNewBlockWriter})
	}

	key := outerNoticeKey{hash: b.Hash, isFork: b.IsFork}
	errs := make([]error, len(targets))
	doneAt := make([]time.Time, len(targets))
	var wg sync.WaitGroup
	for i, t := range targets {
		if c.isDelivered(t.destination, key) {
			continue
		}
		wg.Add(1)
		go func(i int, t target) {
			defer wg.Done()
			errs[i] = c.writeOuter(t.writer, b)
			doneAt[i] = time.Now()
		}(i, t)
	}
	wg.Wait()

	ok := true
	for i, t := range targets {
		if c.isDelivered(t.destination, key) {
			continue
		}
		if errs[i] != nil {
			log.Printf("write outer %s block notice error at height %d hash %s: %+v", t.destination, b.BlockNumber, b.Hash, errs[i])
			ok = false
			continue
		}
		c.markDelivered(t.destination, key)
		observe(t.destination, doneAt[i])
	}
	return ok
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
		if !c.publishOuter(b, func(string, time.Time) {}) {
			return false
		}
	}
	return true
}

func blockEndToEndLatency(now time.Time, hash common.Hash, blockTimings map[common.Hash]time.Time) (time.Duration, string, bool) {
	firstSeenAt, ok := blockTimings[hash]
	if !ok {
		return 0, "missing", false
	}
	latency := now.Sub(firstSeenAt)
	if latency < 0 {
		return 0, "future", false
	}
	return latency, "", true
}

func observeBlockEndToEndLatency(block types.BlockContext, blockTimings map[common.Hash]time.Time, destination string, at time.Time) {
	latency, reason, ok := blockEndToEndLatency(at, block.Hash, blockTimings)
	if !ok {
		metrics.BlockIngressTimingIgnored.WithLabelValues(destination, reason).Inc()
		return
	}
	metrics.BlockIngressToOuterKafkaLatency.WithLabelValues(destination).Observe(latency.Seconds())
}

func (c *Checker) WriteNewBlockNotice(newBlocks []types.BlockContext, blockTimings map[common.Hash]time.Time) bool {
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
		if !c.publishOuter(b, func(destination string, at time.Time) {
			observeBlockEndToEndLatency(block, blockTimings, destination, at)
		}) {
			return false
		}
		c.latestOuterBlockChangeNotification = b
	}
	return true
}

func (c *Checker) Run() {
	defer close(c.done)
	defer c.shutdown()

	go c.runForkScan()
	for {
		select {
		case <-c.quit:
			return
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
			blockTimings := util.DecodeBlockFirstSeenHeaders(msg.Headers)
			for {
				if c.latestMsgOffset != 0 && msg.Offset <= c.latestMsgOffset {
					log.Printf("msg offset %d is same as last offset %d", msg.Offset, c.latestMsgOffset)
					break
				}
				if !c.Process(blockNotice, blockTimings) {
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

}

func (c *Checker) shutdown() {
	nodes.NodeMap.StopWatch()
	c.RevokeChainLeader()
	c.innerNewBlockReader.Close()
	if c.outerVersionNewBlockWriter != nil {
		c.outerVersionNewBlockWriter.Close()
	}
	c.outerSingletonNewBlockWriter.Close()
	if err := c.etcdClient.Close(); err != nil {
		log.Printf("close etcd client error %+v", err)
	}
	if db.DB != nil {
		if err := db.DB.Close(); err != nil {
			log.Printf("close consistency db error %+v", err)
		}
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
