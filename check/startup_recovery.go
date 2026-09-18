package check

import (
	"context"
	"fmt"
	"io"
	"log"
	"slices"
	"strings"
	"time"

	"github.com/Chaintable/consistency-checker/metrics"
	"github.com/Chaintable/pipeline/types"
	"github.com/Chaintable/pipeline/util"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

// processInnerNotice gates only the first decoded Kafka notice after startup.
// A failed recovery retries that same notice without committing its offset.
func (c *Checker) processInnerNotice(notice *types.BlockChangeNotification, timings map[common.Hash]time.Time) bool {
	if notice == nil || len(notice.NewBlocks) == 0 {
		log.Printf("empty new-block notification; refusing to advance Kafka offset")
		return false
	}
	if !c.startupRecovered {
		if err := c.recoverStartupGap(notice); err != nil {
			metrics.StartupGapRecoveryFailures.Inc()
			log.Printf("startup gap recovery failed: %v", err)
			return false
		}
		c.startupRecovered = true
	}
	return c.Process(notice, timings)
}

func (c *Checker) recoverStartupGap(notice *types.BlockChangeNotification) error {
	if c.config.StartupGapRecoveryMaxBlocks == 0 {
		return nil
	}
	c.Lock()
	latest := c.latestOuterBlockChangeNotification
	if latest != nil {
		copyOfLatest := *latest
		latest = &copyOfLatest
	}
	c.Unlock()

	timeout := time.Duration(c.config.StartupGapRecoveryTimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	go func() {
		select {
		case <-c.quit:
			cancel()
		case <-ctx.Done():
		}
	}()

	// Validate the entire parent chain and every S3 payload before publishing any
	// of it. In particular, a missing notification must not hide a fork or a
	// blockfile/validation mismatch.
	blocks, err := planStartupGap(ctx, latest, notice, c.config.StartupGapRecoveryMaxBlocks, c.readRecoveryBlock)
	if err != nil {
		return err
	}
	if len(blocks) == 0 {
		return nil
	}
	log.Printf("startup gap recovery: restoring %d blocks [%d, %d] before height %d",
		len(blocks), blocks[0].BlockNumber, blocks[len(blocks)-1].BlockNumber, notice.NewBlocks[0].BlockNumber)
	for _, block := range blocks {
		select {
		case <-c.quit:
			return context.Canceled
		default:
		}
		// One block per Process preserves its existing per-destination retry
		// tracking. On retry/restart, the published tip determines the remaining
		// suffix. No synthetic Kafka offset is ever created or committed.
		recovered := &types.BlockChangeNotification{ChangeType: 1, NewBlocks: []types.BlockContext{block}}
		if !c.Process(recovered, nil) {
			return fmt.Errorf("process recovered block %d (%s)", block.BlockNumber, block.Hash)
		}
		metrics.StartupGapRecoveredBlocks.Inc()
		log.Printf("startup gap recovery: published height %d hash %s", block.BlockNumber, block.Hash)
	}
	return nil
}

type recoveryBlockReader func(context.Context, common.Hash) (*types.BlockContext, error)

func planStartupGap(ctx context.Context, latest *types.OuterBlockChangeNotification, notice *types.BlockChangeNotification,
	maxBlocks uint64, read recoveryBlockReader) ([]types.BlockContext, error) {
	if maxBlocks == 0 || latest == nil || notice == nil || len(notice.NewBlocks) == 0 ||
		notice.ChangeType != 1 || len(notice.DropBlocks) != 0 || latest.IsFork {
		return nil, nil
	}
	first := notice.NewBlocks[0]
	if first.BlockNumber <= latest.BlockNumber || first.BlockNumber-latest.BlockNumber <= 1 {
		return nil, nil
	}
	gap := first.BlockNumber - latest.BlockNumber - 1
	if gap > maxBlocks {
		return nil, fmt.Errorf("startup gap of %d blocks exceeds limit %d", gap, maxBlocks)
	}
	// The original message must also be internally continuous before it can
	// anchor a recovery. Do not repair a prefix of a malformed batch.
	for i := 1; i < len(notice.NewBlocks); i++ {
		prev, next := notice.NewBlocks[i-1], notice.NewBlocks[i]
		if next.BlockNumber <= prev.BlockNumber || next.BlockNumber-prev.BlockNumber != 1 || next.ParentHash != prev.Hash {
			return nil, fmt.Errorf("incoming notification is not continuous at height %d", next.BlockNumber)
		}
	}
	var reverse []types.BlockContext
	child := first
	for height := first.BlockNumber - 1; height > latest.BlockNumber; height-- {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		block, err := read(ctx, child.ParentHash)
		if err != nil {
			return nil, fmt.Errorf("read missing height %d hash %s: %w", height, child.ParentHash, err)
		}
		if block == nil || block.Hash != child.ParentHash || block.BlockNumber != height || block.Timestamp > child.Timestamp {
			return nil, fmt.Errorf("invalid recovered parent of height %d: %+v", child.BlockNumber, block)
		}
		reverse = append(reverse, *block)
		child = *block
	}
	if child.ParentHash != latest.Hash || child.Timestamp < latest.Timestamp {
		return nil, fmt.Errorf("recovered chain does not extend published height %d hash %s; refusing fork recovery", latest.BlockNumber, latest.Hash)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	slices.Reverse(reverse)
	return reverse, nil
}

func (c *Checker) readOuterBlockFile(ctx context.Context, hash common.Hash) (*types.BlockFile, error) {
	key := fmt.Sprintf("%d/%s", c.config.ChainID, hash.Hex())
	if c.config.IsVersionMode() || c.config.Version != "" {
		key = fmt.Sprintf("%d/%s/%s", c.config.ChainID, c.config.Version, hash.Hex())
	}
	obj, err := c.outerS3Reader.GetObject(ctx, &s3.GetObjectInput{Bucket: &c.config.OuterS3Bucket, Key: &key})
	if err != nil {
		return nil, err
	}
	defer obj.Body.Close()
	data, err := io.ReadAll(obj.Body)
	if err != nil {
		return nil, fmt.Errorf("read blockfile %s: %w", key, err)
	}
	var file types.BlockFile
	if err := util.DecodeFromGzipJson(data, &file); err != nil {
		return nil, fmt.Errorf("decode blockfile %s: %w", key, err)
	}
	return &file, nil
}

func blockContextFromFile(hash common.Hash, file *types.BlockFile) (*types.BlockContext, error) {
	if file == nil || file.Block.Height == nil || !file.Block.Height.IsUint64() ||
		!strings.EqualFold(file.Block.ID, hash.Hex()) {
		return nil, fmt.Errorf("blockfile identity does not match requested hash %s", hash)
	}
	parent, err := hexutil.Decode(file.Block.ParentID)
	if err != nil || len(parent) != common.HashLength {
		return nil, fmt.Errorf("invalid parent hash in blockfile %s", hash)
	}
	return &types.BlockContext{
		Hash: hash, ParentHash: common.BytesToHash(parent),
		BlockNumber: file.Block.Height.Uint64(), Timestamp: file.Block.Timestamp,
	}, nil
}

func (c *Checker) readRecoveryBlock(ctx context.Context, hash common.Hash) (*types.BlockContext, error) {
	file, err := c.readOuterBlockFile(ctx, hash)
	if err != nil {
		return nil, err
	}
	block, err := blockContextFromFile(hash, file)
	if err != nil {
		return nil, err
	}
	validation, err := c.getRawValidationByKeyCtx(ctx, c.blockValidationKey(block))
	if err != nil {
		return nil, fmt.Errorf("read validation for height %d: %w", block.BlockNumber, err)
	}
	expected := file.Validation()
	if *validation != expected {
		return nil, fmt.Errorf("S3 blockfile/validation mismatch at height %d hash %s: stored=%+v calculated=%+v",
			block.BlockNumber, hash, *validation, expected)
	}
	return block, nil
}
