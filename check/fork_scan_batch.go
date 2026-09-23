package check

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/Chaintable/pipeline/types"
	"github.com/ethereum/go-ethereum/common"
)

// There is one scan worker and at most one active batch. Reorgs record the
// lowest affected height while holding c.Lock. Keep that fence even across ABA
// reorgs; comparing only the final hash would incorrectly accept old scans.
type forkScanBatch struct {
	start, upper, generation uint64
	rewind                   atomic.Pointer[uint64]
}

func (b *forkScanBatch) stale(height uint64) bool {
	rewind := b.rewind.Load()
	return rewind != nil && height >= *rewind
}

func (c *Checker) beginForkScanBatch(upper, generation uint64) (*forkScanBatch, error) {
	c.Lock()
	defer c.Unlock()
	s := c.forkState
	if !c.forkAligned || s == nil || s.Pending != nil {
		return nil, errScanNoCanonical
	}
	if c.forkBatch != nil {
		return nil, fmt.Errorf("fork scan batch already active")
	}
	if generation != s.Generation {
		// A reorg can occur between observation and this lock. Its unchanged
		// mature prefix is still safe; never reuse the old upper bound above it.
		if c.forkSchedule.generation != s.Generation || !c.forkSchedule.mature {
			return nil, errScanStale
		}
		upper = min(upper, c.forkSchedule.matureHeight)
	}
	b := &forkScanBatch{start: s.NextHeight, upper: min(upper, s.Published.Height), generation: s.Generation}
	c.forkBatch = b
	return b, nil
}

func (c *Checker) scanContinuousBatch(ctx context.Context, upper, generation uint64) (bool, error) {
	return c.scanContinuousBatchWithClock(ctx, upper, generation, time.Now)
}

func (c *Checker) scanContinuousBatchWithClock(ctx context.Context, upper, generation uint64, now func() time.Time) (bool, error) {
	b, err := c.beginForkScanBatch(upper, generation)
	if err != nil {
		return true, err
	}
	deadline := now().Add(forkScanBatchBudget)
	checked := make([]types.BlockContext, 0, forkScanBatchSize)
	for height := b.start; height <= b.upper && len(checked) < forkScanBatchSize; height++ {
		if err = ctx.Err(); err != nil {
			break
		}
		if b.stale(height) {
			err = errScanStale
			break
		}
		if len(checked) > 0 && !now().Before(deadline) {
			break // Healthy budget yield; immediately continue mature backlog.
		}
		if height == math.MaxUint64 {
			err = fmt.Errorf("fork scan next height overflow")
			break
		}
		var hash common.Hash
		hash, err = c.scanContinuousBatchHeight(ctx, b, height)
		if err != nil {
			break
		}
		checked = append(checked, types.BlockContext{BlockNumber: height, Hash: hash})
	}
	more, commitErr := c.finishForkScanBatch(ctx, b, checked)
	return more, errors.Join(err, commitErr)
}

func (c *Checker) scanContinuousBatchHeight(ctx context.Context, b *forkScanBatch, height uint64) (common.Hash, error) {
	ctx, cancel := context.WithTimeout(ctx, forkScanHeightTimeout)
	defer cancel()
	hash, err := readScanCanonical(height)
	if err != nil {
		return common.Hash{}, fmt.Errorf("height %d: %w", height, err)
	}
	err = c.executeForkScanPlan(ctx, height, hash, func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return c.verifyForkScanBatchHeight(b, height, hash)
	})
	return hash, err
}

// Requires c.Lock. Pending new publication does not invalidate a previously
// published, unchanged prefix. Reorg fences do invalidate it, including empty
// plans and a hash that changes away and back while the batch is in flight.
func (c *Checker) verifyForkScanBatchHeight(b *forkScanBatch, height uint64, hash common.Hash) error {
	s := c.forkState
	if c.forkBatch != b || !c.forkAligned || s == nil || s.NextHeight != b.start || b.stale(height) {
		return errScanStale
	}
	if s.Generation != b.generation && b.rewind.Load() == nil {
		return errScanStale // Defensive: an untracked state replacement.
	}
	if height > s.Published.Height {
		return errScanNoCanonical
	}
	current, err := readScanCanonical(height)
	if err != nil {
		return err
	}
	if current != hash {
		return errScanStale
	}
	return nil
}

func (c *Checker) finishForkScanBatch(ctx context.Context, b *forkScanBatch, checked []types.BlockContext) (bool, error) {
	c.Lock()
	defer c.Unlock()
	defer func() { c.forkBatch = nil }()
	if err := ctx.Err(); err != nil {
		return true, err
	}
	nextHeight := b.start
	var verifyErr error
	for _, block := range checked {
		if verifyErr = c.verifyForkScanBatchHeight(b, block.BlockNumber, block.Hash); verifyErr != nil {
			break
		}
		nextHeight = block.BlockNumber + 1
	}
	if nextHeight > b.start {
		next := *c.forkState
		next.NextHeight = nextHeight
		// Only the verified contiguous prefix is checkpointed. Any S3 failure,
		// reorg, or crash can repeat work, never skip a failed/invalidated height.
		if err := c.saveForkScan(&next); err != nil {
			return true, err
		}
	}
	return nextHeight <= b.upper, verifyErr
}
