package check

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/Chaintable/consistency-checker/db"
	"github.com/Chaintable/consistency-checker/metrics"
	"github.com/Chaintable/pipeline/types"
	"github.com/ethereum/go-ethereum/common"
)

const (
	forkScanBatchSize     = 128
	forkScanBatchTimeout  = 10 * time.Second
	forkScanHeightTimeout = 5 * time.Second
	// Only a single bounded PUT holds the processing lock, with no backoff.
	// A failed PUT is retried by the next scan round.
	forkScanWriteTimeout = time.Second
)

var (
	errScanNoCanonical = errors.New("fork scan has no trusted published canonical block")
	errScanStale       = errors.New("fork scan canonical or cursor generation changed")
)

// loadForkScan runs before the worker starts. Only ErrNotFound means new state;
// corrupt or unreadable state fails startup rather than discarding progress.
func (c *Checker) loadForkScan() error {
	if !c.config.ContinuousForkScan() {
		return nil
	}
	state, err := db.DB.LoadForkScan(c.config.ChainID, c.config.Version)
	if err != nil {
		return fmt.Errorf("load continuous fork scan: %w", err)
	}
	c.forkState = state
	if n := c.latestOuterBlockChangeNotification; n != nil {
		if n.ChainID != c.config.ChainID || n.IsFork {
			return fmt.Errorf("fork scan: invalid startup outer anchor")
		}
		c.forkRecoveryAnchor = &db.ForkScanAnchor{Height: n.BlockNumber, Hash: n.Hash}
	}
	if state != nil {
		metrics.ForkScanNextHeight.Set(float64(state.NextHeight))
		log.Printf("fork scan restored: chain=%d version=%s baseline=%d next=%d generation=%d inner=%+v; waiting for inner/outer alignment", c.config.ChainID, c.config.Version, state.Baseline.Height, state.NextHeight, state.Generation, state.Position)
	}
	return nil
}

func (c *Checker) saveForkScan(state *db.ForkScanState) error {
	if err := db.DB.SaveForkScan(c.config.ChainID, c.config.Version, state); err != nil {
		return err
	}
	c.forkState = state
	metrics.ForkScanNextHeight.Set(float64(state.NextHeight))
	return nil
}

func (c *Checker) initializeForkScan(anchor db.ForkScanAnchor, position *db.ForkScanPosition, reason string) error {
	if anchor.Height == math.MaxUint64 {
		return fmt.Errorf("fork scan baseline height overflows next_scan_height")
	}
	generation := uint64(1)
	if c.forkState != nil {
		if c.forkState.Generation == math.MaxUint64 {
			return fmt.Errorf("fork scan generation overflow")
		}
		generation = c.forkState.Generation + 1
	}
	state := &db.ForkScanState{Format: 1, Baseline: anchor, Published: anchor, NextHeight: anchor.Height + 1, Generation: generation, Position: position}
	if err := c.saveForkScan(state); err != nil {
		return err
	}
	log.Printf("fork scan initialized (%s): chain=%d version=%s baseline=%d hash=%s next=%d inner=%+v; heights <= baseline intentionally skipped, NOT scanned", reason, c.config.ChainID, c.config.Version, anchor.Height, anchor.Hash, state.NextHeight, position)
	return nil
}

// prepareForkScan is called with c.Lock only AFTER msgCheck or a matching
// published-tail replay. It never changes the main flow's continuity rules.
func (c *Checker) prepareForkScan(position *db.ForkScanPosition) bool {
	if !c.config.ContinuousForkScan() || c.forkAligned {
		return true
	}
	anchor := c.forkRecoveryAnchor
	if anchor == nil {
		return true
	} // empty outer topic: establish a baseline after publication
	reason := "first state / rebuilt DB"
	reset := c.forkState == nil
	if s := c.forkState; s != nil && s.Position != nil && position != nil {
		old := s.Position
		// An offset gap alone is insufficient (messages can span many blocks).
		// Require a changed startup outer anchor AND successful continuity checking.
		reset = old.Topic == position.Topic && old.Partition == position.Partition &&
			position.Offset > old.Offset && position.Offset-old.Offset > 1 && s.Published != *anchor
		reason = "inner offset gap with changed, aligned startup outer anchor"
	}
	if reset {
		log.Printf("fork scan recovery evidence: incoming inner=%+v, startup outer=%+v", position, anchor)
		// Keep the new message unacknowledged until finishForkScan succeeds.
		if err := c.initializeForkScan(*anchor, nil, reason); err != nil {
			log.Printf("fork scan initialize error: %v", err)
			return false
		}
	}
	return true
}

// finishForkScan persists the published anchor and message position BEFORE Run
// can commit the Kafka message. On a crash, publication may be replayed safely.
func (c *Checker) finishForkScan(position *db.ForkScanPosition) bool {
	if !c.config.ContinuousForkScan() {
		return true
	}
	n := c.latestOuterBlockChangeNotification
	if n == nil || n.IsFork || n.ChainID != c.config.ChainID {
		return false
	}
	anchor := db.ForkScanAnchor{Height: n.BlockNumber, Hash: n.Hash}
	if c.forkState == nil {
		if err := c.initializeForkScan(anchor, position, "first published block in empty outer topic"); err != nil {
			log.Printf("fork scan initialize error: %v", err)
			return false
		}
	} else {
		next := *c.forkState
		next.Published = anchor
		// Kafka may replay the older published message before the notification
		// whose canonical writes were committed. Do not clear that pending work
		// until its exact tail has been published, even on the duplicate path.
		if next.Pending != nil && *next.Pending == anchor {
			next.Pending = nil
		}
		if position != nil {
			next.Position = position
		}
		if err := c.saveForkScan(&next); err != nil {
			log.Printf("fork scan publish checkpoint error: %v", err)
			return false
		}
	}
	c.forkAligned = true
	return true
}

// One pending observation is enough: mature it only after an entire real-time
// interval, then observe again. Delayed/buffered timer events cannot shorten it.
// Reorg invalidates both the pending observation and the mature upper bound.
type forkScanSchedule struct {
	generation     uint64
	observedAt     time.Time
	observedHeight uint64
	matureHeight   uint64
	mature         bool
}

func (s *forkScanSchedule) observe(now time.Time, interval time.Duration, height, generation uint64) {
	if s.generation != generation {
		*s = forkScanSchedule{generation: generation}
	}
	if s.observedAt.IsZero() {
		s.observedAt, s.observedHeight = now, height
	} else if now.Sub(s.observedAt) >= interval {
		s.matureHeight, s.mature = s.observedHeight, true
		s.observedAt, s.observedHeight = now, height
	}
}

func (c *Checker) observeForkScan(now func() time.Time) (uint64, uint64, bool) {
	c.Lock()
	defer c.Unlock()
	s := c.forkState
	if !c.forkAligned || s == nil || s.Pending != nil {
		return 0, 0, false
	}
	// A baseline may have no local index after an intentional rebuild. It is
	// still a valid observation; the first covered height must have an index.
	c.forkSchedule.observe(now(), time.Duration(c.config.ForkScanInterval)*time.Second, s.Published.Height, s.Generation)
	upper := c.forkSchedule.matureHeight
	ready := c.forkSchedule.mature && s.NextHeight <= upper
	backlog := float64(0)
	if s.Published.Height >= s.NextHeight {
		backlog = float64(s.Published.Height-s.NextHeight) + 1
	}
	metrics.ForkScanBacklog.Set(backlog)
	return upper, s.Generation, ready
}

func (c *Checker) runForkScan() {
	if c.config.ForkScanInterval <= 0 || c.config.ForkScanLookback == 0 {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-c.quit:
			cancel()
		case <-ctx.Done():
		}
	}()
	interval := time.Duration(c.config.ForkScanInterval) * time.Second
	log.Printf("fork scan enabled: interval=%s lookback=%d", interval, c.config.ForkScanLookback)
	if !c.config.ContinuousForkScan() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.scanRecentForkBlocks(ctx)
			}
		}
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		wait := interval
		upper, generation, ready := c.observeForkScan(time.Now)
		if ready {
			batchCtx, stop := context.WithTimeout(ctx, forkScanBatchTimeout)
			more, err := c.scanContinuousBatch(batchCtx, upper, generation)
			stop()
			if err != nil {
				c.reportForkScanError(err)
			} else if more {
				wait = time.Millisecond
			}
		}
		// Timer reset after work deliberately avoids accumulated ticker events.
		timer.Reset(wait)
	}
}

func (c *Checker) scanContinuousBatch(ctx context.Context, upper, generation uint64) (bool, error) {
	for i := 0; i < forkScanBatchSize; i++ {
		if err := ctx.Err(); err != nil {
			return true, err
		}
		c.Lock()
		s := c.forkState
		if s == nil || s.Generation != generation {
			c.Unlock()
			return false, errScanStale
		}
		height := s.NextHeight
		c.Unlock()
		if height > upper {
			return false, nil
		}
		if err := c.scanForkBlocksAtHeight(ctx, height, generation, true); err != nil {
			return true, err
		}
	}
	return true, nil
}

func (c *Checker) scanRecentForkBlocks(ctx context.Context) {
	if c.config.ForkScanLookback <= 0 || c.config.ForkScanInterval <= 0 {
		return
	}
	c.Lock()
	latest := c.latestOuterBlockChangeNotification
	c.Unlock()
	if latest == nil {
		return
	}
	tip := latest.BlockNumber
	start := uint64(0)
	lookback := uint64(c.config.ForkScanLookback) // only after checking the signed mode
	if tip >= lookback {
		start = tip - lookback + 1
	}
	for h := start; ; h++ {
		if ctx.Err() != nil {
			return
		}
		if err := c.scanForkBlocksAtHeight(ctx, h, 0, false); err != nil {
			c.reportForkScanError(err)
		}
		if h == tip {
			return
		} // also safe at MaxUint64
	}
}

func (c *Checker) reportForkScanError(err error) {
	if errors.Is(err, errScanNoCanonical) {
		metrics.ForkScanSkips.Inc()
	} else {
		metrics.ForkScanErrors.Inc()
	}
	log.Printf("fork scan stopped/retry: %v", err)
}

// canonicalForScan requires c.Lock. Continuous mode uses the fully published
// checkpoint, never the DB tip or a partially delivered outer notification.
func (c *Checker) canonicalForScan(height, generation uint64, continuous bool) (common.Hash, error) {
	if continuous {
		s := c.forkState
		if !c.forkAligned || s == nil || s.Pending != nil {
			return common.Hash{}, errScanNoCanonical
		}
		if s.Generation != generation || s.NextHeight != height {
			return common.Hash{}, errScanStale
		}
		if height > s.Published.Height {
			return common.Hash{}, errScanNoCanonical
		}
	} else if c.latestOuterBlockChangeNotification == nil || height > c.latestOuterBlockChangeNotification.BlockNumber {
		return common.Hash{}, errScanNoCanonical
	}
	hash, ok, err := db.DB.GetCanonicalHashByNum(height)
	if err != nil {
		return common.Hash{}, err
	}
	if !ok {
		return common.Hash{}, errScanNoCanonical
	}
	return hash, nil
}

func (c *Checker) scanForkBlocksAtHeight(ctx context.Context, height, generation uint64, continuous bool) error {
	ctx, cancel := context.WithTimeout(ctx, forkScanHeightTimeout)
	defer cancel()
	c.Lock()
	hash, err := c.canonicalForScan(height, generation, continuous)
	c.Unlock()
	if err != nil {
		return fmt.Errorf("height %d: %w", height, err)
	}
	// LIST and GET, including retries, never hold the main processing lock.
	plan, err := c.planForkRewritesCtx(ctx, types.BlockContext{BlockNumber: height, Hash: hash}, nil)
	if err != nil {
		return err
	}
	verify := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		current, err := c.canonicalForScan(height, generation, continuous)
		if err != nil {
			return err
		}
		if current != hash {
			return errScanStale
		}
		return nil
	}
	for _, rw := range plan {
		err := func() error {
			c.Lock()
			defer c.Unlock()
			if err := verify(); err != nil {
				return err
			}
			putCtx, stop := context.WithTimeout(ctx, forkScanWriteTimeout)
			defer stop()
			return c.rewriteValidationAtKeyCtx(putCtx, rw.key, rw.validation)
		}()
		if err != nil {
			return err
		}
		metrics.ForkScanRewrites.Inc()
	}
	c.Lock()
	defer c.Unlock()
	// Recheck even an EMPTY plan: a reorg during LIST must not advance a stale
	// task, and a concurrent rewind must never be overwritten by completion.
	if err := verify(); err != nil {
		return err
	}
	if !continuous {
		return nil
	}
	if height == math.MaxUint64 {
		return fmt.Errorf("fork scan next height overflow")
	}
	next := *c.forkState
	next.NextHeight = height + 1
	// All S3 changes succeeded before this Sync. A crash here repeats the scan.
	return c.saveForkScan(&next)
}
