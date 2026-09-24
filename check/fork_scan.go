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
	forkScanBatchBudget   = 10 * time.Second
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
	c.forkAligned = false
	c.forkRecoveryAnchor = nil
	c.forkRecoveryDrop = false
	c.forkSchedule = forkScanSchedule{}
	if n := c.latestOuterBlockChangeNotification; n != nil {
		if n.ChainID != c.config.ChainID {
			return fmt.Errorf("fork scan: invalid startup outer anchor")
		}
		if n.IsFork {
			// A crash between publishing drops and replacements can leave a drop
			// at the outer tail. Let the main flow replay it; it is not a baseline.
			c.forkRecoveryDrop = true
			log.Printf("fork scan waiting for reorg replay: startup outer tail is a drop at height %d hash=%s", n.BlockNumber, n.Hash)
		} else {
			c.forkRecoveryAnchor = &db.ForkScanAnchor{Height: n.BlockNumber, Hash: n.Hash}
		}
	}
	if state != nil {
		// A completed durable checkpoint matching both the startup outer anchor and
		// local canonical index is already aligned. Do not require another Kafka
		// message to scan existing backlog (the producer may currently be idle).
		if state.Pending == nil && c.forkRecoveryAnchor != nil && state.Published == *c.forkRecoveryAnchor {
			hash, ok, err := db.DB.GetCanonicalHashByNum(state.Published.Height)
			if err != nil {
				return fmt.Errorf("validate restored fork scan anchor: %w", err)
			}
			c.forkAligned = ok && hash == state.Published.Hash
		}
		metrics.ForkScanNextHeight.Set(float64(state.NextHeight))
		log.Printf("fork scan restored: chain=%d version=%s baseline=%d next=%d generation=%d inner=%+v aligned=%v (otherwise waiting for inner/outer alignment)", c.config.ChainID, c.config.Version, state.Baseline.Height, state.NextHeight, state.Generation, state.Position, c.forkAligned)
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
	c.rewindForkScan(0, generation)
	log.Printf("fork scan initialized (%s): chain=%d version=%s baseline=%d hash=%s next=%d inner=%+v; heights <= baseline intentionally skipped, NOT scanned", reason, c.config.ChainID, c.config.Version, anchor.Height, anchor.Hash, state.NextHeight, position)
	return nil
}

// prepareForkScan is called with c.Lock only AFTER msgCheck or a matching
// published-tail replay. It never changes the main flow's continuity rules.
func (c *Checker) prepareForkScan(notice *types.BlockChangeNotification, position *db.ForkScanPosition) bool {
	if !c.config.ContinuousForkScan() || c.forkAligned {
		return true
	}
	anchor := c.forkRecoveryAnchor
	if anchor == nil {
		return true
	} // Empty outer or a partial reorg: wait for a fully published replacement.
	// Window/disabled modes and older binaries do not maintain this cursor.
	// A replay that proves the transition from the saved published head lets us
	// repair its rewind without discarding backlog or requiring an offset gap.
	if c.forkState != nil && c.forkState.Published != *anchor && c.isAlreadyProcessed(notice) && forkNoticeExtends(notice, c.forkState.Published) {
		if err := c.reconcileForkScanReplay(notice, *anchor); err != nil {
			log.Printf("fork scan replay reconciliation error: %v", err)
			return false
		}
		return true
	}
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

// forkNoticeExtends checks the transition from a saved head, not just whether
// the new tail equals today's outer head. Heights also matter for a rewind.
func forkNoticeExtends(notice *types.BlockChangeNotification, head db.ForkScanAnchor) bool {
	if len(notice.NewBlocks) == 0 {
		return false
	}
	first := notice.NewBlocks[0]
	if len(notice.DropBlocks) == 0 {
		return head.Height < math.MaxUint64 && first.BlockNumber == head.Height+1 && first.ParentHash == head.Hash
	}
	dropped := notice.DropBlocks
	tail := dropped[len(dropped)-1]
	return tail.BlockNumber == head.Height && tail.Hash == head.Hash &&
		dropped[0].BlockNumber == first.BlockNumber && dropped[0].ParentHash == first.ParentHash
}

// The main path has already recognized a published-tail replay. Check the
// canonical indexes for the entire replacement before trusting its rewind;
// missing/corrupt indexes are errors, never an invitation to jump to latest.
// Requires c.Lock. No canonical writes race with this metadata-only recovery.
func (c *Checker) reconcileForkScanReplay(notice *types.BlockChangeNotification, anchor db.ForkScanAnchor) error {
	s := c.forkState
	if s.Pending != nil && *s.Pending != anchor {
		return fmt.Errorf("replayed head does not match pending canonical update")
	}
	for i, block := range notice.NewBlocks {
		if i > 0 {
			prev := notice.NewBlocks[i-1]
			if prev.BlockNumber == math.MaxUint64 || block.BlockNumber != prev.BlockNumber+1 || block.ParentHash != prev.Hash {
				return fmt.Errorf("non-contiguous published replay at height %d", block.BlockNumber)
			}
		}
		hash, ok, err := db.DB.GetCanonicalHashByNum(block.BlockNumber)
		if err != nil {
			return err
		}
		if !ok || hash != block.Hash {
			return fmt.Errorf("published replay has no matching canonical at height %d", block.BlockNumber)
		}
	}
	tail := notice.NewBlocks[len(notice.NewBlocks)-1]
	if tail.BlockNumber != anchor.Height || tail.Hash != anchor.Hash {
		return fmt.Errorf("published replay tail does not match recovery anchor")
	}
	next := *s
	if len(notice.DropBlocks) > 0 {
		if next.Generation == math.MaxUint64 {
			return fmt.Errorf("fork scan generation overflow")
		}
		next.Generation++
		next.NextHeight = min(next.NextHeight, notice.NewBlocks[0].BlockNumber)
	}
	next.Published, next.Pending = anchor, nil
	if err := c.saveForkScan(&next); err != nil {
		return err
	}
	if len(notice.DropBlocks) > 0 {
		c.rewindForkScan(notice.NewBlocks[0].BlockNumber, next.Generation)
	}
	log.Printf("fork scan reconciled published replay: head=%d hash=%s next=%d generation=%d", anchor.Height, anchor.Hash, next.NextHeight, next.Generation)
	return nil
}

// finishForkScan persists the published anchor and message position BEFORE Run
// can commit the Kafka message. On a crash, publication may be replayed safely.
func (c *Checker) finishForkScan(position *db.ForkScanPosition) bool {
	if !c.config.ContinuousForkScan() {
		return true
	}
	n := c.latestOuterBlockChangeNotification
	if n == nil || n.ChainID != c.config.ChainID {
		return false
	}
	if n.IsFork {
		// Kafka can replay an older duplicate before the unfinished reorg. Let
		// that replay advance without promoting a drop or clearing pending work.
		return c.forkRecoveryDrop
	}
	anchor := db.ForkScanAnchor{Height: n.BlockNumber, Hash: n.Hash}
	if c.forkState == nil {
		reason := "first published block in empty outer topic"
		if c.forkRecoveryDrop {
			reason = "first fully published replacement after startup drop tail"
		}
		if err := c.initializeForkScan(anchor, position, reason); err != nil {
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
	c.forkRecoveryDrop = false
	return true
}

// One pending observation is enough: mature it only after an entire real-time
// interval, then observe again. Delayed/buffered timer events cannot shorten it.
// Reorg truncates observations to the unchanged prefix. Replacement heights
// must be observed again and wait a full interval, without starving old backlog.
type forkScanSchedule struct {
	generation     uint64
	observedAt     time.Time
	observedHeight uint64
	matureHeight   uint64
	mature         bool
}

func (s *forkScanSchedule) rewind(height, generation uint64) {
	if height == 0 {
		*s = forkScanSchedule{generation: generation}
		return
	}
	s.generation = generation
	s.observedHeight = min(s.observedHeight, height-1)
	s.matureHeight = min(s.matureHeight, height-1)
}

// Called under c.Lock after the atomic canonical/cursor update. The first new
// height is a conservative lower bound for every mapping changed by that write.
func (c *Checker) rewindForkScan(height, generation uint64) {
	c.forkSchedule.rewind(height, generation)
	if b := c.forkBatch; b != nil {
		if old := b.rewind.Load(); old == nil || height < *old {
			b.rewind.Store(&height)
		}
	}
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
			more, err := c.scanContinuousBatch(ctx, upper, generation)
			if errors.Is(err, errScanStale) {
				// Re-evaluate the surviving mature prefix immediately after reorg.
				wait = time.Millisecond
			} else if err != nil {
				c.reportForkScanError(err)
			} else if more {
				wait = time.Millisecond
			}
		}
		// Timer reset after work deliberately avoids accumulated ticker events.
		timer.Reset(wait)
	}
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
		if err := c.scanForkBlocksAtHeight(ctx, h); err != nil {
			c.reportForkScanError(err)
		}
		if h == tip {
			return
		} // also safe at MaxUint64
	}
}

func (c *Checker) reportForkScanError(err error) {
	if errors.Is(err, errScanStale) || errors.Is(err, context.Canceled) {
		return
	}
	if errors.Is(err, errScanNoCanonical) {
		metrics.ForkScanSkips.Inc()
	} else {
		metrics.ForkScanErrors.Inc()
	}
	log.Printf("fork scan stopped/retry: %v", err)
}

// canonicalForScan requires c.Lock and is used by the window scanner.
func (c *Checker) canonicalForScan(height uint64) (common.Hash, error) {
	if c.latestOuterBlockChangeNotification == nil || height > c.latestOuterBlockChangeNotification.BlockNumber {
		return common.Hash{}, errScanNoCanonical
	}
	return readScanCanonical(height)
}

// Pebble reads are concurrent-safe. Continuous batches verify these speculative
// reads under the processing lock before PUTs and before their final checkpoint.
func readScanCanonical(height uint64) (common.Hash, error) {
	hash, ok, err := db.DB.GetCanonicalHashByNum(height)
	if err != nil {
		return common.Hash{}, err
	}
	if !ok {
		return common.Hash{}, errScanNoCanonical
	}
	return hash, nil
}

func (c *Checker) scanForkBlocksAtHeight(ctx context.Context, height uint64) error {
	ctx, cancel := context.WithTimeout(ctx, forkScanHeightTimeout)
	defer cancel()
	c.Lock()
	hash, err := c.canonicalForScan(height)
	c.Unlock()
	if err != nil {
		return fmt.Errorf("height %d: %w", height, err)
	}
	verify := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		current, err := c.canonicalForScan(height)
		if err != nil {
			return err
		}
		if current != hash {
			return errScanStale
		}
		return nil
	}
	if err := c.executeForkScanPlan(ctx, height, hash, verify); err != nil {
		return err
	}
	c.Lock()
	defer c.Unlock()
	return verify()
}

// Only PUTs hold c.Lock. verify is also called under that lock before each PUT.
func (c *Checker) executeForkScanPlan(ctx context.Context, height uint64, hash common.Hash, verify func() error) error {
	plan, err := c.planForkRewritesCtx(ctx, types.BlockContext{BlockNumber: height, Hash: hash}, nil)
	if err != nil {
		return err
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
	return ctx.Err()
}
