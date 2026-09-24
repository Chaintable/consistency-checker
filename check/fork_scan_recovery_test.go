package check

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Chaintable/consistency-checker/db"
	"github.com/Chaintable/pipeline/types"
	"github.com/ethereum/go-ethereum/common"
)

func reopenForkScan(t *testing.T, c *Checker) {
	t.Helper()
	path := db.DB.DBDir
	if err := db.DB.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	db.DB, err = db.NewConsistencyDB(path)
	if err != nil {
		t.Fatal(err)
	}
	c.forkState = nil
	if err := c.loadForkScan(); err != nil {
		t.Fatal(err)
	}
}

func TestForkScanRestartResumesWithoutNewMessage(t *testing.T) {
	c, m := newScanChecker(t, 10)
	publishScanBlocks(t, c, m, []types.BlockContext{scanBlock(11)}, nil, 11)
	late := types.BlockContext{BlockNumber: 11, Hash: common.HexToHash("0xdead")}
	key := c.blockValidationKey(&late)
	m.set(t, key, false)
	reopenForkScan(t, c)
	if !c.forkAligned || c.forkState.NextHeight != 11 {
		t.Fatalf("valid checkpoint did not resume: %+v", c.forkState)
	}
	at := time.Unix(1000, 0)
	for _, now := range []time.Time{at, at.Add(time.Minute - time.Nanosecond)} {
		if _, _, ready := c.observeForkScan(func() time.Time { return now }); ready {
			t.Fatal("restart skipped observation delay")
		}
	}
	upper, _, ready := c.observeForkScan(func() time.Time { return at.Add(time.Minute) })
	if !ready || upper != 11 {
		t.Fatal("idle producer blocked restored backlog")
	}
	scanThrough(t, c, upper)
	if !m.isFork(t, key) || c.forkState.NextHeight != 12 {
		t.Fatal("late fork was not repaired")
	}
	// Background resume must not bypass continuity checking for future messages.
	if c.processNotice(&types.BlockChangeNotification{NewBlocks: []types.BlockContext{{BlockNumber: 12, Hash: scanHash(12), ParentHash: common.HexToHash("0xbad")}}}, nil, scanPosition(12)) {
		t.Fatal("invalid next message accepted")
	}
}

func TestForkScanRestartRequiresMatchingCompletedAnchor(t *testing.T) {
	for _, scenario := range []string{"outer mismatch", "pending", "missing canonical", "canonical mismatch"} {
		t.Run(scenario, func(t *testing.T) {
			c, m := newScanChecker(t, 10)
			publishScanBlocks(t, c, m, []types.BlockContext{scanBlock(11)}, nil, 11)
			switch scenario {
			case "outer mismatch":
				c.latestOuterBlockChangeNotification = &types.OuterBlockChangeNotification{ChainID: 56, BlockNumber: 12, Hash: scanHash(12)}
			case "pending":
				_, err := db.DB.WriteBlockInfosWithForkScan([]types.BlockContext{scanBlock(12)}, []int64{7}, 56, "test", c.forkState)
				if err != nil {
					t.Fatal(err)
				}
			case "missing canonical":
				if err := db.DB.WriteBlockInfos([]types.BlockContext{scanBlock(10)}, []int64{7}); err != nil {
					t.Fatal(err)
				}
			case "canonical mismatch":
				if err := db.DB.WriteBlockInfos([]types.BlockContext{{BlockNumber: 11, Hash: common.HexToHash("0xab")}}, []int64{7}); err != nil {
					t.Fatal(err)
				}
			}
			reopenForkScan(t, c)
			if c.forkAligned {
				t.Fatal("untrusted checkpoint resumed")
			}
			if c.forkState.Baseline.Height != 10 || c.forkState.NextHeight != 11 {
				t.Fatal("untrusted state silently reset")
			}
			if _, _, ready := c.observeForkScan(time.Now); ready {
				t.Fatal("untrusted checkpoint scheduled work")
			}
		})
	}
}

func TestForkScanReenableReconcilesPublishedReorg(t *testing.T) {
	for _, mode := range []int64{0, 64} {
		for _, tip := range []uint64{12, 13, 14} {
			t.Run(fmt.Sprintf("mode=%d/tip=%d", mode, tip), func(t *testing.T) {
				c, m := newScanChecker(t, 10)
				publishScanBlocks(t, c, m, []types.BlockContext{scanBlock(11), scanBlock(12), scanBlock(13)}, nil, 11)
				scanThrough(t, c, 13)
				generation := c.forkState.Generation
				c.config.ForkScanLookback = mode
				var replacement []types.BlockContext
				parent := scanHash(11)
				for h := uint64(12); h <= tip; h++ {
					hash := common.HexToHash(fmt.Sprintf("0xab%02x", h))
					replacement = append(replacement, types.BlockContext{BlockNumber: h, Hash: hash, ParentHash: parent})
					parent = hash
				}
				drops := []types.BlockContext{scanBlock(12), scanBlock(13)}
				publishScanBlocks(t, c, m, replacement, drops, 12)
				c.config.ForkScanLookback = -1
				reopenForkScan(t, c)
				if c.forkAligned {
					t.Fatal("stale saved head accepted at startup")
				}
				notice := &types.BlockChangeNotification{NewBlocks: replacement, DropBlocks: drops}
				for i := 0; i < 2; i++ {
					if !c.processNotice(notice, nil, scanPosition(12)) {
						t.Fatal("valid consecutive replay cannot progress")
					}
				}
				if c.forkState.NextHeight != 12 || c.forkState.Baseline.Height != 10 || c.forkState.Generation != generation+1 {
					t.Fatalf("bad reconciliation: %+v", c.forkState)
				}
				if c.forkState.Position.Offset != 12 || c.forkState.Published.Height != tip {
					t.Fatal("replay checkpoint not saved")
				}
				at := time.Unix(1000, 0)
				if _, _, ready := c.observeForkScan(func() time.Time { return at }); ready {
					t.Fatal("reconciled reorg skipped delay")
				}
				upper, _, ready := c.observeForkScan(func() time.Time { return at.Add(time.Minute) })
				if !ready {
					t.Fatal("reconciled reorg never matured")
				}
				scanThrough(t, c, upper)
				if c.forkState.NextHeight != tip+1 {
					t.Fatal("replacement was not scanned")
				}
			})
		}
	}
}

func TestForkScanReplayPreservesBacklogDespiteOffsetGap(t *testing.T) {
	for _, reorg := range []bool{false, true} {
		t.Run(fmt.Sprint(reorg), func(t *testing.T) {
			c, m := newScanChecker(t, 10)
			publishScanBlocks(t, c, m, []types.BlockContext{scanBlock(11), scanBlock(12), scanBlock(13)}, nil, 11)
			c.config.ForkScanLookback = 64
			blocks := []types.BlockContext{scanBlock(14)}
			var drops []types.BlockContext
			if reorg {
				blocks = []types.BlockContext{{BlockNumber: 12, Hash: common.HexToHash("0xab"), ParentHash: scanHash(11)}}
				drops = []types.BlockContext{scanBlock(12), scanBlock(13)}
			}
			publishScanBlocks(t, c, m, blocks, drops, 99)
			c.config.ForkScanLookback = -1
			reopenForkScan(t, c)
			if !c.processNotice(&types.BlockChangeNotification{NewBlocks: blocks, DropBlocks: drops}, nil, scanPosition(99)) {
				t.Fatal("published replay failed")
			}
			if c.forkState.Baseline.Height != 10 || c.forkState.NextHeight != 11 {
				t.Fatalf("proved replay discarded existing backlog: %+v", c.forkState)
			}
		})
	}
}

func TestForkScanReplayMissingIndexDoesNotReset(t *testing.T) {
	c, m := newScanChecker(t, 10)
	publishScanBlocks(t, c, m, []types.BlockContext{scanBlock(11), scanBlock(12), scanBlock(13)}, nil, 11)
	scanThrough(t, c, 13)
	// Published replay proves a shorter reorg, but its replacement index is
	// unavailable. An offset gap must not turn this ordinary DB hole into reset.
	b := types.BlockContext{BlockNumber: 12, Hash: common.HexToHash("0xab"), ParentHash: scanHash(11)}
	if err := db.DB.WriteBlockInfos([]types.BlockContext{scanBlock(11)}, []int64{7}); err != nil {
		t.Fatal(err)
	}
	c.latestOuterBlockChangeNotification = &types.OuterBlockChangeNotification{ChainID: 56, BlockNumber: 12, Hash: b.Hash}
	reopenForkScan(t, c)
	notice := &types.BlockChangeNotification{NewBlocks: []types.BlockContext{b}, DropBlocks: []types.BlockContext{scanBlock(12), scanBlock(13)}}
	if c.processNotice(notice, nil, scanPosition(99)) {
		t.Fatal("missing replay index was accepted")
	}
	if c.forkState.Baseline.Height != 10 || c.forkState.NextHeight != 14 || c.forkAligned {
		t.Fatal("missing index discarded trusted state")
	}
}

func TestForkScanBatchBudgetYieldsHealthyBacklog(t *testing.T) {
	c, m := newScanChecker(t, 10)
	publishScanBlocks(t, c, m, []types.BlockContext{scanBlock(11), scanBlock(12), scanBlock(13)}, nil, 11)
	now := time.Unix(1000, 0)
	// Two successful requests per height. Advancing only the injected clock
	// exercises the production batch budget without sleeping for ten seconds.
	m.before = func(*http.Request) error { now = now.Add(3 * time.Second); return nil }
	more, err := c.scanContinuousBatchWithClock(context.Background(), 13, c.forkState.Generation, func() time.Time { return now })
	if err != nil || !more || c.forkState.NextHeight != 13 {
		t.Fatalf("budget must yield at a completed height: more=%v err=%v next=%d", more, err, c.forkState.NextHeight)
	}
	more, err = c.scanContinuousBatchWithClock(context.Background(), 13, c.forkState.Generation, func() time.Time { return now })
	if err != nil || more || c.forkState.NextHeight != 14 {
		t.Fatalf("backlog did not continue: more=%v err=%v next=%d", more, err, c.forkState.NextHeight)
	}
}

func TestForkScanBatchBudgetDoesNotHideRequestFailure(t *testing.T) {
	c, m := newScanChecker(t, 10)
	publishScanBlocks(t, c, m, []types.BlockContext{scanBlock(11)}, nil, 11)
	now := time.Unix(1000, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.before = func(*http.Request) error { now = now.Add(forkScanBatchBudget); cancel(); return ctx.Err() }
	more, err := c.scanContinuousBatchWithClock(ctx, 11, c.forkState.Generation, func() time.Time { return now })
	if err == nil || !more || c.forkState.NextHeight != 11 {
		t.Fatalf("request failure became successful yield: more=%v err=%v next=%d", more, err, c.forkState.NextHeight)
	}
}
