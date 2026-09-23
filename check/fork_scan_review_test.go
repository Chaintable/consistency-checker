package check

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Chaintable/consistency-checker/db"
	"github.com/Chaintable/pipeline/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/segmentio/kafka-go"
)

func TestForkScanTipReorgPreservesMatureBacklog(t *testing.T) {
	c, m := newScanChecker(t, 100)
	var blocks []types.BlockContext
	for h := uint64(101); h <= 240; h++ {
		blocks = append(blocks, scanBlock(h))
	}
	publishScanBlocks(t, c, m, blocks, nil, 11)
	at := time.Unix(1000, 0)
	c.observeForkScan(func() time.Time { return at })
	upper, generation, ready := c.observeForkScan(func() time.Time { return at.Add(time.Minute) })
	if !ready {
		t.Fatal("backlog did not mature")
	}
	alt := types.BlockContext{BlockNumber: 240, Hash: common.HexToHash("0xbeef"), ParentHash: scanHash(239)}
	publishScanBlocks(t, c, m, []types.BlockContext{alt}, []types.BlockContext{scanBlock(240)}, 12)
	// A tip reorg between observation and batch startup must keep the mature prefix.
	if _, err := c.scanContinuousBatch(context.Background(), upper, generation); err != nil {
		t.Fatalf("tip reorg stopped unaffected backlog: %v", err)
	}
	upper, _, ready = c.observeForkScan(func() time.Time { return at.Add(61 * time.Second) })
	if !ready || upper != 239 {
		t.Fatalf("lost mature prefix: ready=%v upper=%d", ready, upper)
	}
	scanThrough(t, c, upper)
	if c.forkState.NextHeight != 240 {
		t.Fatal("unaffected backlog did not complete")
	}
	// The replacement tip must receive a fresh observation and full delay.
	for _, elapsed := range []time.Duration{2 * time.Minute, 3*time.Minute - time.Nanosecond} {
		if _, _, ready := c.observeForkScan(func() time.Time { return at.Add(elapsed) }); ready {
			t.Fatal("replacement matured too soon")
		}
	}
	upper, _, ready = c.observeForkScan(func() time.Time { return at.Add(3 * time.Minute) })
	if !ready || upper != 240 {
		t.Fatal("replacement did not mature")
	}
}

func TestForkScanStartupAcceptsDropTailWithoutAligning(t *testing.T) {
	c, _ := newScanChecker(t, 10)
	c.latestOuterBlockChangeNotification = &types.OuterBlockChangeNotification{ChainID: 56, BlockNumber: 10, Hash: scanHash(10), IsFork: true}
	if err := c.loadForkScan(); err != nil {
		t.Fatalf("drop tail prevented Kafka replay: %v", err)
	}
	if c.forkAligned || c.forkRecoveryAnchor != nil {
		t.Fatal("drop notification became a trusted scan anchor")
	}
}

func TestForkScanEmptyPlansDoNotAcquireMainLockPerHeight(t *testing.T) {
	c, m := newScanChecker(t, 10)
	publishScanBlocks(t, c, m, []types.BlockContext{scanBlock(11), scanBlock(12), scanBlock(13)}, nil, 11)
	entered, release, lastRead := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once, lastOnce sync.Once
	m.before = func(r *http.Request) error {
		once.Do(func() { close(entered); <-release })
		if r.Method == "GET" && strings.TrimPrefix(r.URL.Path, "/bucket/") == c.blockValidationKey(&[]types.BlockContext{scanBlock(13)}[0]) {
			lastOnce.Do(func() { close(lastRead) })
		}
		return nil
	}
	result := make(chan error, 1)
	generation := c.forkState.Generation
	go func() { _, err := c.scanContinuousBatch(context.Background(), 13, generation); result <- err }()
	<-entered
	c.Lock()
	close(release)
	readAll := false
	select {
	case <-lastRead:
		readAll = true
	case <-time.After(time.Second):
	}
	state, err := db.DB.LoadForkScan(56, "test")
	c.Unlock()
	if scanErr := <-result; scanErr != nil {
		t.Fatal(scanErr)
	}
	if !readAll {
		t.Fatal("empty-plan scan waited for the main lock between heights")
	}
	if err != nil || state.NextHeight != 11 {
		t.Fatalf("cursor persisted before batch verification: %+v %v", state, err)
	}
	state, err = db.DB.LoadForkScan(56, "test")
	if err != nil || state.NextHeight != 14 {
		t.Fatalf("batch checkpoint missing: %+v %v", state, err)
	}
}

func TestForkScanDropTailReplaysAfterCrash(t *testing.T) {
	for _, rebuilt := range []bool{false, true} {
		t.Run(fmt.Sprintf("rebuilt=%v", rebuilt), func(t *testing.T) {
			c, m := newScanChecker(t, 10)
			publishScanBlocks(t, c, m, []types.BlockContext{scanBlock(11)}, nil, 11)
			alt := types.BlockContext{BlockNumber: 11, Hash: common.HexToHash("0xab"), ParentHash: scanHash(10)}
			m.set(t, c.blockValidationKey(&alt), false)
			notice := &types.BlockChangeNotification{NewBlocks: []types.BlockContext{alt}, DropBlocks: []types.BlockContext{scanBlock(11)}}
			var outerTail *types.OuterBlockChangeNotification
			c.writeOuter = func(_ *kafka.Writer, n *types.OuterBlockChangeNotification) error {
				if !n.IsFork {
					return errors.New("injected Kafka failure after drop publication")
				}
				outerTail = n
				return nil
			}
			if c.processNotice(notice, nil, scanPosition(12)) || outerTail == nil || !outerTail.IsFork {
				t.Fatal("did not stop between drop and replacement publication")
			}
			if c.forkState.Pending == nil || c.forkState.Published.Hash != scanHash(11) {
				t.Fatal("partial publication lost durable checkpoint")
			}
			// Model a fresh process reading the last successful outer notification.
			c.latestOuterBlockChangeNotification = outerTail
			c.delivered = nil
			c.writeOuter = func(*kafka.Writer, *types.OuterBlockChangeNotification) error { return nil }
			if rebuilt {
				if err := db.DB.Close(); err != nil {
					t.Fatal(err)
				}
				var err error
				db.DB, err = db.NewConsistencyDB(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
			}
			reopenForkScan(t, c)
			// An asynchronously committed Kafka offset may replay the older head first.
			if !c.processNotice(&types.BlockChangeNotification{NewBlocks: []types.BlockContext{scanBlock(11)}}, nil, scanPosition(11)) || c.forkAligned {
				t.Fatal("older duplicate blocked reorg replay or trusted the drop")
			}
			if !rebuilt && c.forkState.Pending == nil {
				t.Fatal("older duplicate cleared pending replacement")
			}
			if rebuilt && c.forkState != nil {
				t.Fatal("drop became a rebuilt baseline")
			}
			if !c.processNotice(notice, nil, scanPosition(12)) {
				t.Fatal("unfinished reorg did not replay")
			}
			if !c.forkAligned || c.forkState.Pending != nil || c.forkState.Published.Hash != alt.Hash {
				t.Fatal("replacement failed to align")
			}
			wantBaseline, wantNext := uint64(10), uint64(11)
			if rebuilt {
				wantBaseline, wantNext = 11, 12
			}
			if c.forkState.Baseline.Height != wantBaseline || c.forkState.NextHeight != wantNext {
				t.Fatalf("wrong recovered coverage: %+v", c.forkState)
			}
			if _, _, ready := c.observeForkScan(time.Now); ready {
				t.Fatal("recovery skipped observation delay")
			}
		})
	}
}

func TestForkScanBatchFailureCheckpointsOnlySuccessfulPrefix(t *testing.T) {
	c, m := newScanChecker(t, 10)
	publishScanBlocks(t, c, m, []types.BlockContext{scanBlock(11), scanBlock(12), scanBlock(13)}, nil, 11)
	fork := types.BlockContext{BlockNumber: 12, Hash: common.HexToHash("0xff")}
	m.set(t, c.blockValidationKey(&fork), false)
	m.before = func(r *http.Request) error {
		if r.Method == "PUT" {
			return errors.New("injected PUT failure")
		}
		return nil
	}
	if _, err := c.scanContinuousBatch(context.Background(), 13, c.forkState.Generation); err == nil {
		t.Fatal("PUT failure accepted")
	}
	reopenForkScan(t, c)
	if c.forkState.NextHeight != 12 || m.isFork(t, c.blockValidationKey(&fork)) {
		t.Fatal("failed height was skipped or successful prefix was lost")
	}
	m.before = nil
	scanThrough(t, c, 13)
	if c.forkState.NextHeight != 14 || !m.isFork(t, c.blockValidationKey(&fork)) {
		t.Fatal("failed height did not recover")
	}
}

func TestForkScanBatchCrashRepeatsUncheckpointedPrefix(t *testing.T) {
	c, m := newScanChecker(t, 10)
	publishScanBlocks(t, c, m, []types.BlockContext{scanBlock(11), scanBlock(12), scanBlock(13)}, nil, 11)
	fork := types.BlockContext{BlockNumber: 13, Hash: common.HexToHash("0xff")}
	m.set(t, c.blockValidationKey(&fork), false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.afterPut = cancel // Last repair succeeds, then the process stops before Sync.
	if _, err := c.scanContinuousBatch(ctx, 13, c.forkState.Generation); err == nil {
		t.Fatal("interruption accepted")
	}
	reopenForkScan(t, c)
	if c.forkState.NextHeight != 11 || !m.isFork(t, c.blockValidationKey(&fork)) {
		t.Fatal("batch progress was saved before its final checkpoint")
	}
	m.afterPut = nil
	scanThrough(t, c, 13)
	if c.forkState.NextHeight != 14 {
		t.Fatal("repeated batch did not complete")
	}
}

func TestForkScanConcurrentReorgCheckpointsUnaffectedPrefix(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		rewind uint64
		aba    bool
		want   uint64
	}{
		{"tip above batch", 15, false, 14},
		{"overlapping reorg", 12, false, 12},
		{"overlapping ABA reorg", 12, true, 12},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			c, m := newScanChecker(t, 10)
			var blocks []types.BlockContext
			for h := uint64(11); h <= 15; h++ {
				blocks = append(blocks, scanBlock(h))
			}
			publishScanBlocks(t, c, m, blocks, nil, 11)
			entered, release := make(chan struct{}), make(chan struct{})
			var blocked atomic.Bool
			m.before = func(r *http.Request) error {
				if r.Method == "GET" && strings.TrimPrefix(r.URL.Path, "/bucket/") == c.blockValidationKey(&[]types.BlockContext{scanBlock(13)}[0]) && blocked.CompareAndSwap(false, true) {
					close(entered)
					<-release
				}
				return nil
			}
			generation := c.forkState.Generation
			result := make(chan error, 1)
			go func() { _, err := c.scanContinuousBatch(context.Background(), 13, generation); result <- err }()
			<-entered
			var replacements, drops []types.BlockContext
			parent := scanHash(scenario.rewind - 1)
			for h := scenario.rewind; h <= 15; h++ {
				hash := common.HexToHash(fmt.Sprintf("0xab%02x", h))
				replacements = append(replacements, types.BlockContext{BlockNumber: h, Hash: hash, ParentHash: parent})
				drops = append(drops, scanBlock(h))
				parent = hash
			}
			publishScanBlocks(t, c, m, replacements, drops, 12)
			if scenario.aba {
				publishScanBlocks(t, c, m, drops, replacements, 13)
			}
			close(release)
			err := <-result
			if errors.Is(err, errScanStale) != (scenario.rewind <= 13) || (err != nil && !errors.Is(err, errScanStale)) {
				t.Fatalf("unexpected scan result: %v", err)
			}
			state, loadErr := db.DB.LoadForkScan(56, "test")
			if loadErr != nil || state.NextHeight != scenario.want || state.Generation != c.forkState.Generation {
				t.Fatalf("wrong prefix checkpoint: %+v %v", state, loadErr)
			}
			if scenario.rewind <= 13 && m.isFork(t, c.blockValidationKey(&[]types.BlockContext{scanBlock(13)}[0])) == scenario.aba {
				t.Fatal("old task overwrote current canonical flag")
			}
		})
	}
}
