package check

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Chaintable/consistency-checker/config"
	"github.com/Chaintable/pipeline/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/segmentio/kafka-go"
)

func readyState(height uint64) *ReplicaStateChangeNotification {
	return &ReplicaStateChangeNotification{LatestBlockNumber: (*hexutil.Big)(big.NewInt(int64(height)))}
}

func notReadyState() *ReplicaStateChangeNotification {
	return &ReplicaStateChangeNotification{}
}

func TestWaitReplicasReadyKeepsPollingUntilReady(t *testing.T) {
	c := &Checker{config: &config.Config{CheckInterval: 1}}
	var calls int32
	c.checkReplicas = func(height uint64) (*ReplicaStateChangeNotification, error) {
		if atomic.AddInt32(&calls, 1) < 4 {
			return notReadyState(), nil
		}
		return readyState(height), nil
	}

	state, err := c.waitReplicasReady(100, time.Second)
	if err != nil {
		t.Fatalf("waitReplicasReady() error = %v", err)
	}
	if state == nil || state.LatestBlockNumber == nil || state.LatestBlockNumber.ToInt().Uint64() != 100 {
		t.Fatalf("waitReplicasReady() = %+v, want ready at 100", state)
	}
	if got := atomic.LoadInt32(&calls); got != 4 {
		t.Fatalf("checkReplicas called %d times, want 4 (kept polling past the old 3-attempt window)", got)
	}
}

func TestWaitReplicasReadyGivesUpAfterTimeout(t *testing.T) {
	c := &Checker{config: &config.Config{CheckInterval: 2}}
	var calls int32
	c.checkReplicas = func(uint64) (*ReplicaStateChangeNotification, error) {
		atomic.AddInt32(&calls, 1)
		return notReadyState(), nil
	}

	start := time.Now()
	state, err := c.waitReplicasReady(100, 30*time.Millisecond)
	if err == nil {
		t.Fatal("waitReplicasReady() should fail once the deadline passes")
	}
	if state == nil {
		t.Fatal("waitReplicasReady() should return the last replica state so offline nodes can be pruned")
	}
	// 最后一轮不会在 deadline 之后才发起，所以返回时刻在 deadline 前一个 interval 以内
	if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
		t.Fatalf("returned after %v, well before the %v deadline", elapsed, 30*time.Millisecond)
	}
	if got := atomic.LoadInt32(&calls); got < 2 {
		t.Fatalf("checkReplicas called %d times, want repeated polling", got)
	}
}

func TestWaitReplicasReadyZeroTimeoutChecksOnce(t *testing.T) {
	c := &Checker{config: &config.Config{CheckInterval: 1}}
	var calls int32
	c.checkReplicas = func(uint64) (*ReplicaStateChangeNotification, error) {
		atomic.AddInt32(&calls, 1)
		return notReadyState(), nil
	}
	if _, err := c.waitReplicasReady(100, 0); err == nil {
		t.Fatal("waitReplicasReady() with zero timeout should report not ready")
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("checkReplicas called %d times, want exactly 1 when timeout is zero", got)
	}
}

func TestRetryWithBackoffRecovers(t *testing.T) {
	c := &Checker{}
	calls := 0
	err := c.retryWithBackoff(context.Background(), "op", time.Second, func() error {
		calls++
		if calls < 3 {
			return errors.New("not yet")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("retryWithBackoff() error = %v", err)
	}
	if calls != 3 {
		t.Fatalf("fn called %d times, want 3", calls)
	}
}

func TestRetryWithBackoffStopsOnCancel(t *testing.T) {
	c := &Checker{}
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	start := time.Now()
	err := c.retryWithBackoff(ctx, "op", time.Minute, func() error {
		calls++
		if calls == 1 {
			cancel()
		}
		return errors.New("failing")
	})
	if err == nil {
		t.Fatal("retryWithBackoff() should return the last error once the context is cancelled")
	}
	if calls != 1 {
		t.Fatalf("fn called %d times, want 1 (no retry after cancel)", calls)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("took %v, should return promptly on cancel", elapsed)
	}
}

func TestPrefetchAbortStopsPendingWork(t *testing.T) {
	// 让 semaphore 已满，预取 goroutine 卡在 acquire；abort 后它们必须立刻退出并带回 ctx 错误
	c := &Checker{config: &config.Config{ChainID: 1}, prefetchSem: make(chan struct{}, 1)}
	c.prefetchSem <- struct{}{}
	res := c.prefetchNewBlocks([]types.BlockContext{{BlockNumber: 1, Hash: common.HexToHash("0x01")}})
	res.abort()
	done := make(chan struct{})
	go func() { res.wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("prefetch goroutines did not exit after abort")
	}
	if res.blocks[0].validationErr == nil || res.blocks[0].keysErr == nil {
		t.Fatalf("aborted prefetch should report errors, got %+v", res.blocks[0])
	}
}

func TestRetryWithBackoffStopsAtMaxWait(t *testing.T) {
	c := &Checker{}
	calls := 0
	start := time.Now()
	// 退避序列 50ms, 100ms：第二次等待后累计 150ms >= 120ms，第三次失败即返回
	err := c.retryWithBackoff(context.Background(), "op", 120*time.Millisecond, func() error {
		calls++
		return errors.New("still failing")
	})
	if err == nil {
		t.Fatal("retryWithBackoff() should give up after maxWait")
	}
	if calls != 3 {
		t.Fatalf("fn called %d times, want 3", calls)
	}
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond || elapsed > time.Second {
		t.Fatalf("elapsed %v, want ~150ms of backoff", elapsed)
	}
}

// fakeOuter 按 writer 指针区分 version / singleton 两个目的地，记录写入并按需注入失败
type fakeOuter struct {
	mu        sync.Mutex
	version   *kafka.Writer
	singleton *kafka.Writer
	failV     bool
	failS     bool
	writes    map[string][]common.Hash
}

func newVersionModeChecker(leaderAligned bool) (*Checker, *fakeOuter) {
	f := &fakeOuter{version: &kafka.Writer{}, singleton: &kafka.Writer{}, writes: map[string][]common.Hash{}}
	c := &Checker{
		config:                       &config.Config{ChainID: 1, Version: "v1", OuterVersionNewBlockTopic: "pipeline_1_v1"},
		outerVersionNewBlockWriter:   f.version,
		outerSingletonNewBlockWriter: f.singleton,
		isOuterSingletonAlign:        leaderAligned,
	}
	if leaderAligned {
		c.etcdLock = &EtcdLock{}
	}
	c.writeOuter = func(w *kafka.Writer, b *types.OuterBlockChangeNotification) error {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch w {
		case f.version:
			if f.failV {
				return errors.New("version broker down")
			}
			f.writes[outerVersionDestination] = append(f.writes[outerVersionDestination], b.Hash)
		case f.singleton:
			if f.failS {
				return errors.New("singleton broker down")
			}
			f.writes[outerSingletonDestination] = append(f.writes[outerSingletonDestination], b.Hash)
		default:
			return errors.New("unknown writer")
		}
		return nil
	}
	return c, f
}

func TestPublishOuterWritesBothTopicsWhenLeader(t *testing.T) {
	c, f := newVersionModeChecker(true)
	h := common.HexToHash("0x01")
	var observed []string
	ok := c.publishOuter(&types.OuterBlockChangeNotification{Hash: h}, func(d string, _ time.Time) { observed = append(observed, d) })
	if !ok {
		t.Fatal("publishOuter() = false, want true")
	}
	if len(f.writes[outerVersionDestination]) != 1 || len(f.writes[outerSingletonDestination]) != 1 {
		t.Fatalf("writes = %+v, want one to each topic", f.writes)
	}
	if len(observed) != 2 {
		t.Fatalf("observed destinations = %v, want version and singleton", observed)
	}
	key := outerNoticeKey{hash: h}
	if !c.isDelivered(outerVersionDestination, key) || !c.isDelivered(outerSingletonDestination, key) {
		t.Fatalf("delivered = %+v, want both destinations recorded", c.delivered)
	}
}

func TestPublishOuterSkipsSingletonWhenNotLeader(t *testing.T) {
	c, f := newVersionModeChecker(false)
	if !c.publishOuter(&types.OuterBlockChangeNotification{Hash: common.HexToHash("0x01")}, func(string, time.Time) {}) {
		t.Fatal("publishOuter() = false, want true")
	}
	if len(f.writes[outerSingletonDestination]) != 0 {
		t.Fatalf("singleton written while not leader: %+v", f.writes)
	}
	if len(f.writes[outerVersionDestination]) != 1 {
		t.Fatalf("version writes = %+v, want 1", f.writes)
	}
}

func TestPublishOuterVersionFailureRetryDoesNotDuplicateSingleton(t *testing.T) {
	c, f := newVersionModeChecker(true)
	h := common.HexToHash("0x02")
	b := &types.OuterBlockChangeNotification{Hash: h}

	f.failV = true
	if c.publishOuter(b, func(string, time.Time) {}) {
		t.Fatal("publishOuter() = true while version write failed")
	}
	if len(f.writes[outerSingletonDestination]) != 1 {
		t.Fatalf("singleton writes after first attempt = %d, want 1 (written concurrently)", len(f.writes[outerSingletonDestination]))
	}

	// 整条 Process 重试：version 恢复，singleton 不应再写一次
	f.failV = false
	if !c.publishOuter(b, func(string, time.Time) {}) {
		t.Fatal("publishOuter() = false on retry")
	}
	if got := len(f.writes[outerSingletonDestination]); got != 1 {
		t.Fatalf("singleton writes after retry = %d, want still 1", got)
	}
	if got := len(f.writes[outerVersionDestination]); got != 1 {
		t.Fatalf("version writes after retry = %d, want 1", got)
	}
}

func TestPublishOuterSingletonFailureRetryDoesNotDuplicateVersion(t *testing.T) {
	c, f := newVersionModeChecker(true)
	f.failS = true
	h := common.HexToHash("0x03")
	b := &types.OuterBlockChangeNotification{Hash: h}
	var observed []string
	observe := func(d string, _ time.Time) { observed = append(observed, d) }
	if c.publishOuter(b, observe) {
		t.Fatal("publishOuter() = true while singleton write failed; both destinations must succeed")
	}
	if len(f.writes[outerVersionDestination]) != 1 {
		t.Fatalf("version writes = %+v, want 1", f.writes)
	}
	if !c.isOuterSingletonAlign {
		t.Fatal("a failed singleton write must not flip alignment state")
	}
	if len(observed) != 1 || observed[0] != outerVersionDestination {
		t.Fatalf("observed = %v, want only version after first attempt", observed)
	}

	f.failS = false
	if !c.publishOuter(b, observe) {
		t.Fatal("publishOuter() = false on retry")
	}
	if got := len(f.writes[outerVersionDestination]); got != 1 {
		t.Fatalf("version writes after retry = %d, want still 1 (no duplicate)", got)
	}
	if got := len(f.writes[outerSingletonDestination]); got != 1 {
		t.Fatalf("singleton writes after retry = %d, want 1", got)
	}
	if len(observed) != 2 || observed[1] != outerSingletonDestination {
		t.Fatalf("observed = %v, want singleton observed exactly once on retry", observed)
	}
}

func TestPublishOuterBatchRetryOnlyResendsFailedNotice(t *testing.T) {
	// 一条 reorg 带三个 drop：第三个的 version 写失败（singleton 成功），整条重试后
	// 前两个不重发、第三个只补 version
	c, f := newVersionModeChecker(true)
	hashes := []common.Hash{common.HexToHash("0xa1"), common.HexToHash("0xa2"), common.HexToHash("0xa3")}
	drops := []types.BlockContext{{BlockNumber: 1, Hash: hashes[0]}, {BlockNumber: 2, Hash: hashes[1]}, {BlockNumber: 3, Hash: hashes[2]}}
	notice := &types.BlockChangeNotification{DropBlocks: drops, NewBlocks: []types.BlockContext{{BlockNumber: 1, Hash: common.HexToHash("0xb1")}}}
	c.beginNotice(notice)

	failThird := true
	orig := c.writeOuter
	c.writeOuter = func(w *kafka.Writer, b *types.OuterBlockChangeNotification) error {
		if w == f.version && b.Hash == hashes[2] && failThird {
			return errors.New("version broker hiccup")
		}
		return orig(w, b)
	}
	if c.WriteDropBlockNotice(drops) {
		t.Fatal("WriteDropBlockNotice() = true, want false on version failure")
	}
	if got := len(f.writes[outerSingletonDestination]); got != 3 {
		t.Fatalf("singleton writes after first attempt = %d, want 3", got)
	}
	if got := len(f.writes[outerVersionDestination]); got != 2 {
		t.Fatalf("version writes after first attempt = %d, want 2", got)
	}

	failThird = false
	c.beginNotice(notice) // 重试同一条通知，不应清空已投递记录
	if !c.WriteDropBlockNotice(drops) {
		t.Fatal("WriteDropBlockNotice() = false on retry")
	}
	if got := len(f.writes[outerSingletonDestination]); got != 3 {
		t.Fatalf("singleton writes after retry = %d, want still 3", got)
	}
	if got := len(f.writes[outerVersionDestination]); got != 3 {
		t.Fatalf("version writes after retry = %d, want 3", got)
	}
	if f.writes[outerVersionDestination][2] != hashes[2] {
		t.Fatalf("retry resent %s, want only the failed notice %s", f.writes[outerVersionDestination][2], hashes[2])
	}
}

func TestBeginNoticeResetsDeliveredForNewNotice(t *testing.T) {
	c := &Checker{}
	first := &types.BlockChangeNotification{NewBlocks: []types.BlockContext{{Hash: common.HexToHash("0x01")}}}
	c.beginNotice(first)
	key := outerNoticeKey{hash: common.HexToHash("0x01")}
	c.markDelivered(outerVersionDestination, key)
	c.beginNotice(first)
	if !c.isDelivered(outerVersionDestination, key) {
		t.Fatal("re-processing the same notice must keep delivered records")
	}
	second := &types.BlockChangeNotification{NewBlocks: []types.BlockContext{{Hash: common.HexToHash("0x02")}}}
	c.beginNotice(second)
	if c.isDelivered(outerVersionDestination, key) {
		t.Fatal("a new notice must start with empty delivered records")
	}
}

func TestPublishOuterDistinguishesDropAndNewForSameHash(t *testing.T) {
	c, f := newVersionModeChecker(true)
	h := common.HexToHash("0x04")
	// reorg 来回：同一个 hash 先作为 new，再作为 drop，两条都要投递到 singleton
	if !c.publishOuter(&types.OuterBlockChangeNotification{Hash: h, IsFork: false}, func(string, time.Time) {}) {
		t.Fatal("first publish failed")
	}
	if !c.publishOuter(&types.OuterBlockChangeNotification{Hash: h, IsFork: true}, func(string, time.Time) {}) {
		t.Fatal("second publish failed")
	}
	if got := len(f.writes[outerSingletonDestination]); got != 2 {
		t.Fatalf("singleton writes = %d, want 2 (drop and new are different notices)", got)
	}
}

func TestPublishOuterLegacyModeWritesSingletonOnly(t *testing.T) {
	f := &fakeOuter{version: &kafka.Writer{}, singleton: &kafka.Writer{}, writes: map[string][]common.Hash{}}
	c := &Checker{
		config:                       &config.Config{ChainID: 1},
		outerSingletonNewBlockWriter: f.singleton,
	}
	c.writeOuter = func(w *kafka.Writer, b *types.OuterBlockChangeNotification) error {
		if w != f.singleton {
			t.Fatalf("legacy mode wrote to unexpected writer")
		}
		if f.failS {
			return errors.New("down")
		}
		f.writes[outerSingletonDestination] = append(f.writes[outerSingletonDestination], b.Hash)
		return nil
	}
	if !c.publishOuter(&types.OuterBlockChangeNotification{Hash: common.HexToHash("0x05")}, func(string, time.Time) {}) {
		t.Fatal("publishOuter() = false, want true")
	}
	f.failS = true
	if c.publishOuter(&types.OuterBlockChangeNotification{Hash: common.HexToHash("0x06")}, func(string, time.Time) {}) {
		t.Fatal("legacy mode singleton failure must fail the publish")
	}
}

func TestWriteNewBlockNoticeAdvancesLatestOnlyWhenAllDestinationsSucceed(t *testing.T) {
	c, f := newVersionModeChecker(true)
	f.failS = true
	h := common.HexToHash("0x07")
	blocks := []types.BlockContext{{BlockNumber: 10, Hash: h, Timestamp: 123}}
	if c.WriteNewBlockNotice(blocks, nil) {
		t.Fatal("WriteNewBlockNotice() = true while singleton failed")
	}
	if c.latestOuterBlockChangeNotification != nil {
		t.Fatalf("latest advanced to %+v before singleton succeeded", c.latestOuterBlockChangeNotification)
	}
	f.failS = false
	if !c.WriteNewBlockNotice(blocks, nil) {
		t.Fatal("WriteNewBlockNotice() = false on retry")
	}
	if c.latestOuterBlockChangeNotification == nil || c.latestOuterBlockChangeNotification.Hash != h {
		t.Fatalf("latest = %+v, want advanced to %s", c.latestOuterBlockChangeNotification, h)
	}
	if c.latestOuterBlockChangeNotification.Timestamp != 123 {
		t.Fatalf("latest timestamp = %d, want block timestamp 123", c.latestOuterBlockChangeNotification.Timestamp)
	}
	if got := len(f.writes[outerVersionDestination]); got != 1 {
		t.Fatalf("version writes = %d, want 1 (no duplicate across retry)", got)
	}
}
