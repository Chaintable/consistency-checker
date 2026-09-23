package check

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Chaintable/consistency-checker/config"
	"github.com/Chaintable/consistency-checker/db"
	"github.com/Chaintable/pipeline/types"
	"github.com/Chaintable/pipeline/util"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ethereum/go-ethereum/common"
	"github.com/segmentio/kafka-go"
)

// HTTP-only in-memory S3. No credentials, DNS, sockets, Kafka or etcd are used.
type memoryScanS3 struct {
	mu       sync.Mutex
	objects  map[string][]byte
	lists    map[string]int
	puts     int
	before   func(*http.Request) error
	afterPut func()
}

func (m *memoryScanS3) Do(r *http.Request) (*http.Response, error) {
	m.mu.Lock()
	before := m.before
	m.mu.Unlock()
	if before != nil {
		if err := before(r); err != nil {
			return nil, err
		}
	}
	key := strings.TrimPrefix(r.URL.Path, "/bucket/")
	var body []byte
	code := 200
	m.mu.Lock()
	switch {
	case r.Method == "GET" && r.URL.Query().Get("list-type") == "2":
		prefix := r.URL.Query().Get("prefix")
		m.lists[prefix]++
		var keys []string
		for k := range m.objects {
			if strings.HasPrefix(k, prefix) {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		type entry struct {
			Key string `xml:"Key"`
		}
		result := struct {
			XMLName   xml.Name `xml:"ListBucketResult"`
			Truncated bool     `xml:"IsTruncated"`
			Contents  []entry  `xml:"Contents"`
		}{}
		for _, k := range keys {
			result.Contents = append(result.Contents, entry{k})
		}
		body, _ = xml.Marshal(result)
	case r.Method == "GET":
		var ok bool
		body, ok = m.objects[key]
		if !ok {
			code = 404
			body = []byte(`<Error><Code>NoSuchKey</Code></Error>`)
		}
	case r.Method == "PUT":
		var err error
		body, err = io.ReadAll(r.Body)
		if err != nil {
			m.mu.Unlock()
			return nil, err
		}
		m.objects[key] = append([]byte(nil), body...)
		m.puts++
		body = nil
	default:
		m.mu.Unlock()
		return nil, fmt.Errorf("unexpected S3 operation %s %s", r.Method, r.URL)
	}
	after := m.afterPut
	m.mu.Unlock()
	if r.Method == "PUT" && after != nil {
		after()
	}
	return &http.Response{StatusCode: code, Status: http.StatusText(code), Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(body)), Request: r}, nil
}

func (m *memoryScanS3) set(t *testing.T, key string, fork bool) {
	t.Helper()
	b, err := util.EncodeToJsonGzip(&types.BlockValidation{ValidationHash: 7, IsFork: fork})
	if err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	m.objects[key] = b
	m.mu.Unlock()
}
func (m *memoryScanS3) isFork(t *testing.T, key string) bool {
	t.Helper()
	m.mu.Lock()
	b := m.objects[key]
	m.mu.Unlock()
	var v types.BlockValidation
	if err := util.DecodeFromGzipJson(b, &v); err != nil {
		t.Fatal(err)
	}
	return v.IsFork
}
func scanHash(h uint64) common.Hash { return common.BigToHash(new(big.Int).SetUint64(h + 1)) }
func scanBlock(h uint64) types.BlockContext {
	return types.BlockContext{BlockNumber: h, Hash: scanHash(h), ParentHash: scanHash(h - 1)}
}
func scanPosition(offset int64) *db.ForkScanPosition {
	return &db.ForkScanPosition{Topic: "inner", Partition: 0, Offset: offset}
}

func newScanChecker(t *testing.T, baseline uint64) (*Checker, *memoryScanS3) {
	t.Helper()
	old := db.DB
	var err error
	db.DB, err = db.NewConsistencyDB(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.DB.Close(); db.DB = old })
	m := &memoryScanS3{objects: map[string][]byte{}, lists: map[string]int{}}
	c := &Checker{
		config:                             &config.Config{ChainID: 56, Version: "test", OuterVersionNewBlockTopic: "outer-test", OuterS3Bucket: "bucket", ForkScanInterval: 60, ForkScanLookback: -1},
		outerS3Reader:                      s3.New(s3.Options{Region: "test", BaseEndpoint: aws.String("https://s3.invalid"), UsePathStyle: true, Credentials: aws.AnonymousCredentials{}, HTTPClient: m, RetryMaxAttempts: 1}),
		quit:                               make(chan struct{}),
		latestOuterBlockChangeNotification: &types.OuterBlockChangeNotification{ChainID: 56, BlockNumber: baseline, Hash: scanHash(baseline)},
		checkReplicas:                      func(h uint64) (*ReplicaStateChangeNotification, error) { return readyState(h), nil },
		writeOuter:                         func(*kafka.Writer, *types.OuterBlockChangeNotification) error { return nil },
	}
	if err := c.loadForkScan(); err != nil {
		t.Fatal(err)
	}
	// The matching published message aligns a rebuilt DB without needing an old index.
	if !c.processNotice(&types.BlockChangeNotification{NewBlocks: []types.BlockContext{scanBlock(baseline)}}, nil, scanPosition(10)) {
		t.Fatal("initial alignment failed")
	}
	return c, m
}

func publishScanBlocks(t *testing.T, c *Checker, m *memoryScanS3, blocks []types.BlockContext, drops []types.BlockContext, offset int64) {
	t.Helper()
	for _, b := range blocks {
		m.set(t, c.blockValidationKey(&b), false)
	}
	c.Lock()
	c.lastWrittenBlockNumber = blocks[len(blocks)-1].BlockNumber
	c.Unlock() // no etcd changes
	if !c.processNotice(&types.BlockChangeNotification{NewBlocks: blocks, DropBlocks: drops}, nil, scanPosition(offset)) {
		t.Fatal("local Process failed")
	}
}

func scanThrough(t *testing.T, c *Checker, upper uint64) {
	t.Helper()
	for {
		more, err := c.scanContinuousBatch(context.Background(), upper, c.forkState.Generation)
		if err != nil {
			t.Fatal(err)
		}
		if !more {
			return
		}
	}
}

func TestContinuousScanCoversFastChainAndLateObjects(t *testing.T) {
	c, m := newScanChecker(t, 100)
	if c.forkState.Baseline.Height != 100 || c.forkState.NextHeight != 101 {
		t.Fatal(c.forkState)
	}
	var blocks []types.BlockContext
	for h := uint64(101); h <= 240; h++ {
		blocks = append(blocks, scanBlock(h))
	}
	publishScanBlocks(t, c, m, blocks, nil, 11)
	// This object missed both immediate LISTs in Process and is outside a 64-block window.
	fork := types.BlockContext{BlockNumber: 101, Hash: common.HexToHash("0xabcdef")}
	key := c.blockValidationKey(&fork)
	m.set(t, key, false)
	at := time.Unix(1_000, 0)
	if _, _, ready := c.observeForkScan(func() time.Time { return at }); ready {
		t.Fatal("no initial delay")
	}
	if _, _, ready := c.observeForkScan(func() time.Time { return at.Add(59 * time.Second) }); ready {
		t.Fatal("mature too early")
	}
	upper, _, ready := c.observeForkScan(func() time.Time { return at.Add(time.Minute) })
	if !ready || upper != 240 {
		t.Fatal("not mature")
	}
	m.mu.Lock()
	m.lists = map[string]int{}
	m.mu.Unlock()
	scanThrough(t, c, upper)
	if c.forkState.NextHeight != 241 || !m.isFork(t, key) {
		t.Fatalf("incomplete scan: %+v", c.forkState)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for h := uint64(101); h <= 240; h++ {
		if m.lists[c.blockValidationPrefix(h)] != 1 {
			t.Fatalf("height %d missed/repeated", h)
		}
	}
	if len(m.lists) != 140 {
		t.Fatalf("scanned outside coverage: %d", len(m.lists))
	}
}

func TestContinuousScanFailureDoesNotSkipAndCrashRepeats(t *testing.T) {
	for _, operation := range []string{"LIST", "GET", "PUT"} {
		t.Run(operation, func(t *testing.T) {
			c, m := newScanChecker(t, 10)
			publishScanBlocks(t, c, m, []types.BlockContext{scanBlock(11), scanBlock(12)}, nil, 11)
			fork := types.BlockContext{BlockNumber: 11, Hash: common.HexToHash("0xff")}
			key := c.blockValidationKey(&fork)
			m.set(t, key, false)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			m.before = func(r *http.Request) error {
				match := operation == "LIST" && r.URL.Query().Get("list-type") == "2" || operation == "GET" && r.Method == "GET" && strings.HasSuffix(r.URL.Path, "0x"+strings.Repeat("0", 62)+"ff") || operation == "PUT" && r.Method == "PUT"
				if match {
					cancel()
					return ctx.Err()
				}
				return nil
			}
			_, err := c.scanContinuousBatch(ctx, 12, c.forkState.Generation)
			if err == nil || c.forkState.NextHeight != 11 {
				t.Fatalf("skipped failed %s: %v %+v", operation, err, c.forkState)
			}
			m.before = nil
			// The remote PUT succeeds, then cancellation simulates interruption before cursor Sync.
			crashCtx, crash := context.WithCancel(context.Background())
			defer crash()
			m.afterPut = crash
			_, err = c.scanContinuousBatch(crashCtx, 11, c.forkState.Generation)
			if err == nil || c.forkState.NextHeight != 11 || !m.isFork(t, key) {
				t.Fatalf("crash ordering: %v %+v", err, c.forkState)
			}
			m.afterPut = nil
			path := db.DB.DBDir
			if err := db.DB.Close(); err != nil {
				t.Fatal(err)
			}
			db.DB, err = db.NewConsistencyDB(path)
			if err != nil {
				t.Fatal(err)
			}
			c.forkState = nil
			c.forkAligned = false
			c.forkSchedule = forkScanSchedule{}
			if err := c.loadForkScan(); err != nil {
				t.Fatal(err)
			}
			if c.forkState.NextHeight != 11 || !c.forkAligned {
				t.Fatal("bad restart")
			}
			if !c.processNotice(&types.BlockChangeNotification{NewBlocks: []types.BlockContext{scanBlock(12)}}, nil, scanPosition(11)) {
				t.Fatal("replay failed")
			}
			scanThrough(t, c, 12)
			if c.forkState.NextHeight != 13 {
				t.Fatal(c.forkState)
			}
		})
	}
}

func TestContinuousScanMissingCanonicalAndPendingPublishWait(t *testing.T) {
	c, m := newScanChecker(t, 10)
	publishScanBlocks(t, c, m, []types.BlockContext{scanBlock(11)}, nil, 11)
	c.forkState.Pending = &db.ForkScanAnchor{Height: 12, Hash: scanHash(12)}
	if _, err := c.scanContinuousBatch(context.Background(), 11, c.forkState.Generation); !errors.Is(err, errScanNoCanonical) {
		t.Fatal(err)
	}
	c.forkState.Pending = nil
	// A hole inside coverage must stop; it must never initialize a new baseline.
	c.forkState.NextHeight = 10
	_, err := c.scanContinuousBatch(context.Background(), 11, c.forkState.Generation)
	if !errors.Is(err, errScanNoCanonical) || c.forkState.NextHeight != 10 || c.forkState.Baseline.Height != 10 {
		t.Fatalf("hole was skipped: %v %+v", err, c.forkState)
	}
}

func TestForkScanObservationUsesElapsedTimeAndGeneration(t *testing.T) {
	var schedule forkScanSchedule
	at := time.Unix(1_000, 0)
	schedule.observe(at, time.Minute, 100, 1)
	schedule.observe(at.Add(5*time.Minute), time.Minute, 500, 1) // slow scan; only old observation matures
	if !schedule.mature || schedule.matureHeight != 100 {
		t.Fatal(schedule)
	}
	schedule.observe(at.Add(5*time.Minute+time.Millisecond), time.Minute, 600, 1) // buffered tick
	if schedule.matureHeight != 100 {
		t.Fatal("buffered tick matured newly observed head")
	}
	schedule.observe(at.Add(6*time.Minute), time.Minute, 700, 1)
	if schedule.matureHeight != 500 {
		t.Fatal(schedule)
	}
	schedule.observe(at.Add(6*time.Minute+time.Second), time.Minute, 300, 2)
	if schedule.mature {
		t.Fatal("reorg reused old maturity")
	}
	schedule.observe(at.Add(7*time.Minute), time.Minute, 301, 2)
	if schedule.mature {
		t.Fatal("reorg did not wait full interval")
	}
	schedule.observe(at.Add(7*time.Minute+time.Second), time.Minute, 302, 2)
	if !schedule.mature || schedule.matureHeight != 300 {
		t.Fatal(schedule)
	}
}

func TestForkScanConcurrentReorgRejectsStalePlan(t *testing.T) {
	for _, withRewrite := range []bool{false, true} {
		t.Run(fmt.Sprint(withRewrite), func(t *testing.T) {
			c, m := newScanChecker(t, 10)
			publishScanBlocks(t, c, m, []types.BlockContext{scanBlock(11), scanBlock(12)}, nil, 11)
			if withRewrite {
				b := types.BlockContext{BlockNumber: 11, Hash: common.HexToHash("0xff")}
				m.set(t, c.blockValidationKey(&b), false)
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			m.before = func(r *http.Request) error {
				if r.Method == "GET" && strings.TrimPrefix(r.URL.Path, "/bucket/") == c.blockValidationKey(&[]types.BlockContext{scanBlock(11)}[0]) {
					once.Do(func() { close(entered); <-release })
				}
				return nil
			}
			generation := c.forkState.Generation
			result := make(chan error, 1)
			go func() { _, err := c.scanContinuousBatch(context.Background(), 11, generation); result <- err }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("scan didn't enter GET")
			}
			// Canonical update can take the lock while the scan is blocked in S3.
			newBlock := types.BlockContext{BlockNumber: 11, Hash: common.HexToHash("0xab"), ParentHash: scanHash(10)}
			m.set(t, c.blockValidationKey(&newBlock), false)
			c.Lock()
			next, err := db.DB.WriteBlockInfosWithForkScan([]types.BlockContext{newBlock}, []int64{7}, 56, "test", c.forkState)
			if err == nil {
				c.forkState = next
				c.latestOuterBlockChangeNotification = &types.OuterBlockChangeNotification{ChainID: 56, BlockNumber: 11, Hash: newBlock.Hash}
				if !c.finishForkScan(scanPosition(12)) {
					err = errors.New("finish failed")
				}
			}
			losingBlock := scanBlock(11)
			if err == nil && !withRewrite {
				// ABA: restore exactly the original hash before the EMPTY plan
				// completes. Only the generation check can reject this old task.
				losingBlock, newBlock = newBlock, scanBlock(11)
				next, err = db.DB.WriteBlockInfosWithForkScan([]types.BlockContext{newBlock}, []int64{7}, 56, "test", c.forkState)
				if err == nil {
					c.forkState = next
					c.latestOuterBlockChangeNotification = &types.OuterBlockChangeNotification{ChainID: 56, BlockNumber: 11, Hash: newBlock.Hash}
					if !c.finishForkScan(scanPosition(13)) {
						err = errors.New("ABA finish failed")
					}
				}
			}
			c.Unlock()
			close(release)
			if err != nil {
				t.Fatal(err)
			}
			if err := <-result; !errors.Is(err, errScanStale) {
				t.Fatalf("accepted stale plan (rewrite=%v): %v", withRewrite, err)
			}
			if c.forkState.NextHeight != 11 || c.forkState.Generation <= generation {
				t.Fatal(c.forkState)
			}
			m.mu.Lock()
			m.before = nil
			m.mu.Unlock()
			scanThrough(t, c, 11)
			if !m.isFork(t, c.blockValidationKey(&losingBlock)) || m.isFork(t, c.blockValidationKey(&newBlock)) {
				t.Fatal("wrong marks after reorg")
			}
			if _, ok, _ := db.DB.GetCanonicalHashByNum(12); ok {
				t.Fatal("shorter chain left stale mapping")
			}
		})
	}
}

func TestForkScanRecoveryRequiresAlignmentAndOffsetEvidence(t *testing.T) {
	for _, tt := range []struct {
		name                          string
		offset                        int64
		changed, invalidParent, reset bool
	}{
		{"normal restart", 12, false, false, false},
		{"outer ahead but contiguous replay", 12, true, false, false},
		{"offset gap alone", 100, false, false, false},
		{"aligned latest reset", 100, true, false, true},
		{"unaligned reset", 100, true, true, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, m := newScanChecker(t, 10)
			publishScanBlocks(t, c, m, []types.BlockContext{scanBlock(11)}, nil, 11)
			if tt.changed {
				c.latestOuterBlockChangeNotification = &types.OuterBlockChangeNotification{ChainID: 56, BlockNumber: 100, Hash: scanHash(100)}
			}
			c.forkAligned = false
			c.forkSchedule = forkScanSchedule{}
			if err := c.loadForkScan(); err != nil {
				t.Fatal(err)
			}
			anchor := c.latestOuterBlockChangeNotification
			b := scanBlock(anchor.BlockNumber + 1)
			if tt.invalidParent {
				b.ParentHash = common.HexToHash("0xdead")
			}
			m.set(t, c.blockValidationKey(&b), false)
			c.lastWrittenBlockNumber = b.BlockNumber
			ok := c.processNotice(&types.BlockChangeNotification{NewBlocks: []types.BlockContext{b}}, nil, scanPosition(tt.offset))
			if tt.invalidParent {
				if ok || c.forkAligned {
					t.Fatal("continuity bypassed")
				}
			} else if !ok {
				t.Fatal("process failed")
			}
			want := uint64(10)
			if tt.reset {
				want = 100
			}
			if c.forkState.Baseline.Height != want || c.forkState.NextHeight != want+1 {
				t.Fatalf("unexpected recovery %+v", c.forkState)
			}
			if tt.reset {
				scanThrough(t, c, 101)
			}
		})
	}
}

func TestForkScanWindowAndHeightBoundaries(t *testing.T) {
	c, m := newScanChecker(t, 0)
	publishScanBlocks(t, c, m, []types.BlockContext{scanBlock(1), scanBlock(2), scanBlock(3)}, nil, 11)
	for _, lookback := range []int64{0, -1, 1, 2, 64, math.MaxInt64} {
		c.config.ForkScanLookback = lookback
		m.mu.Lock()
		m.lists = map[string]int{}
		m.mu.Unlock()
		c.scanRecentForkBlocks(context.Background())
		want := 0
		if lookback > 0 {
			want = int(min(lookback, 3))
		}
		if len(m.lists) != want {
			t.Fatalf("lookback %d scanned %d, want %d", lookback, len(m.lists), want)
		}
	}
	for _, interval := range []int{0, -1} {
		c.config.ForkScanInterval = interval
		m.lists = map[string]int{}
		c.scanRecentForkBlocks(context.Background())
		if len(m.lists) != 0 {
			t.Fatal("disabled interval made S3 requests")
		}
	}
	// Window mode must not wrap around and scan genesis at MaxUint64.
	c.config.ForkScanInterval = 60
	c.config.ForkScanLookback = 1
	b := types.BlockContext{BlockNumber: math.MaxUint64, Hash: common.HexToHash("0x123")}
	m.set(t, c.blockValidationKey(&b), false)
	if err := db.DB.WriteBlockInfos([]types.BlockContext{b}, []int64{7}); err != nil {
		t.Fatal(err)
	}
	c.latestOuterBlockChangeNotification = &types.OuterBlockChangeNotification{BlockNumber: b.BlockNumber, Hash: b.Hash}
	c.scanRecentForkBlocks(context.Background())
	if err := c.initializeForkScan(db.ForkScanAnchor{Height: math.MaxUint64, Hash: b.Hash}, nil, "boundary"); err == nil {
		t.Fatal("overflow baseline accepted")
	}
	c.forkState.Published = db.ForkScanAnchor{Height: math.MaxUint64, Hash: b.Hash}
	c.forkState.NextHeight = math.MaxUint64
	if _, err := c.scanContinuousBatch(context.Background(), math.MaxUint64, c.forkState.Generation); err == nil || c.forkState.NextHeight != math.MaxUint64 {
		t.Fatalf("terminal height wrapped: %v", err)
	}
}

func TestForkScanWorkerCancelsBlockedS3(t *testing.T) {
	c, m := newScanChecker(t, 10)
	publishScanBlocks(t, c, m, []types.BlockContext{scanBlock(11)}, nil, 11)
	c.forkSchedule = forkScanSchedule{generation: c.forkState.Generation, observedAt: time.Now().Add(-time.Minute), observedHeight: 11}
	entered := make(chan struct{})
	var once sync.Once
	m.before = func(r *http.Request) error {
		once.Do(func() { close(entered) })
		<-r.Context().Done()
		return r.Context().Err()
	}
	done := make(chan struct{})
	go func() { c.runForkScan(); close(done) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("worker didn't start")
	}
	close(c.quit)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker failed to cancel")
	}
	if c.forkState.NextHeight != 11 {
		t.Fatal("cancel advanced cursor")
	}
}

func TestForkScanPutTimeoutReleasesProcessingLock(t *testing.T) {
	c, m := newScanChecker(t, 10)
	publishScanBlocks(t, c, m, []types.BlockContext{scanBlock(11)}, nil, 11)
	b := types.BlockContext{BlockNumber: 11, Hash: common.HexToHash("0xff")}
	m.set(t, c.blockValidationKey(&b), false)
	entered := make(chan struct{})
	var once sync.Once
	m.before = func(r *http.Request) error {
		if r.Method == "PUT" {
			once.Do(func() { close(entered) })
			<-r.Context().Done()
			return r.Context().Err()
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	generation := c.forkState.Generation
	go func() { _, err := c.scanContinuousBatch(ctx, 11, generation); done <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("no PUT")
	}
	// Use the production PUT deadline, not caller cancellation, to check the bound.
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("PUT unexpectedly succeeded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("PUT held lock without bound")
	}
	if !c.TryLock() {
		t.Fatal("processing lock retained")
	}
	c.Unlock()
	if c.forkState.NextHeight != 11 {
		t.Fatal("timed out PUT advanced progress")
	}
}

func TestContinuousScanReorgRewindsCompletedRange(t *testing.T) {
	c, m := newScanChecker(t, 10)
	publishScanBlocks(t, c, m, []types.BlockContext{scanBlock(11), scanBlock(12), scanBlock(13)}, nil, 11)
	scanThrough(t, c, 13)
	generation := c.forkState.Generation
	b := types.BlockContext{BlockNumber: 12, Hash: common.HexToHash("0xab"), ParentHash: scanHash(11)}
	publishScanBlocks(t, c, m, []types.BlockContext{b}, []types.BlockContext{scanBlock(12), scanBlock(13)}, 12)
	if c.forkState.NextHeight != 12 || c.forkState.Generation <= generation {
		t.Fatalf("didn't rewind completed range: %+v", c.forkState)
	}
	scanThrough(t, c, 12)
	if c.forkState.NextHeight != 13 {
		t.Fatal(c.forkState)
	}
	if _, ok, _ := db.DB.GetCanonicalHashByNum(13); ok {
		t.Fatal("shorter chain mapping retained")
	}
}

func TestForkScanRestartReplayKeepsUnpublishedCanonicalPending(t *testing.T) {
	c, m := newScanChecker(t, 10)
	publishScanBlocks(t, c, m, []types.BlockContext{scanBlock(11)}, nil, 11)
	// Canonical writes for a reorg reach disk, then the process crashes before
	// publishing. Kafka can first replay the older, already-published message.
	b := types.BlockContext{BlockNumber: 11, Hash: common.HexToHash("0xab"), ParentHash: scanHash(10)}
	next, err := db.DB.WriteBlockInfosWithForkScan([]types.BlockContext{b}, []int64{7}, 56, "test", c.forkState)
	if err != nil {
		t.Fatal(err)
	}
	c.forkState = next
	c.forkAligned = false
	if err := c.loadForkScan(); err != nil {
		t.Fatal(err)
	}
	if !c.processNotice(&types.BlockChangeNotification{NewBlocks: []types.BlockContext{scanBlock(11)}}, nil, scanPosition(11)) {
		t.Fatal("older replay couldn't advance")
	}
	if c.forkState.Pending == nil {
		t.Fatal("older replay incorrectly cleared unpublished canonical writes")
	}
	if _, err := c.scanContinuousBatch(context.Background(), 11, c.forkState.Generation); !errors.Is(err, errScanNoCanonical) {
		t.Fatal(err)
	}
	publishScanBlocks(t, c, m, []types.BlockContext{b}, []types.BlockContext{scanBlock(11)}, 12)
	if c.forkState.Pending != nil {
		t.Fatal("published tail still pending")
	}
	scanThrough(t, c, 11)
}
