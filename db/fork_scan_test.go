package db

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Chaintable/pipeline/types"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func scanState() *ForkScanState {
	return &ForkScanState{Format: 1, Baseline: ForkScanAnchor{Height: 1, Hash: hashOf(1)}, NextHeight: 2, Generation: 1, Published: ForkScanAnchor{Height: 1, Hash: hashOf(1)}, Position: &ForkScanPosition{Topic: "inner", Partition: 0, Offset: 10}}
}

func TestForkScanDurabilityAndIsolation(t *testing.T) {
	dir := t.TempDir()
	cdb, err := NewConsistencyDB(dir)
	if err != nil {
		t.Fatal(err)
	}
	want := scanState()
	if err := cdb.SaveForkScan(56, "v/a", want); err != nil {
		t.Fatal(err)
	}
	if err := cdb.Close(); err != nil {
		t.Fatal(err)
	}
	cdb, err = NewConsistencyDB(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer cdb.Close()
	got, err := cdb.LoadForkScan(56, "v/a")
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v err=%v", got, err)
	}
	for _, p := range []struct {
		chain   int64
		version string
	}{{1, "v/a"}, {56, "v"}, {56, ""}} {
		got, err := cdb.LoadForkScan(p.chain, p.version)
		if err != nil || got != nil {
			t.Fatalf("scope leak: %+v %v", got, err)
		}
	}
}

func TestForkScanCorruptStateIsNotNewState(t *testing.T) {
	cdb := newTestDB(t)
	for _, data := range []string{"broken", `{}`, `{"format":2}`, `{"format":1,"generation":1}`} {
		if err := cdb.db.Set(forkScanKey(56, "v"), []byte(data), pebble.Sync); err != nil {
			t.Fatal(err)
		}
		if state, err := cdb.LoadForkScan(56, "v"); err == nil || state != nil {
			t.Fatalf("corrupt state accepted: %s %+v", data, state)
		}
	}
	// An actual Pebble read failure (not a missing key) must propagate too.
	if err := cdb.db.Set(append(NumPrefix, byte(3)), []byte("bad hash"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if _, _, err := cdb.GetCanonicalHashByNum(3); err == nil {
		t.Fatal("corrupt canonical accepted")
	}
}

func TestForkScanReorgAtomicAndShorterChain(t *testing.T) {
	cdb := newTestDB(t)
	blocks := []types.BlockContext{blockCtx(1, hashOf(1)), blockCtx(2, hashOf(2)), blockCtx(3, hashOf(3)), blockCtx(4, hashOf(4))}
	if err := cdb.WriteBlockInfos(blocks, []int64{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	state := scanState()
	state.NextHeight = 5
	state.Published = ForkScanAnchor{Height: 4, Hash: hashOf(4)}
	if err := cdb.SaveForkScan(56, "v", state); err != nil {
		t.Fatal(err)
	}
	next, err := cdb.WriteBlockInfosWithForkScan([]types.BlockContext{blockCtx(2, hashOf(22))}, []int64{22}, 56, "v", state)
	if err != nil {
		t.Fatal(err)
	}
	if next.NextHeight != 2 || next.Generation != 2 || next.Pending == nil || state.NextHeight != 5 {
		t.Fatalf("invalid rewind %+v, original %+v", next, state)
	}
	persisted, err := cdb.LoadForkScan(56, "v")
	if err != nil || !reflect.DeepEqual(next, persisted) {
		t.Fatalf("not atomic: %+v %v", persisted, err)
	}
	for _, h := range []uint64{3, 4} {
		if _, ok, err := cdb.GetCanonicalHashByNum(h); err != nil || ok {
			t.Fatalf("height %d retained: %v", h, err)
		}
	}
	// A failed metadata validation must not commit canonical changes either.
	bad := *next
	bad.Format = 99
	_, err = cdb.WriteBlockInfosWithForkScan([]types.BlockContext{blockCtx(2, hashOf(23))}, []int64{23}, 56, "v", &bad)
	if err == nil {
		t.Fatal("expected rejected batch")
	}
	hash, _, _ := cdb.GetCanonicalHashByNum(2)
	if hash != hashOf(22) {
		t.Fatal("partially committed batch")
	}
}

func TestWriteBlockInfosMaxHeightDoesNotWrap(t *testing.T) {
	cdb := newTestDB(t)
	if err := cdb.WriteBlockInfos([]types.BlockContext{blockCtx(0, hashOf(1)), blockCtx(math.MaxUint64, hashOf(2))}, []int64{1, 2}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := cdb.GetCanonicalHashByNum(0); err != nil || !ok {
		t.Fatalf("wrapped to genesis: %v", err)
	}
	if _, err := cdb.GetBlockInfoByHash(hashOf(3)); !errors.Is(err, pebble.ErrNotFound) {
		t.Fatal(err)
	}
}

// A real SST read error must propagate, independently of JSON decode errors.
type forkReadErrorFS struct {
	vfs.FS
	fail atomic.Bool
}

var errForkRead = errors.New("injected fork metadata SST read error")

func (fs *forkReadErrorFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	if fs.fail.Load() && strings.HasSuffix(name, ".sst") {
		return nil, errForkRead
	}
	return fs.FS.Open(name, opts...)
}

func TestForkScanReadErrorIsNotMissingState(t *testing.T) {
	fs := &forkReadErrorFS{FS: vfs.NewMem()}
	opts := &pebble.Options{FS: fs}
	raw, err := pebble.Open("db", opts)
	if err != nil {
		t.Fatal(err)
	}
	cdb := &ConsistencyDB{db: raw}
	if err := cdb.SaveForkScan(56, "v", scanState()); err != nil {
		t.Fatal(err)
	}
	if err := raw.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err = pebble.Open("db", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	cdb.db = raw
	fs.fail.Store(true)
	state, err := cdb.LoadForkScan(56, "v")
	if !errors.Is(err, errForkRead) || state != nil {
		t.Fatalf("read failure treated as new state: %+v %v", state, err)
	}
}
