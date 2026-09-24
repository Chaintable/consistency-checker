package db

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/common"
)

// ForkScanAnchor is a published block, not an RPC head. Baseline records an
// intentional historical skip; it must never be described as a completed scan.
type ForkScanAnchor struct {
	Height uint64      `json:"height"`
	Hash   common.Hash `json:"hash"`
}

type ForkScanPosition struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

// ForkScanState is immutable once shared by Checker. All mutations use a copy
// and install it only after a synchronous Pebble commit succeeds.
type ForkScanState struct {
	Format     int               `json:"format"`
	Baseline   ForkScanAnchor    `json:"baseline"`
	NextHeight uint64            `json:"next_scan_height"`
	Generation uint64            `json:"generation"`
	Published  ForkScanAnchor    `json:"published"`
	Position   *ForkScanPosition `json:"inner_position,omitempty"`
	// Canonical indexes can precede publication. Do not scan until the notice
	// has been fully published (including replay after a crash).
	Pending *ForkScanAnchor `json:"pending"`
}

func forkScanKey(chainID int64, version string) []byte {
	return []byte(fmt.Sprintf("meta/fork-recheck/v1/%d/%s", chainID, hex.EncodeToString([]byte(version))))
}

func (s *ForkScanState) validate() error {
	if s.Format != 1 || s.Generation == 0 || s.Baseline.Hash == (common.Hash{}) || s.Published.Hash == (common.Hash{}) || s.Baseline.Height == math.MaxUint64 {
		return fmt.Errorf("invalid fork scan state (format, generation or anchor)")
	}
	if s.Published.Height != math.MaxUint64 && s.NextHeight > s.Published.Height+1 {
		return fmt.Errorf("fork scan cursor exceeds published head")
	}
	if s.Pending != nil && s.Pending.Hash == (common.Hash{}) {
		return fmt.Errorf("invalid pending fork scan anchor")
	}
	if s.Position != nil && (s.Position.Topic == "" || s.Position.Partition < 0 || s.Position.Offset < 0) {
		return fmt.Errorf("invalid fork scan message position")
	}
	return nil
}

func (cdb *ConsistencyDB) LoadForkScan(chainID int64, version string) (*ForkScanState, error) {
	data, closer, err := cdb.db.Get(forkScanKey(chainID, version))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	// Reject incomplete records as corruption, including an omitted next height
	// that JSON would otherwise silently decode as zero.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, fmt.Errorf("decode fork scan state: %w", err)
	}
	for _, key := range []string{"format", "baseline", "next_scan_height", "generation", "published", "pending"} {
		value, ok := fields[key]
		if !ok || (key != "pending" && string(value) == "null") {
			return nil, fmt.Errorf("fork scan state missing %s", key)
		}
	}
	var state ForkScanState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("decode fork scan state: %w", err)
	}
	if err := state.validate(); err != nil {
		return nil, err
	}
	return &state, nil
}

func setForkScan(batch *pebble.Batch, chainID int64, version string, state *ForkScanState) error {
	if err := state.validate(); err != nil {
		return err
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return batch.Set(forkScanKey(chainID, version), data, nil)
}

// SaveForkScan must be serialized with canonical writes by Checker's mutex.
func (cdb *ConsistencyDB) SaveForkScan(chainID int64, version string, state *ForkScanState) error {
	batch := cdb.db.NewBatch()
	defer batch.Close()
	if err := setForkScan(batch, chainID, version, state); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}
