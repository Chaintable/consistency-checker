package db

import (
	"math/big"
	"testing"

	"github.com/Chaintable/pipeline/types"
	"github.com/ethereum/go-ethereum/common"
)

func newTestDB(t *testing.T) *ConsistencyDB {
	t.Helper()
	cdb, err := NewConsistencyDB(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { cdb.Close() })
	return cdb
}

func blockCtx(num uint64, hash common.Hash) types.BlockContext {
	return types.BlockContext{BlockNumber: num, Hash: hash}
}

func hashOf(b byte) common.Hash {
	return common.Hash{b}
}

func TestWriteBlockInfosCleansStaleMappings(t *testing.T) {
	cdb := newTestDB(t)

	// 旧链：高度1-5
	var oldBlocks []types.BlockContext
	for i := uint64(1); i <= 5; i++ {
		oldBlocks = append(oldBlocks, blockCtx(i, hashOf(byte(i))))
	}
	if err := cdb.WriteBlockInfos(oldBlocks, []int64{1, 2, 3, 4, 5}); err != nil {
		t.Fatalf("write old blocks: %v", err)
	}

	// reorg到更短链：新链头高度3
	newHash := hashOf(0xB3)
	if err := cdb.WriteBlockInfos([]types.BlockContext{blockCtx(3, newHash)}, []int64{33}); err != nil {
		t.Fatalf("write reorg block: %v", err)
	}

	// 高度3的canonical应为新hash
	bi, err := cdb.GetBlockInfoByNum(big.NewInt(3))
	if err != nil {
		t.Fatalf("get block by num 3: %v", err)
	}
	if bi.ID != newHash || bi.IsFork {
		t.Fatalf("height 3 canonical = %s isFork=%v, want %s isFork=false", bi.ID, bi.IsFork, newHash)
	}

	// 高于新链头的高度4、5映射应被清理
	for _, h := range []uint64{4, 5} {
		if _, ok, err := cdb.GetCanonicalHashByNum(h); err != nil || ok {
			t.Fatalf("height %d mapping ok=%v err=%v, want cleaned", h, ok, err)
		}
	}

	// 被drop的块（高度3旧hash、高度4）按hash查询应判定为fork
	for _, h := range []uint64{3, 4} {
		bi, err := cdb.GetBlockInfoByHash(hashOf(byte(h)))
		if err != nil {
			t.Fatalf("get dropped block at height %d: %v", h, err)
		}
		if !bi.IsFork {
			t.Fatalf("dropped block at height %d: IsFork=false, want true", h)
		}
	}
}

func TestWriteBlockInfosNormalGrowth(t *testing.T) {
	cdb := newTestDB(t)

	if err := cdb.WriteBlockInfos([]types.BlockContext{blockCtx(1, hashOf(1))}, []int64{1}); err != nil {
		t.Fatalf("write block 1: %v", err)
	}
	if err := cdb.WriteBlockInfos([]types.BlockContext{blockCtx(2, hashOf(2))}, []int64{2}); err != nil {
		t.Fatalf("write block 2: %v", err)
	}

	for _, h := range []uint64{1, 2} {
		hash, ok, err := cdb.GetCanonicalHashByNum(h)
		if err != nil || !ok || hash != hashOf(byte(h)) {
			t.Fatalf("height %d: hash=%s ok=%v err=%v", h, hash, ok, err)
		}
		bi, err := cdb.GetBlockInfoByHash(hashOf(byte(h)))
		if err != nil || bi.IsFork {
			t.Fatalf("height %d by hash: isFork=%v err=%v, want canonical", h, bi.IsFork, err)
		}
	}
}
