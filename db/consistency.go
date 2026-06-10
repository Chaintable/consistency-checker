package db

import (
	"errors"
	"fmt"
	"math/big"
	"sync"

	"github.com/Chaintable/pipeline/types"
	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"
)

var (
	// Prefix for the block hash -> BlockInfo
	HashPrefix = []byte("h")
	// Prefix for the block number -> blockhash
	NumPrefix = []byte("n")
)

var (
	DB *ConsistencyDB
)

type BlockInfoRlp struct {
	Num            *big.Int
	ValidationHash *big.Int
}

type BlockInfo struct {
	ID             common.Hash `json:"id"`
	Height         uint64      `json:"num"`
	ValidationHash int64       `json:"validation_hash"`
	IsFork         bool        `json:"is_fork"`
}

type ConsistencyDB struct {
	DBDir  string
	db     *pebble.DB
	latest *BlockInfo
	sync.RWMutex
}

func NewConsistencyDB(dbDir string) (*ConsistencyDB, error) {
	db, err := pebble.Open(dbDir, nil)
	if err != nil {
		return nil, err
	}
	return &ConsistencyDB{
		DBDir: dbDir,
		db:    db,
	}, nil
}

func OpenConsistencyDB(dbDir string) error {
	if DB != nil {
		return nil
	}
	db, err := NewConsistencyDB(dbDir)
	if err != nil {
		return err
	}
	DB = db
	return nil
}

func (cdb *ConsistencyDB) Close() error {
	return cdb.db.Close()
}

func (cdb *ConsistencyDB) GetLatestBlockInfo() (*BlockInfo, error) {
	cdb.RLock()
	defer cdb.RUnlock()
	if cdb.latest == nil {
		return nil, nil
	}
	return cdb.latest, nil
}

func (cdb *ConsistencyDB) GetBlockInfoByHash(hash common.Hash) (*BlockInfo, error) {
	var bi BlockInfoRlp
	data, closer, err := cdb.db.Get(append(HashPrefix, hash[:]...))
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	err = rlp.DecodeBytes(data, &bi)
	if err != nil {
		return nil, err
	}

	canonicalHash, err := cdb.getBlockHashByNum(bi.Num)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			// reorg到更短链后该高度的映射已被清理：当前高度没有canonical块，此块必为fork
			return &BlockInfo{
				ID:             hash,
				Height:         bi.Num.Uint64(),
				ValidationHash: bi.ValidationHash.Int64(),
				IsFork:         true,
			}, nil
		}
		return nil, err
	}
	return &BlockInfo{
		ID:             hash,
		Height:         bi.Num.Uint64(),
		ValidationHash: bi.ValidationHash.Int64(),
		IsFork:         canonicalHash != hash,
	}, nil
}

// GetCanonicalHashByNum 返回某高度的canonical hash；db中没有该高度记录时ok为false
func (cdb *ConsistencyDB) GetCanonicalHashByNum(num uint64) (common.Hash, bool, error) {
	hash, err := cdb.getBlockHashByNum(new(big.Int).SetUint64(num))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return common.Hash{}, false, nil
		}
		return common.Hash{}, false, err
	}
	return hash, true, nil
}

func (cdb *ConsistencyDB) GetBlockInfoByNumOrHash(id *rpc.BlockNumberOrHash) (*BlockInfo, error) {
	number, ok := id.Number()
	if ok {
		if number == -2 || number == -1 {
			return cdb.GetLatestBlockInfo()
		}
		return cdb.GetBlockInfoByNum(big.NewInt(int64(number)))
	}

	hash, ok := id.Hash()
	if ok {
		return cdb.GetBlockInfoByHash(hash)
	}

	return nil, fmt.Errorf("GetBlockInfoByNumOrHash params error")
}

func (cdb *ConsistencyDB) GetBlockInfoByNum(num *big.Int) (*BlockInfo, error) {
	hash, err := cdb.getBlockHashByNum(num)
	if err != nil {
		return nil, err
	}
	return cdb.GetBlockInfoByHash(hash)
}

func (cdb *ConsistencyDB) getBlockHashByNum(num *big.Int) (common.Hash, error) {
	data, closer, err := cdb.db.Get(append(NumPrefix, num.Bytes()...))
	if err != nil {
		return common.Hash{}, err
	}
	defer closer.Close()
	return common.BytesToHash(data), nil
}

func (cdb *ConsistencyDB) WriteBlockInfos(newBlocks []types.BlockContext, validationHashes []int64) error {
	batch := cdb.db.NewBatch()
	for i, block := range newBlocks {
		bi := BlockInfoRlp{
			Num:            new(big.Int).SetUint64(block.BlockNumber),
			ValidationHash: big.NewInt(validationHashes[i]),
		}
		data, err := rlp.EncodeToBytes(bi)
		if err != nil {
			return err
		}
		err = batch.Set(append(HashPrefix, block.Hash[:]...), data, nil)
		if err != nil {
			return err
		}
		err = batch.Set(append(NumPrefix, bi.Num.Bytes()...), block.Hash[:], nil)
		if err != nil {
			return err
		}
	}
	// reorg到更短/等长链时，清理高于新链头的残留num->hash映射，
	// 避免这些高度上被drop的块被误判为canonical
	for h := newBlocks[len(newBlocks)-1].BlockNumber + 1; ; h++ {
		key := append(NumPrefix, new(big.Int).SetUint64(h).Bytes()...)
		_, closer, err := cdb.db.Get(key)
		if errors.Is(err, pebble.ErrNotFound) {
			break
		}
		if err != nil {
			return err
		}
		closer.Close()
		if err := batch.Delete(key, nil); err != nil {
			return err
		}
	}
	err := batch.Commit(pebble.Sync)
	if err != nil {
		return err
	}
	cdb.Lock()
	defer cdb.Unlock()
	cdb.latest = &BlockInfo{
		ID:             newBlocks[len(newBlocks)-1].Hash,
		Height:         newBlocks[len(newBlocks)-1].BlockNumber,
		ValidationHash: validationHashes[len(validationHashes)-1],
	}
	return nil
}
