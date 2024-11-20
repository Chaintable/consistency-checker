package db

import (
	"math/big"

	"github.com/Chaintable/pipeline/types"
	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
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
	DBDir string
	db    *pebble.DB
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
		return nil, err
	}
	return &BlockInfo{
		ID:             hash,
		Height:         bi.Num.Uint64(),
		ValidationHash: bi.ValidationHash.Int64(),
		IsFork:         canonicalHash != hash,
	}, nil
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
	return batch.Commit(pebble.Sync)
}
