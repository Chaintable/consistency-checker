package main

import (
	"encoding/json"
	"io"
	"math/big"

	"github.com/Chaintable/consistency-checker/db"
	"github.com/Chaintable/consistency-checker/nodes"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/gin-gonic/gin"
)

type BlockContext struct {
	BlockId *rpc.BlockNumberOrHash `json:"block_id"`
	Type    string                 `json:"type"`
}

func handleGetLatestBlock(c *gin.Context) {
	if db.DB == nil {
		c.JSON(-39005, gin.H{"error": "db not initialized"})
		return
	}

	latestBlock, err := db.DB.GetLatestBlockInfo()
	if err != nil {
		c.JSON(-39005, gin.H{"error": err.Error()})
		return
	}

	c.JSON(200, latestBlock)
}

func handleGetBlockByHeight(c *gin.Context, req *nodes.JsonRpcReq) {
	if len(req.Params) == 0 {
		c.IndentedJSON(-32602, "params not found")
		return
	}
	height := req.Params[0].(string)
	if height == "" {
		c.IndentedJSON(-32602, "params not found")
		return
	}

	num := new(big.Int)
	num, ok := num.SetString(height, 10)
	if !ok {
		c.IndentedJSON(-32602, "params error")
		return
	}

	if db.DB == nil {
		c.JSON(-39005, gin.H{"error": "db not initialized"})
		return
	}

	block, err := db.DB.GetBlockInfoByNum(num)
	if err != nil {
		c.IndentedJSON(-39005, err.Error())
		return
	}
	c.IndentedJSON(200, block)
}

func handleGetBlockById(c *gin.Context, req *nodes.JsonRpcReq) {
	if len(req.Params) == 0 {
		c.IndentedJSON(-32602, "params not found")
		return
	}
	blockCtxRaw, err := json.Marshal(req.Params[0])
	if err != nil {
		c.IndentedJSON(-32602, "params error")
		return
	}
	var blockCtx BlockContext
	if err := json.Unmarshal(blockCtxRaw, &blockCtx); err != nil {
		c.IndentedJSON(-32602, "params error")
		return
	}

	if db.DB == nil {
		c.JSON(-39005, gin.H{"error": "db not initialized"})
		return
	}

	block, err := db.DB.GetBlockInfoByNumOrHash(blockCtx.BlockId)
	if err != nil {
		c.IndentedJSON(-39005, err.Error())
		return
	}
	c.IndentedJSON(200, block)
}

func handleBlockIsValid(c *gin.Context, req *nodes.JsonRpcReq) {
	if len(req.Params) == 0 {
		c.IndentedJSON(-32602, "params not found")
		return
	}
	blockCtxRaw, err := json.Marshal(req.Params[0])
	if err != nil {
		c.IndentedJSON(-32602, "params error")
		return
	}
	var blockCtx BlockContext
	if err := json.Unmarshal(blockCtxRaw, &blockCtx); err != nil {
		c.IndentedJSON(-32602, "params error")
		return
	}

	if db.DB == nil {
		c.JSON(-39005, gin.H{"error": "db not initialized"})
		return
	}

	block0, err := db.DB.GetBlockInfoByNumOrHash(blockCtx.BlockId)
	if err != nil {
		c.IndentedJSON(-39005, err.Error())
		return
	}

	block1, err := db.DB.GetBlockInfoByNum(big.NewInt(0).SetUint64(block0.Height))
	if err != nil {
		c.IndentedJSON(-39005, err.Error())
		return
	}
	c.IndentedJSON(200, block0.ID == block1.ID)
}

func index(c *gin.Context) {
	req := &nodes.JsonRpcReq{}
	bodyBytes, _ := io.ReadAll(c.Request.Body)
	if err := json.Unmarshal(bodyBytes, req); err != nil {
		c.IndentedJSON(-32700, "request body invalid json")
		return
	}
	if req.JsonRpc != "2.0" {
		c.IndentedJSON(-32600, "jsonrpc version not supported")
		return
	}
	if req.Method == "" {
		c.IndentedJSON(-32601, "method not found")
		return
	}
	switch req.Method {
	case "getLatestBlock":
		handleGetLatestBlock(c)
	case "getBlockByHeight":
		handleGetBlockByHeight(c, req)
	case "getBlockById":
		handleGetBlockById(c, req)
	case "blockIsValid":
		handleBlockIsValid(c, req)
	default:
		c.IndentedJSON(-32601, "method not found")
	}
}

func startHTTPServer(listen string) {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(
		gin.Recovery(),
	)

	// retrace handlers
	router.Any("/", index)

	go func() {
		router.Run(listen)
	}()
}
