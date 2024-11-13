package main

import (
	"math/big"

	"github.com/Chaintable/consistency_checker/config"
	"github.com/Chaintable/consistency_checker/db"
	"github.com/Chaintable/consistency_checker/nodes"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gin-gonic/gin"
)

func handleEthGetBlockInfoByNum(c *gin.Context) {
	req := c.Param("num")

	// Parse request
	reqNum := new(big.Int)
	reqNum, ok := reqNum.SetString(req, 10)
	if !ok {
		c.JSON(400, gin.H{"error": "invalid number"})
		return
	}

	// Get block info by number
	block, err := db.DB.GetBlockInfoByNum(reqNum)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	c.JSON(200, block)
}

func handleEthGetBlockInfoByHash(c *gin.Context) {
	req := c.Param("hash")

	// Parse request
	hash := common.HexToHash(req)

	// Get block info by hash
	block, err := db.DB.GetBlockInfoByHash(hash)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	c.JSON(200, block)
}

type NodeRegisterReq struct {
	Url  string `json:"url"`
	Meta string `json:"meta"`
}

func handleRegisterNode(c *gin.Context) {
	req := NodeRegisterReq{}
	if err := c.BindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	nodes.NodeMap.SetByIP(req.Url, nodes.Node{
		Url:  req.Url,
		Meta: req.Meta,
	})
	c.JSON(200, gin.H{"status": "ok"})
}

type NodeUnRegisterReq struct {
	Url string `json:"url"`
}

func handleUnregisterNode(c *gin.Context) {
	req := NodeUnRegisterReq{}
	if err := c.BindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	nodes.NodeMap.DeleteByIP(req.Url)
	c.JSON(200, gin.H{"status": "ok"})
}

func startHTTPServer(config config.Config) {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(
		gin.Recovery(),
	)

	// retrace handlers
	router.Any("/block_info_by_num/:num", handleEthGetBlockInfoByNum)
	router.Any("/block_info_by_id/:id", handleEthGetBlockInfoByHash)
	router.Any("/register_node", handleRegisterNode)
	router.Any("/unregister_node", handleUnregisterNode)

	go func() {
		router.Run(config.Listen)
	}()
}
