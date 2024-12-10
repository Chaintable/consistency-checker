package nodes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Node struct {
	StateType uint64 `json:"stateType"` // 1 latest, 2 delay, 3 offline
	Address   string `json:"address"`   //
	Port      int    `json:"port"`
	NodeType  uint64 `json:"nodeType"` // 1 state, 2 archive
	Lease     int64  `json:"-"`        // 0: no lease, >0: lease time
}

type NodeWithHeight struct {
	Node
	LatestBlockNumber uint64
	ShouldWrite       bool
}

type JsonRpcReq struct {
	ID      int           `json:"id"`
	JsonRpc string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

type JsonRpcRsp struct {
	ID      int         `json:"id"`
	JsonRpc string      `json:"jsonrpc"`
	Result  interface{} `json:"result,omitempty"`
	Error   *ErrorRsp   `json:"error,omitempty"`
}

type ErrorRsp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (node *Node) EthBlockNumber(timeout time.Duration) (uint64, error) {
	reqBody := JsonRpcReq{
		ID:      1,
		JsonRpc: "2.0",
		Method:  "eth_blockNumber",
		Params:  []interface{}{},
	}

	reqBodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return 0, err
	}

	url := fmt.Sprintf("%s:%d", node.Address, node.Port)
	if !strings.HasPrefix(url, "http://") {
		url = "http://" + url
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(reqBodyBytes))
	if err != nil {
		return 0, err
	}
	req.Header.Add("accept", "application/json")
	req.Header.Add("content-type", "application/json")

	ctx, cancel := context.WithTimeout(req.Context(), timeout)
	defer cancel()

	req = req.WithContext(ctx)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()

	// Parse the HTTP response
	var rsp JsonRpcRsp
	err = json.NewDecoder(res.Body).Decode(&rsp)
	if err != nil {
		return 0, err
	}
	blockNumStr, ok := rsp.Result.(string)
	if !ok {
		log.Printf("eth_blockNumber returns invalid block number: %v\n", rsp.Result)
		return 0, fmt.Errorf("eth_blockNumber returns invalid block number: %v", rsp.Result)
	}
	blockNum, err := strconv.ParseInt(blockNumStr, 0, 64)
	if err != nil {
		log.Printf("eth_blockNumber returns invalid block number: %v\n", blockNumStr)
		return 0, fmt.Errorf("eth_blockNumber returns invalid block number: %v", blockNumStr)
	}
	return uint64(blockNum), nil
}

func (node *Node) Check(kafkaLatestBlockNumber uint64) NodeWithHeight {
	latestBlockNumber, err := node.EthBlockNumber(10 * time.Millisecond)
	nodeWithHeight := NodeWithHeight{Node: *node, LatestBlockNumber: latestBlockNumber}
	if err != nil {
		log.Printf("node %s:%d check failed: %v\n", node.Address, node.Port, err)
		nodeWithHeight.StateType = 3
	} else if latestBlockNumber >= kafkaLatestBlockNumber {
		nodeWithHeight.StateType = 1
	} else {
		nodeWithHeight.StateType = 2
	}
	if node.StateType != nodeWithHeight.StateType {
		nodeWithHeight.ShouldWrite = true
	}
	return nodeWithHeight
}
