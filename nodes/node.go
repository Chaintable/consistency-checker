package nodes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"strconv"
	"time"

	"github.com/Chaintable/pipeline/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

type Node struct {
	Meta string `json:"meta"`
}

func NewNode(meta string) *Node {
	return &Node{
		Meta: meta,
	}
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

	req, _ := http.NewRequest("POST", node.Meta, bytes.NewReader(reqBodyBytes))
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

func (node *Node) Check(kafkaLatestBlockNumber uint64) types.ReplicaState {
	latestBlockNumber, err := node.EthBlockNumber(10 * time.Millisecond)
	if err != nil {
		return types.ReplicaState{
			LatestBlockNumber: nil,
			StateType:         3,
			Meta:              node.Meta,
		}
	}
	if latestBlockNumber >= kafkaLatestBlockNumber {
		return types.ReplicaState{
			LatestBlockNumber: (*hexutil.Big)(big.NewInt(int64(latestBlockNumber))),
			StateType:         1,
			Meta:              node.Meta,
		}
	}
	return types.ReplicaState{
		LatestBlockNumber: (*hexutil.Big)(big.NewInt(int64(latestBlockNumber))),
		StateType:         2,
		Meta:              node.Meta,
	}
}
