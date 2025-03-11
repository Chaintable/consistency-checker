package nodes

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	// NodeMap is a global map of nodes
	NodeMap = NewRWMap()
)

type RWMap struct {
	// ip -> node meta
	m    map[string]Node
	lock sync.RWMutex
}

func NewRWMap() *RWMap {
	return &RWMap{
		m: make(map[string]Node),
	}
}

func InitFromEtcd(chainID int64, cli *clientv3.Client) error {
	prefix := fmt.Sprintf("%d/nodes/", chainID)
	resp, err := cli.Get(context.TODO(), prefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	for _, kv := range resp.Kvs {
		if len(kv.Value) == 0 {
			log.Printf("InitFromEtcd: empty value for key %s\n", string(kv.Key))
			continue
		}
		fmt.Printf("key: %s, value: %s\n", string(kv.Key), string(kv.Value))
		node := Node{}
		err := json.Unmarshal(kv.Value, &node)
		if err != nil {
			log.Printf("InitFromEtcd: failed to unmarshal value for key %s\n", string(kv.Key))
			continue
		}
		node.Lease = kv.Lease
		address := string(kv.Key)
		address = strings.TrimPrefix(address, prefix)
		NodeMap.SetByIP(address, node)
	}

	go func() {
		rch := cli.Watch(context.Background(), prefix, clientv3.WithPrefix(), clientv3.WithRev(resp.Header.Revision))
		for wresp := range rch {
			for _, ev := range wresp.Events {
				switch ev.Type {
				case clientv3.EventTypePut:
					var node Node
					fmt.Printf("put key: %s, value: %s\n", string(ev.Kv.Key), string(ev.Kv.Value))
					err := json.Unmarshal(ev.Kv.Value, &node)
					if err != nil {
						log.Printf("InitFromEtcd: failed to unmarshal value for key %s\n", string(ev.Kv.Key))
						continue
					}
					if len(ev.Kv.Value) == 0 {
						log.Printf("InitFromEtcd: empty value for key %s\n", string(ev.Kv.Key))
						continue
					}
					node.Lease = ev.Kv.Lease
					address := string(ev.Kv.Key)
					address = strings.TrimPrefix(address, prefix)
					NodeMap.SetByIP(address, node)
				case clientv3.EventTypeDelete:
					fmt.Printf("put key: %s, value: %s\n", string(ev.Kv.Key), string(ev.Kv.Value))
					address := string(ev.Kv.Key)
					address = strings.TrimPrefix(address, prefix)
					NodeMap.DeleteByIP(address)
				}
			}
		}
	}()
	return nil
}

func (m *RWMap) GetByIP(ip string) Node {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.m[ip]
}

func (m *RWMap) SetByIP(ip string, node Node) {
	log.Printf("add node: %s\n", node.Address)
	m.lock.Lock()
	defer m.lock.Unlock()
	m.m[ip] = node
}

func (m *RWMap) DeleteByIP(ip string) {
	log.Printf("delete node: %s\n", ip)
	m.lock.Lock()
	defer m.lock.Unlock()
	delete(m.m, ip)
}

func (m *RWMap) GetAll() []Node {
	m.lock.RLock()
	defer m.lock.RUnlock()
	result := make([]Node, 0)
	for _, node := range m.m {
		result = append(result, node)
	}
	log.Printf("get all nodes: %v\n", result)
	return result
}

func (m *RWMap) CheckAll(kafkaLatestBlockNumber uint64) []NodeWithHeight {
	nodes := m.GetAll()
	result := make([]NodeWithHeight, 0)
	lock := sync.Mutex{}

	var wg sync.WaitGroup

	for _, node := range nodes {
		wg.Add(1)
		go func(node Node) {
			defer wg.Done()
			state := node.Check(kafkaLatestBlockNumber)
			lock.Lock()
			result = append(result, state)
			lock.Unlock()
		}(node)
	}
	wg.Wait()

	return result
}
