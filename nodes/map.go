package nodes

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/Chaintable/pipeline/types"
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
	prefix := fmt.Sprintf("replicaState/%d/node/", chainID)
	resp, err := cli.Get(context.TODO(), prefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	for _, kv := range resp.Kvs {
		if len(kv.Value) == 0 {
			log.Printf("InitFromEtcd: empty value for key %s\n", string(kv.Key))
			continue
		}
		node := Node{
			IP: string(kv.Value),
		}
		NodeMap.SetByIP(node.IP, node)
	}

	lastRev := resp.Header.Revision

	go func() {
		rch := cli.Watch(context.Background(), prefix, clientv3.WithPrefix(), clientv3.WithRev(lastRev+1))
		for wresp := range rch {
			for _, ev := range wresp.Events {
				var node Node
				if len(ev.Kv.Value) == 0 {
					log.Printf("InitFromEtcd: empty value for key %s\n", string(ev.Kv.Key))
					continue
				}
				node = Node{
					IP: string(ev.Kv.Value),
				}
				switch ev.Type {
				case clientv3.EventTypePut:
					NodeMap.SetByIP(node.IP, node)
				case clientv3.EventTypeDelete:
					NodeMap.DeleteByIP(node.IP)
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
	log.Printf("InitFromEtcd add node: %s\n", node.IP)
	m.lock.Lock()
	defer m.lock.Unlock()
	m.m[ip] = node
}

func (m *RWMap) DeleteByIP(ip string) {
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
	return result
}

func (m *RWMap) CheckAll(kafkaLatestBlockNumber uint64) []types.ReplicaState {
	nodes := m.GetAll()
	result := make([]types.ReplicaState, 0)
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
