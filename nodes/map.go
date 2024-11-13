package nodes

import (
	"sync"

	"github.com/Chaintable/pipeline/types"
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

func (m *RWMap) GetByIP(ip string) Node {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.m[ip]
}

func (m *RWMap) SetByIP(ip string, node Node) {
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

func (m *RWMap) CheckAll(knownedlatestBlockNumber uint64) []types.ReplicaState {
	nodes := m.GetAll()
	result := make([]types.ReplicaState, len(nodes))
	lock := sync.Mutex{}

	var wg sync.WaitGroup

	for _, node := range nodes {
		wg.Add(1)
		go func(node Node) {
			defer wg.Done()
			state := node.Check(knownedlatestBlockNumber)
			lock.Lock()
			result = append(result, state)
			lock.Unlock()
		}(node)
	}

	return result
}
