package nodes

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	// NodeMap is a global map of nodes
	NodeMap = NewRWMap()
)

type RWMap struct {
	// ip -> node meta
	m           map[string]Node
	lock        sync.RWMutex
	watchCtx    context.Context
	watchCancel context.CancelFunc
	etcdClient  *clientv3.Client
	chainID     int64
	version     string
}

func NewRWMap() *RWMap {
	return &RWMap{
		m: make(map[string]Node),
	}
}

// getNodesPrefix 返回 etcd 中 nodes 的前缀路径
// 版本模式：{chainID}/{version}/nodes/
// 非版本模式：{chainID}/nodes/
func (m *RWMap) getNodesPrefix() string {
	if m.version != "" {
		return fmt.Sprintf("%d/%s/nodes/", m.chainID, m.version)
	}
	return fmt.Sprintf("%d/nodes/", m.chainID)
}

func InitFromEtcd(chainID int64, version string, cli *clientv3.Client) error {
	// Store etcd client and chainID for reconnection
	NodeMap.etcdClient = cli
	NodeMap.chainID = chainID
	NodeMap.version = version

	// Create a cancellable context for the watch
	ctx, cancel := context.WithCancel(context.Background())
	NodeMap.watchCtx = ctx
	NodeMap.watchCancel = cancel

	// Load initial data using reloadFromEtcd for unified logic
	revision, err := NodeMap.reloadFromEtcd()
	if err != nil {
		return fmt.Errorf("failed to load initial data from etcd: %w", err)
	}

	log.Printf("Initial data loaded from etcd at revision: %d\n", revision)

	// Start watching with automatic reconnection
	go NodeMap.watchEtcd(revision)

	return nil
}

func (m *RWMap) GetByIP(ip string) Node {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.m[ip]
}

func (m *RWMap) SetByIP(ip string, node Node) {
	log.Printf("add node: %s\n", ip)
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

// Clear removes all nodes from the map
func (m *RWMap) Clear() {
	log.Printf("Clearing all nodes from map\n")
	m.lock.Lock()
	defer m.lock.Unlock()
	m.m = make(map[string]Node)
}

// reloadFromEtcd clears the map and reloads all data from etcd
func (m *RWMap) reloadFromEtcd() (int64, error) {
	prefix := m.getNodesPrefix()
	// Get all current data from etcd
	resp, err := m.etcdClient.Get(context.TODO(), prefix, clientv3.WithPrefix())
	if err != nil {
		return 0, fmt.Errorf("failed to get data from etcd: %w", err)
	}

	log.Printf("Loading %d nodes from etcd after reconnection\n", len(resp.Kvs))

	// Clear existing data to ensure consistency
	m.Clear()

	// Load all nodes
	for _, kv := range resp.Kvs {
		if len(kv.Value) == 0 {
			log.Printf("reloadFromEtcd: empty value for key %s\n", string(kv.Key))
			continue
		}

		fmt.Printf("key: %s, value: %s\n", string(kv.Key), string(kv.Value))
		var node Node
		err := json.Unmarshal(kv.Value, &node)
		if err != nil {
			log.Printf("reloadFromEtcd: failed to unmarshal value for key %s: %v\n", string(kv.Key), err)
			continue
		}

		node.Lease = kv.Lease
		address := string(kv.Key)
		address = strings.TrimPrefix(address, prefix)
		m.SetByIP(address, node)
	}

	log.Printf("Successfully loaded %d nodes from etcd\n", len(m.GetAll()))
	return resp.Header.Revision, nil
}

func (m *RWMap) GetAll() []Node {
	m.lock.RLock()
	defer m.lock.RUnlock()
	result := make([]Node, 0)
	for _, node := range m.m {
		result = append(result, node)
	}
	log.Printf("get all nodes: %+v\n", result)
	return result
}

func (m *RWMap) CheckAll(kafkaLatestBlockNumber uint64, timeout time.Duration) []NodeWithHeight {
	nodes := m.GetAll()
	result := make([]NodeWithHeight, 0)
	lock := sync.Mutex{}

	var wg sync.WaitGroup

	for _, node := range nodes {
		wg.Add(1)
		go func(node Node) {
			defer wg.Done()
			state := node.Check(kafkaLatestBlockNumber, timeout)
			lock.Lock()
			result = append(result, state)
			lock.Unlock()
		}(node)
	}
	wg.Wait()

	return result
}

// watchEtcd watches etcd for changes with automatic reconnection on failure
func (m *RWMap) watchEtcd(revision int64) {
	prefix := m.getNodesPrefix()
	for {
		select {
		case <-m.watchCtx.Done():
			log.Printf("Etcd watch context cancelled, stopping watch\n")
			return
		default:
		}

		log.Printf("Starting etcd watch for prefix: %s from revision: %d\n", prefix, revision)

		// Create watch channel with current revision
		watchOpts := []clientv3.OpOption{
			clientv3.WithPrefix(),
			clientv3.WithRev(revision + 1), // Start from next revision
		}
		rch := m.etcdClient.Watch(m.watchCtx, prefix, watchOpts...)

		newRevision, err := m.processWatchEvents(rch, prefix)

		if err != nil {
			// Check if it's a StopWatch case (context cancelled)
			select {
			case <-m.watchCtx.Done():
				log.Printf("Watch context cancelled, stopping watch loop\n")
				return
			default:
				// Normal error, attempt to reconnect
				log.Printf("Etcd watch error: %v. Attempting to reconnect...\n", err)
				time.Sleep(5 * time.Second)

				// 重新从 etcd 加载数据并获取最新的 revision
				reloadRevision, reloadErr := m.reloadFromEtcd()
				if reloadErr != nil {
					log.Printf("Failed to reload from etcd: %v. Will retry with old revision.\n", reloadErr)
					// 如果重新加载失败，继续使用当前 revision
				} else {
					// 使用重新加载后的 revision
					revision = reloadRevision
					log.Printf("Reloaded from etcd, new revision: %d\n", revision)
				}
			}
		} else {
			// 正常情况下，更新 revision 以便下次重连使用
			if newRevision > revision {
				revision = newRevision
			}
		}
	}
}

// processWatchEvents processes events from the watch channel
// Returns the latest revision processed and any error encountered
func (m *RWMap) processWatchEvents(rch clientv3.WatchChan, prefix string) (int64, error) {
	var lastRevision int64
	for wresp := range rch {
		// Check for watch errors
		if wresp.Err() != nil {
			return lastRevision, fmt.Errorf("watch response error: %w", wresp.Err())
		}

		// Update last revision from response header
		if wresp.Header.Revision > lastRevision {
			lastRevision = wresp.Header.Revision
		}

		for _, ev := range wresp.Events {
			// Update last revision from event
			if ev.Kv.ModRevision > lastRevision {
				lastRevision = ev.Kv.ModRevision
			}

			switch ev.Type {
			case clientv3.EventTypePut:
				var node Node
				fmt.Printf("put key: %s, value: %s\n", string(ev.Kv.Key), string(ev.Kv.Value))

				if len(ev.Kv.Value) == 0 {
					log.Printf("processWatchEvents: empty value for key %s\n", string(ev.Kv.Key))
					continue
				}

				err := json.Unmarshal(ev.Kv.Value, &node)
				if err != nil {
					log.Printf("processWatchEvents: failed to unmarshal value for key %s: %v\n", string(ev.Kv.Key), err)
					continue
				}

				node.Lease = ev.Kv.Lease
				address := string(ev.Kv.Key)
				address = strings.TrimPrefix(address, prefix)
				m.SetByIP(address, node)

			case clientv3.EventTypeDelete:
				fmt.Printf("del key: %s\n", string(ev.Kv.Key))
				address := string(ev.Kv.Key)
				address = strings.TrimPrefix(address, prefix)
				m.DeleteByIP(address)
			}
		}
	}

	// Channel closed, likely due to network issue or etcd failure
	return lastRevision, fmt.Errorf("watch channel closed")
}

// StopWatch gracefully stops the etcd watch
func (m *RWMap) StopWatch() {
	if m.watchCancel != nil {
		log.Printf("Stopping etcd watch...\n")
		m.watchCancel()
	}
}
