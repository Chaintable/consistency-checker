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
}

func NewRWMap() *RWMap {
	return &RWMap{
		m: make(map[string]Node),
	}
}

func InitFromEtcd(chainID int64, cli *clientv3.Client) error {
	// Store etcd client and chainID for reconnection
	NodeMap.etcdClient = cli
	NodeMap.chainID = chainID

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
	prefix := fmt.Sprintf("%d/nodes/", m.chainID)
	// Get all current data from etcd
	resp, err := m.etcdClient.Get(context.TODO(), prefix, clientv3.WithPrefix())
	if err != nil {
		return 0, fmt.Errorf("failed to get data from etcd: %w", err)
	}

	log.Printf("Loading %d nodes from etcd after reconnection\n", len(resp.Kvs))

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

// watchEtcd watches etcd for changes with automatic reconnection on failure
func (m *RWMap) watchEtcd(revision int64) {
	prefix := fmt.Sprintf("%d/nodes/", m.chainID)
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

		err := m.processWatchEvents(rch, prefix)

		if err != nil {
			// Check if it's a StopWatch case (context cancelled)
			select {
			case <-m.watchCtx.Done():
				log.Printf("Watch context cancelled, stopping watch loop\n")
				return
			default:
				// Normal error, attempt to reconnect
				time.Sleep(5 * time.Second)
				log.Printf("Etcd watch error: %v. Attempting to reconnect...\n", err)
			}
		}
	}
}

// processWatchEvents processes events from the watch channel
func (m *RWMap) processWatchEvents(rch clientv3.WatchChan, prefix string) error {
	for wresp := range rch {
		for _, ev := range wresp.Events {
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
	return fmt.Errorf("watch channel closed")
}

// StopWatch gracefully stops the etcd watch
func (m *RWMap) StopWatch() {
	if m.watchCancel != nil {
		log.Printf("Stopping etcd watch...\n")
		m.watchCancel()
	}
}
