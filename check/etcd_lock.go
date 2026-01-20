package check

import (
	"context"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type EtcdLock struct {
	leaderKey  string
	versionKey string
	version    string
	client     *clientv3.Client
	ttl        int64
	lease      clientv3.LeaseID
	ctx        context.Context
	cancel     context.CancelFunc
	onLost     func() // 锁丢失时的回调函数
}

func NewLock(leaderKey, versionKey, version string, client *clientv3.Client, ttl int64, onLost func()) *EtcdLock {
	return &EtcdLock{
		leaderKey:  leaderKey,
		versionKey: versionKey,
		version:    version,
		client:     client,
		ttl:        ttl,
		onLost:     onLost,
	}
}

func (l *EtcdLock) Acquire() bool {
	// 创建 context 用于取消操作
	l.ctx, l.cancel = context.WithCancel(context.Background())

	// 创建租约
	leaseResp, err := l.client.Grant(context.Background(), l.ttl)
	if err != nil {
		log.Printf("Failed to create lease: %v", err)
		return false
	}
	log.Printf("Lease created with ID: %d for lock key: %s", leaseResp.ID, l.leaderKey)
	l.lease = leaseResp.ID

	// 尝试获取锁
	for {
		// 使用事务来原子性地检查并设置锁
		// 条件: leaderKey 不存在 AND versionKey 的值等于 version
		txn := l.client.Txn(context.Background())
		txn = txn.If(
			clientv3.Compare(clientv3.CreateRevision(l.leaderKey), "=", 0),
			clientv3.Compare(clientv3.Value(l.versionKey), "=", l.version),
		).
			Then(clientv3.OpPut(l.leaderKey, l.version, clientv3.WithLease(l.lease))).
			Else(clientv3.OpGet(l.leaderKey), clientv3.OpGet(l.versionKey))

		txnResp, err := txn.Commit()
		if err != nil {
			log.Printf("Failed to commit transaction: %v", err)
			l.Release()
			return false
		}

		// 如果 If 条件为真，说明获取锁成功
		if txnResp.Succeeded {
			log.Printf("Lock acquired successfully for key: %s", l.leaderKey)
			// 启动 WatchDog 续约
			go l.WatchDog()
			return true
		}

		// 获取锁失败，检查失败原因
		leaderResp := txnResp.Responses[0].GetResponseRange()
		versionResp := txnResp.Responses[1].GetResponseRange()

		// 检查版本是否匹配
		if len(versionResp.Kvs) > 0 {
			currentVersion := string(versionResp.Kvs[0].Value)
			if currentVersion != l.version {
				log.Printf("Version mismatch: expected %s, got %s. Cannot acquire lock for key: %s",
					l.version, currentVersion, l.leaderKey)
				l.Release()
				return false
			}
		} else {
			log.Printf("Version key %s does not exist. Cannot acquire lock for key: %s",
				l.versionKey, l.leaderKey)
			l.Release()
			return false
		}

		// 版本匹配但锁被占用，等待当前持有者释放锁
		log.Printf("Lock is held by another process, waiting for release: %s", l.leaderKey)

		// 获取当前 key 的 revision，用于 watch
		if len(leaderResp.Kvs) == 0 {
			// key 已被删除，重试获取锁
			continue
		}

		// Watch key 的删除事件
		watchChan := l.client.Watch(l.ctx, l.leaderKey, clientv3.WithRev(leaderResp.Kvs[0].ModRevision))

		// 等待锁被释放
		select {
		case watchResp := <-watchChan:
			if watchResp.Err() != nil {
				log.Printf("Watch error: %v, retrying lock acquisition", watchResp.Err())
				time.Sleep(time.Second)
				// watch 出错时重试获取锁，而不是放弃
				continue
			}

			// 检查是否有删除事件
			for _, event := range watchResp.Events {
				if event.Type == clientv3.EventTypeDelete {
					log.Printf("Lock released, retrying acquisition: %s", l.leaderKey)
					break
				}
			}
			// 重试获取锁
			continue

		case <-l.ctx.Done():
			log.Printf("Lock acquisition cancelled: %s", l.leaderKey)
			l.Release()
			return false
		}
	}
}

func (l *EtcdLock) Release() {
	if l.lease != 0 {
		_, _ = l.client.Revoke(context.Background(), l.lease)
		l.lease = 0
		log.Printf("Lock released for key: %s, lease: %d", l.leaderKey, l.lease)
	}

	if l.cancel != nil {
		l.cancel()
		l.cancel = nil
	}
}

func (l *EtcdLock) WatchDog() {
	ticker := time.NewTicker(time.Duration(l.ttl/4) * time.Second)
	defer ticker.Stop()

	failCount := 0
	maxFail := 3 // 允许连续失败 3 次，容忍单节点故障

	for {
		select {
		case <-ticker.C:
			_, err := l.client.KeepAliveOnce(context.Background(), l.lease)
			if err != nil {
				failCount++
				log.Printf("Failed to refresh lease (attempt %d/%d): %v", failCount, maxFail, err)
				if failCount >= maxFail {
					log.Printf("Max retry reached, lock lost")
					l.lease = 0 // 标记 lease 已失效
					if l.onLost != nil {
						l.onLost()
					}
					return
				}
			} else {
				if failCount > 0 {
					log.Printf("Lease refresh recovered after %d failures", failCount)
				}
				failCount = 0 // 成功后重置计数
			}
		case <-l.ctx.Done():
			return
		}
	}
}
