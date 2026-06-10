# Consistency Checker

Go 服务，确保区块链数据在分布式副本节点间的一致性。通过 Kafka 消费区块变更通知，验证副本节点同步状态，检测分叉，并通过 etcd 和 S3 协调状态。

## 架构

```
Inner Kafka (BlockChangeNotification)
  -> 消息校验 & 去重
  -> 轮询副本节点直到 ready_ratio 达标
  -> 从 S3 获取验证哈希
  -> 写入 Pebble DB + Fork 检测
  -> 标记 S3 中同高度的 fork 区块
  -> 发布到 Outer Kafka (drop blocks + new blocks)
  -> 对齐 singleton topic（仅 Leader，版本模式）
```

### 双模式

系统支持**版本模式（Version Mode）**和**传统模式（Legacy Mode）**，由配置项 `version` 和 `outer_version_new_block_topic` 共同决定：

- **版本模式**：同时写入 version topic 和 singleton topic（需通过 etcd 分布式锁进行 Leader 选举）。etcd key 使用 `{chainID}/{version}/` 前缀，S3 路径包含 version 段。
- **传统模式**：仅写入 singleton topic。etcd key 使用 `{chainID}/` 前缀。

### 组件

| 包 | 职责 |
|---|------|
| `check/check.go` | 核心检查器：消息处理、节点轮询、S3 读写、Kafka 发布、etcd 状态同步 |
| `check/etcd_lock.go` | 分布式锁：基于 etcd 租约的 Leader 选举，WatchDog 自动续约 |
| `nodes/map.go` | 全局 NodeMap：通过 etcd watch 实时同步节点列表 |
| `nodes/node.go` | 节点健康检查：JSON-RPC `eth_blockNumber`，状态分类（latest/delayed/offline） |
| `db/consistency.go` | Pebble DB 封装：双索引（`h{hash}` -> BlockInfo, `n{number}` -> hash），RLP 编码 |
| `cmd/checker/server.go` | HTTP 服务：JSON-RPC 2.0 API + Prometheus `/metrics` 端点 |
| `config/config.go` | YAML 配置加载，含默认值 |

## 前置依赖

- Go 1.23+
- Kafka broker（inner 用于消费，outer 用于生产）
- etcd 集群（v3）
- AWS S3 存储桶访问权限
- 副本节点端点（兼容以太坊 JSON-RPC）

## 快速开始

### 构建

```bash
go build -o checker cmd/checker/*.go
```

### 配置

```bash
cp config.yml my-config.yml
# 编辑 my-config.yml 配置你的环境
```

### 运行

```bash
./checker -config my-config.yml -listen :8663
```

### Docker

```bash
docker build -f Dockerfile.debank \
  --build-arg ACCESS_TOKEN=<github-token> \
  -t consistency-checker .

docker run consistency-checker -config /path/to/config.yml
```

## 配置项

| 字段 | 默认值 | 说明 |
|------|--------|------|
| `listen` | `:8663` | HTTP 服务监听地址 |
| `chain_id` | - | 区块链网络 ID（必填） |
| `version` | - | 版本标识（与 `outer_version_new_block_topic` 一起设置时启用版本模式） |
| `ready_ratio` | `0.8` | 副本节点就绪比例阈值 |
| `check_num` | `3` | 节点轮询重试次数 |
| `check_interval_ms` | `20` | 重试间隔（毫秒） |
| `rpc_node_timeout_ms` | `50` | 单节点 RPC 超时（毫秒） |
| `msg_wait_timeout` | `5000` | Kafka 消息拉取超时（毫秒） |
| `consistency_db_path` | - | Pebble DB 数据目录 |
| `outer_s3_bucket` | - | S3 存储桶名称 |
| `outer_s3_region` | - | S3 区域 |
| `inner_brokers` | - | 内部 Kafka broker 地址 |
| `inner_new_block_topic` | - | 内部 Kafka topic |
| `inner_new_block_group_id` | - | 内部 Kafka 消费组 |
| `outer_brokers` | - | 外部 Kafka broker 地址 |
| `outer_new_block_topic` | - | 外部 Kafka singleton topic |
| `outer_version_new_block_topic` | - | 外部 Kafka version topic（启用版本模式） |
| `etcd_endpoints` | - | etcd 集群端点 |
| `etcd_lock_ttl` | `20` | 分布式锁 TTL（秒） |
| `etcd_write_timeout_ms` | `5000` | etcd 写超时（毫秒） |
| `version_check_interval` | `5` | Leader 版本检查间隔（秒） |
| `commit_interval` | - | Kafka commit 间隔（秒） |
| `fork_scan_interval_sec` | `60` | fork 标记巡检间隔（秒，<=0 禁用） |
| `fork_scan_lookback` | `64` | fork 标记巡检回看的高度数 |

CLI 参数 `-config` 和 `-listen` 可覆盖配置文件中的值。

## API

在配置的监听地址上提供 JSON-RPC 2.0 HTTP 接口。

### 方法

**getLatestBlock** - 获取最新已验证区块。

```bash
curl -X POST http://localhost:8663 \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"getLatestBlock","id":1}'
```

**getBlockByHeight** - 按区块高度查询。

```bash
curl -X POST http://localhost:8663 \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"getBlockByHeight","params":["0x100"],"id":1}'
```

**getBlockById** - 按区块哈希查询。

```bash
curl -X POST http://localhost:8663 \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"getBlockById","params":["0xabc..."],"id":1}'
```

**blockIsValid** - 检查区块是否在主链（canonical chain）上。

```bash
curl -X POST http://localhost:8663 \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"blockIsValid","params":["0xabc..."],"id":1}'
```

### 响应格式

```json
{
  "id": 1,
  "jsonrpc": "2.0",
  "result": {
    "id": "0x...",
    "num": 12345,
    "validation_hash": 67890,
    "is_fork": false
  }
}
```

错误响应：

```json
{
  "id": 1,
  "jsonrpc": "2.0",
  "error": {
    "code": -39005,
    "message": "错误描述"
  }
}
```

## 监控

Prometheus 指标通过 `GET /metrics` 暴露：

| 指标 | 类型 | 说明 |
|------|------|------|
| `pipeline_node_info` | Gauge | 节点/角色信息（标签：`chain_id`, `role`） |
| `pipeline_block_num` | Gauge | 最新推送的区块高度 |
| `pipeline_block_time` | Gauge | 最新推送的区块时间戳 |

## 许可证

专有软件。
