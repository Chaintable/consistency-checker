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
docker build -t consistency-checker .

docker run -v /path/to/config:/config consistency-checker -config /config/config.yml
```

## 配置项

| 字段 | 默认值 | 说明 |
|------|--------|------|
| `listen` | `:8663` | HTTP 服务监听地址 |
| `chain_id` | - | 区块链网络 ID（必填） |
| `version` | - | 版本标识（与 `outer_version_new_block_topic` 一起设置时启用版本模式） |
| `ready_ratio` | `0.8` | 副本节点就绪比例阈值 |
| `check_num` | - | 已废弃，不再生效（副本轮询改由 `check_timeout_ms` 限时） |
| `check_interval_ms` | `20` | 副本轮询间隔（毫秒） |
| `check_timeout_ms` | `2000` | 轮询 `ready_ratio` 比例副本追上新块高度的时长上限（毫秒），超时才判失败；每一轮 RPC 仍受 `rpc_node_timeout_ms` 约束 |
| `rpc_node_timeout_ms` | `5000` | 单节点 RPC 超时（毫秒） |
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
| `fork_scan_lookback` | `64` | 正值：最近 N 个高度；`0`：禁用；`-1`：持久化连续巡检；小于 `-1` 配置报错 |

CLI 参数 `-config` 和 `-listen` 可覆盖配置文件中的值。

### 连续 fork 巡检

默认行为仍是每 60 秒检查最新 64 个高度。要覆盖恢复基线之后的每个高度，避免快速出块链在两轮间推进超过 64 块而漏扫，可配置：

```yaml
fork_scan_interval_sec: 60
fork_scan_lookback: -1
```

连续模式观察 checker 已完整发布的链头，等待一个完整巡检周期后，以上一次观察的高度为补扫上限。延迟按实际观察时间计算，不使用区块时间戳，也不额外运行最近 N 块扫描。慢扫描、积压的定时事件只会延长等待，不会让新观察的高度提前成熟。单个后台任务分批追赶积压；LIST/GET 在主处理锁外执行，持锁的每次 PUT 最多等待一秒。批次耗尽时间预算时在高度之间让出执行，随后立即继续处理已成熟的积压，不算 S3 失败；实际失败才保留下一个未完成高度，等待巡检周期后重试。

进度存入现有 `consistency_db_path` 的 Pebble 数据库，元数据 key 为 `meta/fork-recheck/v1/<chainID>/<十六进制编码的version>`。每个高度必须先完成所有必要的 S3 修复，再复核 canonical 和游标代次，最后同步落盘推进 `next_scan_height`。reorg（包括切到更短链）会在同一个批次更新 canonical 索引并回退游标，旧扫描任务不能覆盖回退结果；替代分支上的高度重新等待观察周期。崩溃允许重复检查，不会先记录完成再修复对象。

恢复规则：

- 正常重启时，若已保存的发布锚点同时匹配启动 outer 链头和本地 canonical 索引，且没有 pending 更新，就可直接恢复游标并重新等待观察周期，无须等到新的 Kafka 消息；后续 inner 消息仍执行原有连续性校验。其他恢复状态继续等待对齐。canonical 已写入但通知尚未完整发布时，进度保持 pending，直到通知处理或重放成功。
- 窗口/禁用模式和旧二进制不更新连续游标。重新启用时，如果已发布链尾的重放消息能证明从已保存链头到启动链头的变更，且替代分支的 canonical 索引一致，就保留基线和积压，必要时回退至 reorg 起点；即使 offset 连续，更短链 reorg 也能恢复。此类可验证重放优先于 offset 断档的重建判定，索引错误不会退化为重建基线。
- 游标不存在（首次启用或清库重建）时，固定启动读取的 outer 最新 `H/hash`，在 inner 对齐后建立基线，从 `H+1` 开始。`H` 及以前是主动跳过，不声称已实际扫描。outer topic 为空时，以第一条完整发布的通知链头建立基线。不读取更靠前的 RPC 链头，不重建历史索引。
- 保留 DB 时，只在启动恢复阶段同时满足以下证据才识别为跳过了 inner 消息范围：同一 topic/partition 的新 offset 与已保存位置存在断档、启动 outer 锚点不同于已保存的发布锚点、新通知通过原有连续性校验或已发布链尾的重放校验。此时以该启动 outer 锚点重建基线，并记录证据日志。此识别有明确边界：缺少消息位置、topic/partition 改变、或仅有 offset 断档，都不足以识别所有外部 group reset。主流程不对齐时继续重试；覆盖范围内缺 canonical 时巡检停在该高度，均不会静默跳到 latest。
- 读取错误、元数据损坏、S3 失败、个别索引缺失都不会重建基线。游标损坏或读取失败会阻止启动。checker 不自动清库或 reset Kafka offset。

连续游标保证覆盖范围内的每个高度接受一次延迟检查。如果对象在该高度扫描成功后才上传或被覆盖（例如几天后），仍可能无法发现；连续模式不做无限历史复查。已知 reorg 可以回扫到初始基线以下的高度。`fork_scan_interval_sec <= 0` 关闭所有巡检模式，显式配置 `fork_scan_lookback: 0` 也会关闭巡检。新版本兼容旧配置；旧二进制不能解析 `-1`，回滚镜像时须同时改回正值。

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
| `pipeline_replica_ready_wait_seconds` | Histogram | 等待副本追上通知高度的耗时 |
| `pipeline_replica_ready_timeouts_total` | Counter | 副本未在 `check_timeout_ms` 内追上高度的次数 |
| `pipeline_process_publish_seconds` | Histogram | 从开始处理 inner 通知到 outer 通知全部写完的耗时 |
| `pipeline_block_ingress_to_outer_kafka_seconds` | Histogram | 从写节点收到区块到 outer Kafka 写入成功的端到端延迟（标签：`destination`） |
| `pipeline_block_ingress_timing_ignored_total` | Counter | 因 ingress 时间缺失或非法而丢弃的延迟样本（标签：`destination`, `reason`） |
| `pipeline_fork_scan_rewrites_total` | Counter | fork 巡检改写的对象数；非 0 说明标记曾被覆盖或此前失败 |
| `pipeline_fork_scan_skipped_total` | Counter | 缺少可信 canonical 的检查次数；连续模式停留重试，不推进游标 |
| `pipeline_fork_scan_errors_total` | Counter | fork 巡检错误数 |
| `pipeline_fork_scan_next_height` | Gauge | 连续模式下一个未完成高度；恢复基线属于主动跳过 |
| `pipeline_fork_scan_backlog` | Gauge | 已发布但尚未完成连续巡检的高度数，含观察等待期 |
| `pipeline_drop_block_rewrite_failures_total` | Counter | drop block 的 fork 标记重试后仍失败的次数 |

## 许可证

专有软件。
