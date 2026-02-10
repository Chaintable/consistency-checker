# Consistency Checker

A Go service that ensures blockchain data consistency across distributed replica nodes. It consumes block change notifications from Kafka, validates replica node synchronization, detects forks, and coordinates state through etcd and S3.

## Architecture

```
Inner Kafka (BlockChangeNotification)
  -> Message validation & deduplication
  -> Poll replica nodes until ready_ratio threshold met
  -> Fetch validation hashes from S3
  -> Write to Pebble DB with fork detection
  -> Mark same-height fork blocks in S3
  -> Publish to Outer Kafka (drop blocks + new blocks)
  -> Align singleton topic (leader only, version mode)
```

### Dual Mode

The system supports **Version Mode** and **Legacy Mode**, determined by `version` and `outer_version_new_block_topic` config fields:

- **Version Mode**: Writes to both version topic and singleton topic (requires leader election via etcd distributed lock). etcd keys use `{chainID}/{version}/` prefix. S3 paths include the version segment.
- **Legacy Mode**: Writes to singleton topic only. etcd keys use `{chainID}/` prefix.

### Components

| Package | Responsibility |
|---------|---------------|
| `check/check.go` | Core checker: message processing, node polling, S3 I/O, Kafka publishing, etcd state sync |
| `check/etcd_lock.go` | Distributed lock: etcd lease-based leader election with watchdog auto-renewal |
| `nodes/map.go` | Global NodeMap: real-time node list sync via etcd watch |
| `nodes/node.go` | Node health check: JSON-RPC `eth_blockNumber`, state classification (latest/delayed/offline) |
| `db/consistency.go` | Pebble DB wrapper: dual index (`h{hash}` -> BlockInfo, `n{number}` -> hash), RLP encoding |
| `cmd/checker/server.go` | HTTP server: JSON-RPC 2.0 API + Prometheus `/metrics` endpoint |
| `config/config.go` | YAML config loading with defaults |

## Prerequisites

- Go 1.23+
- Kafka brokers (inner for consuming, outer for producing)
- etcd cluster (v3)
- AWS S3 bucket access
- Replica node endpoints (Ethereum JSON-RPC compatible)

## Quick Start

### Build

```bash
go build -o checker cmd/checker/*.go
```

### Configure

```bash
cp config.yml my-config.yml
# Edit my-config.yml with your environment settings
```

### Run

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

## Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `listen` | `:8663` | HTTP server listen address |
| `chain_id` | - | Blockchain network ID (required) |
| `version` | - | Version identifier (enables version mode when set with `outer_version_new_block_topic`) |
| `ready_ratio` | `0.8` | Fraction of replica nodes that must be synced |
| `check_num` | `3` | Number of retry attempts for node polling |
| `check_interval_ms` | `20` | Delay between retries (ms) |
| `rpc_node_timeout_ms` | `50` | Timeout per node RPC call (ms) |
| `msg_wait_timeout` | `5000` | Kafka message fetch timeout (ms) |
| `consistency_db_path` | - | Pebble DB data directory |
| `outer_s3_bucket` | - | S3 bucket for block validation data |
| `outer_s3_region` | - | S3 region |
| `inner_brokers` | - | Inner Kafka broker addresses |
| `inner_new_block_topic` | - | Inner Kafka topic |
| `inner_new_block_group_id` | - | Inner Kafka consumer group |
| `outer_brokers` | - | Outer Kafka broker addresses |
| `outer_new_block_topic` | - | Outer Kafka singleton topic |
| `outer_version_new_block_topic` | - | Outer Kafka version topic (enables version mode) |
| `etcd_endpoints` | - | etcd cluster endpoints |
| `etcd_lock_ttl` | `20` | Distributed lock TTL (seconds) |
| `etcd_write_timeout_ms` | `5000` | etcd write timeout (ms) |
| `version_check_interval` | `5` | Leader version check interval (seconds) |
| `commit_interval` | - | Kafka commit interval (seconds) |

CLI flags `-config` and `-listen` override the config file.

## API

JSON-RPC 2.0 over HTTP at the configured listen address.

### Methods

**getLatestBlock** - Returns the latest verified block.

```bash
curl -X POST http://localhost:8663 \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"getLatestBlock","id":1}'
```

**getBlockByHeight** - Query block by number.

```bash
curl -X POST http://localhost:8663 \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"getBlockByHeight","params":["0x100"],"id":1}'
```

**getBlockById** - Query block by hash.

```bash
curl -X POST http://localhost:8663 \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"getBlockById","params":["0xabc..."],"id":1}'
```

**blockIsValid** - Check if a block is on the canonical chain.

```bash
curl -X POST http://localhost:8663 \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"blockIsValid","params":["0xabc..."],"id":1}'
```

### Response Format

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

Error response:

```json
{
  "id": 1,
  "jsonrpc": "2.0",
  "error": {
    "code": -39005,
    "message": "error description"
  }
}
```

## Monitoring

Prometheus metrics at `GET /metrics`:

| Metric | Type | Description |
|--------|------|-------------|
| `pipeline_node_info` | Gauge | Node/role information (labels: `chain_id`, `role`) |
| `pipeline_block_num` | Gauge | Latest pushed block number |
| `pipeline_block_time` | Gauge | Latest pushed block timestamp |

## License

Proprietary.
