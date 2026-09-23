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
docker build -t consistency-checker .

docker run -v /path/to/config:/config consistency-checker -config /config/config.yml
```

## Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `listen` | `:8663` | HTTP server listen address |
| `chain_id` | - | Blockchain network ID (required) |
| `version` | - | Version identifier (enables version mode when set with `outer_version_new_block_topic`) |
| `ready_ratio` | `0.8` | Fraction of replica nodes that must be synced |
| `check_num` | - | Deprecated, ignored (replica polling is now bounded by `check_timeout_ms`) |
| `check_interval_ms` | `20` | Replica polling interval (ms) |
| `check_timeout_ms` | `2000` | How long to keep polling for `ready_ratio` of replicas to reach the notified height before failing (ms); each poll round is still bounded by `rpc_node_timeout_ms` |
| `rpc_node_timeout_ms` | `5000` | Timeout per node RPC call (ms) |
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
| `fork_scan_interval_sec` | `60` | Periodic fork-mark scan interval (seconds, <=0 disables) |
| `fork_scan_lookback` | `64` | Positive: latest N heights; `0`: disabled; `-1`: persistent continuous scan; values below `-1` are rejected |

CLI flags `-config` and `-listen` override the config file.

### Continuous fork scan

The default remains a scan of the latest 64 heights every 60 seconds. To cover every height after a recovery baseline, including chains that advance more than 64 blocks between scans, use:

```yaml
fork_scan_interval_sec: 60
fork_scan_lookback: -1
```

Continuous mode observes the checker's fully published head and scans through the previous observation only after a full interval of elapsed time. It does not use block timestamps or run an additional recent-height scan. Slow scans and delayed timer events can extend the delay but cannot shorten it. A single background worker catches up in bounded batches; LIST/GET run outside the processing lock, and each locked PUT has a one-second deadline. A batch yields between heights when its time budget is spent, then immediately continues mature backlog; this is not a failed S3 request. Actual failures retain the next incomplete height for retry after the scan interval.

Progress is stored in the existing `consistency_db_path` Pebble database, under `meta/fork-recheck/v1/<chainID>/<hex-encoded-version>`. Each height advances `next_scan_height` with a synchronous write only after all required S3 repairs and a final canonical/generation check succeed. Reorgs, including shorter chains, atomically update canonical indexes and rewind progress to the earliest affected height; old scan tasks cannot overwrite the rewind. Heights on the replacement branch wait for a fresh observation interval. A crash can repeat work, but cannot record a repair as complete before it succeeds.

Recovery follows these rules:

- A normal restart can resume without another Kafka message when the saved published anchor matches both the startup outer head and the local canonical index, and no canonical update is pending. It starts a fresh observation delay; future inner messages still undergo the existing continuity check. Other recovery states wait for alignment. Partially published canonical updates remain pending until the notification is successfully completed or replayed.
- Window/disabled modes and older binaries do not update the continuous cursor. On re-enabling, a published-tail replay that proves the transition from the saved head and matches the replacement canonical indexes preserves the baseline and backlog, rewinding to the reorg start when necessary. This handles shorter-chain reorgs even at a consecutive offset. Such a proven replay takes precedence over offset-gap reset detection; an index error does not fall back to a reset.
- When no cursor exists (first enablement or a rebuilt DB), the fixed startup outer head `H/hash` becomes the baseline after inner alignment. Scanning starts at `H+1`; `H` and earlier heights are intentionally skipped, not reported as scanned. If the outer topic is empty, the first fully published notification establishes the baseline. The checker does not fetch an RPC head or reconstruct historical indexes.
- With a retained DB, startup recovery recognizes a skipped inner range only when the incoming offset has a gap relative to the saved position in the same topic/partition, the startup outer anchor differs from the saved published anchor, and the incoming notification passes the existing continuity or published-tail replay check. It then uses that startup outer anchor as the new baseline and logs the evidence. This is deliberately limited: missing position evidence, a changed partition/topic, or an offset gap alone cannot identify every external group reset. An unaligned main flow keeps retrying; a missing canonical height stalls the scan. Neither case silently jumps to latest.
- Read errors, corrupt metadata, S3 failures, and individual index holes never create a new baseline. Corrupt/unreadable cursor state fails startup. The checker does not delete local data or reset Kafka offsets automatically.

The guarantee is one delayed check for each covered height. Objects uploaded or overwritten after that height's successful scan (for example, days later) may remain undetected; continuous mode does not repeatedly rescan all history. Known reorgs can revisit heights even below the initial baseline. `fork_scan_interval_sec <= 0` disables all scan modes, and an explicit `fork_scan_lookback: 0` also disables scanning. New binaries accept old configurations; old binaries cannot parse `-1`, so change it back to a positive value when rolling back the image.

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
| `pipeline_replica_ready_wait_seconds` | Histogram | Time spent waiting for enough replicas to reach the notified height |
| `pipeline_replica_ready_timeouts_total` | Counter | Times replicas did not reach the height within `check_timeout_ms` |
| `pipeline_process_publish_seconds` | Histogram | Time from starting to process an inner notification to all outer notices being written |
| `pipeline_block_ingress_to_outer_kafka_seconds` | Histogram | Writer ingress to a successful outer Kafka write (label: `destination`) |
| `pipeline_block_ingress_timing_ignored_total` | Counter | Latency samples dropped because ingress timing was missing or invalid (labels: `destination`, `reason`) |
| `pipeline_fork_scan_rewrites_total` | Counter | Objects rewritten by the fork scan; non-zero means a mark was overwritten or previously failed |
| `pipeline_fork_scan_skipped_total` | Counter | Attempts without a trusted canonical record; continuous mode stalls without advancing |
| `pipeline_fork_scan_errors_total` | Counter | Fork scan errors |
| `pipeline_fork_scan_next_height` | Gauge | Next incomplete height in continuous mode; the recovery baseline is intentionally skipped |
| `pipeline_fork_scan_backlog` | Gauge | Published heights awaiting continuous scan, including the observation delay |
| `pipeline_drop_block_rewrite_failures_total` | Counter | Drop-block fork marks that still failed after retries |

## License

Proprietary.
