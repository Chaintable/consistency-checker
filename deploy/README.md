# Deployment Guide

## Directory Structure

```
deploy/
├── compose.yml           # Docker Compose orchestration file
├── config/
│   └── config.yml        # Application config (modify per environment)
└── README.md             # This document
```

## Prerequisites

### Infrastructure

| Service | Description |
|---------|-------------|
| Kafka | Inner broker (consume block notifications) and outer broker (publish verified results); can be the same cluster |
| etcd v3 | 3-node cluster recommended; used for node registry, leader election, and state storage |
| AWS S3 | Read/write access to a bucket for block validation data |
| Replica nodes | At least 1 Ethereum JSON-RPC compatible node (must support `eth_blockNumber`) |

### Host Requirements

- Docker and Docker Compose
- Network connectivity to Kafka / etcd / S3 / replica nodes
- Local disk space for Pebble DB data persistence
- AWS credentials configured (environment variables, IAM Role, or `~/.aws/credentials`)

## Configuration

### 1. Edit the Config File

```bash
cp config/config.yml config/config.yml.bak
vi config/config.yml
```

**Required fields:**

```yaml
chain_id: 1                              # Target chain ID
inner_brokers:                           # Inner Kafka broker addresses
  - "kafka-broker-1:9092"
inner_new_block_topic: "nodex_pipeline_1" # Inner topic name
inner_new_block_group_id: "consistency-group-1"
outer_brokers:                           # Outer Kafka broker addresses
  - "kafka-broker-1:9092"
outer_new_block_topic: "pipeline_1"      # Outer singleton topic
etcd_endpoints:                          # etcd cluster endpoints
  - "etcd-1:2379"
  - "etcd-2:2379"
  - "etcd-3:2379"
outer_s3_bucket: "your-bucket"           # S3 bucket name
outer_s3_region: "ap-northeast-1"        # S3 region
consistency_db_path: "/eth/consistency_db" # DB path inside the container (matches volume mount)
```

**Version mode (optional):**

Set both fields below to enable version mode with leader election and dual-topic writes:

```yaml
version: "v1"                                    # Version identifier
outer_version_new_block_topic: "pipeline_1_v1"   # Version topic
```

**Tunable fields:**

```yaml
listen: "0.0.0.0:8882"      # HTTP listen address (watch for port conflicts in host network mode)
ready_ratio: 0.8             # Node readiness ratio (0.0~1.0); lower value increases tolerance
check_num: 3                 # Node polling retry count
check_interval_ms: 20        # Retry interval (ms)
rpc_node_timeout_ms: 5000    # Per-node RPC timeout (ms); increase for high-latency networks
node_offline_threshold: 3    # Consecutive failed health checks before a node is deleted from etcd
etcd_lock_ttl: 20            # Distributed lock TTL (seconds); version mode only
```

### 2. Modify compose.yml (if needed)

The default compose.yml uses `host` network mode with data mounted at `/data/consistency-eth`:

```yaml
services:
  consistency-checkerx:
    image: 294354037686.dkr.ecr.ap-northeast-1.amazonaws.com/consistency-checkerx:latest
    network_mode: "host"
    container_name: consistency-eth
    volumes:
      - /data/consistency-eth:/eth     # Pebble DB data persistence
      - ./config:/config               # Config file mount
    command: ["-config", "/config/config.yml"]
```

Adjust as needed:

- **Image tag**: Replace `latest` with a specific version (e.g., `amd64-v1.0.0`)
- **Data path**: Change `/data/consistency-eth` to your actual persistent storage directory
- **Container name**: Use unique names for multi-chain deployments (e.g., `consistency-bsc`)
- **Port**: In `host` network mode the service binds directly to the `listen` port; ensure no conflicts

## Deployment

### Start

```bash
cd deploy
docker compose up -d
```

### View Logs

```bash
docker compose logs -f
```

Expected startup logs:

```
[main] config: {Listen:0.0.0.0:8882 ...}
version mode enabled: version=v1, topic=pipeline_1_v1
Starting periodic check for version key: 1/version (interval: 5s)
```

### Verify the Service

```bash
# Check HTTP service
curl -s http://localhost:8882 \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"getLatestBlock","id":1}'

# Check Prometheus metrics
curl -s http://localhost:8882/metrics | grep pipeline_block_num
```

### Stop

```bash
docker compose down
```

## CI/CD Image Builds

Images are built and pushed to AWS ECR automatically via GitHub Actions:

| Trigger | Image Tag Format | Workflow |
|---------|-----------------|----------|
| PR to main | `amd64-{commit-sha}` | `.github/workflows/build.debank.yml` |
| Release created | `amd64-{tag}` | `.github/workflows/release.debank.yml` |

ECR registry: `294354037686.dkr.ecr.ap-northeast-1.amazonaws.com/consistency-checkerx`

Manual image build:

```bash
# Run from project root
docker build -f Dockerfile.debank \
  --build-arg ACCESS_TOKEN=<github-token> \
  -t consistency-checkerx:local .
```

## Multi-Chain / Multi-Version Deployment

When deploying multiple chains or multiple versions of the same chain on one host, create a separate deployment directory for each instance:

```
deploy/
├── eth/
│   ├── compose.yml          # container_name: consistency-eth
│   └── config/
│       └── config.yml       # chain_id: 1, listen: :8882
└── bsc/
    ├── compose.yml          # container_name: consistency-bsc
    └── config/
        └── config.yml       # chain_id: 56, listen: :8883
```

Ensure the following do not conflict across instances:
- **Port** (`listen` field)
- **Container name** (`container_name`)
- **Data path** (volume mount path)
- **Kafka consumer group** (`inner_new_block_group_id`)

## Troubleshooting

### No log output after startup

Check Kafka inner broker connectivity and whether the topic exists. The service blocks at `FetchMessage` waiting for messages.

### "no node" error

No replica nodes registered in etcd. Verify:
1. The etcd cluster is reachable
2. Nodes are registered under the correct key prefix (`{chainID}/nodes/` or `{chainID}/{version}/nodes/`)

### "check many times but not ready"

The replica node readiness ratio did not reach the `ready_ratio` threshold. Possible causes:
- Replica nodes are still syncing
- `rpc_node_timeout_ms` is too low, causing timeouts
- Try increasing `check_num` or `check_interval_ms`

### "both version and outer_version_new_block_topic must be set or both must be empty"

`version` and `outer_version_new_block_topic` must either both be set or both be empty.

### Pebble DB permission error

Ensure the container has read/write access to the mounted directory:

```bash
chmod -R 777 /data/consistency-eth
```
