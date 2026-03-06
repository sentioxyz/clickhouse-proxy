# ClickHouse Proxy

A lightweight ClickHouse native TCP protocol proxy. It sits transparently between clients and ClickHouse servers, providing **query auditing**, **JWS authentication**, **SQL rewriting**, and **Prometheus monitoring** capabilities.

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Build](#build)
  - [Go Native Build](#go-native-build)
  - [Bazel Build](#bazel-build)
- [Configuration](#configuration)
  - [Config File](#config-file)
  - [Environment Variables](#environment-variables)
  - [Full Parameter Reference](#full-parameter-reference)
- [Running](#running)
- [Deployment](#deployment)
  - [Docker Deployment](#docker-deployment)
  - [Bare-metal Deployment](#bare-metal-deployment)
  - [Kubernetes Deployment](#kubernetes-deployment)
- [Authentication](#authentication)
- [SQL Rewriter](#sql-rewriter)
- [Testing](#testing)
- [Metrics](#metrics)

---

## Prerequisites

| Dependency | Version | Notes |
|-----------|---------|-------|
| Go | 1.25+ | Required for building |
| Bazel | 8.0+ | Optional, the project also supports Bazel builds |
| Docker | 20.10+ | Optional, for containerized deployment |

## Quick Start

Minimal steps to get a proxy running (assuming ClickHouse is at `localhost:9000`):

```bash
# 1. Clone the repository
git clone git@github.com:sentioxyz/clickhouse-proxy.git
cd clickhouse-proxy

# 2. Build
go build -o clickhouse-proxy ./cmd/proxy/

# 3. Run (using environment variables to specify upstream)
CK_LISTEN=":9001" CK_UPSTREAM="localhost:9000" ./clickhouse-proxy

# 4. Connect via clickhouse-client through the proxy
clickhouse-client --host localhost --port 9001
```

---

## Build

### Go Native Build

```bash
# Standard build
go build -o clickhouse-proxy ./cmd/proxy/

# Static build (recommended for production, no CGO dependency)
CGO_ENABLED=0 go build -o clickhouse-proxy ./cmd/proxy/

# Or use the Makefile
make build
```

This produces a `clickhouse-proxy` binary in the current directory.

### Bazel Build

The project uses Bazel 8.0 with bzlmod for build management. Go SDK version is 1.25.3.

```bash
# Install Bazel (if not already installed)
# macOS:
brew install bazel
# See https://bazel.build/install for other platforms

# Verify Bazel version (project requires 8.0, see .bazelversion)
bazel --version

# Build
bazel build //cmd/proxy:proxy

# The output binary is located at:
ls bazel-bin/cmd/proxy/proxy_/proxy

# Run tests
bazel test //pkg/proxy:proxy_test
```

> **Note**: The first Bazel build downloads all dependencies and may take a while. Subsequent incremental builds will be much faster.

---

## Configuration

### Config File

The proxy uses JSON configuration files. The loading order is:

1. CLI flag `-config /path/to/config.json`
2. Path specified by the `CK_CONFIG` environment variable
3. `config.json` in the current directory (auto-detected)
4. Built-in defaults if none of the above are found

Example configuration (`config.example.json`):

```json
{
    "listen": ":9001",
    "upstream": "127.0.0.1:9000",
    "stats_interval": "30s",
    "dial_timeout": "5s",
    "idle_timeout": "5m",
    "log_queries": true,
    "log_data": false,
    "max_query_log_bytes": 300,
    "max_data_log_bytes": 200,
    "metrics_listen": ":9091",
    "auth_enabled": false,
    "rewriter_enabled": false
}
```

### Environment Variables

The following config options can be overridden via environment variables (lower priority than config file):

| Variable | Config Field | Default |
|----------|-------------|---------|
| `CK_LISTEN` | `listen` | `:9001` |
| `CK_UPSTREAM` | `upstream` | `clickhouse:9000` |
| `CK_METRICS_LISTEN` | `metrics_listen` | `:9091` |
| `CK_CONFIG` | Config file path | (none) |
| `CK_REWRITER_ADDR` | `rewriter_service_addr` | `localhost:50051` |
| `CK_NETWORK_STATE_SOURCE` | `network_state_source` | `file` |
| `CK_NETWORK_STATE_FILE` | `network_state_file` | (none) |
| `CK_NETWORK_STATE_POSTGRES` | `network_state_postgres` | (none) |
| `CK_CH_USER` | `ch_user` | `default` |
| `CK_CH_PASSWORD` | `ch_password` | (none) |

### Full Parameter Reference

#### Core Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `listen` | string | `:9001` | Proxy listen address and port |
| `upstream` | string | `clickhouse:9000` | Upstream ClickHouse server address |
| `dial_timeout` | duration | `5s` | Timeout for connecting to the upstream |
| `idle_timeout` | duration | `5m` | Idle connection timeout; connections are closed after this period |
| `max_connection_lifetime` | duration | `24h` | Maximum lifetime of a single connection, prevents slow clients from holding resources indefinitely |
| `shutdown_timeout` | duration | `30s` | Maximum time to wait for in-flight connections to drain during graceful shutdown |
| `stats_interval` | duration | `10s` | Interval for printing packet statistics to the log |
| `metrics_listen` | string | `:9091` | Prometheus metrics HTTP endpoint listen address |

#### Logging

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `log_queries` | bool | `true` | Log SQL query content |
| `log_data` | bool | `false` | Log Data packet content (usually off, for debugging only) |
| `max_query_log_bytes` | int | `300` | Maximum query log truncation length (bytes) |
| `max_data_log_bytes` | int | `200` | Maximum Data packet log truncation length (bytes) |

#### Authentication

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `auth_enabled` | bool | `false` | Enable JWS / Ethereum signature authentication |
| `auth_allowed_addresses` | []string | `[]` | List of Ethereum addresses allowed to execute queries |
| `auth_max_token_age` | duration | `1m` | Maximum age of JWS tokens |
| `auth_allow_no_auth` | bool | `false` | Allow requests without an auth token to pass through |

#### SQL Rewriter

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `rewriter_enabled` | bool | `false` | Enable SQL rewriting |
| `rewriter_service_addr` | string | `localhost:50051` | sql-rewriter gRPC service address |
| `rewriter_local_indexer_id` | uint64 | `0` | Local Indexer node ID |
| `rewriter_timeout` | duration | `5s` | SQL rewrite request timeout |

#### Network State

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `network_state_source` | string | `file` | Network state data source; supports `file` |
| `network_state_file` | string | (empty) | Path to the network state YAML file |
| `network_state_postgres` | string | (empty) | PostgreSQL connection string (reserved) |

#### ClickHouse Credentials

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `ch_user` | string | `default` | Username for connecting to the upstream ClickHouse |
| `ch_password` | string | (empty) | Password for connecting to the upstream ClickHouse |

#### Advanced

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `streaming_buf_size` | int | `131072` | Bufio buffer size for streaming protocol parsing (bytes), default 128 KB |
| `validate_checksum` | bool | `false` | Enable CityHash128 checksum validation for compressed data blocks |

> **Tip**: All `duration` parameters accept human-readable formats such as `"5s"`, `"1m"`, `"24h"`. Raw nanosecond numbers are also accepted.

---

## Running

### With a Config File

```bash
./clickhouse-proxy -config config.json
```

### With Environment Variables

```bash
CK_LISTEN=":9001" CK_UPSTREAM="10.0.0.5:9000" ./clickhouse-proxy
```

### With go run (Development)

```bash
go run ./cmd/proxy/ -config config.json
```

On startup the proxy logs its listen address and key configuration:

```
clickhouse-proxy starting. listen=:9001 upstream=127.0.0.1:9000 ...
metrics listening on :9091
```

Press `Ctrl+C` for a graceful shutdown; final statistics are printed before exit.

---

## Deployment

### Docker Deployment

#### Build the Image

```bash
# Local build
docker build -t clickhouse-proxy:latest .

# Build and push to the private registry
make docker push
```

#### Run a Container

```bash
# Basic run (uses the default config path /app/config.json inside the container)
docker run -d \
  --name clickhouse-proxy \
  -p 9001:9001 \
  -p 9091:9091 \
  clickhouse-proxy:latest

# Mount an external config file
docker run -d \
  --name clickhouse-proxy \
  -p 9001:9001 \
  -p 9091:9091 \
  -v /path/to/config.json:/app/config.json \
  clickhouse-proxy:latest

# Use environment variables (no config file needed)
docker run -d \
  --name clickhouse-proxy \
  -p 9001:9001 \
  -e CK_LISTEN=":9001" \
  -e CK_UPSTREAM="clickhouse-server:9000" \
  clickhouse-proxy:latest
```

> **Note**: The Docker image uses a multi-stage build based on `alpine:latest`, resulting in a very small image. The runtime binary is `/app/clickhouse-proxy`.

#### Image Details

| Item | Value |
|------|-------|
| Build stage base image | `golang:1.25-alpine` |
| Runtime base image | `alpine:latest` |
| Working directory | `/app` |
| Default config path | `/app/config.json` |
| Runtime dependencies | `ca-certificates`, `tzdata` |

### Bare-metal Deployment

For environments without Docker, build and run the binary directly:

```bash
# 1. Static cross-compile (recommended when deploying to a machine without Go)
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o clickhouse-proxy ./cmd/proxy/

# 2. Copy the binary and config to the target host
scp clickhouse-proxy config.json user@target-host:/opt/clickhouse-proxy/

# 3. Run on the target host
ssh user@target-host
cd /opt/clickhouse-proxy
./clickhouse-proxy -config config.json

# 4. (Optional) Manage as a systemd service
```

Example systemd unit file:

```ini
[Unit]
Description=ClickHouse Proxy
After=network.target

[Service]
Type=simple
User=clickhouse-proxy
WorkingDirectory=/opt/clickhouse-proxy
ExecStart=/opt/clickhouse-proxy/clickhouse-proxy -config /opt/clickhouse-proxy/config.json
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### Kubernetes Deployment

The repository includes `auth_ck.yaml` with a complete Kubernetes deployment example using ConfigMap + Sidecar pattern:

```bash
kubectl apply -f auth_ck.yaml
```

For production, **ConfigMap** is the recommended way to manage the configuration file.

---

## Authentication

The proxy supports **JWS authentication with Ethereum secp256k1 signatures**. When enabled, clients must pass a JWS token via the ClickHouse custom setting `SQL_x_auth_token`.

### Enable Authentication

```json
{
    "auth_enabled": true,
    "auth_allowed_addresses": [
        "0x1111111111111111111111111111111111111111"
    ],
    "auth_max_token_age": "1m",
    "auth_allow_no_auth": false
}
```

### Client Example

```go
// Using clickhouse-go SDK
ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
    "SQL_x_auth_token": clickhouse.CustomSetting{Value: jwsToken},
}))
rows, err := conn.Query(ctx, "SELECT 1")
```

### JWS Token Format

The **payload** contains two fields:
- `iat` — Unix timestamp (issued at)
- `qhash` — Keccak256 hash of the SQL query (hex with `0x` prefix)

Both JWS Compact Serialization (single signature) and JWS JSON Serialization (multi-signature) formats are supported.

---

## SQL Rewriter

The proxy supports **Sentio Network SQL rewriting**, transforming virtual table names in the format `sentio_<processor_id>.<table_name>` into actual ClickHouse `remote()` expressions. This feature requires an external gRPC rewriter service.

### Enable SQL Rewriting

```json
{
    "rewriter_enabled": true,
    "rewriter_service_addr": "localhost:50051",
    "rewriter_local_indexer_id": 1,
    "rewriter_timeout": "5s",
    "network_state_source": "file",
    "network_state_file": "./network_state.yaml",
    "ch_user": "default",
    "ch_password": ""
}
```

---

## Testing

### Unit Tests

```bash
# Go native
go test ./...

# Bazel
bazel test //pkg/proxy:proxy_test
```

### Local Integration Tests

Verify that the proxy correctly forwards queries and data:

```bash
make test-forwarding
```

### Stream Replay Tests (Production-grade Verification)

Stream real query logs from a running ClickHouse pod and replay them against the local proxy:

```bash
# Prerequisites: kubectl configured with ClickHouse cluster access

# Replay the last hour of queries
make test-stream-replay POD=<pod-name>

# Replay only the last 100 queries
make test-stream-replay POD=<pod-name> N=100

# Replay all queries from the last 30 days (stress test)
make test-stream-replay POD=<pod-name> SINCE="30 day" N=0
```

Success criteria:
- Test ends with `✅ All queries forwarded!`
- Failures count is 0
- No panics in the proxy log summary

---

## Metrics

The proxy exposes Prometheus metrics on the `metrics_listen` port (default `:9091`).

### Key Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `clickhouse_proxy_active_connections` | Gauge | Current number of active connections |
| `clickhouse_proxy_packets_total` | Counter | Total client→server packets (by type) |
| `clickhouse_proxy_server_packets_total` | Counter | Total server→client packets (by type) |
| `clickhouse_proxy_bytes_transferred_total` | Counter | Total bytes transferred (by direction) |
| `clickhouse_proxy_queries_forwarded_total` | Counter | Total queries successfully forwarded |
| `clickhouse_proxy_errors_total` | Counter | Total errors (by phase and error type) |
| `clickhouse_proxy_upstream_health` | Gauge | Upstream ClickHouse health (1=healthy, 0=unreachable) |
| `clickhouse_proxy_query_decode_duration_seconds` | Histogram | Query packet decode latency |
| `clickhouse_proxy_rewrite_duration_seconds` | Histogram | SQL rewrite latency |
| `clickhouse_proxy_handshake_duration_seconds` | Histogram | TCP handshake latency |
| `clickhouse_proxy_fallback_total` | Counter | Fallbacks to raw copy mode |
| `clickhouse_proxy_streaming_data_blocks_total` | Counter | Data blocks processed in streaming mode |

### Prometheus Scrape Config

```yaml
scrape_configs:
  - job_name: 'clickhouse-proxy'
    static_configs:
      - targets: ['localhost:9091']
```

### Grafana Dashboard

Import the pre-configured `dashboard.json` included in this repository into Grafana for out-of-the-box monitoring.
