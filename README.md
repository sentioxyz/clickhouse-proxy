# ClickHouse Proxy

A lightweight ClickHouse native TCP protocol proxy. It sits transparently between clients and ClickHouse servers, providing **query auditing**, **JWS authentication**, **SQL rewriting**, **dynamic upstream routing**, and **Prometheus monitoring** capabilities.

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Build](#build)
- [Configuration](#configuration)
  - [Config File](#config-file)

  - [Full Parameter Reference](#full-parameter-reference)
- [Running](#running)
- [Deployment](#deployment)
  - [Bare-metal Deployment](#bare-metal-deployment)
- [Authentication](#authentication)
  - [Relay Token Propagation](#relay-token-propagation)
- [SQL Rewriter](#sql-rewriter)
- [Testing](#testing)

---

## Prerequisites

| Dependency | Version | Notes |
|-----------|---------|-------|
| Go | 1.25+ | Required for building |
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

This project supports both native Go compilation and Bazel build (consistent with `sentio-core`, recommended).

### Using Bazel (Recommended)

This project uses Bazel `8.5.1` and Bzlmod for dependency management, including support for precompiled `protoc` and cross-compilation C toolchains:

```bash
# Build the proxy binary
bazel build //cmd/proxy:clickhouse-proxy

# Build and run all tests
bazel test //...

# If you need to update third-party CGO dependencies (e.g., adjusting patches):
bazel mod tidy
bazel run //:gazelle
```

Once built, the binary will be generated under the `bazel-bin/cmd/proxy/clickhouse-proxy_/%workspace%/cmd/proxy/clickhouse-proxy` path. You can also run it directly using `bazel run //cmd/proxy:clickhouse-proxy`.

### Using Native Go

```bash
# Standard build
go build -o clickhouse-proxy ./cmd/proxy/

# Static build (recommended for production, no CGO dependency)
CGO_ENABLED=0 go build -o clickhouse-proxy ./cmd/proxy/

# Or use the Makefile
make build
```

This produces a `clickhouse-proxy` binary in the current directory.

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
    "auth_enabled": false
}
```



### Full Parameter Reference

#### Core Settings

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `listen` | string | No | `:9001` | Proxy listen address and port |
| `upstream` | string | No | `clickhouse:9000` | Upstream ClickHouse server address |
| `dial_timeout` | duration | No | `5s` | Timeout for connecting to the upstream |
| `idle_timeout` | duration | No | `5m` | Idle connection timeout; connections are closed after this period |
| `max_connection_lifetime` | duration | No | `24h` | Maximum lifetime of a single connection, prevents slow clients from holding resources indefinitely |
| `shutdown_timeout` | duration | No | `30s` | Maximum time to wait for in-flight connections to drain during graceful shutdown |
| `stats_interval` | duration | No | `10s` | Interval for printing packet statistics to the log |
| `metrics_listen` | string | No | `:9091` | Prometheus metrics HTTP endpoint listen address |

#### Logging

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `log_queries` | bool | No | `true` | Log SQL query content |
| `log_data` | bool | No | `false` | Log Data packet content (usually off, for debugging only) |
| `max_query_log_bytes` | int | No | `300` | Maximum query log truncation length (bytes) |
| `max_data_log_bytes` | int | No | `200` | Maximum Data packet log truncation length (bytes) |

#### Authentication

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `auth_enabled` | bool | No | `false` | Enable JWS / Ethereum signature authentication |
| `auth_allowed_addresses` | []string | No | `[]` | List of Ethereum addresses allowed to execute queries |
| `auth_max_token_age` | duration | No | `1m` | Maximum age of JWS tokens |
| `auth_allow_no_auth` | bool | No | `false` | Allow requests without an auth token to pass through |
| `relay_private_key_hex` | string | No | (empty) | Ethereum private key for signing relay JWS tokens in proxy-to-proxy (`__route__`) connections. All proxies in a cluster should share the same key. The corresponding address must be in `auth_allowed_addresses` |

#### SQL Rewriter

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `rewriter_service_addr` | string | No | `localhost:50051` | sql-rewriter gRPC service address |
| `rewriter_timeout` | duration | No | `5s` | SQL rewrite request timeout |

#### Network State

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `network_state_redis` | string | Yes | (empty) | Redis address for statemirror-based network state (e.g. `localhost:6379`) |


#### Advanced

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `streaming_buf_size` | int | No | `131072` | Bufio buffer size for streaming protocol parsing (bytes), default 128 KB |
| `validate_checksum` | bool | No | `false` | Enable CityHash128 checksum validation for compressed data blocks |

> **Tip**: All `duration` parameters accept human-readable formats such as `"5s"`, `"1m"`, `"24h"`. Raw nanosecond numbers are also accepted.

---

## Running

### With a Config File

```bash
./clickhouse-proxy -config config.json
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

### Relay Token Propagation

In a multi-proxy cluster, when ClickHouse initiates a `remote()` sub-query via `__route__` connections (Proxy1 → ClickHouse → Proxy2), Proxy1 automatically signs a relay JWS token and injects it into the query settings before forwarding to Proxy2.

**How it works:**
1. Proxy1 receives a `__route__` connection from the local ClickHouse.
2. Proxy1 intercepts the first Query packet and signs a relay JWS token using the shared Ethereum private key.
3. Proxy1 injects the token via `SQL_x_auth_token` setting and forwards to Proxy2.
4. Proxy2 validates the relay token through its normal JWS validation flow.

**Configuration:**

```json
{
    "auth_enabled": true,
    "relay_private_key_hex": "your-shared-ethereum-private-key-hex",
    "auth_allowed_addresses": [
        "0x<address-derived-from-relay-private-key>"
    ]
}
```

> **Important**: All proxies in the cluster must share the same `relay_private_key_hex`, and the corresponding Ethereum address must be in `auth_allowed_addresses`.

---

## SQL Rewriter

The proxy supports **Sentio Network SQL rewriting**, transforming virtual table names in the format `sentio_<processor_id>.<table_name>` into actual ClickHouse `remote()` expressions. This feature requires an external gRPC rewriter service and a network state provider.

### Enable SQL Rewriting

```json
{
    "rewriter_service_addr": "localhost:50051",
    "rewriter_timeout": "5s",
    "network_state_redis": "localhost:6379"
}
```

### Per-Query Skip Rewrite

When SQL rewriting is globally enabled, clients can skip rewriting on a **per-query basis** by setting the custom ClickHouse setting `SQL_skip_rewrite=1`. This is especially useful for `INSERT` statements that should not be rewritten.

```go
// Go client example: skip rewriting for INSERT
ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
    "SQL_skip_rewrite": clickhouse.CustomSetting{Value: "1"},
}))
conn.Exec(ctx, "INSERT INTO sentio_eth.transfer ...")
```

> **Note**: `SQL_skip_rewrite` is a proxy-only custom setting. It is automatically stripped before forwarding to the upstream ClickHouse server.



---

## Testing

### Unit Tests

```bash
# Go native
go test ./...
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


