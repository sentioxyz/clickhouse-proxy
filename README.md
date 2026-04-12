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

---

## Prerequisites

| Dependency | Version | Notes |
|-----------|---------|-------|
| Go | 1.25+ | Required for building |
| Docker | 20.10+ | Optional, for containerized deployment |

## Quick Start

Minimal steps to get a proxy running (assuming ClickHouse is at `localhost:9000`):

### Using go install (Fastest)

```bash
# Install the latest version
go install github.com/sentioxyz/clickhouse-proxy/cmd/proxy@latest

# Run (using environment variables to specify upstream)
CK_LISTEN=":9001" CK_UPSTREAM="localhost:9000" proxy
```

### From Source

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

Example configuration (`config.example.json`) without authentication:

```json
{
    // === Core Settings ===
    "listen": ":9001",
    "upstream": "127.0.0.1:9000",
    "dial_timeout": "5s",
    "idle_timeout": "5m",
    "max_connection_lifetime": "24h",
    "shutdown_timeout": "30s",
    "stats_interval": "30s",
    "metrics_listen": ":9091",

    // === Logging ===
    "log_queries": true,
    "log_data": false,
    "max_query_log_bytes": 300,
    "max_data_log_bytes": 200,

    // === Authentication ===
    "auth_enabled": false,

    // === SQL Rewriter ===
    "rewriter_service_addr": "localhost:50051",
    "rewriter_timeout": "5s",

    // === Network State ===
    "network_state_redis": "localhost:6379",

    // === Advanced ===
    "streaming_buf_size": 131072,
    "validate_checksum": false
}
```

Example configuration with authentication enabled:

```json
{
    // === Core Settings ===
    "listen": ":9001",
    "upstream": "127.0.0.1:9000",
    "dial_timeout": "5s",
    "idle_timeout": "5m",
    "max_connection_lifetime": "24h",
    "shutdown_timeout": "30s",
    "stats_interval": "30s",
    "metrics_listen": ":9091",

    // === Logging ===
    "log_queries": true,
    "log_data": false,
    "max_query_log_bytes": 300,
    "max_data_log_bytes": 200,

    // === Authentication ===
    "auth_enabled": true,
    "auth_allowed_addresses": [
      "0x12345678901234567890123456...",
      "YOUR_NEW_ADDRESS_HERE"
    ],
    "auth_max_token_age": "1m",
    "auth_allow_no_auth": false,
    "relay_private_key_hex": "0x12345678901234567890123456...",

    // === SQL Rewriter ===
    "rewriter_service_addr": "localhost:50051",
    "rewriter_timeout": "5s",

    // === Network State ===
    "network_state_redis": "localhost:6379",

    // === Advanced ===
    "streaming_buf_size": 131072,
    "validate_checksum": false
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
