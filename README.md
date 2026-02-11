# ClickHouse Proxy

A high-performance ClickHouse native TCP protocol proxy built in Go. It provides transparent packet inspection, Ethereum-based authentication (JWS), SQL rewriting for the Sentio decentralized network, and Prometheus metrics — all with zero-copy forwarding for non-inspected traffic.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Build](#build)
  - [Run](#run)
- [Configuration](#configuration)
  - [Configuration File](#configuration-file)
  - [Full Configuration Reference](#full-configuration-reference)
  - [Environment Variables](#environment-variables)
- [Authentication (JWS / Ethereum Signature)](#authentication-jws--ethereum-signature)
  - [Overview](#overview)
  - [Auth Configuration](#auth-configuration)
  - [Client Usage](#client-usage)
  - [JWS Token Format](#jws-token-format)
- [SQL Rewriting (Sentio Network)](#sql-rewriting-sentio-network)
- [Prometheus Metrics](#prometheus-metrics)
- [Docker](#docker)
  - [Build Image](#build-image)
  - [Run Container](#run-container)
- [Kubernetes Deployment](#kubernetes-deployment)
- [Testing](#testing)
  - [Unit Tests](#unit-tests)
  - [Local Integration Forwarding Test](#local-integration-forwarding-test)
  - [Stream Replay Verification](#stream-replay-verification)
- [Local Multi-Instance Verification](#local-multi-instance-verification)
- [Makefile Reference](#makefile-reference)
- [License](#license)

---

## Features

| Feature | Description |
|---|---|
| **Native TCP Proxy** | Transparently forwards ClickHouse native protocol traffic (including `remote()` / cluster hops) between clients and upstream servers. |
| **Packet Inspection** | Identifies and counts all ClickHouse client→server packet types (Hello, Query, Data, Ping, Cancel, etc.) with periodic statistics output. |
| **Ethereum Signature Auth** | Validates queries using ES256K (secp256k1) JWS tokens. Supports single and multi-signature authorization with an Ethereum address allowlist. |
| **SQL Rewriting** | Rewrites Sentio-Network–style table references (`sentio_<processor_id>.<table>`) into real ClickHouse SQL via a gRPC rewriter service. |
| **Prometheus Metrics** | Exposes connection, packet, byte-throughput, error, and upstream-health metrics at a configurable `/metrics` endpoint. |
| **Graceful Lifecycle** | TCP keepalive, configurable dial/idle timeouts, and clean shutdown on SIGINT/SIGTERM. |
| **Flexible Configuration** | JSON config file, environment variables, and CLI flags with sensible defaults — works out of the box. |

---

## Architecture

```
                  ┌──────────────────────────────────────────────┐
                  │              ClickHouse Proxy                │
                  │                                              │
Client ──TCP──▶   │  Packet Parser ──▶ Validator (Auth)          │
                  │                  ──▶ Rewriter (SQL Rewrite)  │  ──TCP──▶ ClickHouse Upstream
                  │                  ──▶ Observer (Metrics)      │
                  │                                              │
                  │  [Prometheus /metrics on :9091]              │
                  └──────────────────────────────────────────────┘
```

Key source files:

| File | Responsibility |
|---|---|
| `main.go` | Entry point, wiring of config / validator / rewriter / proxy |
| `proxy.go` | TCP listener, per-connection goroutine, packet forwarding |
| `validator.go` | `Validator` interface + `EthValidator` (JWS/secp256k1 auth) |
| `rewriter.go` | `Rewriter` interface + `SentioNetworkRewriter` (gRPC SQL rewrite) |
| `observer.go` | Prometheus metrics registration and recording |
| `config.go` | Configuration loading (JSON file / env / defaults) |

---

## Getting Started

### Prerequisites

- **Go 1.25+**
- A running ClickHouse instance (upstream)

### Build

```bash
# Build binary
make build            # produces ./ck-proxy

# Or directly with go
CGO_ENABLED=0 go build -o ck-proxy .
```

### Run

```bash
# With a config file
./ck-proxy -config config.json

# With Go toolchain (development mode)
go run . -config config.json

# Without a config file (uses defaults + env vars)
./ck-proxy
```

The proxy starts the TCP listener on the configured `listen` address and a Prometheus metrics HTTP server on `metrics_listen`. Statistics are logged periodically. Press **Ctrl+C** for graceful shutdown with final statistics output.

---

## Configuration

### Configuration File

By default the proxy looks for `config.json` in the current directory. Override with:

```bash
./ck-proxy -config /path/to/config.json
# or
CK_CONFIG=/path/to/config.json ./ck-proxy
```

### Full Configuration Reference

```json
{
  "listen": "0.0.0.0:9001",
  "upstream": "127.0.0.1:9000",
  "stats_interval": "10s",
  "dial_timeout": "5s",
  "idle_timeout": "5m",
  "log_queries": true,
  "log_data": false,
  "max_query_log_bytes": 300,
  "max_data_log_bytes": 200,
  "metrics_listen": ":9091",

  "auth_enabled": false,
  "auth_allowed_addresses": [],
  "auth_max_token_age": "1m",
  "auth_allow_no_auth": false,

  "rewriter_enabled": false,
  "rewriter_service_addr": "localhost:50051",
  "rewriter_local_indexer_id": 0,
  "rewriter_timeout": "5s",

  "network_state_source": "file",
  "network_state_file": "",

  "ch_user": "default",
  "ch_password": ""
}
```

<details>
<summary><strong>Field Descriptions</strong></summary>

#### Core

| Field | Type | Default | Description |
|---|---|---|---|
| `listen` | string | `:9001` | Proxy TCP listen address. |
| `upstream` | string | `clickhouse:9000` | Upstream ClickHouse native TCP address. |
| `stats_interval` | duration | `10s` | Interval for packet statistics log output. |
| `dial_timeout` | duration | `5s` | Timeout for connecting to upstream. |
| `idle_timeout` | duration | `5m` | Idle timeout before a connection is closed. |
| `log_queries` | bool | `true` | Log SQL from Query packets. |
| `log_data` | bool | `false` | Log Data packet summaries. |
| `max_query_log_bytes` | int | `300` | Max bytes of SQL to include in logs. |
| `max_data_log_bytes` | int | `200` | Max bytes of Data payload to include in logs. |
| `metrics_listen` | string | `:9091` | HTTP listen address for Prometheus `/metrics`. |

#### Authentication

| Field | Type | Default | Description |
|---|---|---|---|
| `auth_enabled` | bool | `false` | Enable Ethereum signature authentication. |
| `auth_allowed_addresses` | []string | `[]` | Ethereum addresses permitted to execute queries. |
| `auth_max_token_age` | duration | `1m` | Maximum accepted age of JWS tokens. |
| `auth_allow_no_auth` | bool | `false` | Allow requests that carry no auth token. |

#### SQL Rewriter

| Field | Type | Default | Description |
|---|---|---|---|
| `rewriter_enabled` | bool | `false` | Enable SQL rewriting. |
| `rewriter_service_addr` | string | `localhost:50051` | gRPC address of the sql-rewriter service. |
| `rewriter_local_indexer_id` | uint64 | `0` | Local Indexer ID for network state. |
| `rewriter_timeout` | duration | `5s` | Timeout for each rewrite RPC call. |
| `network_state_source` | string | `file` | Network state data source (`file` or `postgres`). |
| `network_state_file` | string | `""` | Path to a YAML network state file (when source = `file`). |
| `ch_user` | string | `default` | ClickHouse username for remote table access. |
| `ch_password` | string | `""` | ClickHouse password for remote table access. |

</details>

### Environment Variables

All environment variables serve as defaults when no config file field is set:

| Variable | Config Field |
|---|---|
| `CK_LISTEN` | `listen` |
| `CK_UPSTREAM` | `upstream` |
| `CK_CONFIG` | _(config file path)_ |
| `CK_METRICS_LISTEN` | `metrics_listen` |
| `CK_REWRITER_ADDR` | `rewriter_service_addr` |
| `CK_NETWORK_STATE_SOURCE` | `network_state_source` |
| `CK_NETWORK_STATE_FILE` | `network_state_file` |
| `CK_NETWORK_STATE_POSTGRES` | `network_state_postgres` |
| `CK_CH_USER` | `ch_user` |
| `CK_CH_PASSWORD` | `ch_password` |

---

## Authentication (JWS / Ethereum Signature)

### Overview

When `auth_enabled` is `true`, every Query packet must carry a valid JWS token signed with an Ethereum secp256k1 key. The recovered signer address must be present in `auth_allowed_addresses`.

Key properties:
- **ES256K** (secp256k1) — the same curve used by Ethereum wallets.
- **Multi-Signature** support via JWS JSON Serialization (all signatures must be valid).
- **Token Expiry** controlled by `auth_max_token_age`.
- **No-Auth Passthrough** via `auth_allow_no_auth` for mixed environments.

### Auth Configuration

See `jwk_proxy_config.json` for a full example:

```json
{
  "listen": ":9002",
  "upstream": "127.0.0.1:9000",
  "auth_enabled": true,
  "auth_allowed_addresses": [
    "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23",
    "0x86cE23361B15507dDbf734EE32904312C6A16eE3"
  ],
  "auth_max_token_age": "1m",
  "auth_allow_no_auth": false
}
```

### Client Usage

Clients pass the JWS token via the `SQL_x_auth_token` custom ClickHouse setting (prefixed with `SQL_` to avoid ClickHouse unknown-setting errors):

```go
// Using clickhouse-go SDK
ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
    "SQL_x_auth_token": clickhouse.CustomSetting{Value: jwsToken},
}))
rows, err := conn.Query(ctx, "SELECT 1")
```

### JWS Token Format

#### Compact Serialization (Single Signature)

```
BASE64(header).BASE64(payload).BASE64(signature)
```

**Header:**
```json
{"alg": "ES256K", "typ": "JWS"}
```

**Payload:**
```json
{"iat": 1737525600, "qhash": "0x..."}
```

- `iat` — Unix timestamp (issued-at).
- `qhash` — Keccak256 hash of the SQL query (hex, `0x`-prefixed).

**Signature:** 65-byte recoverable ECDSA signature (`R || S || V`).

#### JSON Serialization (Multi-Signature)

```json
{
  "payload": "BASE64_PAYLOAD",
  "signatures": [
    {"protected": "BASE64_HEADER", "signature": "BASE64_SIG"},
    {"protected": "BASE64_HEADER", "signature": "BASE64_SIG"}
  ]
}
```

All signatures must be valid and from allowed addresses.

---

## SQL Rewriting (Sentio Network)

When `rewriter_enabled` is `true`, Query packets matching the Sentio-Network table pattern `sentio_<processor_id>.<table_name>` are transparently rewritten:

1. The proxy parses `processor_id` / `table_name` from the SQL.
2. It consults the **Network State** (file or postgres) for processor allocations and indexer endpoints.
3. It calls the **sql-rewriter gRPC service** to produce the final SQL (which may introduce `remote()` calls to other indexers).
4. The rewritten SQL replaces the original in the Query packet before forwarding.

A built-in **simple-rewrite fallback** is used when the gRPC service is unavailable.

---

## Prometheus Metrics

Metrics are served via HTTP at the `metrics_listen` address (default `:9091`).

| Metric | Type | Labels | Description |
|---|---|---|---|
| `clickhouse_proxy_active_connections` | Gauge | — | Active client connections |
| `clickhouse_proxy_packets_total` | Counter | `type` | Client→server packets by type |
| `clickhouse_proxy_server_packets_total` | Counter | `type` | Server→client packets by type |
| `clickhouse_proxy_bytes_transferred_total` | Counter | `direction` | Bytes forwarded (`client_to_upstream`, `upstream_to_client`) |
| `clickhouse_proxy_queries_forwarded_total` | Counter | — | Query packets written to upstream |
| `clickhouse_proxy_upstream_health` | Gauge | — | `1` if upstream is reachable, `0` otherwise |
| `clickhouse_proxy_errors_total` | Counter | `type`, `error` | Errors classified by phase and kind |

For Kubernetes, add Prometheus scrape annotations:

```yaml
prometheus.io/scrape: "true"
prometheus.io/port: "9091"
```

---

## Docker

### Build Image

```bash
# Local build
docker build -t clickhouse-proxy:latest .

# Build and push to registry
make docker      # tags with git commit SHA
make push

# Auth-tagged image
make auth_proxy  # builds and pushes with auth-<commit> tag
```

The multi-stage Dockerfile uses `golang:1.25-alpine` for building and `alpine:latest` for runtime (includes `ca-certificates` and `tzdata`). The binary is located at `/app/ck-proxy`.

### Run Container

```bash
# Basic — uses built-in default config at /app/config.json
docker run -d --name clickhouse-proxy -p 9001:9001 clickhouse-proxy:latest

# With a custom config file
docker run -d --name clickhouse-proxy \
  -p 9001:9001 \
  -v /host/path/config.json:/app/config.json \
  clickhouse-proxy:latest \
  -config /app/config.json
```

> **Tip:** Ensure the upstream ClickHouse address is reachable from the container network, and avoid host port conflicts.

---

## Kubernetes Deployment

A complete Kubernetes manifest is provided in `auth_ck.yaml`, which includes:

- **ConfigMap** with proxy configuration
- **Sidecar container** in a `ClickHouseInstallation` resource
- **Service port** mappings

```bash
kubectl apply -f auth_ck.yaml
```

Use `make update-yaml` or `make update-yaml-auth` to update image tags in the external deployment manifests, then `make apply` or `make apply-auth` to apply.

---

## Testing

### Unit Tests

```bash
go test ./...
```

### Local Integration Forwarding Test

Validates that the proxy correctly forwards queries and data between a local client and a mock server:

```bash
make test-forwarding
```

### Stream Replay Verification

**The most critical test.** Streams real query logs from a running ClickHouse pod and replays them against the local proxy to verify that requests are processed without errors or panics.

**Prerequisites:** `kubectl` configured with access to a ClickHouse cluster.

```bash
# Replay the last hour of queries from a specific pod
make test-stream-replay POD=clickhouse-user-part-a-0-0-0

# Replay only the last 100 queries
make test-stream-replay POD=clickhouse-user-part-a-0-0-0 N=100

# Full 30-day history load test
make test-stream-replay POD=clickhouse-user-part-a-0-0-0 SINCE="30 day" N=0
```

**Success Criteria:**
- Output ends with `✅ All queries forwarded!`
- `Failures` count is `0`
- No panics reported in the proxy log summary

### Auth Test Client

```bash
cd tests/auth_test_client
go run main.go -addr 127.0.0.1:9002
```

---

## Local Multi-Instance Verification

For local development, you can run two ClickHouse instances with two proxy instances:

```bash
# Start two ClickHouse instances
clickhouse server --config-file local/ck-a-config.xml --daemon   # http 18123, tcp 19000
clickhouse server --config-file local/ck-b-config.xml --daemon   # http 28123, tcp 29000

# Start proxies
go run . -config local/proxy-a.json   # listen 9001 → upstream 19000
go run . -config local/proxy-b.json   # listen 9002 → upstream 29000
```

Test cross-instance queries via `remote()`:

```sql
-- Create table through proxy B
CREATE TABLE default.t_mem (n UInt32) ENGINE=Memory;

-- Insert through proxy A's remote function
INSERT INTO FUNCTION remote('172.17.0.2:9001', 'default', 't_mem', 'default', '') VALUES (1),(2),(3);

-- Query through proxy A
SELECT sum(n) FROM remote('172.17.0.2:9001', 'default', 't_mem', 'default', '');
```

---

## Makefile Reference

| Target | Description |
|---|---|
| `make build` | Compile the binary (`ck-proxy`) |
| `make docker` | Build Docker image with git-SHA tag |
| `make push` | Push image to registry |
| `make docker-auth` | Build auth-tagged image |
| `make push-auth` | Push auth-tagged image |
| `make auth_proxy` | Build + push auth image in one step |
| `make update-yaml` | Update image tag in K8s manifests |
| `make apply` | `kubectl apply` deployment manifests |
| `make test-forwarding` | Run local integration tests |
| `make test-stream-replay` | Run stream replay verification |

---

## License

See repository for license details.
