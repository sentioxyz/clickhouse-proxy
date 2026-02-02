# ck_remote_proxy Demo

A simple ClickHouse native protocol proxy for sniffing and counting packet types in the client→server direction, with forwarding support. It now supports configuration file control for listen/upstream parameters and provides a Query validation entry point (currently defaults to allowing all queries).

## Running

### Configuration
For local dual-instance + dual-proxy behavior testing, refer to the example configurations in the `local/` directory (one for A and one for B).

You can place a `config.json` file (defaults to the current directory; if it doesn't exist, built-in defaults and `CK_LISTEN` / `CK_UPSTREAM` environment variables are used):
```json
{
  "listen": "0.0.0.0:9001",
  "upstream": "127.0.0.1:19000",
  "stats_interval": "10s",
  "dial_timeout": "5s",
  "idle_timeout": "5m",
  "log_queries": true,
  "log_data": false,
  "max_query_log_bytes": 300,
  "max_data_log_bytes": 200
}
```

Other optional parameters: `-config /path/to/config.json` or use the `CK_CONFIG` environment variable to specify the config file path.

### Startup

In the proxy container:
```bash
cd /workspace/clickhouse-proxy-demo
GOCACHE=/workspace/clickhouse-proxy-demo/.cache/go-build \
go run . -config config.json
```
Statistics are printed every 10s by default, Ctrl+C will print final statistics. When no config file is provided, built-in defaults and `CK_LISTEN` / `CK_UPSTREAM` environment variables are used.

### Local Dual-Instance + Dual-Proxy Verification Example

Prepare two CK configurations:
- A: `local/ck-a-config.xml` (ports: http 18123 / tcp 19000 / interserver 19009, data directory `.ck_runtime/a`)
- B: `local/ck-b-config.xml` (ports: http 28123 / tcp 29000 / interserver 29009, data directory `.ck_runtime/b`)

Example startup (prepare directory permissions first, note port usage):
```bash
# Start CK A / CK B (requires clickhouse-server to be available and allowed to listen on ports)
clickhouse server --config-file local/ck-a-config.xml --daemon
clickhouse server --config-file local/ck-b-config.xml --daemon

# Start respective proxies
go run . -config local/proxy-a.json   # listen 9001 -> upstream 19000
go run . -config local/proxy-b.json   # listen 9002 -> upstream 29000
```

Testing approach: Use Go SDK to connect to proxy B (9002) for table creation/insertion/querying, then use the remote function pointing to proxy A (9001) for cross-instance queries, confirming that both proxies have statistics/logs.

## ck-dev Side Example

Assuming proxy IP is `172.17.0.2`:

Create table (via proxy):
```sql
CREATE TABLE default.t_mem (n UInt32) ENGINE=Memory;
```

Insert (Query + Data):
```sql
INSERT INTO FUNCTION remote('172.17.0.2:9001', 'default', 't_mem', 'default', '') VALUES (1),(2),(3);
```

Query:
```sql
SELECT sum(n) FROM remote('172.17.0.2:9001', 'default', 't_mem', 'default', '');
SELECT * FROM remote('172.17.0.2:9001', 'system', 'one', 'default', '');
SELECT name FROM remote('172.17.0.2:9001', 'system', 'tables', 'default', '') WHERE database='default' LIMIT 5;
```

## Production Extension Points

- All native TCP traffic (including remote/cluster hops) goes through the proxy; `Query` packets call `Validator.ValidateQuery` before forwarding (see `main.go`), current implementation is noop, can add authentication/auditing here.
- Listen/upstream, timeouts (dial/idle), statistics interval and log granularity can all be adjusted via config file; defaults are used when not configured to ensure the service can start directly.
- TCP keepalive and read/write deadlines are preserved to avoid connection leaks from prolonged idle time.

## Example Results (Excerpt)

Packet summary logs (Query/Data):
```
[conn 1] Query packet ... DESC TABLE system.numbers ...
[conn 1] Query packet ... SELECT sum(`__table1`.`number`) AS `sum(number)` FROM `system`.`numbers` ...
[conn 1] Data packet (38 bytes): ...
[conn 2] Query packet ... CREATE TABLE default.t_mem (n UInt32) ENGINE=Memory;
[conn 2] Query packet ... INSERT INTO FUNCTION remote('172.17.0.2:9001', 'default', 't_mem', ...)
[conn 2] Data packet ... n UInt32 ...
[conn 5] Query packet ... SELECT sum(`__table1`.`n`) AS `sum(n)` FROM `default`.`t_mem` ...
[conn 6] Query packet ... SELECT `__table1`.`dummy` AS `dummy` FROM `system`.`one` ...
[conn 7] Query packet ... SELECT `__table1`.`name` AS `name` FROM `system`.`tables` WHERE `database` = 'default' ...
```

Statistics output (final):
```
==== ck_remote_proxy stats ====
Hello             : 14
Query             : 17
Data              : 7
Ping              : 6
Cancel            : 0
TablesStatusRequest: 9
KeepAlive         : 0
Scalar            : 0
Poll              : 0
Data (portable)   : 0
unknown           : 0
===============================
```

Notes:
- No unknown types appeared, all common packet types were identified.
- Query/Data logs show SQL fragments and data block summaries, do not affect forwarding.

## Testing

### 1. Unit Tests
Run standard Go unit tests for internal packages:
```bash
go test ./...
```

### 2. Stream Replay Verification
Streams real query logs from a running ClickHouse pod and replays them against the local proxy.
```bash
make test-stream-replay POD=<pod-name>
```

For authentication details and demo usage, see [Authentication Guide](#clickhouse-auth-proxy) below.

# ClickHouse Auth Proxy

ClickHouse Proxy with **Ethereum Signature Authentication** support. This proxy sits between clients and ClickHouse, intercepting the handshake to authenticate users via Ethereum SECP256K1 signatures (JWS tokens) and transparently swapping credentials for upstream connection.

## Features

- **Credential Swapping**: Clients connect using Ethereum Address (User) and JWS Token (Password). Proxy authenticates them and swaps to real ClickHouse credentials.
- **Query Verification**: Enforces that the executed query matches the hash signed in the JWS token.
- **Ethereum Signature Auth**: Validates queries using ES256K (secp256k1) signatures.
- **Multi-Signature Support**: Supports JWS JSON Serialization for multi-sig authorization.
- **Allowlist**: Only whitelisted Ethereum addresses can execute queries.
- **Token Expiration**: Configurable max token age.
- **Backward Compatible**: Works with standard ClickHouse clients (CLI, JDBC, Go, Python, etc.) without modification.

## Quick Start

### 1. Build

```bash
make build
# or
go build -o ck-proxy .
```

### 2. Configuration

Create a config file (e.g., `config.json`):

```json
{
  "listen": ":9000",
  "upstream": "127.0.0.1:19000",
  "auth_enabled": true,
  "auth_allowed_addresses": [
    "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23",
    "0x86cE23361B15507dDbf734EE32904312C6A16eE3"
  ],
  "users": {
    "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23": {
      "clickhouse_user": "default",
      "clickhouse_password": ""
    }
  },
  "auth_max_token_age": "5m"
}
```

### 3. Client Usage

Clients authenticate by passing:
- **Username**: Your Ethereum Address (e.g. `0x2c...`)
- **Password**: Your JWS Token

Example with ClickHouse CLI:

```bash
clickhouse-client --host 127.0.0.1 --port 9000 \
  --user "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23" \
  --password "eyJhbGciOiJFUzI1..." \
  --query "SELECT 1"
```

The proxy will:
1. Verify the JWS token signature against the Ethereum address.
2. Verify the token is not expired.
3. Check if the address is in `auth_allowed_addresses` (if configured).
4. Look up the `users` mapping to find the real ClickHouse credentials (`default` / `""`).
5. Rewrite the Hello packet with the real credentials and forward to upstream.
6. Verify that the query hash in the token matches the query being executed.

## JWS Token Format

### Compact Serialization (Single Signature)

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

- `iat`: Unix timestamp (issued at)
- `qhash`: Keccak256 hash of the SQL query (hex with 0x prefix)

**Signature:** 65-byte recoverable ECDSA signature (R || S || V)

## Demo & Testing

The `demo/` directory contains tools to test the auth flow without a real ClickHouse server.

### 1. Start Mock Server
Simulates a ClickHouse server that accepts any login (for testing rewrites).
```bash
go run demo/mock_server.go
```

### 2. Start Proxy
Connects to Mock Server, validates tokens using `demo/proxy_auth.json`.
```bash
go run . -config demo/proxy_auth.json
```

### 3. Run Verify Client
Generates a fresh token with a private key and executes a query.
```bash
go run demo/verify_client.go -addr 127.0.0.1:9002 -query "SELECT 1"
```
