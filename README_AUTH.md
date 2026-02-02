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
