# ClickHouse Auth Proxy

ClickHouse Proxy with **JWK/Ethereum Signature Authentication** support. This proxy sits between clients and ClickHouse, validating requests using Ethereum-style secp256k1 signatures (JWS tokens).

## Features

- **Ethereum Signature Auth**: Validates queries using ES256K (secp256k1) signatures
- **Multi-Signature Support**: Supports JWS JSON Serialization for multi-sig authorization
- **Allowlist**: Only whitelisted Ethereum addresses can execute queries
- **Token Expiration**: Configurable max token age
- **No-Auth Mode**: Optional bypass for requests without tokens (`auth_allow_no_auth`)
- **Backward Compatible**: Works with standard ClickHouse clients

## Quick Start

### 1. Build Auth Proxy

```bash
# Build and push with auth-{commit_id} tag
make auth_proxy

# Or manually:
make docker-auth
make push-auth
```

### 2. Configuration

Create a config file (see `jwk_proxy_config.json` for example):

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

### 3. Configuration Options

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `auth_enabled` | bool | `false` | Enable authentication |
| `auth_allowed_addresses` | []string | `[]` | Ethereum addresses allowed to execute queries |
| `auth_max_token_age` | string | `"1m"` | Max age of JWS tokens (e.g., `"1m"`, `"30s"`) |
| `auth_allow_no_auth` | bool | `false` | If true, requests without tokens pass through |

### 4. Client Usage

Clients must pass a JWS token via the `x_auth_token` custom setting:

```go
// Using clickhouse-go SDK
ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
    "x_auth_token": clickhouse.CustomSetting{Value: jwsToken},
}))
rows, err := conn.Query(ctx, "SELECT 1")
```

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

### JSON Serialization (Multi-Signature)

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

## Testing

Run the test client:

```bash
cd tests/auth_test_client
go run main.go -addr 127.0.0.1:9002
```

## Kubernetes Deployment

See `auth_ck.yaml` for a complete Kubernetes example with:
- ConfigMap for proxy config
- Sidecar container in ClickHouseInstallation
- Service port mappings

```bash
kubectl apply -f auth_ck.yaml
```
