# ClickHouse Proxy

一个轻量级的 ClickHouse 原生 TCP 协议代理。它在客户端与 ClickHouse 服务器之间透明转发流量，同时提供 **查询审计**、**JWS 认证**、**SQL 重写**、**动态上游路由** 和 **Prometheus 监控** 等能力。

---

## 目录

- [环境准备](#环境准备)
- [快速开始](#快速开始)
- [编译指南](#编译指南)
- [配置详解](#配置详解)
  - [配置文件](#配置文件)

  - [完整参数表](#完整参数表)
- [运行](#运行)
- [部署](#部署)
  - [裸机部署](#裸机部署)
- [认证配置](#认证配置)
  - [Relay Token 传播](#relay-token-传播)
- [SQL 重写配置](#sql-重写配置)
- [测试](#测试)

---

## 环境准备

| 依赖 | 版本要求 | 说明 |
|------|---------|------|
| Go   | 1.25+   | 必需，用于编译 |
| Docker | 20.10+ | 可选，用于容器化部署 |

## 快速开始

最少步骤跑起一个 proxy（假设 ClickHouse 运行在 `localhost:9000`）：

```bash
# 1. 克隆仓库
git clone git@github.com:sentioxyz/clickhouse-proxy.git
cd clickhouse-proxy

# 2. 编译
go build -o clickhouse-proxy ./cmd/proxy/

# 3. 运行（使用环境变量指定上游地址）
CK_LISTEN=":9001" CK_UPSTREAM="localhost:9000" ./clickhouse-proxy

# 4. 用 clickhouse-client 连接 proxy 测试
clickhouse-client --host localhost --port 9001
```

---

## 编译指南

本项目同时支持原生 Go 编译和 Bazel 构建（与 `sentio-core` 保持一致，推荐使用）。

### 使用 Bazel 编译

本项目使用 Bazel `8.5.1` 和 Bzlmod 进行依赖管理，包括对预编译版的 `protoc` 及交叉编译 C 工具链的支持：

```bash
# 编译 proxy 二进制文件
bazel build //cmd/proxy:clickhouse-proxy

# 编译并运行所有测试
bazel test //...

# 如果需要更新第三方 CGO 依赖（如需调整 patch）：
bazel mod tidy
bazel run //:gazelle
```

编译完成后，二进制文件会生成在 `bazel-bin/cmd/proxy/clickhouse-proxy_/%workspace%/cmd/proxy/clickhouse-proxy` 路径下，你也可以通过 `bazel run //cmd/proxy:clickhouse-proxy` 直接执行。

### 使用 Go 原生编译

```bash
# 标准编译
go build -o clickhouse-proxy ./cmd/proxy/

# 静态编译（推荐用于生产，无 CGO 依赖）
CGO_ENABLED=0 go build -o clickhouse-proxy ./cmd/proxy/

# 也可以使用 Makefile
make build
```

编译完成后会在当前目录生成 `clickhouse-proxy` 二进制文件。

---

## 配置详解

### 配置文件

proxy 支持 JSON 格式的配置文件。配置的加载顺序为：

1. 命令行参数 `-config /path/to/config.json`
2. 环境变量 `CK_CONFIG` 指定的路径
3. 当前目录下的 `config.json`（自动检测）
4. 以上都没有时，使用内置默认值

示例配置文件 (`config.example.json`)：

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

### 完整参数表

#### 基础配置

| 参数 | 类型 | 是否必填 | 默认值 | 说明 |
|------|------|---------|-------|------|
| `listen` | string | 否 | `:9001` | proxy 监听地址和端口 |
| `upstream` | string | 否 | `clickhouse:9000` | 上游 ClickHouse 服务器地址 |
| `dial_timeout` | duration | 否 | `5s` | 连接上游的超时时间 |
| `idle_timeout` | duration | 否 | `5m` | 空闲连接超时时间，超时后断开 |
| `max_connection_lifetime` | duration | 否 | `24h` | 单个连接的最大存活时间，防止慢客户端无限占用资源 |
| `shutdown_timeout` | duration | 否 | `30s` | 优雅关闭时等待在途连接排水的最大时间 |
| `stats_interval` | duration | 否 | `10s` | 统计信息打印间隔 |
| `metrics_listen` | string | 否 | `:9091` | Prometheus metrics HTTP 端点监听地址 |

#### 日志配置

| 参数 | 类型 | 是否必填 | 默认值 | 说明 |
|------|------|---------|-------|------|
| `log_queries` | bool | 否 | `true` | 是否在日志中记录 SQL 查询内容 |
| `log_data` | bool | 否 | `false` | 是否在日志中记录 Data 包内容（通常关闭，仅调试用） |
| `max_query_log_bytes` | int | 否 | `300` | 查询日志最大截断长度（字节） |
| `max_data_log_bytes` | int | 否 | `200` | Data 包日志最大截断长度（字节） |

#### 认证配置

| 参数 | 类型 | 是否必填 | 默认值 | 说明 |
|------|------|---------|-------|------|
| `auth_enabled` | bool | 否 | `false` | 是否启用 JWS/以太坊签名认证 |
| `auth_allowed_addresses` | []string | 否 | `[]` | 允许执行查询的以太坊地址列表 |
| `auth_max_token_age` | duration | 否 | `1m` | JWS token 最大有效期 |
| `auth_allow_no_auth` | bool | 否 | `false` | 是否允许不携带 token 的请求通过 |
| `relay_private_key_hex` | string | 否 | （空） | 用于签发 relay JWS token 的以太坊私钥，在 proxy 间 `__route__` 连接中使用。集群内所有 proxy 应使用相同的私钥，对应地址须在 `auth_allowed_addresses` 中 |

#### SQL 重写配置

| 参数 | 类型 | 是否必填 | 默认值 | 说明 |
|------|------|---------|-------|------|
| `rewriter_service_addr` | string | 否 | `localhost:50051` | sql-rewriter gRPC 服务地址 |
| `rewriter_timeout` | duration | 否 | `5s` | SQL 重写请求超时时间 |

#### 网络状态配置

| 参数 | 类型 | 是否必填 | 默认值 | 说明 |
|------|------|---------|-------|------|
| `network_state_redis` | string | 是 | （空） | Redis statemirror 地址（如 `localhost:6379`） |


#### 高级配置

| 参数 | 类型 | 是否必填 | 默认值 | 说明 |
|------|------|---------|-------|------|
| `streaming_buf_size` | int | 否 | `131072` | 流式协议解析的 bufio 缓冲区大小（字节），默认 128KB |
| `validate_checksum` | bool | 否 | `false` | 是否启用压缩数据块的 CityHash128 校验 |

> **提示**：所有 `duration` 类型参数支持人类可读格式，如 `"5s"`、`"1m"`、`"24h"`。也支持纳秒数字。

---

## 运行

### 使用配置文件

```bash
./clickhouse-proxy -config config.json
```



### 使用 go run（开发模式）

```bash
go run ./cmd/proxy/ -config config.json
```

proxy 启动后，日志会显示监听地址和所有关键配置：

```
clickhouse-proxy starting. listen=:9001 upstream=127.0.0.1:9000 ...
metrics listening on :9091
```

按 `Ctrl+C` 优雅关闭，关闭前会打印最终统计信息。

---



在没有 Docker 的环境下，直接编译并运行二进制文件：

```bash
# 1. 静态编译（推荐，适用于目标机器没有 Go 环境的情况）
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o clickhouse-proxy ./cmd/proxy/

# 2. 将二进制文件和配置拷贝到目标机器
scp clickhouse-proxy config.json user@target-host:/opt/clickhouse-proxy/

# 3. 在目标机器上运行
ssh user@target-host
cd /opt/clickhouse-proxy
./clickhouse-proxy -config config.json

# 4. (可选) 使用 systemd 管理服务
```

systemd 服务文件示例：

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

## 认证配置

proxy 支持基于 **以太坊 secp256k1 签名的 JWS 认证**。启用后，客户端必须通过 ClickHouse 自定义设置 `SQL_x_auth_token` 传递 JWS token。

### 启用认证

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

### 客户端使用示例

```go
// 使用 clickhouse-go SDK
ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
    "SQL_x_auth_token": clickhouse.CustomSetting{Value: jwsToken},
}))
rows, err := conn.Query(ctx, "SELECT 1")
```

### JWS Token 格式

**Payload** 包含两个字段：
- `iat`：Unix 时间戳（签发时间）
- `qhash`：SQL 查询的 Keccak256 哈希（带 `0x` 前缀）

支持 JWS Compact 序列化（单签名）和 JWS JSON 序列化（多签名）两种格式。

### Relay Token 传播

在多 proxy 集群中，当 ClickHouse 通过 `__route__` 连接发起 `remote()` 子查询时（Proxy1 → ClickHouse → Proxy2），Proxy1 会自动签发 relay JWS token 并注入到查询设置中，然后转发到 Proxy2。

**工作流程：**
1. Proxy1 收到来自本地 ClickHouse 的 `__route__` 连接。
2. Proxy1 拦截第一个 Query 包，使用共享的以太坊私钥签发 relay JWS token。
3. Proxy1 通过 `SQL_x_auth_token` 设置注入 token，转发到 Proxy2。
4. Proxy2 通过普通的 JWS 验证流程校验 relay token。

**配置：**

```json
{
    "auth_enabled": true,
    "relay_private_key_hex": "shared-ethereum-private-key-hex",
    "auth_allowed_addresses": [
        "0x<relay-private-key-对应的以太坊地址>"
    ]
}
```

> **重要**：集群内所有 proxy 必须共享相同的 `relay_private_key_hex`，且对应的以太坊地址必须在 `auth_allowed_addresses` 中。

---

## SQL 重写配置

proxy 支持 **Sentio Network SQL 重写**，将 `sentio_<processor_id>.<table_name>` 格式的虚拟表名重写为实际的 ClickHouse `remote()` 表达式。该功能需要外部 gRPC 重写服务和网络状态数据源。

### 启用 SQL 重写

```json
{
    "rewriter_service_addr": "localhost:50051",
    "rewriter_timeout": "5s",
    "network_state_redis": "localhost:6379"
}
```

### 按查询跳过重写

当 SQL 重写全局开启时，客户端可以通过 ClickHouse 自定义设置 `SQL_skip_rewrite` 在 **单条查询** 级别跳过重写。这对于 `INSERT` 等不需要重写的语句尤其有用。

```go
// Go 客户端示例：INSERT 时跳过重写
ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
    "SQL_skip_rewrite": clickhouse.CustomSetting{Value: "1"},
}))
conn.Exec(ctx, "INSERT INTO sentio_eth.transfer ...")
```

> **注意**：`SQL_skip_rewrite` 是 Proxy 自定义设置，会在转发前自动剥离，不会发送到 ClickHouse Server。



---

## 测试

### 单元测试

```bash
# Go 原生
go test ./...
```

### 本地集成测试

验证 proxy 能正确转发查询和数据：

```bash
make test-forwarding
```

### 流式回放测试（生产级验证）

从运行中的 ClickHouse Pod 流式回放真实查询日志，验证 proxy 的正确性：

```bash
# 前提：需要配置好 kubectl 和 ClickHouse 集群访问权限

# 回放最近 1 小时的查询
make test-stream-replay POD=<pod-name>

# 只回放最近 100 条查询
make test-stream-replay POD=<pod-name> N=100

# 回放最近 30 天全部查询（压力测试）
make test-stream-replay POD=<pod-name> SINCE="30 day" N=0
```

成功标志：
- 测试结束时显示 `✅ All queries forwarded!`
- Failures 计数为 0
- proxy 日志中无 panic


