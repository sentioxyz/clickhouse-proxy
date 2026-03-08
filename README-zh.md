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

示例配置文件（不开启认证）：

```json
{
    // === 基础配置 ===
    "listen": ":9001",
    "upstream": "127.0.0.1:9000",
    "dial_timeout": "5s",
    "idle_timeout": "5m",
    "max_connection_lifetime": "24h",
    "shutdown_timeout": "30s",
    "stats_interval": "30s",
    "metrics_listen": ":9091",

    // === 日志配置 ===
    "log_queries": true,
    "log_data": false,
    "max_query_log_bytes": 300,
    "max_data_log_bytes": 200,

    // === 认证配置 ===
    "auth_enabled": false,

    // === SQL 重写配置 ===
    "rewriter_service_addr": "localhost:50051",
    "rewriter_timeout": "5s",

    // === 网络状态配置 ===
    "network_state_redis": "localhost:6379",

    // === 高级配置 ===
    "streaming_buf_size": 131072,
    "validate_checksum": false
}
```

示例配置文件（开启认证）：

```json
{
    // === 基础配置 ===
    "listen": ":9001",
    "upstream": "127.0.0.1:9000",
    "dial_timeout": "5s",
    "idle_timeout": "5m",
    "max_connection_lifetime": "24h",
    "shutdown_timeout": "30s",
    "stats_interval": "30s",
    "metrics_listen": ":9091",

    // === 日志配置 ===
    "log_queries": true,
    "log_data": false,
    "max_query_log_bytes": 300,
    "max_data_log_bytes": 200,

    // === 认证配置 ===
    "auth_enabled": true,
    "auth_allowed_addresses": [
      "0x12345678901234567890123456...",
      "YOUR_NEW_ADDRESS_HERE"
    ],
    "auth_max_token_age": "1m",
    "auth_allow_no_auth": false,
    "relay_private_key_hex": "0x12345678901234567890123456...",

    // === SQL 重写配置 ===
    "rewriter_service_addr": "localhost:50051",
    "rewriter_timeout": "5s",

    // === 网络状态配置 ===
    "network_state_redis": "localhost:6379",

    // === 高级配置 ===
    "streaming_buf_size": 131072,
    "validate_checksum": false
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
