# ClickHouse Proxy

一个轻量级的 ClickHouse 原生 TCP 协议代理。它在客户端与 ClickHouse 服务器之间透明转发流量，同时提供 **查询审计**、**JWS 认证**、**SQL 重写** 和 **Prometheus 监控** 等能力。

---

## 目录

- [环境准备](#环境准备)
- [快速开始](#快速开始)
- [编译指南](#编译指南)
  - [Go 原生编译](#go-原生编译)
  - [Bazel 编译](#bazel-编译)
- [配置详解](#配置详解)
  - [配置文件](#配置文件)
  - [环境变量](#环境变量)
  - [完整参数表](#完整参数表)
- [运行](#运行)
- [部署](#部署)
  - [Docker 部署](#docker-部署)
  - [裸机部署](#裸机部署)
  - [Kubernetes 部署](#kubernetes-部署)
- [认证配置](#认证配置)
- [SQL 重写配置](#sql-重写配置)
- [测试](#测试)
- [监控指标](#监控指标)

---

## 环境准备

| 依赖 | 版本要求 | 说明 |
|------|---------|------|
| Go   | 1.25+   | 必需，用于编译 |
| Bazel | 8.0+   | 可选，项目也支持 Bazel 编译 |
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

### Go 原生编译

```bash
# 标准编译
go build -o clickhouse-proxy ./cmd/proxy/

# 静态编译（推荐用于生产，无 CGO 依赖）
CGO_ENABLED=0 go build -o clickhouse-proxy ./cmd/proxy/

# 也可以使用 Makefile
make build
```

编译完成后会在当前目录生成 `clickhouse-proxy` 二进制文件。

### Bazel 编译

项目使用 Bazel 8.0 + bzlmod 管理构建，Go SDK 版本为 1.25.3。

```bash
# 安装 Bazel（如未安装）
# macOS:
brew install bazel
# 或参考 https://bazel.build/install

# 确认 Bazel 版本（项目要求 8.0，见 .bazelversion 文件）
bazel --version

# 编译
bazel build //cmd/proxy:proxy

# 输出的二进制位于：
ls bazel-bin/cmd/proxy/proxy_/proxy

# 运行测试
bazel test //pkg/proxy:proxy_test
```

> **注意**：Bazel 首次编译会下载依赖，时间较长。后续的增量编译会非常快。

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
    "auth_enabled": false,
    "rewriter_enabled": false
}
```

### 环境变量

以下配置项支持通过环境变量覆盖（优先级低于配置文件）：

| 环境变量 | 对应配置项 | 默认值 |
|---------|-----------|-------|
| `CK_LISTEN` | `listen` | `:9001` |
| `CK_UPSTREAM` | `upstream` | `clickhouse:9000` |
| `CK_METRICS_LISTEN` | `metrics_listen` | `:9091` |
| `CK_CONFIG` | 配置文件路径 | （无） |
| `CK_REWRITER_ADDR` | `rewriter_service_addr` | `localhost:50051` |
| `CK_NETWORK_STATE_SOURCE` | `network_state_source` | `file` |
| `CK_NETWORK_STATE_FILE` | `network_state_file` | （无） |
| `CK_NETWORK_STATE_POSTGRES` | `network_state_postgres` | （无） |
| `CK_CH_USER` | `ch_user` | `default` |
| `CK_CH_PASSWORD` | `ch_password` | （无） |

### 完整参数表

#### 基础配置

| 参数 | 类型 | 默认值 | 说明 |
|------|------|-------|------|
| `listen` | string | `:9001` | proxy 监听地址和端口 |
| `upstream` | string | `clickhouse:9000` | 上游 ClickHouse 服务器地址 |
| `dial_timeout` | duration | `5s` | 连接上游的超时时间 |
| `idle_timeout` | duration | `5m` | 空闲连接超时时间，超时后断开 |
| `max_connection_lifetime` | duration | `24h` | 单个连接的最大存活时间，防止慢客户端无限占用资源 |
| `shutdown_timeout` | duration | `30s` | 优雅关闭时等待在途连接排水的最大时间 |
| `stats_interval` | duration | `10s` | 统计信息打印间隔 |
| `metrics_listen` | string | `:9091` | Prometheus metrics HTTP 端点监听地址 |

#### 日志配置

| 参数 | 类型 | 默认值 | 说明 |
|------|------|-------|------|
| `log_queries` | bool | `true` | 是否在日志中记录 SQL 查询内容 |
| `log_data` | bool | `false` | 是否在日志中记录 Data 包内容（通常关闭，仅调试用） |
| `max_query_log_bytes` | int | `300` | 查询日志最大截断长度（字节） |
| `max_data_log_bytes` | int | `200` | Data 包日志最大截断长度（字节） |

#### 认证配置

| 参数 | 类型 | 默认值 | 说明 |
|------|------|-------|------|
| `auth_enabled` | bool | `false` | 是否启用 JWS/以太坊签名认证 |
| `auth_allowed_addresses` | []string | `[]` | 允许执行查询的以太坊地址列表 |
| `auth_max_token_age` | duration | `1m` | JWS token 最大有效期 |
| `auth_allow_no_auth` | bool | `false` | 是否允许不携带 token 的请求通过 |

#### SQL 重写配置

| 参数 | 类型 | 默认值 | 说明 |
|------|------|-------|------|
| `rewriter_enabled` | bool | `false` | 是否启用 SQL 重写功能 |
| `rewriter_service_addr` | string | `localhost:50051` | sql-rewriter gRPC 服务地址 |
| `rewriter_local_indexer_id` | uint64 | `0` | 本地 Indexer 节点 ID |
| `rewriter_timeout` | duration | `5s` | SQL 重写请求超时时间 |

#### 网络状态配置

| 参数 | 类型 | 默认值 | 说明 |
|------|------|-------|------|
| `network_state_source` | string | `file` | 网络状态数据来源，支持 `file` |
| `network_state_file` | string | （空） | 网络状态 YAML 文件路径 |
| `network_state_postgres` | string | （空） | PostgreSQL 连接串（预留） |

#### ClickHouse 凭证

| 参数 | 类型 | 默认值 | 说明 |
|------|------|-------|------|
| `ch_user` | string | `default` | 连接上游 ClickHouse 的用户名 |
| `ch_password` | string | （空） | 连接上游 ClickHouse 的密码 |

#### 高级配置

| 参数 | 类型 | 默认值 | 说明 |
|------|------|-------|------|
| `streaming_buf_size` | int | `131072` | 流式协议解析的 bufio 缓冲区大小（字节），默认 128KB |
| `validate_checksum` | bool | `false` | 是否启用压缩数据块的 CityHash128 校验 |

> **提示**：所有 `duration` 类型参数支持人类可读格式，如 `"5s"`、`"1m"`、`"24h"`。也支持纳秒数字。

---

## 运行

### 使用配置文件

```bash
./clickhouse-proxy -config config.json
```

### 使用环境变量

```bash
CK_LISTEN=":9001" CK_UPSTREAM="10.0.0.5:9000" ./clickhouse-proxy
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

## 部署

### Docker 部署

#### 构建镜像

```bash
# 本地构建
docker build -t clickhouse-proxy:latest .

# 构建并推送到私有仓库
make docker push
```

#### 运行容器

```bash
# 最简运行（使用容器内默认配置路径 /app/config.json）
docker run -d \
  --name clickhouse-proxy \
  -p 9001:9001 \
  -p 9091:9091 \
  clickhouse-proxy:latest

# 挂载配置文件运行
docker run -d \
  --name clickhouse-proxy \
  -p 9001:9001 \
  -p 9091:9091 \
  -v /path/to/config.json:/app/config.json \
  clickhouse-proxy:latest

# 使用环境变量运行（不需要配置文件）
docker run -d \
  --name clickhouse-proxy \
  -p 9001:9001 \
  -e CK_LISTEN=":9001" \
  -e CK_UPSTREAM="clickhouse-server:9000" \
  clickhouse-proxy:latest
```

> **说明**：Docker 镜像使用多阶段构建，基于 `alpine:latest`，体积很小。运行时进程为 `/app/clickhouse-proxy`。

#### 镜像信息

| 项目 | 说明 |
|------|------|
| 构建阶段基础镜像 | `golang:1.25-alpine` |
| 运行时基础镜像 | `alpine:latest` |
| 工作目录 | `/app` |
| 默认配置路径 | `/app/config.json` |
| 运行时依赖 | `ca-certificates`、`tzdata` |

### 裸机部署

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
# 创建 /etc/systemd/system/clickhouse-proxy.service
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

### Kubernetes 部署

项目中的 `auth_ck.yaml` 提供了完整的 Kubernetes 部署示例，包含 ConfigMap + Sidecar 模式：

```bash
kubectl apply -f auth_ck.yaml
```

生产环境推荐使用 **ConfigMap** 管理配置文件。

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

---

## SQL 重写配置

proxy 支持 **Sentio Network SQL 重写**，将 `sentio_<processor_id>.<table_name>` 格式的虚拟表名重写为实际的 ClickHouse `remote()` 表达式。该功能需要外部 gRPC 重写服务支持。

### 启用 SQL 重写

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

## 测试

### 单元测试

```bash
# Go 原生
go test ./...

# Bazel
bazel test //pkg/proxy:proxy_test
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

---

## 监控指标

proxy 通过 `metrics_listen` 端口（默认 `:9091`）暴露 Prometheus 指标。

### 关键指标

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `clickhouse_proxy_active_connections` | Gauge | 当前活跃连接数 |
| `clickhouse_proxy_packets_total` | Counter | 客户端→服务器方向的包总数（按类型分） |
| `clickhouse_proxy_server_packets_total` | Counter | 服务器→客户端方向的包总数（按类型分） |
| `clickhouse_proxy_bytes_transferred_total` | Counter | 传输字节总数（按方向分） |
| `clickhouse_proxy_queries_forwarded_total` | Counter | 成功转发的查询总数 |
| `clickhouse_proxy_errors_total` | Counter | 错误总数（按阶段和错误类型分） |
| `clickhouse_proxy_upstream_health` | Gauge | 上游 ClickHouse 健康状态（1=健康, 0=不可达） |
| `clickhouse_proxy_query_decode_duration_seconds` | Histogram | Query 包解码耗时 |
| `clickhouse_proxy_rewrite_duration_seconds` | Histogram | SQL 重写耗时 |
| `clickhouse_proxy_handshake_duration_seconds` | Histogram | TCP 握手耗时 |
| `clickhouse_proxy_fallback_total` | Counter | 降级到原始拷贝模式的次数 |
| `clickhouse_proxy_streaming_data_blocks_total` | Counter | 流式模式处理的数据块数 |

### 接入 Prometheus

```yaml
scrape_configs:
  - job_name: 'clickhouse-proxy'
    static_configs:
      - targets: ['localhost:9091']
```

### 接入 Grafana

项目中的 `dashboard.json` 提供了预配置的 Grafana 仪表板，可直接导入使用。
