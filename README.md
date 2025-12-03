# ck_remote_proxy demo 说明

一个简易 ClickHouse 原生协议代理，用于嗅探客户端→服务端方向的包类型并统计，支持转发。现在支持通过配置文件控制监听/上游等参数，并预留了 Query 校验入口（当前默认放行）。

## 运行

### 配置

可以放置一个 `config.json`（默认为当前目录，如果不存在则使用内置默认值和 `CK_LISTEN` / `CK_UPSTREAM` 环境变量）：
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

其他可选参数：`-config /path/to/config.json` 或环境变量 `CK_CONFIG` 指定配置文件路径。

### 启动

在 proxy 容器里：
```bash
cd /workspace/clickhouse-proxy-demo
GOCACHE=/workspace/clickhouse-proxy-demo/.cache/go-build \
go run . -config config.json
```
默认每 10s 打印统计，Ctrl+C 会打印最终统计。未提供配置文件时，使用内置默认值与 `CK_LISTEN` / `CK_UPSTREAM` 环境变量。

## ck-dev 侧示例

假设 proxy IP 为 `172.17.0.2`：

建表（通过代理）：
```sql
CREATE TABLE default.t_mem (n UInt32) ENGINE=Memory;
```

插入（Query + Data）：
```sql
INSERT INTO FUNCTION remote('172.17.0.2:9001', 'default', 't_mem', 'default', '') VALUES (1),(2),(3);
```

查询：
```sql
SELECT sum(n) FROM remote('172.17.0.2:9001', 'default', 't_mem', 'default', '');
SELECT * FROM remote('172.17.0.2:9001', 'system', 'one', 'default', '');
SELECT name FROM remote('172.17.0.2:9001', 'system', 'tables', 'default', '') WHERE database='default' LIMIT 5;
```

## 线上扩展点

- 所有 native TCP 流量（包含 remote/cluster 跳转）都会走代理；`Query` 包在转发前调用 `Validator.ValidateQuery`（见 `main.go`），当前实现为 noop，可在此补充鉴权/审计。
- 监听/上游、超时（dial/idle）、统计周期与日志粒度均可通过配置文件调整；未配置时沿用默认值以保证服务能直接启动。
- 保留 TCP keepalive、读写 deadline，避免长时间空闲导致连接泄漏。

## 示例结果（节选）

包摘要日志（Query/Data）：
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

统计输出（最终一次）：
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

说明：
- 未出现 unknown，常见包类型均被识别。
- Query/Data 日志展示了 SQL 片段和数据块摘要，不影响转发。
