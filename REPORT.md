# remote() 嗅探结果

## 包类型统计
```
Hello             : 14
Query             : 17
Data              : 7
Ping              : 6
TablesStatusRequest: 9
unknown           : 0
```
- 常见包类型全部识别，unknown=0，未漏统。

## 捕获的包内容（部分）
- Query 包示例：
  - `DESC TABLE system.numbers`
  - `SELECT sum(number) FROM system.numbers`
  - `CREATE TABLE default.t_mem (n UInt32) ENGINE=Memory;`
  - `INSERT INTO FUNCTION remote('172.17.0.2:9001', 'default', 't_mem', 'default', '') VALUES ...`
  - `SELECT sum(n) FROM remote('172.17.0.2:9001', 'default', 't_mem', 'default', '')`
  - `SELECT name FROM system.tables WHERE database='default' LIMIT 5`
- Data 包示例（可读性不高）：
  - `Data packet (99 bytes): ... n UInt32 ...`
  - `Data packet (61 bytes): g C +   n UInt32`
  - `Data packet (38 bytes): l \z| F`


## 结论
- remote() 函数执行过程中出现 Hello/Query/Data/Ping/TablesStatusRequest 包型，未出现未知类型。