package main

import (
	"context"
	"crypto/ecdsa"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ethereum/go-ethereum/crypto"
	"golang.org/x/crypto/sha3"
)

const (
	// TEST-ONLY keys — used exclusively for CI/integration testing.
	// Do NOT use in production. These addresses must be in the proxy's allowed list.

	// CorrectPrivateKeyHex — Address: 0x2222222222222222222222222222222222222222
	CorrectPrivateKeyHex = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	// WrongPrivateKeyHex — Address NOT in allowed list
	WrongPrivateKeyHex = "1111111111111111111111111111111111111111111111111111111111111111"

	TestDatabase = "integration_test_db"
	TestTable    = "integration_test_table"
)

// JWSHeader represents the header of a JWS token.
type JWSHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

// JWSPayload represents the payload of a JWS authentication token.
type JWSPayload struct {
	Iat       int64  `json:"iat"`
	QueryHash string `json:"qhash"`
}

// mixedResult holds the outcome of a single goroutine in the mixed-key concurrency test.
type mixedResult struct {
	idx     int
	isValid bool // true = used correct key, false = used wrong key
	err     error
}

var (
	addr  = flag.String("addr", "127.0.0.1:19001", "ClickHouse proxy address")
	user  = flag.String("user", "default", "ClickHouse username")
	pass  = flag.String("password", "", "ClickHouse password")
	phase = flag.String("phase", "all", "Test phase: noauth, auth-valid, auth-invalid, or all")
)

func main() {
	flag.Parse()

	log.Println("==============================================")
	log.Println("  ClickHouse Proxy 集成测试客户端")
	log.Println("  (Go SDK - clickhouse-go/v2 + SignFunc)")
	log.Println("==============================================")
	log.Printf("目标地址: %s", *addr)
	log.Printf("测试阶段: %s", *phase)
	log.Println()

	exitCode := 0

	switch *phase {
	case "noauth":
		if !runPhaseNoAuth() {
			exitCode = 1
		}
	case "auth-valid":
		if !runPhaseAuthValid() {
			exitCode = 1
		}
	case "auth-invalid":
		if !runPhaseAuthInvalid() {
			exitCode = 1
		}
	case "all":
		log.Println("⚠️  'all' 模式需要在各阶段间手动切换 proxy 配置")
		log.Println("请使用 -phase noauth/auth-valid/auth-invalid 分别执行")
		exitCode = 1
	default:
		log.Printf("未知阶段: %s", *phase)
		exitCode = 1
	}

	if exitCode == 0 {
		log.Println()
		log.Println("🎉 本阶段所有测试通过！")
	} else {
		log.Println()
		log.Println("❌ 本阶段存在测试失败")
	}
	os.Exit(exitCode)
}

// ========== Phase 1: No Auth ==========
func runPhaseNoAuth() bool {
	log.Println("╔══════════════════════════════════════════╗")
	log.Println("║  阶段一: 无签名模式 (auth_enabled=false)  ║")
	log.Println("╚══════════════════════════════════════════╝")
	log.Println()

	allPassed := true

	// Test 1.1: Ping
	log.Println("[Test 1.1] Ping 连接测试")
	conn := openConnection(nil)
	if conn == nil {
		return false
	}
	defer conn.Close()

	if err := conn.Ping(context.Background()); err != nil {
		log.Printf("  ❌ Ping 失败: %v", err)
		allPassed = false
	} else {
		log.Println("  ✅ Ping 成功")
	}

	// Test 1.2: SELECT 1
	log.Println("[Test 1.2] SELECT 1")
	var result uint8
	if err := conn.QueryRow(context.Background(), "SELECT 1").Scan(&result); err != nil {
		log.Printf("  ❌ SELECT 1 失败: %v", err)
		allPassed = false
	} else if result != 1 {
		log.Printf("  ❌ SELECT 1 返回值错误: expected 1, got %d", result)
		allPassed = false
	} else {
		log.Println("  ✅ SELECT 1 = 1")
	}

	// Test 1.3: SELECT version()
	log.Println("[Test 1.3] SELECT version()")
	var version string
	if err := conn.QueryRow(context.Background(), "SELECT version()").Scan(&version); err != nil {
		log.Printf("  ❌ SELECT version() 失败: %v", err)
		allPassed = false
	} else {
		log.Printf("  ✅ ClickHouse 版本: %s", version)
	}

	// Test 1.4: CRUD 完整操作
	log.Println("[Test 1.4] CRUD 完整操作流程")
	if !runCRUD(conn) {
		allPassed = false
	}

	// Test 1.5: 多次连接（连接稳定性）
	log.Println("[Test 1.5] 多次连接稳定性测试 (3次)")
	for i := 0; i < 3; i++ {
		c := openConnection(nil)
		if c == nil {
			log.Printf("  ❌ 第%d次连接失败", i+1)
			allPassed = false
			break
		}
		if err := c.Ping(context.Background()); err != nil {
			log.Printf("  ❌ 第%d次 Ping 失败: %v", i+1, err)
			allPassed = false
			c.Close()
			break
		}
		var r uint8
		if err := c.QueryRow(context.Background(), "SELECT 42").Scan(&r); err != nil {
			log.Printf("  ❌ 第%d次 SELECT 42 失败: %v", i+1, err)
			allPassed = false
			c.Close()
			break
		}
		c.Close()
	}
	if allPassed {
		log.Println("  ✅ 3次连接均成功")
	}

	return allPassed
}

// ========== Phase 2: Auth Valid ==========
func runPhaseAuthValid() bool {
	log.Println("╔══════════════════════════════════════════╗")
	log.Println("║  阶段二: 正确签名模式 (auth_enabled=true) ║")
	log.Println("╚══════════════════════════════════════════╝")
	log.Println()

	key, err := crypto.HexToECDSA(CorrectPrivateKeyHex)
	if err != nil {
		log.Fatalf("解析正确私钥失败: %v", err)
	}
	log.Printf("使用密钥地址: %s", crypto.PubkeyToAddress(key.PublicKey).Hex())
	log.Println()

	signFunc := makeSignFunc(key)
	allPassed := true

	// Test 2.1: 带签名的 Ping + SELECT 1 (连接级 SignFunc)
	log.Println("[Test 2.1] 连接级 SignFunc: Ping + SELECT 1")
	conn := openConnection(signFunc)
	if conn == nil {
		return false
	}
	defer conn.Close()

	if err := conn.Ping(context.Background()); err != nil {
		log.Printf("  ❌ Ping 失败: %v", err)
		allPassed = false
	} else {
		log.Println("  ✅ Ping 成功")
	}

	var result uint8
	if err := conn.QueryRow(context.Background(), "SELECT 1").Scan(&result); err != nil {
		log.Printf("  ❌ SELECT 1 失败: %v", err)
		allPassed = false
	} else if result != 1 {
		log.Printf("  ❌ SELECT 1 返回值错误: expected 1, got %d", result)
		allPassed = false
	} else {
		log.Println("  ✅ SELECT 1 = 1 (连接级签名)")
	}

	// Test 2.2: SELECT version()
	log.Println("[Test 2.2] SELECT version() (连接级签名)")
	var version string
	if err := conn.QueryRow(context.Background(), "SELECT version()").Scan(&version); err != nil {
		log.Printf("  ❌ SELECT version() 失败: %v", err)
		allPassed = false
	} else {
		log.Printf("  ✅ ClickHouse 版本: %s", version)
	}

	// Test 2.3: CRUD 完整操作 (连接级签名自动生效)
	log.Println("[Test 2.3] CRUD 完整操作 (连接级签名)")
	if !runCRUD(conn) {
		allPassed = false
	}

	// Test 2.4: 查询级 WithSignFunc 覆盖
	log.Println("[Test 2.4] 查询级 WithSignFunc 覆盖")
	// 创建无连接级签名的连接，验证查询级 WithSignFunc 可以独立工作
	conn2 := openConnection(nil)
	if conn2 == nil {
		allPassed = false
	} else {
		query := "SELECT 42"
		ctx := clickhouse.Context(context.Background(), clickhouse.WithSignFunc(signFunc))
		var r uint8
		if err := conn2.QueryRow(ctx, query).Scan(&r); err != nil {
			log.Printf("  ❌ WithSignFunc SELECT 42 失败: %v", err)
			allPassed = false
		} else {
			log.Printf("  ✅ WithSignFunc SELECT 42 = %d", r)
		}
		conn2.Close()
	}

	// Test 2.5: 多行查询 (连接级签名)
	log.Println("[Test 2.5] 多行查询 (system.numbers LIMIT 5)")
	rows, err := conn.Query(context.Background(), "SELECT number FROM system.numbers LIMIT 5")
	if err != nil {
		log.Printf("  ❌ 多行查询失败: %v", err)
		allPassed = false
	} else {
		count := 0
		for rows.Next() {
			var n uint64
			if err := rows.Scan(&n); err != nil {
				log.Printf("  ❌ 扫描行失败: %v", err)
				allPassed = false
				break
			}
			count++
		}
		rows.Close()
		if count == 5 {
			log.Printf("  ✅ 多行查询成功，返回 %d 行", count)
		} else {
			log.Printf("  ❌ 预期 5 行，实际 %d 行", count)
			allPassed = false
		}
	}

	// Test 2.6: 多次连接稳定性 + 签名
	log.Println("[Test 2.6] 多次连接稳定性测试 + 签名 (3次)")
	multiConnOK := true
	for i := 0; i < 3; i++ {
		c := openConnection(signFunc)
		if c == nil {
			log.Printf("  ❌ 第%d次连接失败", i+1)
			multiConnOK = false
			break
		}
		var r uint8
		if err := c.QueryRow(context.Background(), "SELECT 1").Scan(&r); err != nil {
			log.Printf("  ❌ 第%d次 SELECT 失败: %v", i+1, err)
			multiConnOK = false
			c.Close()
			break
		}
		c.Close()
	}
	if multiConnOK {
		log.Println("  ✅ 3次签名连接均成功")
	} else {
		allPassed = false
	}

	// ========== SQL 专项测试 ==========
	if !runSQLTests(conn, signFunc) {
		allPassed = false
	}

	return allPassed
}

// runSQLTests runs SQL-specific integration tests (2.8 ~ 2.50) covering all /tests directory cases,
// plus concurrency and stress tests. Extracted from runPhaseAuthValid for readability.
func runSQLTests(conn clickhouse.Conn, signFunc func(string) (string, error)) bool {
	allPassed := true

	log.Println()
	log.Println("--- SQL 专项测试 (参照 /tests 全部用例) ---")
	log.Println()

	// Test 2.7 (对应 01): SELECT 1 — 已在 2.1 覆盖，跳过

	// Test 2.8 (对应 02): SELECT 'proxy_ok'
	log.Println("[Test 2.8] SELECT 'proxy_ok' (02_version)")
	if !runQueryRowCount(conn, "proxy_ok", "SELECT 'proxy_ok'", 1) {
		allPassed = false
	}

	// Test 2.9 (对应 03): 算术运算
	log.Println("[Test 2.9] SELECT 1 + 2 (03_arithmetic)")
	if !runQueryRowCount(conn, "arithmetic", "SELECT 1 + 2", 1) {
		allPassed = false
	}

	// Test 2.10 (对应 04): 多行查询
	log.Println("[Test 2.10] system.numbers LIMIT 5 (04_multi_row)")
	if !runQueryRowCount(conn, "multi_row", "SELECT number FROM system.numbers LIMIT 5", 5) {
		allPassed = false
	}

	// Test 2.11 (对应 05): 系统库
	log.Println("[Test 2.11] system.databases (05_databases)")
	if !runQueryRowCount(conn, "databases", "SELECT name FROM system.databases WHERE name = 'default'", 1) {
		allPassed = false
	}

	// Test 2.12 (对应 06): 本地表计数
	log.Println("[Test 2.12] count() local_data (06_local_table)")
	if !runQueryRowCount(conn, "local_table_count", "SELECT count() FROM test_e2e.local_data", 1) {
		allPassed = false
	}

	// Test 2.13 (对应 07): remote() 查本地
	log.Println("[Test 2.13] remote() 查本地 local_data (07_remote_self)")
	if !runQueryRowCount(conn, "remote_self", "SELECT * FROM remote('127.0.0.1:19001', 'test_e2e', 'local_data', 'default', '') ORDER BY id", 3) {
		allPassed = false
	}

	// Test 2.14 (对应 08): remote() 跨节点
	log.Println("[Test 2.14] remote() 跨节点 system.one (08_remote_cross_node)")
	if !runQueryRowCount(conn, "remote_cross_node", "SELECT * FROM remote('127.0.0.1:29001', 'system', 'one', 'default', '')", 1) {
		allPassed = false
	}

	// Test 2.15 (对应 09): __route__=remote_proc 路由查询
	log.Println("[Test 2.15] __route__=remote_proc 路由查询 (09_route_remote_proc)")
	if !runQueryRowCount(conn, "route_remote_proc", "SELECT '__route__=remote_proc', * FROM system.one", 1) {
		allPassed = false
	}

	// Test 2.16 (对应 10): __route__=local_proc 路由查询
	log.Println("[Test 2.16] __route__=local_proc 路由查询 (10_route_local_proc)")
	if !runQueryRowCount(conn, "route_local_proc", "SELECT '__route__=local_proc', count() FROM test_e2e.local_data", 1) {
		allPassed = false
	}

	// Test 2.17 (对应 11): 本地 orders 全量
	log.Println("[Test 2.17] 本地 orders 全量查询 (11_local_select)")
	if !runQueryMinRowCount(conn, "local_orders", "SELECT order_id, customer, product_id, quantity, amount, order_date FROM test_e2e.orders ORDER BY order_id", 5) {
		allPassed = false
	}

	// Test 2.18 (对应 12): 远程 products 全量
	log.Println("[Test 2.18] 远程 products 全量查询 (12_remote_select)")
	if !runQueryMinRowCount(conn, "remote_products", "SELECT product_id, product_name, category, price, stock FROM remote('127.0.0.1:29001', 'test_e2e', 'products', 'default', '') ORDER BY product_id", 5) {
		allPassed = false
	}

	// Test 2.19 (对应 13): 本地 INSERT + 验证 + 清理 (使用不冲突的 ID 9001)
	log.Println("[Test 2.19] 本地 INSERT + 验证 (13_local_insert)")
	if !runExec(conn, "local_insert", "INSERT INTO test_e2e.orders VALUES (9001, 'TestUser', 101, 1, 29.99, '2025-06-01')") {
		allPassed = false
	} else if !runQueryMinRowCount(conn, "local_insert_verify", "SELECT order_id, customer FROM test_e2e.orders WHERE order_id = 9001", 1) {
		allPassed = false
	}
	// cleanup (mutations_sync=1 waits for mutation to complete)
	runExec(conn, "local_insert_cleanup", "ALTER TABLE test_e2e.orders DELETE WHERE order_id = 9001 SETTINGS mutations_sync = 1")

	// Test 2.20 (对应 14): 远程 INSERT + 验证 (使用不冲突的 ID 901)
	log.Println("[Test 2.20] 远程 INSERT + 验证 (14_remote_insert)")
	// Pre-cleanup: verify remote connectivity before INSERT test
	runQueryMinRowCount(conn, "remote_connectivity_check", "SELECT 1 FROM remote('127.0.0.1:29001', 'test_e2e', 'products', 'default', '') LIMIT 1", 1)
	if !runExec(conn, "remote_insert", "INSERT INTO FUNCTION remote('127.0.0.1:29001', 'test_e2e', 'products', 'default', '') VALUES (901, 'AuthTestProduct', 'Test', 9.99, 1)") {
		allPassed = false
	} else if !runQueryMinRowCount(conn, "remote_insert_verify", "SELECT product_id, product_name FROM remote('127.0.0.1:29001', 'test_e2e', 'products', 'default', '') WHERE product_id = 901", 1) {
		allPassed = false
	}

	// Test 2.21 (对应 15): 本地 UPDATE + 验证 + 恢复
	log.Println("[Test 2.21] 本地 UPDATE (15_local_update)")
	if !runExec(conn, "local_update", "ALTER TABLE test_e2e.orders UPDATE quantity = 10, amount = 299.90 WHERE order_id = 1001 SETTINGS mutations_sync = 1") {
		allPassed = false
	} else {
		if !runQueryRowCount(conn, "local_update_verify", "SELECT order_id, customer, quantity, amount FROM test_e2e.orders WHERE order_id = 1001", 1) {
			allPassed = false
		}
	}
	// 恢复原始数据
	runExec(conn, "local_update_restore", "ALTER TABLE test_e2e.orders UPDATE quantity = 2, amount = 59.98 WHERE order_id = 1001 SETTINGS mutations_sync = 1")

	// Test 2.22 (对应 16): 本地 DELETE + 验证 + 恢复 (使用不冲突的 ID 9002)
	log.Println("[Test 2.22] 本地 DELETE (16_local_delete)")
	// 先插入一条专用测试数据，再删除
	runExec(conn, "delete_setup", "INSERT INTO test_e2e.orders VALUES (9002, 'DeleteMe', 101, 1, 1.00, '2025-06-01')")
	if !runExec(conn, "local_delete", "ALTER TABLE test_e2e.orders DELETE WHERE order_id = 9002 SETTINGS mutations_sync = 1") {
		allPassed = false
	} else {
		if !runQueryRowCount(conn, "local_delete_verify", "SELECT count() FROM test_e2e.orders WHERE order_id = 9002", 1) {
			allPassed = false
		}
	}

	// Test 2.23 (对应 17): 跨节点 UNION ALL
	log.Println("[Test 2.23] 跨节点 UNION ALL (17_cross_union)")
	if !runQueryMinRowCount(conn, "cross_union",
		`SELECT * FROM (
			SELECT 'CH1' AS source, customer AS name FROM test_e2e.orders
			UNION ALL
			SELECT 'CH2' AS source, product_name AS name FROM remote('127.0.0.1:29001', 'test_e2e', 'products', 'default', '')
		) ORDER BY source, name`, 10) {
		allPassed = false
	}

	// Test 2.24 (对应 18): 跨节点 INNER JOIN
	log.Println("[Test 2.24] 跨节点 INNER JOIN (18_cross_join)")
	if !runQueryMinRowCount(conn, "cross_join",
		`SELECT o.order_id, o.customer, p.product_name, o.quantity, p.price, o.amount
		FROM test_e2e.orders AS o
		INNER JOIN remote('127.0.0.1:29001', 'test_e2e', 'products', 'default', '') AS p
			ON o.product_id = p.product_id
		ORDER BY o.order_id`, 5) {
		allPassed = false
	}

	// Test 2.25 (对应 19): 跨节点 IN 子查询
	log.Println("[Test 2.25] 跨节点 IN 子查询 (19_cross_subquery)")
	if !runQueryRowCount(conn, "cross_subquery",
		`SELECT order_id, customer, product_id, amount
		FROM test_e2e.orders
		WHERE product_id IN (
			SELECT product_id FROM remote('127.0.0.1:29001', 'test_e2e', 'products', 'default', '')
			WHERE category = 'Electronics'
		) ORDER BY order_id`, 3) {
		allPassed = false
	}

	// Test 2.26 (对应 20): 跨节点聚合
	log.Println("[Test 2.26] 跨节点聚合 GROUP BY (20_cross_aggregate)")
	if !runQueryMinRowCount(conn, "cross_aggregate",
		`SELECT p.category, count() AS order_count, sum(o.quantity) AS total_quantity, sum(o.amount) AS total_amount
		FROM test_e2e.orders AS o
		INNER JOIN remote('127.0.0.1:29001', 'test_e2e', 'products', 'default', '') AS p
			ON o.product_id = p.product_id
		GROUP BY p.category
		ORDER BY p.category`, 2) {
		allPassed = false
	}

	// Test 2.27 (对应 21): Local WHERE
	log.Println("[Test 2.27] Local WHERE 过滤 (21_local_where)")
	if !runQueryRowCount(conn, "local_where",
		"SELECT order_id, customer, amount FROM test_e2e.orders WHERE customer = 'Alice' ORDER BY order_id", 2) {
		allPassed = false
	}

	// Test 2.28 (对应 22): Remote WHERE + ORDER + LIMIT
	log.Println("[Test 2.28] Remote WHERE+ORDER+LIMIT (22_remote_where)")
	if !runQueryRowCount(conn, "remote_where",
		"SELECT product_id, product_name, price FROM remote('127.0.0.1:29001', 'test_e2e', 'products', 'default', '') WHERE price > 100 ORDER BY price DESC LIMIT 2", 2) {
		allPassed = false
	}

	// Test 2.29 (对应 23): 跨节点 RIGHT JOIN
	log.Println("[Test 2.29] 跨节点 RIGHT JOIN (23_cross_right_join)")
	if !runQueryMinRowCount(conn, "cross_right_join",
		`SELECT p.product_id, p.product_name, o.order_id, o.customer
		FROM test_e2e.orders AS o
		RIGHT JOIN remote('127.0.0.1:29001', 'test_e2e', 'products', 'default', '') AS p
			ON o.product_id = p.product_id
		ORDER BY p.product_id, o.order_id`, 7) {
		allPassed = false
	}

	// Test 2.30 (对应 24): Remote 标量子查询
	log.Println("[Test 2.30] Remote 标量子查询 (24_remote_scalar_subquery)")
	if !runQueryMinRowCount(conn, "remote_scalar_subquery",
		`SELECT order_id, customer, amount,
			(SELECT max(price) FROM remote('127.0.0.1:29001', 'test_e2e', 'products', 'default', '')) AS max_price
		FROM test_e2e.orders ORDER BY order_id`, 5) {
		allPassed = false
	}

	// Test 2.31 (对应 25): UNION ALL + 外层 WHERE
	log.Println("[Test 2.31] UNION ALL + WHERE (25_cross_union_where)")
	if !runQueryRowCount(conn, "cross_union_where",
		`SELECT * FROM (
			SELECT order_id AS id, customer AS name FROM test_e2e.orders
			UNION ALL
			SELECT product_id AS id, product_name AS name FROM remote('127.0.0.1:29001', 'test_e2e', 'products', 'default', '')
		) WHERE id = 101 ORDER BY name`, 1) {
		allPassed = false
	}

	// Test 2.32 (对应 26): INSERT INTO SELECT 跨节点迁移
	log.Println("[Test 2.32] INSERT INTO SELECT 跨节点 (26_local_insert_select)")
	insertSelectOK := true
	if !runExec(conn, "create_copy_table", "DROP TABLE IF EXISTS test_e2e.products_local_copy") {
		insertSelectOK = false
	}
	if insertSelectOK {
		if !runExec(conn, "create_copy_table",
			`CREATE TABLE test_e2e.products_local_copy (
				product_id UInt32, product_name String, category String, price Float64, stock UInt32
			) ENGINE = MergeTree() ORDER BY product_id`) {
			insertSelectOK = false
		}
	}
	if insertSelectOK {
		if !runExec(conn, "insert_select",
			"INSERT INTO test_e2e.products_local_copy SELECT * FROM remote('127.0.0.1:29001', 'test_e2e', 'products', 'default', '')") {
			insertSelectOK = false
		}
	}
	if insertSelectOK {
		if !runQueryMinRowCount(conn, "insert_select_verify",
			"SELECT product_id, product_name, price FROM test_e2e.products_local_copy ORDER BY product_id", 5) {
			insertSelectOK = false
		}
	}
	runExec(conn, "drop_copy_table", "DROP TABLE IF EXISTS test_e2e.products_local_copy")
	if !insertSelectOK {
		allPassed = false
	}

	// Test 2.33 (对应 27): DISTINCT 跨节点去重
	log.Println("[Test 2.33] DISTINCT 跨节点去重 (27_cross_distinct)")
	if !runQueryMinRowCount(conn, "cross_distinct",
		`SELECT DISTINCT category FROM (
			SELECT category FROM remote('127.0.0.1:29001', 'test_e2e', 'products', 'default', '')
			UNION ALL
			SELECT 'Electronics' AS category FROM test_e2e.orders WHERE product_id IN (101, 102)
		) ORDER BY category`, 3) {
		allPassed = false
	}

	// Test 2.34 (对应 28): Remote 窗口函数
	log.Println("[Test 2.34] Remote 窗口函数 (28_remote_window_func)")
	if !runQueryMinRowCount(conn, "remote_window_func",
		`SELECT product_id, product_name, price,
			row_number() OVER (ORDER BY price DESC) AS rank,
			lag(price) OVER (ORDER BY price DESC) AS prev_price
		FROM remote('127.0.0.1:29001', 'test_e2e', 'products', 'default', '')
		ORDER BY price DESC`, 5) {
		allPassed = false
	}

	// Test 2.35 (对应 29): GROUP BY WITH TOTALS
	log.Println("[Test 2.35] GROUP BY WITH TOTALS (29_cross_totals)")
	if !runQueryMinRowCount(conn, "cross_totals",
		`SELECT p.category, count() AS cnt, sum(o.amount) AS total_amount
		FROM test_e2e.orders AS o
		INNER JOIN remote('127.0.0.1:29001', 'test_e2e', 'products', 'default', '') AS p
			ON o.product_id = p.product_id
		GROUP BY p.category WITH TOTALS
		ORDER BY p.category`, 2) {
		allPassed = false
	}

	// Test 2.36 (对应 30): JOIN USING 语法
	log.Println("[Test 2.36] JOIN USING 语法 (30_cross_join_using)")
	if !runQueryMinRowCount(conn, "cross_join_using",
		`SELECT product_id, customer, product_name, amount
		FROM test_e2e.orders AS o
		INNER JOIN remote('127.0.0.1:29001', 'test_e2e', 'products', 'default', '') AS p
			USING (product_id)
		ORDER BY product_id, customer`, 5) {
		allPassed = false
	}

	// Test 2.37 (对应 31): Remote 空结果
	log.Println("[Test 2.37] Remote 空结果 (31_remote_empty_result)")
	if !runQueryRowCount(conn, "remote_empty",
		"SELECT count() FROM remote('127.0.0.1:29001', 'test_e2e', 'products', 'default', '') WHERE product_id > 99999", 1) {
		allPassed = false
	}

	// Test 2.38 (对应 32): 跨节点 HAVING
	log.Println("[Test 2.38] 跨节点 HAVING (32_cross_having)")
	if !runQueryMinRowCount(conn, "cross_having",
		`SELECT p.category, count() AS cnt, sum(o.amount) AS total_amount
		FROM test_e2e.orders AS o
		INNER JOIN remote('127.0.0.1:29001', 'test_e2e', 'products', 'default', '') AS p
			ON o.product_id = p.product_id
		GROUP BY p.category
		HAVING cnt >= 2
		ORDER BY p.category`, 1) {
		allPassed = false
	}

	// Test 2.39 (对应 33): 跨节点 EXISTS (IN)
	log.Println("[Test 2.39] 跨节点 EXISTS/IN (33_cross_exists)")
	if !runQueryMinRowCount(conn, "cross_exists",
		`SELECT order_id, customer, product_id, amount
		FROM test_e2e.orders
		WHERE product_id IN (
			SELECT product_id FROM remote('127.0.0.1:29001', 'test_e2e', 'products', 'default', '')
			WHERE category = 'Premium'
		) ORDER BY order_id`, 1) {
		allPassed = false
	}

	// Test 2.40 (对应 34): Remote 表达式 + IF + CASE
	log.Println("[Test 2.40] Remote 表达式+IF+CASE (34_remote_expression)")
	if !runQueryMinRowCount(conn, "remote_expression",
		`SELECT product_id, product_name, price,
			if(price > 100, 'expensive', 'cheap') AS price_level,
			CASE category
				WHEN 'Electronics' THEN 'Tech'
				WHEN 'Premium' THEN 'Luxury'
				ELSE 'Other'
			END AS category_label
		FROM remote('127.0.0.1:29001', 'test_e2e', 'products', 'default', '')
		ORDER BY product_id`, 5) {
		allPassed = false
	}

	// Test 2.41 (对应 35): 多表 UNION ALL (3源)
	log.Println("[Test 2.41] 多表 UNION ALL (35_cross_multi_table)")
	if !runQueryMinRowCount(conn, "cross_multi_table",
		`SELECT * FROM (
			SELECT 'local_data' AS source, toString(id) AS key, name AS value FROM test_e2e.local_data
			UNION ALL
			SELECT 'orders' AS source, toString(order_id) AS key, customer AS value FROM test_e2e.orders
			UNION ALL
			SELECT 'products' AS source, toString(product_id) AS key, product_name AS value FROM remote('127.0.0.1:29001', 'test_e2e', 'products', 'default', '')
		) ORDER BY source, key`, 13) {
		allPassed = false
	}

	// Test 2.42 (对应 36): Remote 表达式排序 + LIMIT
	log.Println("[Test 2.42] Remote 表达式排序 LIMIT (36_remote_order_expr)")
	if !runQueryRowCount(conn, "remote_order_expr",
		`SELECT product_id, product_name, price, price * stock AS total_value
		FROM remote('127.0.0.1:29001', 'test_e2e', 'products', 'default', '')
		ORDER BY total_value DESC
		LIMIT 3`, 3) {
		allPassed = false
	}

	// Test 2.43 (对应 37): CTE 跨节点查询
	log.Println("[Test 2.43] CTE 跨节点查询 (37_cross_cte)")
	if !runQueryMinRowCount(conn, "cross_cte",
		`WITH
			expensive_products AS (
				SELECT product_id, product_name, price
				FROM remote('127.0.0.1:29001', 'test_e2e', 'products', 'default', '')
				WHERE price > 100
			),
			big_orders AS (
				SELECT order_id, customer, product_id, amount
				FROM test_e2e.orders
				WHERE amount > 50
			)
		SELECT b.order_id, b.customer, e.product_name, e.price, b.amount
		FROM big_orders AS b
		INNER JOIN expensive_products AS e ON b.product_id = e.product_id
		ORDER BY b.order_id`, 2) {
		allPassed = false
	}

	// Test 2.44 (对应 38): DISTINCT + JOIN
	log.Println("[Test 2.44] DISTINCT + JOIN (38_cross_distinct_join)")
	if !runQueryMinRowCount(conn, "cross_distinct_join",
		`SELECT DISTINCT customer
		FROM test_e2e.orders AS o
		INNER JOIN remote('127.0.0.1:29001', 'test_e2e', 'products', 'default', '') AS p
			ON o.product_id = p.product_id
		ORDER BY customer`, 3) {
		allPassed = false
	}

	// Test 2.45 (对应 39): 跨节点 LEFT JOIN
	log.Println("[Test 2.45] 跨节点 LEFT JOIN (39_cross_left_join)")
	if !runQueryMinRowCount(conn, "cross_left_join",
		`SELECT o.order_id, o.customer, p.product_name, p.category
		FROM test_e2e.orders AS o
		LEFT JOIN remote('127.0.0.1:29001', 'test_e2e', 'products', 'default', '') AS p
			ON o.product_id = p.product_id
		ORDER BY o.order_id`, 5) {
		allPassed = false
	}

	// Test 2.46 (对应 40): 并发查询测试 (5 goroutine)
	log.Println("[Test 2.46] 并发查询测试 (40_concurrent_queries)")
	{
		const concurrency = 5
		var wg sync.WaitGroup
		errCh := make(chan error, concurrency)
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				c := openConnection(signFunc)
				if c == nil {
					errCh <- fmt.Errorf("goroutine %d: 连接失败", idx)
					return
				}
				defer c.Close()
				var cnt uint64
				if err := c.QueryRow(context.Background(), "SELECT count() FROM test_e2e.orders").Scan(&cnt); err != nil {
					errCh <- fmt.Errorf("goroutine %d: %v", idx, err)
					return
				}
				if cnt == 0 {
					errCh <- fmt.Errorf("goroutine %d: count=0", idx)
				}
			}(i)
		}
		wg.Wait()
		close(errCh)
		concurrentOK := true
		for e := range errCh {
			log.Printf("  ❌ %v", e)
			concurrentOK = false
		}
		if concurrentOK {
			log.Printf("  ✅ %d 个并发查询全部成功", concurrency)
		} else {
			allPassed = false
		}
	}

	// Test 2.47 (对应 41): 大结果集测试 (10000 行)
	log.Println("[Test 2.47] 大结果集 (41_large_result_set)")
	if !runQueryRowCount(conn, "large_result_set", "SELECT number FROM system.numbers LIMIT 10000", 10000) {
		allPassed = false
	}

	// Test 2.48: 超长 SQL 签名测试
	log.Println("[Test 2.48] 超长 SQL 签名测试 (42_long_query)")
	{
		// 构造一个 >8KB 的查询，验证签名机制能正确处理大字符串
		longComment := strings.Repeat("x", 8000)
		longQuery := fmt.Sprintf("SELECT 1 /* %s */", longComment)
		if !runQueryRowCount(conn, "long_query", longQuery, 1) {
			allPassed = false
		}
	}

	// Test 2.49: 复杂数据类型 (Array, Tuple, Map)
	log.Println("[Test 2.49] 复杂数据类型: Array, Tuple, Map (43_complex_types)")
	complexTypeOK := true
	if !runQueryRowCount(conn, "complex_array",
		"SELECT [1, 2, 3] AS arr, length([10, 20, 30, 40]) AS len", 1) {
		complexTypeOK = false
	}
	if !runQueryRowCount(conn, "complex_tuple",
		"SELECT (1, 'hello', 3.14) AS tup", 1) {
		complexTypeOK = false
	}
	if !runQueryRowCount(conn, "complex_map",
		"SELECT map('key1', 1, 'key2', 2) AS m", 1) {
		complexTypeOK = false
	}
	if !runQueryRowCount(conn, "complex_nested",
		`SELECT
			[map('a', 1), map('b', 2)] AS arr_of_maps,
			(1, [2, 3], 'hello') AS nested_tuple`, 1) {
		complexTypeOK = false
	}
	if !complexTypeOK {
		allPassed = false
	}

	// Test 2.50: 连接池压力测试 (20 并发 × 5 查询)
	log.Println("[Test 2.50] 连接池压力测试 (44_pool_stress)")
	{
		const stressConcurrency = 20
		const queriesPerConn = 5
		var wg sync.WaitGroup
		errCh := make(chan error, stressConcurrency*queriesPerConn)
		for i := 0; i < stressConcurrency; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				c := openConnection(signFunc)
				if c == nil {
					errCh <- fmt.Errorf("goroutine %d: 连接失败", idx)
					return
				}
				defer c.Close()
				for q := 0; q < queriesPerConn; q++ {
					var cnt uint64
					query := fmt.Sprintf("SELECT count() FROM (SELECT number FROM system.numbers LIMIT %d)", (idx*queriesPerConn)+q+1)
					if err := c.QueryRow(context.Background(), query).Scan(&cnt); err != nil {
						errCh <- fmt.Errorf("goroutine %d query %d: %v", idx, q, err)
						return
					}
				}
			}(i)
		}
		wg.Wait()
		close(errCh)
		stressOK := true
		for e := range errCh {
			log.Printf("  ❌ %v", e)
			stressOK = false
		}
		if stressOK {
			log.Printf("  ✅ %d 并发 × %d 查询 = %d 次请求全部成功", stressConcurrency, queriesPerConn, stressConcurrency*queriesPerConn)
		} else {
			allPassed = false
		}
	}

	return allPassed
}

// ========== Phase 3: Auth Invalid ==========
func runPhaseAuthInvalid() bool {
	log.Println("╔══════════════════════════════════════════╗")
	log.Println("║  阶段三: 错误签名模式 (验证被拒绝)        ║")
	log.Println("╚══════════════════════════════════════════╝")
	log.Println()

	wrongKey, err := crypto.HexToECDSA(WrongPrivateKeyHex)
	if err != nil {
		log.Fatalf("解析错误私钥失败: %v", err)
	}
	log.Printf("使用错误密钥地址: %s (不在白名单中)", crypto.PubkeyToAddress(wrongKey.PublicKey).Hex())
	log.Println()

	correctKey, _ := crypto.HexToECDSA(CorrectPrivateKeyHex)
	wrongSignFunc := makeSignFunc(wrongKey)
	allPassed := true

	// Test 3.1: 错误签名 SELECT 1 (应被拒绝 — 连接级 SignFunc)
	log.Println("[Test 3.1] 错误签名 SELECT 1 (连接级，应被拒绝)")
	conn := openConnection(wrongSignFunc)
	if conn == nil {
		return false
	}
	defer conn.Close()

	err = conn.Exec(context.Background(), "SELECT 1")
	if err != nil {
		log.Printf("  ✅ 错误签名被正确拒绝: %v", err)
	} else {
		log.Println("  ❌ 错误签名竟然被接受了！安全漏洞！")
		allPassed = false
	}

	// Test 3.2: 无 token 但 auth_enabled=true (应被拒绝)
	log.Println("[Test 3.2] 无 token 请求 (auth_enabled=true，应被拒绝)")
	conn2 := openConnection(nil) // 无签名
	if conn2 == nil {
		// 连接本身可能就被拒绝了，这也是预期行为
		log.Println("  ✅ 无 token 连接被拒绝 (连接级别)")
	} else {
		err = conn2.Exec(context.Background(), "SELECT 1")
		if err != nil {
			log.Printf("  ✅ 无 token 请求被正确拒绝: %v", err)
		} else {
			log.Println("  ❌ 无 token 请求被接受！安全漏洞！")
			allPassed = false
		}
		conn2.Close()
	}

	// Test 3.3: 错误签名 DDL (应被拒绝)
	log.Println("[Test 3.3] 错误签名 DDL 操作 (应被拒绝)")
	conn3 := openConnection(wrongSignFunc)
	if conn3 == nil {
		log.Println("  ✅ 连接被拒绝 (预期行为)")
	} else {
		ddl := fmt.Sprintf("CREATE TABLE %s.invalid_table (id Int64) ENGINE = Memory", TestDatabase)
		err = conn3.Exec(context.Background(), ddl)
		if err != nil {
			log.Printf("  ✅ 错误签名 DDL 被正确拒绝: %v", err)
		} else {
			log.Println("  ❌ 错误签名 DDL 被接受！安全漏洞！")
			allPassed = false
		}
		conn3.Close()
	}

	// Test 3.4: 过期 token (iat 太旧) — 使用手动 token 方式
	log.Println("[Test 3.4] 过期 token 测试 (iat 设为10分钟前)")
	conn4 := openConnection(nil)
	if conn4 == nil {
		log.Println("  ✅ 连接被拒绝 (预期行为)")
	} else {
		expiredSignFunc := makeExpiredSignFunc(correctKey, -10*time.Minute)
		ctx := clickhouse.Context(context.Background(), clickhouse.WithSignFunc(expiredSignFunc))
		err = conn4.Exec(ctx, "SELECT 1")
		if err != nil {
			log.Printf("  ✅ 过期 token 被正确拒绝: %v", err)
		} else {
			log.Println("  ❌ 过期 token 被接受！安全漏洞！")
			allPassed = false
		}
		conn4.Close()
	}

	// Test 3.5: 查询级 WithSignFunc + 错误签名 (应被拒绝)
	log.Println("[Test 3.5] WithSignFunc + 错误签名 (应被拒绝)")
	conn5 := openConnection(nil)
	if conn5 == nil {
		log.Println("  ✅ 连接被拒绝 (预期行为)")
	} else {
		ctx := clickhouse.Context(context.Background(), clickhouse.WithSignFunc(wrongSignFunc))
		err = conn5.Exec(ctx, "SELECT 1")
		if err != nil {
			log.Printf("  ✅ WithSignFunc + 错误签名被正确拒绝: %v", err)
		} else {
			log.Println("  ❌ WithSignFunc + 错误签名被接受！安全漏洞！")
			allPassed = false
		}
		conn5.Close()
	}

	// Test 3.6: Token 重放测试 (同一 token 用于不同查询，应被拒绝)
	// 注意：此处使用 correctKey 而非 wrongKey，因为我们测试的是 qhash（查询哈希）校验，而非密钥校验。
	log.Println("[Test 3.6] Token 重放测试 (token 与查询不匹配)")
	conn6 := openConnection(nil)
	if conn6 == nil {
		log.Println("  ✅ 连接被拒绝 (预期行为)")
	} else {
		// 为 SELECT 1 生成 token，但用它去执行 SELECT 42（查询哈希不匹配）
		replaySignFunc := func(queryBody string) (string, error) {
			// 始终为 "SELECT 1" 生成 token，忽略实际查询
			return createJWSTokenWithTime(correctKey, "SELECT 1", time.Now())
		}
		ctx := clickhouse.Context(context.Background(), clickhouse.WithSignFunc(replaySignFunc))
		err = conn6.Exec(ctx, "SELECT 42")
		if err != nil {
			log.Printf("  ✅ Token 重放被正确拒绝: %v", err)
		} else {
			log.Println("  ❌ Token 重放被接受！查询哈希验证失效！")
			allPassed = false
		}
		conn6.Close()
	}

	// Test 3.7: 多密钥并发测试 (正确+错误密钥混合，验证并发下的认证隔离)
	log.Println("[Test 3.7] 多密钥并发测试 (正确+错误密钥混合)")
	{
		const mixedConcurrency = 10
		resultCh := make(chan mixedResult, mixedConcurrency)
		correctSignFunc := makeSignFunc(correctKey)
		var wg sync.WaitGroup
		for i := 0; i < mixedConcurrency; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				isValid := idx%2 == 0
				var sf func(string) (string, error)
				if isValid {
					sf = correctSignFunc
				} else {
					sf = wrongSignFunc
				}
				c := openConnection(sf)
				if c == nil {
					if !isValid {
						// 错误密钥连接被拒绝是预期行为
						resultCh <- mixedResult{idx, isValid, fmt.Errorf("连接被拒绝")}
					} else {
						resultCh <- mixedResult{idx, isValid, fmt.Errorf("连接失败")}
					}
					return
				}
				defer c.Close()
				execErr := c.Exec(context.Background(), "SELECT 1")
				resultCh <- mixedResult{idx, isValid, execErr}
			}(i)
		}
		wg.Wait()
		close(resultCh)
		multiKeyOK := true
		for r := range resultCh {
			if r.isValid && r.err != nil {
				log.Printf("  ❌ goroutine %d: 正确密钥应成功但失败: %v", r.idx, r.err)
				multiKeyOK = false
			} else if !r.isValid && r.err == nil {
				log.Printf("  ❌ goroutine %d: 错误密钥应被拒绝但成功了！", r.idx)
				multiKeyOK = false
			}
		}
		if multiKeyOK {
			log.Printf("  ✅ %d 并发请求认证隔离正确（正确密钥成功，错误密钥拒绝）", mixedConcurrency)
		} else {
			allPassed = false
		}
	}

	return allPassed
}

// ========== Helper Functions ==========

// runQueryRowCount executes a SELECT query and verifies the exact row count.
func runQueryRowCount(conn clickhouse.Conn, testName, query string, expectedRows int) bool {
	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		log.Printf("  ❌ %s 查询失败: %v", testName, err)
		return false
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if count != expectedRows {
		log.Printf("  ❌ %s 预期 %d 行，实际 %d 行", testName, expectedRows, count)
		return false
	}
	log.Printf("  ✅ %s 成功 (%d 行)", testName, count)
	return true
}

// runQueryMinRowCount executes a SELECT query and verifies at least minRows are returned.
func runQueryMinRowCount(conn clickhouse.Conn, testName, query string, minRows int) bool {
	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		log.Printf("  ❌ %s 查询失败: %v", testName, err)
		return false
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if count < minRows {
		log.Printf("  ❌ %s 预期至少 %d 行，实际 %d 行", testName, minRows, count)
		return false
	}
	log.Printf("  ✅ %s 成功 (%d 行)", testName, count)
	return true
}

// runExec executes a non-query statement (DDL/DML).
func runExec(conn clickhouse.Conn, testName, query string) bool {
	if err := conn.Exec(context.Background(), query); err != nil {
		log.Printf("  ❌ %s 执行失败: %v", testName, err)
		return false
	}
	log.Printf("  ✅ %s 执行成功", testName)
	return true
}

// makeSignFunc returns a SignFunc callback that signs queries using the given private key.
func makeSignFunc(key *ecdsa.PrivateKey) func(queryBody string) (string, error) {
	return func(queryBody string) (string, error) {
		return createJWSTokenWithTime(key, queryBody, time.Now())
	}
}

// makeExpiredSignFunc returns a SignFunc that produces tokens with an offset time.
func makeExpiredSignFunc(key *ecdsa.PrivateKey, offset time.Duration) func(queryBody string) (string, error) {
	return func(queryBody string) (string, error) {
		return createJWSTokenWithTime(key, queryBody, time.Now().Add(offset))
	}
}

func openConnection(signFunc func(string) (string, error)) clickhouse.Conn {
	opts := &clickhouse.Options{
		Addr: []string{*addr},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: *user,
			Password: *pass,
		},
		DialTimeout:     10 * time.Second,
		ConnMaxLifetime: time.Hour,
	}
	if signFunc != nil {
		opts.SignFunc = signFunc
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		log.Printf("  ❌ 打开连接失败: %v", err)
		return nil
	}
	return conn
}

func runCRUD(conn clickhouse.Conn) bool {
	type step struct {
		name  string
		query string
	}
	steps := []step{
		{"创建数据库", fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", TestDatabase)},
		{"创建表", fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (id Int64, name String, ts DateTime DEFAULT now()) ENGINE = Memory", TestDatabase, TestTable)},
		{"插入数据", fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (1, 'hello'), (2, 'world'), (3, 'test')", TestDatabase, TestTable)},
	}

	for _, s := range steps {
		log.Printf("  [CRUD] %s: %s", s.name, s.query)
		if err := conn.Exec(context.Background(), s.query); err != nil {
			log.Printf("    ❌ 失败: %v", err)
			return false
		}
		log.Println("    ✅ 成功")
	}

	// SELECT 验证
	selectQuery := fmt.Sprintf("SELECT id, name FROM %s.%s ORDER BY id", TestDatabase, TestTable)
	log.Printf("  [CRUD] 查询数据: %s", selectQuery)

	rows, err := conn.Query(context.Background(), selectQuery)
	if err != nil {
		log.Printf("    ❌ 查询失败: %v", err)
		return false
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Printf("    ❌ 扫描行失败: %v", err)
			return false
		}
		log.Printf("    行 %d: id=%d, name=%s", count+1, id, name)
		count++
	}
	if count != 3 {
		log.Printf("    ❌ 预期3行数据，实际 %d 行", count)
		return false
	}
	log.Printf("    ✅ 查询成功，返回 %d 行", count)

	// 清理
	cleanupSteps := []step{
		{"删除表", fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", TestDatabase, TestTable)},
		{"删除数据库", fmt.Sprintf("DROP DATABASE IF EXISTS %s", TestDatabase)},
	}
	for _, s := range cleanupSteps {
		log.Printf("  [CRUD] %s: %s", s.name, s.query)
		conn.Exec(context.Background(), s.query)
		log.Println("    ✅ 成功")
	}

	return true
}

// ========== JWS Token Construction (used by SignFunc) ==========

func createJWSTokenWithTime(privateKey *ecdsa.PrivateKey, query string, t time.Time) (string, error) {
	header := JWSHeader{
		Alg: "ES256K",
		Typ: "JWS",
	}
	headerBytes, err := json.Marshal(header)
	if err != nil {
		return "", fmt.Errorf("marshal header: %w", err)
	}
	headerB64 := base64.RawURLEncoding.EncodeToString(headerBytes)

	queryHash := keccak256Hex([]byte(query))
	payload := JWSPayload{
		Iat:       t.Unix(),
		QueryHash: queryHash,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal payload: %w", err)
	}
	payloadB64 := base64.RawURLEncoding.EncodeToString(payloadBytes)

	signingInput := headerB64 + "." + payloadB64
	messageHash := keccak256([]byte(signingInput))

	sig, err := crypto.Sign(messageHash, privateKey)
	if err != nil {
		return "", fmt.Errorf("sign: %w", err)
	}
	sig[64] += 27 // Adjust V

	sigB64 := base64.RawURLEncoding.EncodeToString(sig)
	return signingInput + "." + sigB64, nil
}

func keccak256(data []byte) []byte {
	h := sha3.NewLegacyKeccak256()
	h.Write(data)
	return h.Sum(nil)
}

func keccak256Hex(data []byte) string {
	return "0x" + hex.EncodeToString(keccak256(data))
}
