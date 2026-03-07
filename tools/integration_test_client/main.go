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
	case "auth-relay":
		if !runPhaseAuthRelay() {
			exitCode = 1
		}
	case "all":
		log.Println("⚠️  'all' 模式需要在各阶段间手动切换 proxy 配置")
		log.Println("请使用 -phase noauth/auth-valid/auth-invalid/auth-relay 分别执行")
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

	conn := openConnection(signFunc)
	if conn == nil {
		return false
	}
	defer conn.Close()

	// Test 2.1: 多次连接稳定性 + 签名
	log.Println("[Test 2.1] 多次连接稳定性测试 + 签名 (3次)")
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

	// Test 2.2: SQL_skip_rewrite 跳过重写
	log.Println("[Test 2.2] SQL_skip_rewrite 跳过重写")
	{
		skipCtx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
			"SQL_skip_rewrite": clickhouse.CustomSetting{Value: "1"},
		}))
		rows, err := conn.Query(skipCtx, "SELECT number FROM system.numbers LIMIT 5")
		if err != nil {
			log.Printf("  ❌ SQL_skip_rewrite 查询失败: %v", err)
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
				log.Printf("  ✅ SQL_skip_rewrite 成功，返回 %d 行", count)
			} else {
				log.Printf("  ❌ 预期 5 行，实际 %d 行", count)
				allPassed = false
			}
		}
	}

	// ========== SQL 专项测试 ==========
	if !runSQLTests(conn, signFunc) {
		allPassed = false
	}

	return allPassed
}

// runSQLTests runs SQL-specific integration tests covering all /tests/all_tests.sql cases.
// Each test is executed twice: once with connection-level signing, once with query-level signing.
func runSQLTests(conn clickhouse.Conn, signFunc func(string) (string, error)) bool {
	allPassed := true

	// Two signing modes: connection-level and query-level
	type signMode struct {
		name    string
		connSF  func(string) (string, error)
		makeCtx func() context.Context
	}
	modes := []signMode{
		{
			name:   "连接级签名",
			connSF: signFunc,
			makeCtx: func() context.Context {
				return context.Background()
			},
		},
		{
			name:   "查询级签名",
			connSF: nil,
			makeCtx: func() context.Context {
				return clickhouse.Context(context.Background(), clickhouse.WithSignFunc(signFunc))
			},
		},
	}

	for mi, mode := range modes {
		log.Println()
		log.Printf("========== SQL 专项测试 - 模式 %d: %s ==========", mi+1, mode.name)
		log.Println()

		c := openConnection(mode.connSF)
		if c == nil {
			log.Printf("  ❌ 模式 [%s] 打开连接失败", mode.name)
			allPassed = false
			continue
		}

		ctx := mode.makeCtx()
		prefix := fmt.Sprintf("[%s]", mode.name)

		// 01: SELECT 1
		log.Printf("%s [01] SELECT 1", prefix)
		if !runQueryRowCountCtx(c, ctx, "01_basic_select", "SELECT 1", 1) {
			allPassed = false
		}

		// 02: SELECT version()
		log.Printf("%s [02] SELECT version()", prefix)
		if !runQueryMinRowCountCtx(c, ctx, "02_version", "SELECT version()", 1) {
			allPassed = false
		}

		// 03: 算术运算
		log.Printf("%s [03] SELECT 1 + 2", prefix)
		if !runQueryRowCountCtx(c, ctx, "03_arithmetic", "SELECT 1 + 2", 1) {
			allPassed = false
		}

		// 04: 本地表计数
		log.Printf("%s [04] count() sentio_local_proc.local_data", prefix)
		if !runQueryRowCountCtx(c, ctx, "04_local_count", "SELECT count() FROM sentio_local_proc.local_data", 1) {
			allPassed = false
		}

		// 05: 本地全量 SELECT
		log.Printf("%s [05] sentio_local_proc.orders 全量", prefix)
		if !runQueryMinRowCountCtx(c, ctx, "05_local_select", "SELECT order_id, customer, product_id, quantity, amount, order_date FROM sentio_local_proc.orders ORDER BY order_id", 5) {
			allPassed = false
		}

		// 06: 本地 WHERE
		log.Printf("%s [06] sentio_local_proc.orders WHERE customer='Alice'", prefix)
		if !runQueryRowCountCtx(c, ctx, "06_local_where", "SELECT order_id, customer, amount FROM sentio_local_proc.orders WHERE customer = 'Alice' ORDER BY order_id", 2) {
			allPassed = false
		}

		// 07: 远程查本地
		log.Printf("%s [07] sentio_local_proc.local_data ORDER BY id", prefix)
		if !runQueryMinRowCountCtx(c, ctx, "07_remote_self", "SELECT id, name, value FROM sentio_local_proc.local_data ORDER BY id", 3) {
			allPassed = false
		}

		// 08: 远程基础
		log.Printf("%s [08] count() sentio_remote_proc.products", prefix)
		if !runQueryRowCountCtx(c, ctx, "08_remote_basic", "SELECT count() FROM sentio_remote_proc.products", 1) {
			allPassed = false
		}

		// 09: 远程全量
		log.Printf("%s [09] sentio_remote_proc.products 全量", prefix)
		if !runQueryMinRowCountCtx(c, ctx, "09_remote_select", "SELECT product_id, product_name, category, price, stock FROM sentio_remote_proc.products ORDER BY product_id", 5) {
			allPassed = false
		}

		// 10: 远程 WHERE+ORDER+LIMIT
		log.Printf("%s [10] 远程 WHERE+ORDER+LIMIT", prefix)
		if !runQueryRowCountCtx(c, ctx, "10_remote_where", "SELECT product_id, product_name, price FROM sentio_remote_proc.products WHERE price > 100 ORDER BY price DESC LIMIT 2", 2) {
			allPassed = false
		}

		// 11: 远程空结果
		log.Printf("%s [11] 远程空结果", prefix)
		if !runQueryRowCountCtx(c, ctx, "11_remote_empty", "SELECT count() FROM sentio_remote_proc.products WHERE product_id > 99999", 1) {
			allPassed = false
		}

		// 12: 远程 IF/CASE 表达式
		log.Printf("%s [12] 远程表达式 IF/CASE", prefix)
		if !runQueryMinRowCountCtx(c, ctx, "12_remote_expression",
			`SELECT product_id, product_name, price,
			if(price > 100, 'expensive', 'cheap') AS level,
			CASE category
				WHEN 'Electronics' THEN 'Tech'
				WHEN 'Premium' THEN 'Luxury'
				ELSE 'Other'
			END AS label
		FROM sentio_remote_proc.products
		ORDER BY product_id`, 5) {
			allPassed = false
		}

		// 13: 远程表达式排序+LIMIT
		log.Printf("%s [13] 远程表达式排序 LIMIT", prefix)
		if !runQueryRowCountCtx(c, ctx, "13_remote_order_expr",
			`SELECT product_id, product_name, price, price * stock AS total_value
		FROM sentio_remote_proc.products
		ORDER BY total_value DESC
		LIMIT 3`, 3) {
			allPassed = false
		}

		// 14: 远程窗口函数
		log.Printf("%s [14] 远程窗口函数", prefix)
		if !runQueryMinRowCountCtx(c, ctx, "14_remote_window",
			`SELECT product_id, product_name, price,
			row_number() OVER (ORDER BY price DESC) AS rank,
			lag(price) OVER (ORDER BY price DESC) AS prev_price
		FROM sentio_remote_proc.products
		ORDER BY price DESC`, 5) {
			allPassed = false
		}

		// 15: 本地 SELF JOIN
		log.Printf("%s [15] 本地 SELF JOIN", prefix)
		if !runQueryMinRowCountCtx(c, ctx, "15_local_self_join",
			`SELECT a.order_id, a.customer, b.order_id AS other_order, b.customer AS other_customer
		FROM sentio_local_proc.orders AS a
		INNER JOIN sentio_local_proc.orders AS b ON a.customer = b.customer AND a.order_id < b.order_id
		ORDER BY a.order_id, b.order_id`, 1) {
			allPassed = false
		}

		// 16: 本地两表 JOIN
		log.Printf("%s [16] 本地两表 JOIN", prefix)
		if !runQueryMinRowCountCtx(c, ctx, "16_local_two_table_join",
			`SELECT o.order_id, o.customer, d.name
		FROM sentio_local_proc.orders AS o
		INNER JOIN sentio_local_proc.local_data AS d ON o.order_id % 3 + 1 = d.id
		ORDER BY o.order_id`, 5) {
			allPassed = false
		}

		// 17: 本地 UNION ALL
		log.Printf("%s [17] 本地 UNION ALL", prefix)
		if !runQueryMinRowCountCtx(c, ctx, "17_local_union",
			`SELECT * FROM (
			SELECT 'order' AS type, toString(order_id) AS id, customer AS name FROM sentio_local_proc.orders
			UNION ALL
			SELECT 'data' AS type, toString(id) AS id, name FROM sentio_local_proc.local_data
		) ORDER BY type, id`, 8) {
			allPassed = false
		}

		// 18: 本地标量子查询
		log.Printf("%s [18] 本地标量子查询", prefix)
		if !runQueryMinRowCountCtx(c, ctx, "18_local_subquery",
			`SELECT order_id, customer, amount
		FROM sentio_local_proc.orders
		WHERE amount > (SELECT avg(value) * 10 FROM sentio_local_proc.local_data)
		ORDER BY order_id`, 1) {
			allPassed = false
		}

		// 19: 跨节点 INNER JOIN
		log.Printf("%s [19] 跨节点 INNER JOIN", prefix)
		if !runQueryMinRowCountCtx(c, ctx, "19_cross_inner_join",
			`SELECT o.order_id, o.customer, p.product_name, o.quantity, p.price, o.amount
		FROM sentio_local_proc.orders AS o
		INNER JOIN sentio_remote_proc.products AS p ON o.product_id = p.product_id
		ORDER BY o.order_id`, 5) {
			allPassed = false
		}

		// 20: 跨节点 RIGHT JOIN
		log.Printf("%s [20] 跨节点 RIGHT JOIN", prefix)
		if !runQueryMinRowCountCtx(c, ctx, "20_cross_right_join",
			`SELECT p.product_id, p.product_name, o.order_id, o.customer
		FROM sentio_local_proc.orders AS o
		RIGHT JOIN sentio_remote_proc.products AS p ON o.product_id = p.product_id
		ORDER BY p.product_id, o.order_id`, 6) {
			allPassed = false
		}

		// 21: 跨节点 JOIN USING
		log.Printf("%s [21] 跨节点 JOIN USING", prefix)
		if !runQueryMinRowCountCtx(c, ctx, "21_cross_join_using",
			`SELECT product_id, customer, product_name, amount
		FROM sentio_local_proc.orders AS o
		INNER JOIN sentio_remote_proc.products AS p USING (product_id)
		ORDER BY product_id, customer`, 5) {
			allPassed = false
		}

		// 22: 跨节点 DISTINCT + JOIN
		log.Printf("%s [22] DISTINCT + JOIN", prefix)
		if !runQueryMinRowCountCtx(c, ctx, "22_cross_distinct_join",
			`SELECT DISTINCT customer
		FROM sentio_local_proc.orders AS o
		INNER JOIN sentio_remote_proc.products AS p ON o.product_id = p.product_id
		ORDER BY customer`, 3) {
			allPassed = false
		}

		// 23: 跨节点 LEFT JOIN
		log.Printf("%s [23] 跨节点 LEFT JOIN", prefix)
		if !runQueryMinRowCountCtx(c, ctx, "23_cross_left_join",
			`SELECT o.order_id, o.customer, p.product_name, p.category
		FROM sentio_local_proc.orders AS o
		LEFT JOIN sentio_remote_proc.products AS p ON o.product_id = p.product_id
		ORDER BY o.order_id`, 5) {
			allPassed = false
		}

		// 24: 跨节点 UNION ALL
		log.Printf("%s [24] 跨节点 UNION ALL", prefix)
		if !runQueryMinRowCountCtx(c, ctx, "24_cross_union",
			`SELECT * FROM (
			SELECT 'orders' AS source, customer AS name FROM sentio_local_proc.orders
			UNION ALL
			SELECT 'products' AS source, product_name AS name FROM sentio_remote_proc.products
		) ORDER BY source, name`, 10) {
			allPassed = false
		}

		// 25: 跨节点 UNION ALL + WHERE
		log.Printf("%s [25] UNION ALL + WHERE", prefix)
		if !runQueryRowCountCtx(c, ctx, "25_cross_union_where",
			`SELECT * FROM (
			SELECT order_id AS id, customer AS name FROM sentio_local_proc.orders
			UNION ALL
			SELECT product_id AS id, product_name AS name FROM sentio_remote_proc.products
		) WHERE id = 101 ORDER BY name`, 1) {
			allPassed = false
		}

		// 26: 跨节点 DISTINCT
		log.Printf("%s [26] 跨节点 DISTINCT", prefix)
		if !runQueryMinRowCountCtx(c, ctx, "26_cross_distinct",
			`SELECT DISTINCT category FROM (
			SELECT category FROM sentio_remote_proc.products
			UNION ALL
			SELECT 'Electronics' AS category FROM sentio_local_proc.orders WHERE product_id IN (101, 102)
		) ORDER BY category`, 3) {
			allPassed = false
		}

		// 27: 跨节点 IN 子查询
		log.Printf("%s [27] 跨节点 IN 子查询", prefix)
		if !runQueryRowCountCtx(c, ctx, "27_cross_in_subquery",
			`SELECT order_id, customer, product_id, amount
		FROM sentio_local_proc.orders
		WHERE product_id IN (
			SELECT product_id FROM sentio_remote_proc.products WHERE category = 'Electronics'
		) ORDER BY order_id`, 3) {
			allPassed = false
		}

		// 28: 跨节点标量子查询
		log.Printf("%s [28] 跨节点标量子查询", prefix)
		if !runQueryMinRowCountCtx(c, ctx, "28_cross_scalar_subquery",
			`SELECT order_id, customer, amount,
			(SELECT max(price) FROM sentio_remote_proc.products) AS max_price
		FROM sentio_local_proc.orders ORDER BY order_id`, 5) {
			allPassed = false
		}

		// 29: 跨节点 EXISTS (IN Premium)
		log.Printf("%s [29] 跨节点 EXISTS/IN Premium", prefix)
		if !runQueryMinRowCountCtx(c, ctx, "29_cross_exists",
			`SELECT order_id, customer, product_id, amount
		FROM sentio_local_proc.orders
		WHERE product_id IN (
			SELECT product_id FROM sentio_remote_proc.products WHERE category = 'Premium'
		) ORDER BY order_id`, 1) {
			allPassed = false
		}

		// 30: 跨节点 GROUP BY 聚合
		log.Printf("%s [30] 跨节点 GROUP BY 聚合", prefix)
		if !runQueryMinRowCountCtx(c, ctx, "30_cross_aggregate",
			`SELECT p.category, count() AS cnt, sum(o.quantity) AS total_qty, sum(o.amount) AS total_amt
		FROM sentio_local_proc.orders AS o
		INNER JOIN sentio_remote_proc.products AS p ON o.product_id = p.product_id
		GROUP BY p.category ORDER BY p.category`, 2) {
			allPassed = false
		}

		// 31: 跨节点 HAVING
		log.Printf("%s [31] 跨节点 HAVING", prefix)
		if !runQueryMinRowCountCtx(c, ctx, "31_cross_having",
			`SELECT p.category, count() AS cnt, sum(o.amount) AS total
		FROM sentio_local_proc.orders AS o
		INNER JOIN sentio_remote_proc.products AS p ON o.product_id = p.product_id
		GROUP BY p.category HAVING cnt >= 2 ORDER BY p.category`, 1) {
			allPassed = false
		}

		// 32: 跨节点 GROUP BY WITH TOTALS
		log.Printf("%s [32] GROUP BY WITH TOTALS", prefix)
		if !runQueryMinRowCountCtx(c, ctx, "32_cross_totals",
			`SELECT p.category, count() AS cnt, sum(o.amount) AS total
		FROM sentio_local_proc.orders AS o
		INNER JOIN sentio_remote_proc.products AS p ON o.product_id = p.product_id
		GROUP BY p.category WITH TOTALS ORDER BY p.category`, 2) {
			allPassed = false
		}

		// 33: 三源 UNION ALL
		log.Printf("%s [33] 三源 UNION ALL", prefix)
		if !runQueryMinRowCountCtx(c, ctx, "33_cross_multi_table",
			`SELECT * FROM (
			SELECT 'local_data' AS src, toString(id) AS key, name AS val FROM sentio_local_proc.local_data
			UNION ALL
			SELECT 'orders' AS src, toString(order_id) AS key, customer AS val FROM sentio_local_proc.orders
			UNION ALL
			SELECT 'products' AS src, toString(product_id) AS key, product_name AS val FROM sentio_remote_proc.products
		) ORDER BY src, key`, 13) {
			allPassed = false
		}

		// 34: CTE 跨节点
		log.Printf("%s [34] CTE 跨节点", prefix)
		if !runQueryMinRowCountCtx(c, ctx, "34_cross_cte",
			`WITH
			expensive AS (SELECT product_id, product_name, price FROM sentio_remote_proc.products WHERE price > 100),
			big_orders AS (SELECT order_id, customer, product_id, amount FROM sentio_local_proc.orders WHERE amount > 50)
		SELECT b.order_id, b.customer, e.product_name, e.price, b.amount
		FROM big_orders AS b
		INNER JOIN expensive AS e ON b.product_id = e.product_id
		ORDER BY b.order_id`, 2) {
			allPassed = false
		}

		// 35: 远程 SELF JOIN
		log.Printf("%s [35] 远程 SELF JOIN", prefix)
		if !runQueryMinRowCountCtx(c, ctx, "35_remote_self_join",
			`SELECT p1.product_id, p1.product_name, p2.product_name AS related_name
		FROM sentio_remote_proc.products AS p1
		INNER JOIN sentio_remote_proc.products AS p2 ON p1.category = p2.category AND p1.product_id < p2.product_id
		ORDER BY p1.product_id, p2.product_id`, 1) {
			allPassed = false
		}

		// 36: 远程 UNION ALL
		log.Printf("%s [36] 远程 UNION ALL", prefix)
		if !runQueryMinRowCountCtx(c, ctx, "36_remote_union",
			`SELECT * FROM (
			SELECT product_id AS id, product_name AS name, 'cheap' AS tag FROM sentio_remote_proc.products WHERE price < 50
			UNION ALL
			SELECT product_id AS id, product_name AS name, 'expensive' AS tag FROM sentio_remote_proc.products WHERE price >= 100
		) ORDER BY id`, 1) {
			allPassed = false
		}

		// 37: 远程标量子查询
		log.Printf("%s [37] 远程标量子查询", prefix)
		if !runQueryMinRowCountCtx(c, ctx, "37_remote_subquery",
			`SELECT product_id, product_name, price
		FROM sentio_remote_proc.products
		WHERE price > (SELECT avg(price) FROM sentio_remote_proc.products)
		ORDER BY product_id`, 1) {
			allPassed = false
		}

		// 38: __route__ 本地路由
		log.Printf("%s [38] __route__=local_proc", prefix)
		if !runQueryRowCountCtx(c, ctx, "38_route_local", "SELECT '__route__=local_proc', count() FROM sentio_local_proc.local_data", 1) {
			allPassed = false
		}

		// 39: arrayJoin
		log.Printf("%s [39] arrayJoin 展开数组", prefix)
		if !runQueryRowCountCtx(c, ctx, "39_array_join",
			`SELECT order_id, customer, arrayJoin([1, 2, 3]) AS arr_val
		FROM sentio_local_proc.orders
		WHERE order_id = 1001
		ORDER BY arr_val`, 3) {
			allPassed = false
		}

		// 40: GLOBAL IN
		log.Printf("%s [40] GLOBAL IN 跨节点子查询", prefix)
		if !runQueryRowCountCtx(c, ctx, "40_global_in",
			`SELECT order_id, customer, product_id, amount
		FROM sentio_local_proc.orders
		WHERE product_id GLOBAL IN (
			SELECT product_id FROM sentio_remote_proc.products WHERE category = 'Electronics'
		) ORDER BY order_id`, 3) {
			allPassed = false
		}

		c.Close()
	} // end dual-mode loop

	// ========== 错误用例 (E01-E08) — 仅在连接级签名下执行一次 ==========
	log.Println()
	log.Println("--- 错误用例测试 (E01-E08) ---")
	log.Println()

	ec := openConnection(signFunc)
	if ec == nil {
		log.Println("  ❌ 错误用例: 打开连接失败")
		return false
	}
	defer ec.Close()
	ectx := context.Background()

	// E01: 不存在的 processor_id
	log.Println("[E01] unknown processor")
	if !runExpectError(ec, ectx, "E01_unknown_proc", "SELECT * FROM sentio_unknown_proc.orders") {
		allPassed = false
	}

	// E02: 不存在的表名
	log.Println("[E02] nonexistent table")
	if !runExpectError(ec, ectx, "E02_nonexistent_table", "SELECT * FROM sentio_local_proc.nonexistent_table") {
		allPassed = false
	}

	// E06: 字符串中的虚拟表名不应被重写
	log.Println("[E06] string literal 不被重写")
	if !runExpectSuccess(ec, ectx, "E06_string_literal", "SELECT 'sentio_local_proc.orders' AS table_name") {
		allPassed = false
	}

	// E07: 注释中的虚拟表名不应被重写
	log.Println("[E07] comment 不被重写")
	if !runExpectSuccess(ec, ectx, "E07_comment", "SELECT 1 -- FROM sentio_local_proc.orders") {
		allPassed = false
	}

	// E08: 并发查询 (5 goroutine)
	log.Println("[E08] 并发查询 Rewriter 线程安全")
	{
		const concurrency = 5
		var wg sync.WaitGroup
		errCh := make(chan error, concurrency)
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				gc := openConnection(signFunc)
				if gc == nil {
					errCh <- fmt.Errorf("goroutine %d: 连接失败", idx)
					return
				}
				defer gc.Close()
				var cnt uint64
				if err := gc.QueryRow(context.Background(), "SELECT count() FROM sentio_local_proc.orders").Scan(&cnt); err != nil {
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

	// 连接池压力测试 (20 并发 × 5 查询)
	log.Println("[Extra] 连接池压力测试 (20×5)")
	{
		const stressConcurrency = 20
		const queriesPerConn = 5
		var wg sync.WaitGroup
		errCh := make(chan error, stressConcurrency*queriesPerConn)
		for i := 0; i < stressConcurrency; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				sc := openConnection(signFunc)
				if sc == nil {
					errCh <- fmt.Errorf("goroutine %d: 连接失败", idx)
					return
				}
				defer sc.Close()
				for q := 0; q < queriesPerConn; q++ {
					var cnt uint64
					query := "SELECT count() FROM sentio_local_proc.orders"
					if err := sc.QueryRow(context.Background(), query).Scan(&cnt); err != nil {
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

// ========== Phase 4: Auth Relay ==========
// Tests proxy-to-proxy relay token propagation.
// Requires: Auth Proxy1 (39001) with rewriter + Auth Proxy2 (39002), both with relay_private_key_hex.
// Queries using sentio_remote_proc.* tables go through:
//
//	Client → Proxy1 (JWS) → CH1 → __route__ → Proxy1 (relay JWS) → Proxy2 (validate) → CH2
func runPhaseAuthRelay() bool {
	log.Println("╔══════════════════════════════════════════╗")
	log.Println("║  阶段四: Relay Token 跨节点认证测试        ║")
	log.Println("╚══════════════════════════════════════════╝")
	log.Println()

	key, err := crypto.HexToECDSA(CorrectPrivateKeyHex)
	if err != nil {
		log.Fatalf("解析私钥失败: %v", err)
	}
	log.Printf("客户端密钥地址: %s", crypto.PubkeyToAddress(key.PublicKey).Hex())
	log.Println()

	signFunc := makeSignFunc(key)
	allPassed := true

	// 建立带 auth 的连接到 Proxy1
	conn := openConnection(signFunc)
	if conn == nil {
		log.Println("❌ 无法连接到 Auth Proxy1")
		return false
	}
	defer conn.Close()

	// Test R.1: 基础连通性 — SELECT 1 (本地，不经过 relay)
	log.Println("[Test R.1] 基础连通性: SELECT 1")
	var result uint8
	if err := conn.QueryRow(context.Background(), "SELECT 1").Scan(&result); err != nil {
		log.Printf("  ❌ SELECT 1 失败: %v", err)
		allPassed = false
	} else {
		log.Println("  ✅ SELECT 1 = 1")
	}

	// Test R.2: 本地表查询 (rewriter 重写为本地表，不经过 relay)
	log.Println("[Test R.2] 本地表查询 (sentio_local_proc.local_data → 本地重写)")
	if !runQueryRowCount(conn, "relay_local",
		"SELECT id, name, value FROM sentio_local_proc.local_data ORDER BY id", 3) {
		allPassed = false
	}

	// Test R.3: 远程表查询 — 核心 relay 测试
	// sentio_remote_proc.products → rewriter → __route__ → Proxy1 inject relay token → Proxy2 validate → CH2
	log.Println("[Test R.3] 远程表查询 (sentio_remote_proc.products → __route__ + relay token)")
	if !runQueryMinRowCount(conn, "relay_remote_basic",
		"SELECT product_id, product_name, category, price, stock FROM sentio_remote_proc.products ORDER BY product_id", 5) {
		log.Println("  ⚠️  这是核心 relay 测试，失败意味着 relay token 传播有问题")
		allPassed = false
	}

	// Test R.4: 远程表 WHERE + ORDER + LIMIT
	log.Println("[Test R.4] 远程 WHERE+ORDER+LIMIT (relay)")
	if !runQueryRowCount(conn, "relay_remote_where",
		"SELECT product_id, product_name, price FROM sentio_remote_proc.products WHERE price > 100 ORDER BY price DESC LIMIT 2", 2) {
		allPassed = false
	}

	// Test R.5: 远程表 count()
	log.Println("[Test R.5] 远程 count() (relay)")
	if !runQueryRowCount(conn, "relay_remote_count",
		"SELECT count() FROM sentio_remote_proc.products", 1) {
		allPassed = false
	}

	// Test R.6: 跨节点 INNER JOIN (本地 orders + 远程 products)
	log.Println("[Test R.6] 跨节点 INNER JOIN (local orders × remote products via relay)")
	if !runQueryMinRowCount(conn, "relay_cross_join",
		`SELECT o.order_id, o.customer, p.product_name, o.quantity, p.price, o.amount
		FROM sentio_local_proc.orders AS o
		INNER JOIN sentio_remote_proc.products AS p
			ON o.product_id = p.product_id
		ORDER BY o.order_id`, 5) {
		allPassed = false
	}

	// Test R.7: 跨节点 UNION ALL
	log.Println("[Test R.7] 跨节点 UNION ALL (local + remote via relay)")
	if !runQueryMinRowCount(conn, "relay_cross_union",
		`SELECT * FROM (
			SELECT 'CH1' AS source, customer AS name FROM sentio_local_proc.orders
			UNION ALL
			SELECT 'CH2' AS source, product_name AS name FROM sentio_remote_proc.products
		) ORDER BY source, name`, 10) {
		allPassed = false
	}

	// Test R.8: 跨节点 IN 子查询
	log.Println("[Test R.8] 跨节点 IN 子查询 (relay)")
	if !runQueryMinRowCount(conn, "relay_cross_subquery",
		`SELECT order_id, customer, product_id, amount
		FROM sentio_local_proc.orders
		WHERE product_id IN (
			SELECT product_id FROM sentio_remote_proc.products
			WHERE category = 'Electronics'
		) ORDER BY order_id`, 2) {
		allPassed = false
	}

	// Test R.9: 跨节点聚合 GROUP BY
	log.Println("[Test R.9] 跨节点聚合 GROUP BY (relay)")
	if !runQueryMinRowCount(conn, "relay_cross_aggregate",
		`SELECT p.category, count() AS order_count, sum(o.amount) AS total_amount
		FROM sentio_local_proc.orders AS o
		INNER JOIN sentio_remote_proc.products AS p
			ON o.product_id = p.product_id
		GROUP BY p.category
		ORDER BY p.category`, 2) {
		allPassed = false
	}

	// Test R.10: CTE 跨节点查询
	log.Println("[Test R.10] CTE 跨节点查询 (relay)")
	if !runQueryMinRowCount(conn, "relay_cross_cte",
		`WITH
			expensive AS (
				SELECT product_id, product_name, price
				FROM sentio_remote_proc.products
				WHERE price > 100
			),
			big_orders AS (
				SELECT order_id, customer, product_id, amount
				FROM sentio_local_proc.orders
				WHERE amount > 50
			)
		SELECT b.order_id, b.customer, e.product_name, e.price, b.amount
		FROM big_orders AS b
		INNER JOIN expensive AS e ON b.product_id = e.product_id
		ORDER BY b.order_id`, 2) {
		allPassed = false
	}

	// Test R.11: 远程空结果 (relay)
	log.Println("[Test R.11] 远程空结果 (relay)")
	if !runQueryRowCount(conn, "relay_remote_empty",
		"SELECT count() FROM sentio_remote_proc.products WHERE product_id > 99999", 1) {
		allPassed = false
	}

	// Test R.12: 跨节点 LEFT JOIN
	log.Println("[Test R.12] 跨节点 LEFT JOIN (relay)")
	if !runQueryMinRowCount(conn, "relay_cross_left_join",
		`SELECT o.order_id, o.customer, p.product_name, p.category
		FROM sentio_local_proc.orders AS o
		LEFT JOIN sentio_remote_proc.products AS p
			ON o.product_id = p.product_id
		ORDER BY o.order_id`, 5) {
		allPassed = false
	}

	log.Println()
	if allPassed {
		log.Println("✅ 全部 Relay 测试通过！跨节点 __route__ + JWS relay token 验证成功")
	} else {
		log.Println("❌ 存在 Relay 测试失败")
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

// runQueryRowCountCtx executes a SELECT query with ctx and verifies exact row count.
func runQueryRowCountCtx(conn clickhouse.Conn, ctx context.Context, testName, query string, expectedRows int) bool {
	rows, err := conn.Query(ctx, query)
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

// runQueryMinRowCountCtx executes a SELECT query with ctx and verifies at least minRows.
func runQueryMinRowCountCtx(conn clickhouse.Conn, ctx context.Context, testName, query string, minRows int) bool {
	rows, err := conn.Query(ctx, query)
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

// runExecCtx executes a non-query statement (DDL/DML) with ctx.
func runExecCtx(conn clickhouse.Conn, ctx context.Context, testName, query string) bool {
	if err := conn.Exec(ctx, query); err != nil {
		log.Printf("  ❌ %s 执行失败: %v", testName, err)
		return false
	}
	log.Printf("  ✅ %s 执行成功", testName)
	return true
}

// runExpectError executes a query and expects it to fail.
func runExpectError(conn clickhouse.Conn, ctx context.Context, testName, query string) bool {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		log.Printf("  ✅ %s 预期错误: %v", testName, err)
		return true
	}
	defer rows.Close()
	for rows.Next() {
	}
	log.Printf("  ❌ %s 应该返回错误但成功了", testName)
	return false
}

// runExpectSuccess executes a query and expects it to succeed.
func runExpectSuccess(conn clickhouse.Conn, ctx context.Context, testName, query string) bool {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		log.Printf("  ❌ %s 预期成功但失败: %v", testName, err)
		return false
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	log.Printf("  ✅ %s 成功 (%d 行)", testName, count)
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
