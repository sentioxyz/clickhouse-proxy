package main

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
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
	addr    = flag.String("addr", "127.0.0.1:19001", "ClickHouse proxy address")
	user    = flag.String("user", "default", "ClickHouse username")
	pass    = flag.String("password", "", "ClickHouse password")
	phase   = flag.String("phase", "all", "Test phase: noauth, auth-valid, auth-invalid, or all")
	sqlfile = flag.String("sqlfile", "", "SQL 测试文件路径 (-- [TEST: name] 格式)")
	runOnly = flag.String("run", "", "仅运行指定名称的测试用例 (可选)")
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

	// ========== SQL 文件驱动测试 ==========
	if *sqlfile != "" {
		if !runSQLFileTests(nil, *sqlfile, *runOnly) {
			allPassed = false
		}
	} else {
		log.Println("ℹ️  未指定 -sqlfile，跳过 SQL 专项测试")
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

	// Test 2.3: SQL_skip_rewrite 反向用例 — 不带 skip 时，system.numbers 应被 Rewriter 拦截报错
	log.Println("[Test 2.3] SQL_skip_rewrite 反向用例 (不带 skip，预期报错)")
	{
		rows, err := conn.Query(context.Background(), "SELECT number FROM system.numbers LIMIT 5")
		if err != nil {
			log.Printf("  ✅ 不带 skip_rewrite 正确被拒绝: %v", err)
		} else {
			// 如果没有报错，消费结果并报告失败
			count := 0
			for rows.Next() {
				count++
			}
			rows.Close()
			log.Printf("  ❌ 不带 skip_rewrite 竟然成功了 (返回 %d 行)！Rewriter 应该拦截此查询", count)
			allPassed = false
		}
	}

	// Test 2.4: INSERT/UPDATE/DELETE 操作 sentio.local_data + 数据恢复 (SQL_skip_rewrite)
	log.Println("[Test 2.4] INSERT/UPDATE/DELETE 操作 sentio.local_data (SQL_skip_rewrite)")
	{
		crudOK := true
		skipCtx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
			"SQL_skip_rewrite": clickhouse.CustomSetting{Value: "1"},
		}))

		// Step 1: INSERT 新行
		log.Println("  [2.4.1] INSERT 新行 (id=99)")
		if err := conn.Exec(skipCtx, "INSERT INTO sentio.local_data VALUES (99, 'test_insert', 99.9)"); err != nil {
			log.Printf("    ❌ INSERT 失败: %v", err)
			crudOK = false
		} else {
			log.Println("    ✅ INSERT 成功")
		}

		// Step 2: 验证 INSERT
		if crudOK {
			log.Println("  [2.4.2] 验证 INSERT")
			var cnt uint64
			if err := conn.QueryRow(skipCtx, "SELECT count() FROM sentio.local_data WHERE id = 99").Scan(&cnt); err != nil {
				log.Printf("    ❌ 验证查询失败: %v", err)
				crudOK = false
			} else if cnt == 0 {
				log.Println("    ❌ INSERT 后查不到 id=99")
				crudOK = false
			} else {
				log.Printf("    ✅ INSERT 验证成功 (count=%d)", cnt)
			}
		}

		// Step 3: ALTER TABLE UPDATE
		if crudOK {
			log.Println("  [2.4.3] ALTER TABLE UPDATE (id=99 name → 'test_updated')")
			if err := conn.Exec(skipCtx, "ALTER TABLE sentio.local_data UPDATE name = 'test_updated' WHERE id = 99"); err != nil {
				log.Printf("    ❌ UPDATE 失败: %v", err)
				crudOK = false
			} else {
				log.Println("    ✅ UPDATE 执行成功")
				// 等待 mutation 完成
				time.Sleep(2 * time.Second)
			}
		}

		// Step 4: 验证 UPDATE
		if crudOK {
			log.Println("  [2.4.4] 验证 UPDATE")
			var name string
			if err := conn.QueryRow(skipCtx, "SELECT name FROM sentio.local_data WHERE id = 99").Scan(&name); err != nil {
				log.Printf("    ❌ 验证查询失败: %v", err)
				crudOK = false
			} else if name != "test_updated" {
				log.Printf("    ❌ UPDATE 验证失败: name='%s', expected 'test_updated'", name)
				crudOK = false
			} else {
				log.Printf("    ✅ UPDATE 验证成功 (name='%s')", name)
			}
		}

		// Step 5: ALTER TABLE DELETE (清理测试数据)
		log.Println("  [2.4.5] ALTER TABLE DELETE (id=99)")
		if err := conn.Exec(skipCtx, "ALTER TABLE sentio.local_data DELETE WHERE id = 99"); err != nil {
			log.Printf("    ❌ DELETE 失败: %v", err)
			crudOK = false
		} else {
			log.Println("    ✅ DELETE 执行成功")
			// 等待 mutation 完成
			time.Sleep(2 * time.Second)
		}

		// Step 6: 验证 DELETE + 数据恢复
		log.Println("  [2.4.6] 验证数据恢复 (应回到原始 3 行)")
		{
			var cnt uint64
			if err := conn.QueryRow(skipCtx, "SELECT count() FROM sentio.local_data").Scan(&cnt); err != nil {
				log.Printf("    ❌ 验证查询失败: %v", err)
				crudOK = false
			} else if cnt != 3 {
				log.Printf("    ❌ 数据恢复失败: 预期 3 行，实际 %d 行", cnt)
				crudOK = false
			} else {
				log.Printf("    ✅ 数据恢复成功 (count=%d)", cnt)
			}
		}

		if !crudOK {
			allPassed = false
		}
	}

	// ========== SQL 文件驱动测试 ==========
	if *sqlfile != "" {
		if !runSQLFileTests(signFunc, *sqlfile, *runOnly) {
			allPassed = false
		}
	} else {
		log.Println("ℹ️  未指定 -sqlfile，跳过 SQL 专项测试")
	}

	return allPassed
}

// SQLTestCase represents a single test case parsed from a SQL file.
type SQLTestCase struct {
	Name     string
	Query    string
	Settings map[string]string // per-test settings (e.g. SQL_skip_rewrite=1)
}

// parseSQLFile reads a SQL file and splits it into test cases by -- [TEST: name] markers.
func parseSQLFile(path string) ([]SQLTestCase, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("打开 SQL 文件失败: %w", err)
	}
	defer f.Close()

	var cases []SQLTestCase
	var currentName string
	var currentLines []string
	var currentSettings map[string]string

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "-- [TEST: ") && strings.HasSuffix(line, "]") {
			// 保存上一个用例
			if currentName != "" {
				query := strings.TrimSpace(strings.Join(currentLines, "\n"))
				if query != "" {
					cases = append(cases, SQLTestCase{Name: currentName, Query: query, Settings: currentSettings})
				}
			}
			// 提取新用例名称
			currentName = line[len("-- [TEST: ") : len(line)-1]
			currentLines = nil
			currentSettings = nil
		} else {
			trimmed := strings.TrimSpace(line)
			// 解析 SETTINGS 注解 (e.g. -- SETTINGS: SQL_skip_rewrite=1)
			if strings.HasPrefix(trimmed, "-- SETTINGS:") {
				parts := strings.SplitN(strings.TrimPrefix(trimmed, "-- SETTINGS:"), "=", 2)
				if len(parts) == 2 {
					if currentSettings == nil {
						currentSettings = make(map[string]string)
					}
					currentSettings[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
				}
				continue
			}
			// 跳过纯注释行 (非 SQL 的中文说明)
			if strings.HasPrefix(trimmed, "--") && !strings.HasPrefix(trimmed, "-- FROM") {
				continue
			}
			currentLines = append(currentLines, line)
		}
	}
	// 最后一个用例
	if currentName != "" {
		query := strings.TrimSpace(strings.Join(currentLines, "\n"))
		if query != "" {
			cases = append(cases, SQLTestCase{Name: currentName, Query: query, Settings: currentSettings})
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("读取 SQL 文件失败: %w", err)
	}
	return cases, nil
}

// parseResultFile reads a .result file and returns a map of test_name -> expected_output.
func parseResultFile(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("打开结果文件失败: %w", err)
	}
	defer f.Close()

	results := make(map[string]string)
	var currentName string
	var currentLines []string

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "-- [TEST: ") && strings.HasSuffix(line, "]") {
			if currentName != "" {
				results[currentName] = strings.TrimSpace(strings.Join(currentLines, "\n"))
			}
			currentName = line[len("-- [TEST: ") : len(line)-1]
			currentLines = nil
		} else {
			currentLines = append(currentLines, line)
		}
	}
	if currentName != "" {
		results[currentName] = strings.TrimSpace(strings.Join(currentLines, "\n"))
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("读取结果文件失败: %w", err)
	}
	return results, nil
}

// runSQLFileTests loads test cases from a SQL file and executes them with dual signing modes.
// If a corresponding .result file exists, it compares actual output against expected.
func runSQLFileTests(signFunc func(string) (string, error), sqlFilePath, runFilter string) bool {
	log.Printf("📄 加载 SQL 测试文件: %s", sqlFilePath)

	testCases, err := parseSQLFile(sqlFilePath)
	if err != nil {
		log.Printf("  ❌ %v", err)
		return false
	}

	// 尝试加载同名 .result 文件
	resultPath := strings.TrimSuffix(sqlFilePath, filepath.Ext(sqlFilePath)) + ".result"
	var expectedResults map[string]string
	if _, err := os.Stat(resultPath); err == nil {
		expectedResults, err = parseResultFile(resultPath)
		if err != nil {
			log.Printf("  ⚠️  加载结果文件失败: %v (将只检查查询是否成功)", err)
		} else {
			log.Printf("📄 加载结果文件: %s (%d 个预期结果)", resultPath, len(expectedResults))
		}
	} else {
		log.Printf("ℹ️  未找到结果文件 %s，将只检查查询是否成功", resultPath)
	}

	// 过滤
	if runFilter != "" {
		var filtered []SQLTestCase
		for _, tc := range testCases {
			if tc.Name == runFilter || strings.Contains(tc.Name, runFilter) {
				filtered = append(filtered, tc)
			}
		}
		if len(filtered) == 0 {
			log.Printf("  ❌ 没有找到匹配 '%s' 的测试用例", runFilter)
			return false
		}
		testCases = filtered
		log.Printf("🔍 过滤后: %d 个测试用例", len(testCases))
	}

	log.Printf("🔍 共 %d 个测试用例", len(testCases))
	log.Println()

	allPassed := true

	// 签名模式: signFunc != nil 时双模式 (连接级+查询级)，nil 时单模式 (无签名)
	type signMode struct {
		name    string
		connSF  func(string) (string, error)
		makeCtx func() context.Context
	}
	var modes []signMode
	if signFunc != nil {
		modes = []signMode{
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
	} else {
		modes = []signMode{
			{
				name:   "无签名",
				connSF: nil,
				makeCtx: func() context.Context {
					return context.Background()
				},
			},
		}
	}

	for mi, mode := range modes {
		log.Printf("========== SQL 文件测试 - 模式 %d: %s ==========", mi+1, mode.name)
		log.Println()

		c := openConnection(mode.connSF)
		if c == nil {
			log.Printf("  ❌ 模式 [%s] 打开连接失败", mode.name)
			allPassed = false
			continue
		}

		ctx := mode.makeCtx()
		prefix := fmt.Sprintf("[%s]", mode.name)

		for _, tc := range testCases {
			log.Printf("%s [%s]", prefix, tc.Name)

			// 构建 per-test context (合并签名模式 + 用例级 SETTINGS)
			testCtx := ctx
			if len(tc.Settings) > 0 {
				chSettings := clickhouse.Settings{}
				for k, v := range tc.Settings {
					chSettings[k] = clickhouse.CustomSetting{Value: v}
				}
				testCtx = clickhouse.Context(testCtx, clickhouse.WithSettings(chSettings))
			}

			if strings.HasPrefix(tc.Name, "E") {
				// E* 类错误用例
				switch {
				case tc.Name == "E08_concurrent":
					// E08 需要并发执行，跳过普通流程
					log.Printf("  ⏭️  跳过 (需要特殊并发执行)")
				case tc.Name == "E06_string_literal" || tc.Name == "E07_comment":
					// E06/E07 预期成功
					if !runExpectSuccess(c, testCtx, tc.Name, tc.Query) {
						allPassed = false
					}
				default:
					// E01-E05 等预期错误
					if !runExpectError(c, testCtx, tc.Name, tc.Query) {
						allPassed = false
					}
				}
			} else if isDDLOrDML(tc.Query) {
				// DDL/DML: 使用 Exec 执行 (INSERT/CREATE/ALTER/DROP)
				if !runExecCtx(c, testCtx, tc.Name, tc.Query) {
					allPassed = false
				}
			} else if expected, ok := expectedResults[tc.Name]; ok && expected != "" {
				// 有预期结果 → 对比输出
				if !runQueryCompareResultCtx(c, testCtx, tc.Name, tc.Query, expected) {
					allPassed = false
				}
			} else {
				// 无预期结果 → 仅检查执行成功
				if !runExpectSuccess(c, testCtx, tc.Name, tc.Query) {
					allPassed = false
				}
			}
		}

		c.Close()
	}

	// ========== 特殊测试: E08 并发 + 连接池压力 (仅签名模式执行) ==========
	if signFunc == nil {
		log.Println()
		log.Println("--- 并发 & 压力测试: ⏭️ 无签名模式跳过 ---")
		log.Println()
		return allPassed
	}
	log.Println()
	log.Println("--- 并发 & 压力测试 ---")
	log.Println()

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

// formatValue converts a scanned column value to its CLI-compatible string representation.
// Handles time.Time formatting, Nullable pointer dereferencing, and nil values.
func formatValue(v interface{}) string {
	if v == nil {
		return ""
	}
	rv := reflect.ValueOf(v)
	// Dereference pointers (Nullable columns like *float64, *string, etc.)
	if rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return ""
		}
		return formatValue(rv.Elem().Interface())
	}
	// Format time.Time as date-only (matching clickhouse client output)
	if t, ok := v.(time.Time); ok {
		if t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 && t.Nanosecond() == 0 {
			return t.Format("2006-01-02")
		}
		return t.Format("2006-01-02 15:04:05")
	}
	return fmt.Sprintf("%v", v)
}

// runQueryCompareResultCtx executes a query and compares tab-separated row output against expected.
func runQueryCompareResultCtx(conn clickhouse.Conn, ctx context.Context, testName, query, expected string) bool {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		log.Printf("  ❌ %s 查询失败: %v", testName, err)
		return false
	}
	defer rows.Close()

	var outputLines []string
	var cachedScanTypes []reflect.Type
	for rows.Next() {
		if cachedScanTypes == nil {
			colTypes := rows.ColumnTypes()
			cachedScanTypes = make([]reflect.Type, len(colTypes))
			for i, ct := range colTypes {
				cachedScanTypes[i] = ct.ScanType()
			}
		}
		vals := make([]interface{}, len(cachedScanTypes))
		for i, st := range cachedScanTypes {
			vals[i] = reflect.New(st).Interface()
		}
		if err := rows.Scan(vals...); err != nil {
			log.Printf("  ❌ %s 扫描行失败: %v", testName, err)
			return false
		}
		parts := make([]string, len(vals))
		for i, v := range vals {
			parts[i] = formatValue(reflect.ValueOf(v).Elem().Interface())
		}
		// Trim trailing empty columns (matches CLI behavior for NULL columns)
		for len(parts) > 0 && parts[len(parts)-1] == "" {
			parts = parts[:len(parts)-1]
		}
		outputLines = append(outputLines, strings.Join(parts, "\t"))
	}

	// Handle WITH TOTALS: read totals row via rows.Totals() and append with empty line separator
	if cachedScanTypes != nil {
		totalsVals := make([]interface{}, len(cachedScanTypes))
		for i, st := range cachedScanTypes {
			totalsVals[i] = reflect.New(st).Interface()
		}
		if err := rows.Totals(totalsVals...); err == nil {
			totalsParts := make([]string, len(totalsVals))
			for i, v := range totalsVals {
				totalsParts[i] = formatValue(reflect.ValueOf(v).Elem().Interface())
			}
			// Only append if at least one totals value is non-empty
			hasValue := false
			for _, p := range totalsParts {
				if p != "" && p != "0" {
					hasValue = true
					break
				}
			}
			if hasValue {
				outputLines = append(outputLines, "")
				for len(totalsParts) > 0 && totalsParts[len(totalsParts)-1] == "" {
					totalsParts = totalsParts[:len(totalsParts)-1]
				}
				outputLines = append(outputLines, strings.Join(totalsParts, "\t"))
			}
		}
	}

	actual := strings.Join(outputLines, "\n")
	if actual == expected {
		log.Printf("  ✅ %s PASS (%d 行)", testName, len(outputLines))
		return true
	}

	log.Printf("  ❌ %s FAIL", testName)
	log.Printf("   ┌─ Expected ─")
	for _, line := range strings.Split(expected, "\n") {
		log.Printf("   │ %s", line)
	}
	log.Printf("   ├─ Actual ─")
	for _, line := range strings.Split(actual, "\n") {
		log.Printf("   │ %s", line)
	}
	log.Printf("   └────────────────────────────────────────")
	return false
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

// isDDLOrDML returns true if the query is a DDL/DML statement (INSERT/CREATE/ALTER/DROP).
func isDDLOrDML(query string) bool {
	upper := strings.ToUpper(strings.TrimSpace(query))
	return strings.HasPrefix(upper, "INSERT") ||
		strings.HasPrefix(upper, "CREATE") ||
		strings.HasPrefix(upper, "ALTER") ||
		strings.HasPrefix(upper, "DROP")
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
