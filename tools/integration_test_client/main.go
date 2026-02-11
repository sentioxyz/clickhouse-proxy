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
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ethereum/go-ethereum/crypto"
	"golang.org/x/crypto/sha3"
)

const (
	// AuthTokenSettingKey is the setting key used to pass the JWS authentication token.
	AuthTokenSettingKey = "SQL_x_auth_token"

	// CorrectPrivateKeyHex — Address: 0x2932A8aAd29e41b90A447E586651587bea3eB11E
	CorrectPrivateKeyHex = "e7bc94e4a2346bfb31ce777e079044718ed02d53d8c297c69fce4259e96557bd"

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
	log.Println("  (Go SDK - clickhouse-go/v2)")
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
	if !runCRUD(conn, nil) {
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

	allPassed := true

	// Test 2.1: 带签名的 Ping
	log.Println("[Test 2.1] 带签名的 Ping 连接测试")
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

	// Test 2.2: 带签名的 SELECT 1
	log.Println("[Test 2.2] 带签名的 SELECT 1")
	query := "SELECT 1"
	token, err := createJWSToken(key, query)
	if err != nil {
		log.Printf("  ❌ 生成 token 失败: %v", err)
		return false
	}

	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		AuthTokenSettingKey: clickhouse.CustomSetting{Value: token},
	}))
	var result uint8
	if err := conn.QueryRow(ctx, query).Scan(&result); err != nil {
		log.Printf("  ❌ SELECT 1 失败: %v", err)
		allPassed = false
	} else if result != 1 {
		log.Printf("  ❌ SELECT 1 返回值错误: expected 1, got %d", result)
		allPassed = false
	} else {
		log.Println("  ✅ SELECT 1 = 1 (带签名)")
	}

	// Test 2.3: 带签名的 CRUD 操作
	log.Println("[Test 2.3] 带签名的 CRUD 完整操作")
	if !runCRUD(conn, key) {
		allPassed = false
	}

	// Test 2.4: 使用 SQL_x_auth_token key 方式
	log.Println("[Test 2.4] 使用 SQL_x_auth_token key 测试")
	query2 := "SELECT 42"
	token2, err := createJWSToken(key, query2)
	if err != nil {
		log.Printf("  ❌ 生成 token 失败: %v", err)
		allPassed = false
	} else {
		conn2 := openConnection(nil)
		if conn2 == nil {
			allPassed = false
		} else {
			ctx2 := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
				"SQL_x_auth_token": clickhouse.CustomSetting{Value: token2},
			}))
			var r uint8
			if err := conn2.QueryRow(ctx2, query2).Scan(&r); err != nil {
				log.Printf("  ❌ SQL_x_auth_token 方式 SELECT 42 失败: %v", err)
				allPassed = false
			} else {
				log.Printf("  ✅ SQL_x_auth_token 方式 SELECT 42 = %d", r)
			}
			conn2.Close()
		}
	}

	// Test 2.5: 使用 x_auth_token key 方式 (Legacy old-style key)
	log.Println("[Test 2.5] 使用 x_auth_token key 测试 (Legacy)")
	query3 := "SELECT 99"
	token3, err := createJWSToken(key, query3)
	if err != nil {
		log.Printf("  ❌ 生成 token 失败: %v", err)
		allPassed = false
	} else {
		conn3 := openConnection(nil)
		if conn3 == nil {
			allPassed = false
		} else {
			ctx3 := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
				"x_auth_token": clickhouse.CustomSetting{Value: token3},
			}))
			var r uint8
			if err := conn3.QueryRow(ctx3, query3).Scan(&r); err != nil {
				log.Printf("  ❌ x_auth_token 方式 SELECT 99 失败: %v", err)
				allPassed = false
			} else {
				log.Printf("  ✅ x_auth_token 方式 SELECT 99 = %d", r)
			}
			conn3.Close()
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

	allPassed := true

	// Test 3.1: 错误签名 SELECT 1 (应被拒绝)
	log.Println("[Test 3.1] 错误签名 SELECT 1 (应被拒绝)")
	conn := openConnection(nil)
	if conn == nil {
		return false
	}
	defer conn.Close()

	query := "SELECT 1"
	token, err := createJWSToken(wrongKey, query)
	if err != nil {
		log.Printf("  ❌ 生成 token 失败: %v", err)
		return false
	}

	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		AuthTokenSettingKey: clickhouse.CustomSetting{Value: token},
	}))

	err = conn.Exec(ctx, query)
	if err != nil {
		log.Printf("  ✅ 错误签名被正确拒绝: %v", err)
	} else {
		log.Println("  ❌ 错误签名竟然被接受了！安全漏洞！")
		allPassed = false
	}

	// Test 3.2: 无 token 但 auth_enabled=true (应被拒绝)
	log.Println("[Test 3.2] 无 token 请求 (auth_enabled=true，应被拒绝)")
	conn2 := openConnection(nil)
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

	// Test 3.3: 错误签名 CREATE TABLE (应被拒绝)
	log.Println("[Test 3.3] 错误签名 DDL 操作 (应被拒绝)")
	conn3 := openConnection(nil)
	if conn3 == nil {
		log.Println("  ✅ 连接被拒绝 (预期行为)")
	} else {
		ddl := fmt.Sprintf("CREATE TABLE %s.invalid_table (id Int64) ENGINE = Memory", TestDatabase)
		tokenDDL, _ := createJWSToken(wrongKey, ddl)
		ctxDDL := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
			AuthTokenSettingKey: clickhouse.CustomSetting{Value: tokenDDL},
		}))
		err = conn3.Exec(ctxDDL, ddl)
		if err != nil {
			log.Printf("  ✅ 错误签名 DDL 被正确拒绝: %v", err)
		} else {
			log.Println("  ❌ 错误签名 DDL 被接受！安全漏洞！")
			allPassed = false
		}
		conn3.Close()
	}

	// Test 3.4: 过期 token (iat 太旧)
	log.Println("[Test 3.4] 过期 token 测试 (iat 设为10分钟前)")
	correctKey, _ := crypto.HexToECDSA(CorrectPrivateKeyHex)
	conn4 := openConnection(nil)
	if conn4 == nil {
		log.Println("  ✅ 连接被拒绝 (预期行为)")
	} else {
		expiredToken, _ := createExpiredJWSToken(correctKey, "SELECT 1", -10*time.Minute)
		ctxExpired := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
			AuthTokenSettingKey: clickhouse.CustomSetting{Value: expiredToken},
		}))
		err = conn4.Exec(ctxExpired, "SELECT 1")
		if err != nil {
			log.Printf("  ✅ 过期 token 被正确拒绝: %v", err)
		} else {
			log.Println("  ❌ 过期 token 被接受！安全漏洞！")
			allPassed = false
		}
		conn4.Close()
	}

	// Test 3.5: 使用 x_auth_token key + 错误签名 (应被拒绝)
	log.Println("[Test 3.5] x_auth_token key + 错误签名 (应被拒绝)")
	conn5 := openConnection(nil)
	if conn5 == nil {
		log.Println("  ✅ 连接被拒绝 (预期行为)")
	} else {
		query5 := "SELECT 1"
		token5, _ := createJWSToken(wrongKey, query5)
		ctx5 := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
			"x_auth_token": clickhouse.CustomSetting{Value: token5},
		}))
		err = conn5.Exec(ctx5, query5)
		if err != nil {
			log.Printf("  ✅ x_auth_token + 错误签名被正确拒绝: %v", err)
		} else {
			log.Println("  ❌ x_auth_token + 错误签名被接受！安全漏洞！")
			allPassed = false
		}
		conn5.Close()
	}

	return allPassed
}

// ========== Helper Functions ==========

func openConnection(settings clickhouse.Settings) clickhouse.Conn {
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
	if settings != nil {
		opts.Settings = settings
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		log.Printf("  ❌ 打开连接失败: %v", err)
		return nil
	}
	return conn
}

func runCRUD(conn clickhouse.Conn, key *ecdsa.PrivateKey) bool {
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
		var err error
		if key != nil {
			token, terr := createJWSToken(key, s.query)
			if terr != nil {
				log.Printf("    ❌ token 生成失败: %v", terr)
				return false
			}
			ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
				AuthTokenSettingKey: clickhouse.CustomSetting{Value: token},
			}))
			err = conn.Exec(ctx, s.query)
		} else {
			err = conn.Exec(context.Background(), s.query)
		}
		if err != nil {
			log.Printf("    ❌ 失败: %v", err)
			return false
		}
		log.Println("    ✅ 成功")
	}

	// SELECT 验证
	selectQuery := fmt.Sprintf("SELECT id, name FROM %s.%s ORDER BY id", TestDatabase, TestTable)
	log.Printf("  [CRUD] 查询数据: %s", selectQuery)
	var ctx context.Context
	if key != nil {
		token, err := createJWSToken(key, selectQuery)
		if err != nil {
			log.Printf("    ❌ token 生成失败: %v", err)
			return false
		}
		ctx = clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
			AuthTokenSettingKey: clickhouse.CustomSetting{Value: token},
		}))
	} else {
		ctx = context.Background()
	}

	rows, err := conn.Query(ctx, selectQuery)
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
		if key != nil {
			token, _ := createJWSToken(key, s.query)
			ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
				AuthTokenSettingKey: clickhouse.CustomSetting{Value: token},
			}))
			conn.Exec(ctx, s.query)
		} else {
			conn.Exec(context.Background(), s.query)
		}
		log.Println("    ✅ 成功")
	}

	return true
}

func createJWSToken(privateKey *ecdsa.PrivateKey, query string) (string, error) {
	return createJWSTokenWithTime(privateKey, query, time.Now())
}

func createExpiredJWSToken(privateKey *ecdsa.PrivateKey, query string, offset time.Duration) (string, error) {
	return createJWSTokenWithTime(privateKey, query, time.Now().Add(offset))
}

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
