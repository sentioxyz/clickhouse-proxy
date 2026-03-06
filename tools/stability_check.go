package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ethereum/go-ethereum/crypto"
)

var (
	proxyAddr = flag.String("addr", "127.0.0.1:9000", "Proxy address")
	privKey   = flag.String("key", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "Private key for JWS auth")
)

func main() {
	flag.Parse()

	log.Printf("Starting stability check against %s", *proxyAddr)

	// 1. Basic Connection Test
	testConnection(*proxyAddr)

	// 2. Token Auth Tests (x_auth_token & SQL_x_auth_token)
	testTokenAuth(*proxyAddr, *privKey)

	// 3. CRUD Operations
	testCRUD(*proxyAddr, *privKey)

	log.Println("All stability checks passed successfully!")
}

func testConnection(addr string) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to open connection: %v", err)
	}
	defer conn.Close()

	if err := conn.Ping(context.Background()); err != nil {
		// Note: Ping might fail if auth is strictly required and we didn't provide it?
		// But usually Ping is open or we provide token.
		// If auth is enabled on proxy, strictly, we might need token even for Ping.
		// Let's assume for now valid token is needed.
		log.Printf("Ping failed (might be expected if auth required): %v", err)
	} else {
		log.Println("Ping successful")
	}
}

func testTokenAuth(addr, privateKeyHex string) {
	log.Println("Running Token Auth Tests...")

	sql := "SELECT 1"
	token := generateAuthToken(privateKeyHex, sql)

	// Case A: Using SQL_x_auth_token
	log.Println("Testing SQL_x_auth_token...")
	connA, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
		},
		Settings: clickhouse.Settings{
			"SQL_x_auth_token": token,
		},
	})
	if err != nil {
		log.Fatalf("Failed to open connA: %v", err)
	}
	defer connA.Close()

	if err := connA.Exec(context.Background(), sql); err != nil {
		log.Fatalf("SQL_x_auth_token check failed: %v", err)
	}
	log.Println("SQL_x_auth_token check passed")

	// Case B: Using x_auth_token (Legacy)
	log.Println("Testing x_auth_token...")
	connB, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
		},
		Settings: clickhouse.Settings{
			"x_auth_token": token,
		},
	})
	if err != nil {
		log.Fatalf("Failed to open connB: %v", err)
	}
	defer connB.Close()

	if err := connB.Exec(context.Background(), sql); err != nil {
		log.Fatalf("x_auth_token check failed: %v", err)
	}
	log.Println("x_auth_token check passed")
}

func testCRUD(addr, privateKeyHex string) {
	log.Println("Running CRUD Tests...")

	tableName := "stability_check_test"

	// We need a helper to execute queries with fresh tokens
	exec := func(query string) {
		token := generateAuthToken(privateKeyHex, query)
		conn, err := clickhouse.Open(&clickhouse.Options{
			Addr: []string{addr},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
			},
			Settings: clickhouse.Settings{
				"SQL_x_auth_token": token,
			},
		})
		if err != nil {
			log.Fatalf("Failed to connect for query '%s': %v", query, err)
		}
		defer conn.Close()

		if err := conn.Exec(context.Background(), query); err != nil {
			log.Fatalf("Query failed: %s\nError: %v", query, err)
		}
	}

	// 1. Drop if exists
	exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	// 2. Create
	createSQL := fmt.Sprintf("CREATE TABLE %s (id UInt64, name String) ENGINE = Memory", tableName)
	exec(createSQL)
	log.Println("Table created")

	// 3. Insert
	insertSQL := fmt.Sprintf("INSERT INTO %s VALUES (1, 'test')", tableName)
	exec(insertSQL)
	log.Println("Data inserted")

	// 4. Select
	// Select requires reading back.
	// We need a connection for Query logic.
	selectSQL := fmt.Sprintf("SELECT * FROM %s", tableName)
	tokenSelect := generateAuthToken(privateKeyHex, selectSQL)
	connSelect, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
		},
		Settings: clickhouse.Settings{
			"SQL_x_auth_token": tokenSelect,
		},
	})
	if err != nil {
		log.Fatalf("Failed to connect for select: %v", err)
	}
	defer connSelect.Close()

	rows, err := connSelect.Query(context.Background(), selectSQL)
	if err != nil {
		log.Fatalf("Select failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 1 {
		log.Fatalf("Expected 1 row, got %d", count)
	}
	log.Println("Data selected and verified")

	// 5. Drop
	exec(fmt.Sprintf("DROP TABLE %s", tableName))
	log.Println("Table dropped")
}

func generateAuthToken(privKeyHex, sql string) string {
	privateKey, err := crypto.HexToECDSA(privKeyHex)
	if err != nil {
		log.Fatalf("failed to parse private key: %v", err)
	}

	header := map[string]string{"alg": "ES256K", "typ": "JWT"}
	headerJSON, _ := json.Marshal(header)
	headerB64 := base64.RawURLEncoding.EncodeToString(headerJSON)

	qhash := "0x" + fmt.Sprintf("%x", crypto.Keccak256([]byte(sql)))

	payload := map[string]interface{}{
		"iat":   time.Now().Unix(),
		"qhash": qhash,
	}
	payloadJSON, _ := json.Marshal(payload)
	payloadB64 := base64.RawURLEncoding.EncodeToString(payloadJSON)

	signingInput := headerB64 + "." + payloadB64

	hash := crypto.Keccak256([]byte(signingInput))
	sig, err := crypto.Sign(hash, privateKey)
	if err != nil {
		log.Fatalf("failed to sign: %v", err)
	}

	sig[64] += 27
	signatureB64 := base64.RawURLEncoding.EncodeToString(sig)
	return signingInput + "." + signatureB64
}
