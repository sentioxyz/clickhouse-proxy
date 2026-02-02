package main

import (
	"context"
	"crypto/ecdsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"golang.org/x/crypto/sha3"
)

func main() {
    // Address: 0x2932A8aAd29e41b90A447E586651587bea3eB11E
	privateKeyHex := "e7bc94e4a2346bfb31ce777e079044718ed02d53d8c297c69fce4259e96557bd"
	proxyAddr := "127.0.0.1:9001"
	query := "SELECT 1"

	privateKey, err := crypto.HexToECDSA(privateKeyHex)
	if err != nil {
		log.Fatal(err)
	}
    
    // Hash for "SELECT 1" (keccak256)
    // 0x...
    queryHash := keccak256Hex([]byte(query))
	fmt.Printf("Query Hash: %s\n", queryHash)

	token, err := createJWSToken(privateKey, queryHash)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{proxyAddr},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: crypto.PubkeyToAddress(privateKey.PublicKey).Hex(),
			Password: token,
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	var result uint8
    // Note: Use QueryRow to trigger the query.
	if err := conn.QueryRow(context.Background(), query).Scan(&result); err != nil {
        // If validation fails, we expect error here (e.g. EOF or Exception)
		log.Fatal("Query failed:", err)
	}
	fmt.Printf("Success! Result: %d\n", result)
}

func createJWSToken(privateKey *ecdsa.PrivateKey, queryHash string) (string, error) {
	header := map[string]string{
		"alg": "ES256K", // Correct alg name for EthValidator check
		"typ": "JWT",
	}
	headerBytes, _ := json.Marshal(header)
	headerB64 := base64.RawURLEncoding.EncodeToString(headerBytes)

	payload := map[string]interface{}{
		"iat":        time.Now().Unix(),
		"qhash":      queryHash, // Using qhash as per validator code
	}
	payloadBytes, _ := json.Marshal(payload)
	payloadB64 := base64.RawURLEncoding.EncodeToString(payloadBytes)

	signingInput := headerB64 + "." + payloadB64
	msgHash := keccak256([]byte(signingInput))

	sig, err := crypto.Sign(msgHash, privateKey)
	if err != nil {
		return "", err
	}
	// Transform V from 0/1 to 27/28 if needed? 
    // crypto.Sign returns V as 0/1. ethereum/go-ethereum/crypto.Ecrecover expects V as 0/1.
    // BUT common conventions for serialized sigs often use 27/28.
    // Let's check validator.go:
    // func recoverAddress: if sig[64] >= 27 { sig[64] -= 27 }
    // It handles both!
    // But typically serialized JWS uses raw R || S || V? 
    // JWS typically doesn't include V? No, ES256K usually implies full signature if recovering?
    // Wait, the standard ES256K signature in JWS is just R || S (64 bytes).
    // But we are recovering the address, so we NEED V (Recovery ID).
    // So 65 bytes is required for recovery.
    // The previous implementation used 65 bytes.
	sigB64 := base64.RawURLEncoding.EncodeToString(sig) // 65 bytes

	return signingInput + "." + sigB64, nil
}

func keccak256(data []byte) []byte {
	h := sha3.NewLegacyKeccak256()
	h.Write(data)
	return h.Sum(nil)
}

func keccak256Hex(data []byte) string {
	return "0x" + hexutil.Encode(keccak256(data))[2:]
}
