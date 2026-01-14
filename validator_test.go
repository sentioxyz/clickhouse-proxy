package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jwt"
)

func TestJWKValidator(t *testing.T) {
	// 1. Generate a test key
	rawKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}
	key, err := jwk.FromRaw(rawKey)
	if err != nil {
		t.Fatalf("failed to create JWK: %v", err)
	}
	if err := key.Set(jwk.KeyIDKey, "test-key-id"); err != nil {
		t.Fatalf("failed to set key ID: %v", err)
	}
	if err := key.Set(jwk.AlgorithmKey, jwa.RS256); err != nil {
		t.Fatalf("failed to set algorithm: %v", err)
	}

	keySet := jwk.NewSet()
	keySet.AddKey(key)

	validator := NewJWKValidator(keySet)

	// 2. Create a valid token
	token := jwt.New()
	token.Set(jwt.IssuerKey, "test-issuer")
	token.Set(jwt.SubjectKey, "test-user")
	token.Set(jwt.IssuedAtKey, time.Now())
	token.Set(jwt.ExpirationKey, time.Now().Add(time.Hour))

	signed, err := jwt.Sign(token, jwt.WithKey(jwa.RS256, key))
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}

	// 3. Test valid signature
	goodMeta := QueryMeta{
		ClientAddr: "127.0.0.1",
		Signature:  string(signed),
		SQL:        "SELECT 1",
	}
	if err := validator.ValidateQuery(context.Background(), goodMeta); err != nil {
		t.Errorf("validate valid token failed: %v", err)
	}

	// 4. Test invalid signature
	badMeta := QueryMeta{
		ClientAddr: "127.0.0.1",
		Signature:  string(signed) + "junk",
		SQL:        "SELECT 1",
	}
	if err := validator.ValidateQuery(context.Background(), badMeta); err == nil {
		t.Error("validate invalid signature should fail")
	}

	// 5. Test expired token
	expiredToken := jwt.New()
	expiredToken.Set(jwt.ExpirationKey, time.Now().Add(-time.Hour))
	expiredSigned, _ := jwt.Sign(expiredToken, jwt.WithKey(jwa.RS256, key))

	expiredMeta := QueryMeta{
		ClientAddr: "127.0.0.1",
		Signature:  string(expiredSigned),
		SQL:        "SELECT 1",
	}
	if err := validator.ValidateQuery(context.Background(), expiredMeta); err == nil {
		t.Error("validate expired token should fail")
	}
}
