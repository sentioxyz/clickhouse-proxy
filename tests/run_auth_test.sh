#!/bin/bash
set -e

cleanup() {
    echo "Stopping processes..."
    kill $(jobs -p) 2>/dev/null || true
}
trap cleanup EXIT

echo "Building tools..."
go build -o tests/bin/mock_server tests/mock_server/main.go
go build -o tests/bin/proxy .

echo "Starting Mock Server..."
./tests/bin/mock_server -addr :19001 > tests/mock.log 2>&1 &
MOCK_PID=$!
sleep 1

echo "Starting Proxy with Auth..."
export CK_CONFIG=tests/auth_config.json
./tests/bin/proxy > tests/proxy_auth.log 2>&1 &
PROXY_PID=$!
sleep 1

echo "Running Auth Test Driver..."
# Key: ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
# Addr: 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
go run tests/auth_test_driver/main.go -addr 127.0.0.1:19000 -priv ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80

echo "---------------------------------------------------"
echo "Check Logs:"
grep "authenticated query" tests/proxy_auth.log | tail -n 2
grep "unauthorized signer" tests/proxy_auth.log | tail -n 2
echo "---------------------------------------------------"
