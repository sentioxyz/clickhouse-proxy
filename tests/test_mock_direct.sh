#!/bin/bash
set -e

cleanup() {
    echo "Stopping processes..."
    kill $(jobs -p) 2>/dev/null || true
}
trap cleanup EXIT

echo "Building tools..."
go build -o tests/bin/mock_server tests/mock_server/main.go

echo "Starting Mock Server..."
./tests/bin/mock_server -addr :19002 > tests/mock_direct.log 2>&1 &
MOCK_PID=$!
sleep 1

echo "Running Auth Test Driver Direct..."
# We don't need auth key for direct test as mock server ignores it
go run tests/auth_test_driver/main.go -addr 127.0.0.1:19002 -priv ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80

echo "Success!"
