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
./tests/bin/mock_server -addr :19001 > tests/mock_load.log 2>&1 &
MOCK_PID=$!
sleep 1

echo "Starting Proxy with Auth..."
# Use the same auth config as before
export CK_CONFIG=tests/auth_config.json
# Override listen port for load test isolation if needed, but config.json has strict listen :19000
# We will use the config as is.
./tests/bin/proxy > tests/proxy_load.log 2>&1 &
PROXY_PID=$!
sleep 1

echo "Running Auth Load Test..."
# N=1000, C=10
# Key: ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
# Build simple client
go build -o tests/bin/simple_client tests/simple_client/main.go
# Run load test
./tests/bin/simple_client -addr 127.0.0.1:19000 -priv ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80 -n 1000 -c 10

echo "Check logs at tests/proxy_load.log"
