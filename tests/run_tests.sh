#!/bin/bash
set -e

cleanup() {
    echo "Stopping processes..."
    kill $(jobs -p) 2>/dev/null || true
}
trap cleanup EXIT

echo "Building tools..."
go build -o tests/bin/mock_server tests/mock_server/main.go
go build -o tests/bin/load_client tests/load_client/main.go
go build -o tests/bin/proxy .

echo "Starting Mock Server..."
./tests/bin/mock_server -addr :19001 > tests/mock.log 2>&1 &
MOCK_PID=$!
sleep 1

echo "Starting Proxy..."
export CK_CONFIG=tests/config.json
# Redirect stderr to stdout to see logs
./tests/bin/proxy > tests/proxy.log 2>&1 &
PROXY_PID=$!
sleep 1

# Default to 1000 queries if not specified
N=${1:-1000}
echo "Running Load Test with N=$N..."
# Run N queries with concurrency 10
./tests/bin/load_client -target "127.0.0.1:19000" -file tests/data/exported_queries.json -c 10 -n $N

echo "---------------------------------------------------"
echo "Test Summary:"
echo "---------------------------------------------------"

# Check for Panics
if grep -q "panic:" tests/proxy.log; then
    echo "❌ FAILED: Proxy crashed with panic!"
    grep "panic:" tests/proxy.log
    exit 1
fi

# Check for connections processed (looking for closed connection logs or stats)
CONN_COUNT=$(grep -c "closed" tests/proxy.log || true)
if [ "$CONN_COUNT" -gt 0 ]; then
     echo "✅ SUCCESS: Proxy processed $CONN_COUNT connections."
else
     echo "❌ FAILED: No connections processed found in logs."
     exit 1
fi

# Check for Stats Output
if grep -q "==== ck_remote_proxy stats ====" tests/proxy.log; then
    echo "✅ SUCCESS: Stats printed."
else
    echo "⚠️  WARNING: No stats printed (test might be too short)."
fi

echo "For full details, check tests/proxy.log"
