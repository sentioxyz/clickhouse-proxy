#!/bin/bash
set -e

cleanup() {
    echo "Stopping processes..."
    kill $(jobs -p) 2>/dev/null || true
}
trap cleanup EXIT

echo "Building tools..."
go build -o tools/bin/mock_server tools/mock_server/main.go
go build -o tools/bin/load_client tools/load_client/main.go
go build -o tools/bin/proxy ./cmd/proxy/

echo "Starting Mock Server..."
./tools/bin/mock_server -addr :19001 > tools/mock.log 2>&1 &
MOCK_PID=$!
sleep 1

echo "Starting Proxy..."
export CK_CONFIG=tools/config.json
# Redirect stderr to stdout to see logs
./tools/bin/proxy > tools/proxy.log 2>&1 &
PROXY_PID=$!
sleep 1

# Default to 1000 queries if not specified
N=${1:-1000}
echo "Running Load Test with N=$N..."
# Run N queries with concurrency 10
./tools/bin/load_client -target "127.0.0.1:19000" -file tools/data/exported_queries.json -c 10 -n $N

echo "---------------------------------------------------"
echo "Test Summary:"
echo "---------------------------------------------------"

# Check for Panics
if grep -q "panic:" tools/proxy.log; then
    echo "❌ FAILED: Proxy crashed with panic!"
    grep "panic:" tools/proxy.log
    exit 1
fi

# Check for connections processed (looking for closed connection logs or stats)
CONN_COUNT=$(grep -c "closed" tools/proxy.log || true)
if [ "$CONN_COUNT" -gt 0 ]; then
     echo "✅ SUCCESS: Proxy processed $CONN_COUNT connections."
else
     echo "❌ FAILED: No connections processed found in logs."
     exit 1
fi

# Check for Stats Output
if grep -q "==== ck_remote_proxy stats ====" tools/proxy.log; then
    echo "✅ SUCCESS: Stats printed."
else
    echo "⚠️  WARNING: No stats printed (test might be too short)."
fi

echo "For full details, check tools/proxy.log"
