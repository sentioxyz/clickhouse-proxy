#!/bin/bash
# Stream replay test with full environment setup
# Usage: ./run_stream_replay.sh [POD] [NAMESPACE] [SINCE] [N]

set -e

POD=${1:-""}
NS=${2:-"clickhouse"}
SINCE=${3:-"1 hour"}
N=${4:-0}
LOCAL_PORT=19001
PROXY_PORT=19000
MOCK_PORT=19002

if [ -z "$POD" ]; then
    echo "Usage: $0 <pod-name> [namespace] [since] [n]"
    echo "Example: $0 clickhouse-user-part-a-0-0-0 clickhouse '1 hour' 0"
    exit 1
fi

# Cleanup function
cleanup() {
    echo ""
    echo "🧹 Cleaning up..."
    kill $MOCK_PID 2>/dev/null || true
    kill $PROXY_PID 2>/dev/null || true
    kill $PORTFWD_PID 2>/dev/null || true
    pkill -f "port-forward.*$POD" 2>/dev/null || true
    echo "✅ Cleanup complete"
    exit 0
}
trap cleanup EXIT INT TERM

echo "=========================================="
echo "  Stream Replay Test"
echo "=========================================="
echo "Source Pod: $POD"
echo "Namespace:  $NS"
echo "Since:      $SINCE"
echo "Limit:      $N (0=all)"
echo "=========================================="

# Build everything
echo "📦 Building binaries..."
bazel build //tools/mock_server //tools/stream_client //cmd/proxy:clickhouse-proxy
mkdir -p tools/bin
cp bazel-bin/tools/mock_server/mock_server_/mock_server tools/bin/mock_server
cp bazel-bin/tools/stream_client/stream_client_/stream_client tools/bin/stream_client
cp bazel-bin/cmd/proxy/clickhouse-proxy_/clickhouse-proxy tools/bin/proxy

# Start mock server
echo "🚀 Starting mock server on :$MOCK_PORT..."
./tools/bin/mock_server -addr :$MOCK_PORT > tools/mock.log 2>&1 &
MOCK_PID=$!
sleep 1

# Create temp config for proxy
cat > tools/stream_config.json << EOF
{
    "listen": ":$PROXY_PORT",
    "upstream": "127.0.0.1:$MOCK_PORT",
    "dial_timeout": "1s",
    "idle_timeout": "10s",
    "stats_interval": "5s",
    "log_queries": true,
    "log_data": false,
    "max_query_log_bytes": 200,
    "max_data_log_bytes": 0
}
EOF

# Start proxy
echo "🚀 Starting proxy on :$PROXY_PORT (upstream -> :$MOCK_PORT)..."
CK_CONFIG=tools/stream_config.json ./tools/bin/proxy > tools/proxy.log 2>&1 &
PROXY_PID=$!
sleep 1

# Start port-forward
echo "🔗 Setting up port-forward to $POD..."
kubectl port-forward -n $NS pod/$POD $LOCAL_PORT:9000 > /dev/null 2>&1 &
PORTFWD_PID=$!
sleep 2

# Verify port-forward is working
if ! kill -0 $PORTFWD_PID 2>/dev/null; then
    echo "❌ Failed to start port-forward"
    exit 1
fi

echo "▶️  Starting stream replay..."
echo ""

# Run stream client
./tools/bin/stream_client \
    -source "127.0.0.1:$LOCAL_PORT" \
    -target "127.0.0.1:$PROXY_PORT" \
    -n $N \
    -since "$SINCE"

echo ""
echo "📋 Proxy log summary:"
echo "   Connections: $(grep -c 'closed' tools/proxy.log 2>/dev/null || echo 0)"
if grep -q "panic:" tools/proxy.log; then
    echo "   ❌ PANIC detected in proxy!"
    grep "panic:" tools/proxy.log
else
    echo "   ✅ No panics"
fi
