#!/bin/bash
# Example: start clickhouse-proxy in sidecar mode using CLI flags.
#
# The private key should be passed via environment variable to avoid
# leaking it in process listings (ps, /proc).
#
# Usage:
#   export CK_SIDECAR_KEY=0xYOUR_PRIVATE_KEY_HERE
#   ./sidecar-cli.sh [upstream] [listen]
#
# Arguments (optional, override defaults):
#   $1  Server-side proxy address  (default: 10.0.0.8:9001)
#   $2  Local listen address       (default: :9001)

set -euo pipefail

UPSTREAM="${1:-10.0.0.8:9001}"
LISTEN="${2:-:9001}"
BINARY="${CLICKHOUSE_PROXY_BIN:-./clickhouse-proxy}"

if [[ -z "${CK_SIDECAR_KEY:-}" ]]; then
  echo "Error: CK_SIDECAR_KEY environment variable is required." >&2
  echo "  export CK_SIDECAR_KEY=0xYOUR_PRIVATE_KEY_HERE" >&2
  exit 1
fi

exec "$BINARY" \
  -sidecar \
  -sidecar-upstream "$UPSTREAM" \
  -listen "$LISTEN"
# CK_SIDECAR_KEY is read automatically from the environment — no -sidecar-key flag needed.
