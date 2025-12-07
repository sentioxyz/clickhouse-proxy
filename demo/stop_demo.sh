#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CK_BIN="/workspace/Clickhouse/build/programs/clickhouse"

echo "[stop_demo] stopping proxies..."
# 尝试根据配置路径和进程名匹配 proxy 进程。
pkill -f "ck_remote_proxy -config ${ROOT_DIR}/local/proxy-a.json" 2>/dev/null || true
pkill -f "ck_remote_proxy -config ${ROOT_DIR}/local/proxy-b.json" 2>/dev/null || true
pkill -f "ck_remote_proxy -config local/proxy-a.json" 2>/dev/null || true
pkill -f "ck_remote_proxy -config local/proxy-b.json" 2>/dev/null || true

echo "[stop_demo] stopping ClickHouse A/B via SYSTEM SHUTDOWN..."
"${CK_BIN}-client" --host 127.0.0.1 --port 19000 --query "SYSTEM SHUTDOWN" 2>/dev/null || true
"${CK_BIN}-client" --host 127.0.0.1 --port 29000 --query "SYSTEM SHUTDOWN" 2>/dev/null || true

echo "[stop_demo] stopping any remaining ClickHouse server processes for ck-a/ck-b configs..."
pkill -f "clickhouse server --config-file ${ROOT_DIR}/local/ck-a-config.xml" 2>/dev/null || true
pkill -f "clickhouse server --config-file ${ROOT_DIR}/local/ck-b-config.xml" 2>/dev/null || true

echo "[stop_demo] done."

