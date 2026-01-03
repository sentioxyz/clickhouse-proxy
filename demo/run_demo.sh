#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CK_BIN="/workspace/Clickhouse/build/programs/clickhouse"

echo "[run_demo] using root dir: ${ROOT_DIR}"

CK_RUNTIME_DIR="/workspace/clickhouse-proxy-demo/.ck_runtime"

init_dirs() {
  echo "[run_demo] initializing ClickHouse runtime directories..."
  for node in a b; do
    mkdir -p "${CK_RUNTIME_DIR}/${node}/logs"
    mkdir -p "${CK_RUNTIME_DIR}/${node}/data"
    mkdir -p "${CK_RUNTIME_DIR}/${node}/tmp"
    mkdir -p "${CK_RUNTIME_DIR}/${node}/user_files"
    mkdir -p "${CK_RUNTIME_DIR}/${node}/format_schemas"
    mkdir -p "${CK_RUNTIME_DIR}/${node}/access"
  done
}

start_clickhouse() {
  init_dirs
  echo "[run_demo] starting ClickHouse A/B (may already be running)..."
  "${CK_BIN}" server --config-file "${ROOT_DIR}/local/ck-a-config.xml" --daemon || true
  "${CK_BIN}" server --config-file "${ROOT_DIR}/local/ck-b-config.xml" --daemon || true
}

stop_clickhouse() {
  echo "[run_demo] stopping ClickHouse A/B..."
  "${CK_BIN}-client" --host 127.0.0.1 --port 19000 --query "SYSTEM SHUTDOWN" || true
  "${CK_BIN}-client" --host 127.0.0.1 --port 29000 --query "SYSTEM SHUTDOWN" || true
}

start_proxies() {
  echo "[run_demo] starting proxies..."
  mkdir -p "${ROOT_DIR}/.ck_runtime"
  # 每次运行前清空旧日志，确保本次运行从空文件开始。
  : > "${ROOT_DIR}/.ck_runtime/proxy-a.log"
  : > "${ROOT_DIR}/.ck_runtime/proxy-b.log"

  # 前台 go run + 重定向日志到固定文件。
  (cd "${ROOT_DIR}" && go run . -config local/proxy-a.json > .ck_runtime/proxy-a.log 2>&1) &
  PROXY_A_PID=$!
  (cd "${ROOT_DIR}" && go run . -config local/proxy-b.json > .ck_runtime/proxy-b.log 2>&1) &
  PROXY_B_PID=$!
}

stop_proxies() {
  echo "[run_demo] stopping proxies..."
  kill "${PROXY_A_PID}" "${PROXY_B_PID}" 2>/dev/null || true
}

run_demo() {
  echo "[run_demo] running Go demo..."
  (cd "${ROOT_DIR}" && go run ./demo)
}

main() {
  start_clickhouse
  # Debug build needs more time to start
  sleep 15

  start_proxies
  # 等待 proxy 监听起来
  sleep 5

  run_demo

  stop_proxies
  stop_clickhouse

  echo "[run_demo] done."
}

main "$@"
