#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_PID=""
FRONTEND_PID=""

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "[error] Missing required command: $1"
    exit 1
  fi
}

cleanup() {
  local exit_code=$?
  if [[ -n "${FRONTEND_PID}" ]] && kill -0 "${FRONTEND_PID}" >/dev/null 2>&1; then
    kill "${FRONTEND_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${BACKEND_PID}" ]] && kill -0 "${BACKEND_PID}" >/dev/null 2>&1; then
    kill "${BACKEND_PID}" >/dev/null 2>&1 || true
  fi
  wait >/dev/null 2>&1 || true
  exit "$exit_code"
}

trap cleanup INT TERM EXIT

require_cmd python
require_cmd npm

if [[ ! -f "${ROOT_DIR}/parquet_viewer/api.py" ]]; then
  echo "[error] Backend entrypoint not found: parquet_viewer/api.py"
  exit 1
fi

if [[ ! -f "${ROOT_DIR}/frontend/package.json" ]]; then
  echo "[error] Frontend entrypoint not found: frontend/package.json"
  exit 1
fi

echo "[info] Starting backend on http://127.0.0.1:8000"
(
  cd "${ROOT_DIR}"
  python -m uvicorn parquet_viewer.api:app --host 127.0.0.1 --port 8000 --reload
) &
BACKEND_PID=$!

echo "[info] Starting frontend on http://localhost:5173"
(
  cd "${ROOT_DIR}/frontend"
  npm run dev
) &
FRONTEND_PID=$!

echo "[info] Backend PID: ${BACKEND_PID}"
echo "[info] Frontend PID: ${FRONTEND_PID}"
echo "[info] Press Ctrl+C to stop both."

wait -n "${BACKEND_PID}" "${FRONTEND_PID}"

