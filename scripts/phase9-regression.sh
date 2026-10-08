#!/usr/bin/env bash
# Production-config regression baseline (local algod recommended).
# Captures fetch-only / archive / postgres / both under durable settings.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

COUNT="${COUNT:-200}"
WORKERS="${WORKERS:-32}"
BATCH="${COMMIT_BATCH_SIZE:-10}"
ALGOD="${VOI_ALGOD_URL:-http://127.0.0.1:4001}"
TOKEN="${VOI_ALGOD_TOKEN:-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa}"
DSN="${DATABASE_URL:-postgres://follower:follower@localhost:5432/voi_follower?sslmode=disable}"
OUT_DIR="${OUT_DIR:-$ROOT/docs/baselines}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="$OUT_DIR/regression-$STAMP.txt"

mkdir -p "$OUT_DIR"
{
  echo "=== Phase 9 regression baseline ==="
  echo "date_utc=$STAMP count=$COUNT workers=$WORKERS batch=$BATCH"
  echo "algod=$ALGOD"
  echo "PG_ASYNC_COMMIT=false (durable)"
  echo
} | tee "$OUT"

run_pipeline() {
  local pipe="$1"
  local arch="$ROOT/.bench-archive-$pipe"
  rm -rf "$arch"
  echo "==> pipeline=$pipe" | tee -a "$OUT"
  go run ./cmd/bench -mode e2e -pipeline "$pipe" -count "$COUNT" -workers "$WORKERS" \
    -batch "$BATCH" -algod "$ALGOD" -token "$TOKEN" -database "$DSN" \
    -archive "$arch" -insert-mode unnest 2>&1 | tee -a "$OUT"
  echo | tee -a "$OUT"
}

echo "==> fetch-only" | tee -a "$OUT"
go run ./cmd/bench -mode fetch-only -count "$COUNT" -workers "$WORKERS" \
  -algod "$ALGOD" -token "$TOKEN" 2>&1 | tee -a "$OUT"
echo | tee -a "$OUT"

run_pipeline archive
run_pipeline postgres
run_pipeline both

echo "wrote $OUT"
echo "Commit this file under docs/baselines/ when recording a release reference point."
