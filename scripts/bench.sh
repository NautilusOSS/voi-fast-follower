#!/usr/bin/env bash
# Benchmark catch-up against mainnet algod.
# Distinguishes fetch-only vs end-to-end (Postgres) throughput, and can sweep
# COMMIT_BATCH_SIZE values for Phase 2.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

ALGOD_URL="${VOI_ALGOD_URL:-https://mainnet-api.voi.nodely.dev}"
DATABASE_URL="${DATABASE_URL:-postgres://follower:follower@localhost:5432/voi_follower?sslmode=disable}"
WORKERS="${WORKERS:-${PREFETCH_WORKERS:-32}}"
COUNT="${COUNT:-1000}"
MODE="${MODE:-sweep}" # fetch-only | e2e | sweep | both
BATCH="${COMMIT_BATCH_SIZE:-50}"

TIP="$(curl -fsS "$ALGOD_URL/v2/status" | python3 -c 'import sys,json; print(json.load(sys.stdin)["last-round"])')"
START_ROUND="${VOI_START_ROUND:-${START_ROUND:-$((TIP - COUNT + 1))}}"

echo "Building bench..."
go build -o /tmp/voi-fast-follower-bench ./cmd/bench

run_mode() {
  local m="$1"
  local extra=()
  if [[ "$m" == "e2e" || "$m" == "sweep" ]]; then
    extra+=(-database "$DATABASE_URL" -reset -batch "$BATCH")
  fi
  /tmp/voi-fast-follower-bench \
    -mode "$m" \
    -algod "$ALGOD_URL" \
    -token "${VOI_ALGOD_TOKEN:-}" \
    -start "$START_ROUND" \
    -count "$COUNT" \
    -workers "$WORKERS" \
    -window "${FETCH_WINDOW:-0}" \
    "${extra[@]}"
  echo
}

if [[ "$MODE" == "both" ]]; then
  run_mode fetch-only
  run_mode sweep
elif [[ "$MODE" == "fetch-only" || "$MODE" == "e2e" || "$MODE" == "sweep" ]]; then
  run_mode "$MODE"
else
  echo "unknown MODE=$MODE" >&2
  exit 2
fi
