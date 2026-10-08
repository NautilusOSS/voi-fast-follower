#!/usr/bin/env bash
# Phase 4: UNNEST vs COPY sink bake-off against local algod + Postgres.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export VOI_ALGOD_URL="${VOI_ALGOD_URL:-http://127.0.0.1:4001}"
export VOI_ALGOD_TOKEN="${VOI_ALGOD_TOKEN:-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa}"
export DATABASE_URL="${DATABASE_URL:-postgres://follower:follower@localhost:5432/voi_follower?sslmode=disable}"
export WORKERS="${WORKERS:-32}"
export COUNT="${COUNT:-5000}"
export MODE=sink-compare

echo "Phase 4 sink-compare: algod=$VOI_ALGOD_URL workers=$WORKERS count=$COUNT"
exec "$ROOT/scripts/bench.sh"
