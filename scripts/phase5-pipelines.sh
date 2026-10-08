#!/usr/bin/env bash
# Phase 5: compare fetch-only / postgres / archive / both pipelines.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export VOI_ALGOD_URL="${VOI_ALGOD_URL:-http://127.0.0.1:4001}"
export VOI_ALGOD_TOKEN="${VOI_ALGOD_TOKEN:-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa}"
export DATABASE_URL="${DATABASE_URL:-postgres://follower:follower@localhost:5432/voi_follower?sslmode=disable}"
export WORKERS="${WORKERS:-32}"
export COUNT="${COUNT:-900}"
ARCHIVE="${ARCHIVE_PATH:-/tmp/voi-ff-phase5-arch}"

go build -o /tmp/voi-fast-follower-bench ./cmd/bench
exec /tmp/voi-fast-follower-bench \
  -mode pipelines \
  -algod "$VOI_ALGOD_URL" \
  -token "$VOI_ALGOD_TOKEN" \
  -count "$COUNT" \
  -workers "$WORKERS" \
  -batch "${COMMIT_BATCH_SIZE:-10}" \
  -database "$DATABASE_URL" \
  -archive "$ARCHIVE"
