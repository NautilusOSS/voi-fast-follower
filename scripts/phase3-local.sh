#!/usr/bin/env bash
# Phase 3: wait for local algod, then run Nodely vs local benchmarks.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

LOCAL_URL="${LOCAL_ALGOD_URL:-http://127.0.0.1:4001}"
LOCAL_TOKEN="${LOCAL_ALGOD_TOKEN:-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa}"
PUBLIC_URL="${VOI_ALGOD_URL:-https://mainnet-api.voi.nodely.dev}"
DATABASE_URL="${DATABASE_URL:-postgres://follower:follower@localhost:5432/voi_follower?sslmode=disable}"
COUNT="${COUNT:-1000}"
SUSTAINED="${SUSTAINED_COUNT:-10000}"
WORKERS="${WORKERS:-32}"

go build -o /tmp/voi-fast-follower-bench ./cmd/bench

echo "== Waiting for local algod catch-up =="
VOI_ALGOD_URL="$LOCAL_URL" ./scripts/wait-local-algod.sh

LOCAL_TIP="$(curl -fsS -H "X-Algo-API-Token: $LOCAL_TOKEN" "$LOCAL_URL/v2/status" | python3 -c 'import sys,json;print(json.load(sys.stdin)["last-round"])')"
PUBLIC_TIP="$(curl -fsS "$PUBLIC_URL/v2/status" | python3 -c 'import sys,json;print(json.load(sys.stdin)["last-round"])')"
# Use rounds available on BOTH endpoints (near tip of the slower/local node).
END="$LOCAL_TIP"
START=$((END - COUNT + 1))
if (( START < 1 )); then START=1; fi
echo "Using shared range $START..$END (local_tip=$LOCAL_TIP public_tip=$PUBLIC_TIP)"

echo
echo "== Fetch-only worker scaling: Nodely =="
/tmp/voi-fast-follower-bench -mode workers -algod "$PUBLIC_URL" -start "$START" -count "$COUNT"

echo
echo "== Fetch-only worker scaling: Local =="
/tmp/voi-fast-follower-bench -mode workers -algod "$LOCAL_URL" -token "$LOCAL_TOKEN" -start "$START" -count "$COUNT"

echo
echo "== Fetch-only 32 workers detail: Nodely =="
/tmp/voi-fast-follower-bench -mode fetch-only -algod "$PUBLIC_URL" -workers "$WORKERS" -start "$START" -count "$COUNT"

echo
echo "== Fetch-only 32 workers detail: Local =="
/tmp/voi-fast-follower-bench -mode fetch-only -algod "$LOCAL_URL" -token "$LOCAL_TOKEN" -workers "$WORKERS" -start "$START" -count "$COUNT"

echo
echo "== E2E batch sweep: Nodely =="
/tmp/voi-fast-follower-bench -mode sweep -algod "$PUBLIC_URL" -workers "$WORKERS" -start "$START" -count "$COUNT" -database "$DATABASE_URL" -reset

echo
echo "== E2E batch sweep: Local =="
/tmp/voi-fast-follower-bench -mode sweep -algod "$LOCAL_URL" -token "$LOCAL_TOKEN" -workers "$WORKERS" -start "$START" -count "$COUNT" -database "$DATABASE_URL" -reset

if [[ "${RUN_SUSTAINED:-1}" == "1" ]]; then
  S_START=$((END - SUSTAINED + 1))
  if (( S_START < 1 )); then S_START=1; fi
  echo
  echo "== Sustained 10k e2e local batch=50 =="
  /usr/bin/time -l /tmp/voi-fast-follower-bench \
    -mode e2e -algod "$LOCAL_URL" -token "$LOCAL_TOKEN" \
    -workers "$WORKERS" -batch 50 -start "$S_START" -count "$SUSTAINED" \
    -database "$DATABASE_URL" -reset 2>&1 || true
fi

echo
echo "Phase 3 benchmark script complete."
