#!/usr/bin/env bash
# Benchmark catch-up: start_round -> tip (or START_ROUND..END_ROUND).
# Requires a running follower pointed at Postgres, or runs a one-shot local binary.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

ALGOD_URL="${VOI_ALGOD_URL:-https://mainnet-api.voi.nodely.dev}"
DATABASE_URL="${DATABASE_URL:-postgres://follower:follower@localhost:5432/voi_follower?sslmode=disable}"
WORKERS="${PREFETCH_WORKERS:-16}"
METRICS_URL="${METRICS_URL:-http://localhost:9090/metrics}"

TIP="$(curl -fsS "$ALGOD_URL/v2/status" | python3 -c 'import sys,json; print(json.load(sys.stdin)["last-round"])')"
START_ROUND="${VOI_START_ROUND:-$((TIP - 500))}"
END_ROUND="${END_ROUND:-$TIP}"

echo "=== Voi Fast Follower bench ==="
echo "algod:        $ALGOD_URL"
echo "start_round:  $START_ROUND"
echo "end/tip:      $END_ROUND"
echo "workers:      $WORKERS"
echo

# Reset checkpoint/blocks in the configured range for a clean run (optional).
if [[ "${RESET_DB:-0}" == "1" ]]; then
  echo "Resetting DB rows >= $START_ROUND ..."
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 <<SQL
DELETE FROM transactions WHERE round >= $START_ROUND;
DELETE FROM blocks WHERE round >= $START_ROUND;
INSERT INTO sync_state(key, value) VALUES ('last_processed_round', $((START_ROUND - 1)))
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
SQL
fi

export VOI_ALGOD_URL="$ALGOD_URL"
export VOI_ALGOD_TOKEN="${VOI_ALGOD_TOKEN:-}"
export VOI_START_ROUND="$START_ROUND"
export DATABASE_URL
export PREFETCH_WORKERS="$WORKERS"
export METRICS_ADDR="${METRICS_ADDR:-:9090}"

echo "Building..."
go build -o /tmp/voi-fast-follower ./cmd/follower

echo "Starting follower..."
START_TS="$(date +%s)"
/tmp/voi-fast-follower -migrations migrations &
PID=$!

cleanup() {
  kill "$PID" 2>/dev/null || true
  wait "$PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait until checkpoint reaches END_ROUND (or timeout).
TIMEOUT_SEC="${TIMEOUT_SEC:-600}"
deadline=$((START_TS + TIMEOUT_SEC))
peak_bps="0"

while true; do
  now="$(date +%s)"
  if (( now > deadline )); then
    echo "TIMEOUT after ${TIMEOUT_SEC}s" >&2
    exit 1
  fi

  if curl -fsS "$METRICS_URL" >/tmp/vff-metrics.txt 2>/dev/null; then
    last="$(awk '/^voi_follower_last_processed_round /{print $2}' /tmp/vff-metrics.txt | cut -d. -f1)"
    bps="$(awk '/^voi_follower_blocks_per_second /{print $2}' /tmp/vff-metrics.txt)"
    lag="$(awk '/^voi_follower_catchup_lag /{print $2}' /tmp/vff-metrics.txt)"
    if [[ -n "${bps:-}" ]]; then
      peak_bps="$(python3 -c "print(max(float('$peak_bps'), float('${bps:-0}')))")"
    fi
    echo "t+$((now-START_TS))s last=${last:-?} lag=${lag:-?} bps=${bps:-?} peak=$peak_bps"
    if [[ -n "${last:-}" ]] && (( last >= END_ROUND )); then
      break
    fi
  else
    echo "t+$((now-START_TS))s waiting for metrics..."
  fi
  sleep 2
done

END_TS="$(date +%s)"
ELAPSED=$((END_TS - START_TS))
ROUNDS=$((END_ROUND - START_ROUND + 1))
AVG="$(python3 -c "print(round($ROUNDS / max($ELAPSED,1), 2))")"

echo
echo "=== Results ==="
echo "start_round:     $START_ROUND"
echo "end_round:       $END_ROUND"
echo "rounds:          $ROUNDS"
echo "elapsed_sec:     $ELAPSED"
echo "avg_blocks_sec:  $AVG"
echo "peak_blocks_sec: $peak_bps"
echo
echo "Tip: capture CPU/RAM via 'docker stats' or 'ps' during the run for full reporting."
