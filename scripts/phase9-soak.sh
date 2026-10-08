#!/usr/bin/env bash
# Long-duration soak against the reference deployment.
# Default: 2 hours. Override with HOURS=8.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
METRICS="${METRICS_URL:-http://127.0.0.1:9090}"
HOURS="${HOURS:-2}"
INTERVAL="${INTERVAL:-60}"
OUT="${OUT:-$ROOT/docs/baselines/soak-$(date -u +%Y%m%dT%H%M%SZ).log}"

mkdir -p "$(dirname "$OUT")"
end=$((SECONDS + HOURS * 3600))
echo "soak hours=$HOURS interval=${INTERVAL}s log=$OUT"
echo "ts mode tip cp lag rss_kb" | tee "$OUT"

while (( SECONDS < end )); do
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  body="$(curl -fsS "$METRICS/readyz" 2>/dev/null || echo '{}')"
  mode="$(echo "$body" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("mode","?"))' 2>/dev/null || echo '?')"
  tip="$(echo "$body" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("tip_round",0))' 2>/dev/null || echo 0)"
  cp="$(echo "$body" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("checkpoint_round",0))' 2>/dev/null || echo 0)"
  lag="$(echo "$body" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("lag",0))' 2>/dev/null || echo 0)"
  rss="n/a"
  if command -v docker >/dev/null; then
    rss="$(docker stats --no-stream --format '{{.MemUsage}}' "$(docker compose -f docker-compose.prod.yml ps -q follower 2>/dev/null | head -1)" 2>/dev/null || echo n/a)"
  fi
  echo "$ts $mode tip=$tip cp=$cp lag=$lag mem=$rss" | tee -a "$OUT"
  sleep "$INTERVAL"
done
echo "soak complete → $OUT"
