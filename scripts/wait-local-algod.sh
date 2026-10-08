#!/usr/bin/env bash
# Wait until local voi-node is serving /v2/status near network tip and BlockRaw works.
set -euo pipefail

ALGOD_URL="${VOI_ALGOD_URL:-http://127.0.0.1:4001}"
TOKEN="${VOI_ALGOD_TOKEN:-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa}"
CONTAINER="${VOI_NODE_CONTAINER:-voi-fast-follower-voi-node-1}"
MAX_LAG="${MAX_LAG:-2000}"
TIMEOUT_SEC="${TIMEOUT_SEC:-7200}"
PUBLIC_TIP_URL="${PUBLIC_TIP_URL:-https://mainnet-api.voi.nodely.dev/v2/status}"

status_json() {
  curl -fsS -H "X-Algo-API-Token: $TOKEN" "$ALGOD_URL/v2/status"
}

public_tip() {
  curl -fsS "$PUBLIC_TIP_URL" | python3 -c 'import sys,json; print(json.load(sys.stdin)["last-round"])'
}

echo "Waiting for local algod at $ALGOD_URL ..."
start_ts="$(date +%s)"
deadline=$((start_ts + TIMEOUT_SEC))

while true; do
  now="$(date +%s)"
  if (( now > deadline )); then
    echo "TIMEOUT after ${TIMEOUT_SEC}s" >&2
    exit 1
  fi

  if st="$(status_json 2>/dev/null)"; then
    local_round="$(python3 -c 'import sys,json; print(json.load(sys.stdin)["last-round"])' <<<"$st")"
    catchup="$(python3 -c 'import sys,json; print(json.load(sys.stdin).get("catchup-time",0))' <<<"$st")"
    cp="$(python3 -c 'import sys,json; print(json.load(sys.stdin).get("catchpoint") or "")' <<<"$st")"
    tip="$(public_tip 2>/dev/null || echo 0)"
    lag=0
    if [[ "$tip" != "0" ]]; then
      lag=$((tip - local_round))
    fi
    echo "t+$((now-start_ts))s local=$local_round tip=$tip lag=$lag catchup_ns=$catchup catchpoint=${cp:0:24}"

    if [[ -z "$cp" && "$tip" != "0" && "$lag" -le "$MAX_LAG" && "$local_round" -gt 1000 ]]; then
      probe=$((local_round - 10))
      code="$(curl -s -o /dev/null -w '%{http_code}' -H "X-Algo-API-Token: $TOKEN" "$ALGOD_URL/v2/blocks/${probe}?format=msgpack")"
      if [[ "$code" == "200" ]]; then
        echo "READY probe_round=$probe"
        printf '%s' "$TOKEN" > /tmp/voi-local-algod.token
        exit 0
      fi
      echo "BlockRaw probe HTTP $code; continuing..."
    fi
  else
    echo "t+$((now-start_ts))s waiting for HTTP..."
  fi
  sleep 10
done
