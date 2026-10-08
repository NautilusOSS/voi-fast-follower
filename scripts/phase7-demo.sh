#!/usr/bin/env bash
# Phase 7 demo: Voi → Fast Follower archive → Conduit voi_archive → file_writer
#
# Offline (no Voi):
#   ARCHIVE_ONLY=1 ARCHIVE_PATH=./archive START=N END=M ./scripts/phase7-demo.sh
#
# Live capture then Conduit:
#   VOI_ALGOD_URL=http://127.0.0.1:4001 VOI_ALGOD_TOKEN=aaa... ./scripts/phase7-demo.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

ARCHIVE_PATH="${ARCHIVE_PATH:-$ROOT/archive}"
DEMO_DIR="${DEMO_DIR:-$ROOT/plugins/conduit/examples/demo-data}"
COUNT="${COUNT:-20}"
START="${START:-}"
END="${END:-}"

mkdir -p "$DEMO_DIR" bin
chmod +x "$ROOT/scripts/phase7-demo.sh" 2>/dev/null || true

echo "==> Building custom Conduit binary with voi_archive importer"
(cd plugins/conduit && go mod tidy && go build -o "$ROOT/bin/conduit" ./cmd/conduit)
"$ROOT/bin/conduit" list | grep -E 'voi_archive' >/dev/null
echo "    voi_archive registered"

if [[ -z "${ARCHIVE_ONLY:-}" ]]; then
  if [[ -z "${VOI_ALGOD_URL:-}" ]]; then
    echo "VOI_ALGOD_URL required (or ARCHIVE_ONLY=1 with an existing archive)" >&2
    exit 2
  fi
  echo "==> Capturing $COUNT rounds into archive (follower e2e pipeline=archive)"
  go run ./cmd/bench -mode e2e -pipeline archive -count "$COUNT" -workers "${WORKERS:-16}" \
    -archive "$ARCHIVE_PATH" -batch 10 ${START:+-start "$START"}
fi

if [[ ! -f "$ARCHIVE_PATH/checkpoint" ]]; then
  echo "archive checkpoint missing at $ARCHIVE_PATH" >&2
  exit 1
fi

CP=$(tr -d '[:space:]' <"$ARCHIVE_PATH/checkpoint")
if [[ -z "$END" ]]; then
  END="$CP"
fi
if [[ -z "$START" ]]; then
  START=$((END - COUNT + 1))
  if (( START < 1 )); then START=1; fi
fi
echo "==> Archive checkpoint=$CP range $START-$END ($((END - START + 1)) blocks)"

if [[ ! -f "$DEMO_DIR/genesis.json" ]]; then
  if [[ -n "${VOI_ALGOD_URL:-}" ]]; then
    echo "==> Fetching genesis once from algod (Init only — not sync)"
    curl -fsS "${VOI_ALGOD_URL%/}/genesis" -o "$DEMO_DIR/genesis.json"
  else
    echo "Provide $DEMO_DIR/genesis.json or set VOI_ALGOD_URL" >&2
    exit 2
  fi
fi

# Fresh Conduit data dir so pipeline starts cleanly; set next round via conduit init if available.
rm -rf "$DEMO_DIR/.conduit" "$DEMO_DIR/exporter" 2>/dev/null || true
cat >"$DEMO_DIR/conduit.yml" <<EOF
log-level: info
importer:
  name: voi_archive
  config:
    archive_path: "$ARCHIVE_PATH"
    genesis_file: "$DEMO_DIR/genesis.json"
    mode: offline
processors:
exporter:
  name: file_writer
  config: {}
EOF

echo "==> Running Conduit (offline archive importer → file_writer)"
set +e
if command -v timeout >/dev/null 2>&1; then
  timeout "${TIMEOUT:-45s}" "$ROOT/bin/conduit" -d "$DEMO_DIR" 2>&1 | tee "$DEMO_DIR/conduit.log"
else
  # macOS may lack timeout(1)
  "$ROOT/bin/conduit" -d "$DEMO_DIR" 2>&1 | tee "$DEMO_DIR/conduit.log" &
  PID=$!
  sleep "${TIMEOUT_SEC:-45}"
  kill "$PID" 2>/dev/null || true
  wait "$PID" 2>/dev/null || true
fi
set -e

BLOCKS=$(find "$DEMO_DIR" -type f \( -name '*.block' -o -name '*.json' -o -name '*.msgp' \) 2>/dev/null | wc -l | tr -d ' ')
echo "==> Done"
echo "first_round=$START final_round=$END block_count=$((END - START + 1))"
echo "archive_checkpoint=$CP conduit_output_files≈$BLOCKS"
echo "log=$DEMO_DIR/conduit.log"
echo "Architecture: Fast Follower sync → archive → Conduit indexing/export"
