# Voi Fast Follower

**A high-throughput, gap-free, raw Voi block delivery layer.**

It sits between a Voi algod endpoint and downstream consumers and does one job well:

> Acquire, validate, and deliver contiguous raw blocks quickly—starting from a recent round, not genesis.

See the [stream contract](docs/stream-contract.md) for ordering, at-least-once delivery, restart, and failure guarantees.

### What it is / is not

| Is | Is not |
|---|---|
| Ordered raw block stream | An indexer |
| Durable Postgres + local archive sinks | A DEX / ARC-200 / ARC-72 processor |
| Offline archive replay | A Conduit fork |
| Bounded, backpressured catch-up + live follow | Exactly-once messaging |

```text
                    Voi Network
                         ↓
                    local algod
                         ↓
                  Fast Follower
                         ↓
                 durable archive
                   ↓           ↓
                live          replay / bootstrap
              consumers       consumers (Postgres, Conduit, …)
```

**Voi provides the live chain. Fast Follower provides synchronization. Archive provides durable history.**

Consumers bootstrap from the archive (no historical Voi lookback) then follow live blocks. Optional Conduit importer: `voi_archive`. See [docs/phase8-bootstrap-history.md](docs/phase8-bootstrap-history.md) and [docs/phase7-conduit-adapter.md](docs/phase7-conduit-adapter.md).

## Guarantees (summary)

Full text: [docs/stream-contract.md](docs/stream-contract.md) · Phase 6: [docs/phase6-stream-contract.md](docs/phase6-stream-contract.md)

- **Order:** strict round order; no `N+2` before `N+1`
- **Continuity:** gaps are errors
- **Delivery:** at-least-once; effectively-once with idempotent sinks (not exactly-once)
- **Restart:** `resume = checkpoint + 1`
- **Failure:** sink error blocks checkpoint advance past the failed batch

### How do I consume it?

External applications read the **durable archive** (or follow it while the follower runs). Use the reference consumer or `pkg/consumer` — not follower internals.

```bash
# start Voi + follower (production-style)
cp .env.example .env   # set POSTGRES_PASSWORD, VOI_ALGOD_URL, token
docker compose -f docker-compose.prod.yml up -d

# verify archive
go run ./cmd/verify -archive ./archive
go run ./cmd/archive-info -archive ./archive

# inspect health
curl -s http://127.0.0.1:9090/healthz | jq .
curl -s http://127.0.0.1:9090/readyz | jq .

# consume historical range (no Voi network contact)
go run ./examples/block-consumer \
  --archive ./archive --start 1000000 --end 1000100 \
  --checkpoint ./consumer.cp

# follow growing archive (follower still acquiring)
go run ./examples/block-consumer \
  --archive ./archive --follow --checkpoint ./consumer.cp

# historical → live after archive tip
go run ./examples/block-consumer \
  --archive ./archive --follow \
  --algod http://127.0.0.1:4001 --token "$VOI_ALGOD_TOKEN" \
  --checkpoint ./consumer.cp
```

Consumer checkpoint files are **independent** from the follower checkpoint. A consumer may lag thousands of rounds; the follower never rewinds. See [docs/phase10-ecosystem-validation.md](docs/phase10-ecosystem-validation.md).

### Integrate as an in-process sink

Implement `storage.BlockSink` / `BatchBlockSink` and attach via `BuildSinks` or `MultiSink`. Do not depend on fetch workers or buffer internals.

## Features

- Configurable algod URL/token (no hard-coded endpoints; no node internals)
- `GetBlock` / `WaitForBlockAfter` over standard algod v2 msgpack `BlockRaw`
- Concurrent fetch worker pool (default **32**) with bounded `FETCH_WINDOW`
- Strictly ordered commits; out-of-order fetches are buffered until the gap fills
- **Batched catch-up commits** (`COMMIT_BATCH_SIZE`, default **10**) via `BatchBlockSink`
- Live follow forces batch size **1** for low latency
- Postgres bulk insert: `PG_INSERT_MODE=unnest` (default) or `copy` (staging)
- Optional **local segment archive** sink (`ARCHIVE_PATH`) and multi-sink fan-out
- Durable checkpoint advanced only after all required sinks accept the batch
- Idempotent writes; archive crash reconcile via checkpoint file
- Offline **replay** from archive → any sink (`cmd/replay`)
- Archive **verify** / **archive-info** tools (no network)
- Concurrent MultiSink with min-checkpoint semantics
- Live follow via `wait-for-block-after` using the same commit path
- `/healthz` + `/readyz` operational modes; Prometheus metrics on `:9090/metrics`
- Docker Compose: `follower` + `postgres` (+ optional archive volume)

### Correctness invariant

> Every committed round is contiguous, and the checkpoint always represents a fully committed round.

Never advance `last_processed_round` past a missing round. On restart: `resume_round = last_processed_round + 1`.

## Quick start

### Docker Compose

```bash
# Optional: catch up the last 200 rounds instead of tip-only
export VOI_START_ROUND=$(($(curl -fsS https://mainnet-api.voi.nodely.dev/v2/status | python3 -c 'import sys,json;print(json.load(sys.stdin)["last-round"])') - 200))

docker compose up --build
```

| Variable | Description |
|---|---|
| `VOI_ALGOD_URL` | Algod base URL |
| `VOI_ALGOD_TOKEN` | API token if required |
| `VOI_START_ROUND` / `START_ROUND` | `latest` or integer round |
| `WORKERS` | Concurrent fetch workers (default 32) |
| `FETCH_WINDOW` | Max in-flight rounds (raised to ≥ batch size) |
| `COMMIT_BATCH_SIZE` | Contiguous blocks per catch-up commit (default 10) |
| `COMMIT_FLUSH_INTERVAL` | Partial-batch flush while catching up (default 200ms) |
| `PG_INSERT_MODE` | `unnest` (default) or `copy` |
| `PG_ASYNC_COMMIT` | Experimental async commit (`false` default) |
| `DATABASE_URL` | Postgres DSN (optional if archive-only) |
| `ARCHIVE_PATH` / `ARCHIVE_ENABLED` | Local segment archive sink |
| `ARCHIVE_SEGMENT_SIZE` | Rounds per `.seg` (default 1000) |
| `LOG_LEVEL` | `debug` / `info` / `warn` / `error` |
| `METRICS_ADDR` | Metrics listen address (default `:9090`) |
| `SHUTDOWN_TIMEOUT` | Graceful HTTP shutdown (default `10s`) |

See [config.example.yaml](config.example.yaml). Full production table: [docs/phase9-production.md](docs/phase9-production.md). Legacy `PREFETCH_WORKERS` / `PREFETCH_BUFFER` still work.

### Running in production

```bash
cp .env.example .env   # set POSTGRES_PASSWORD, VOI_ALGOD_URL, token
docker compose -f docker-compose.prod.yml up --build -d

curl -s http://127.0.0.1:9090/readyz | jq .
./scripts/phase9-backup-restore.sh backup
./scripts/phase9-failure-inject.sh   # optional chaos checks
```

Production defaults keep Postgres durable (`PG_ASYNC_COMMIT=false`), live batch size 1, bounded fetch window, and archive fsync on. **Back up the archive** for historical blocks; consumers can restart/replay without reacquiring Voi.

See [docs/phase9-production.md](docs/phase9-production.md) for storage layout, alerts, upgrades, and security notes.

### Local binary

```bash
docker compose up -d postgres

export VOI_ALGOD_URL=https://mainnet-api.voi.nodely.dev
export DATABASE_URL=postgres://follower:follower@localhost:5432/voi_follower?sslmode=disable
export VOI_START_ROUND=latest

go run ./cmd/follower -config config.example.yaml -migrations migrations
```

## How it works

1. **Checkpoint** – If `sync_state.last_processed_round` exists, resume at `N+1`. Otherwise use `start_round` (`latest` → current tip).
2. **Worker pool** – Up to `WORKERS` goroutines fetch `BlockRaw` concurrently. The orchestrator never has more than `FETCH_WINDOW` rounds in-flight.
3. **Ordered buffer** – Results may arrive out of order; they are held until the next expected round is present.
4. **Batched commit (catch-up)** – Contiguous ready rounds are committed together via `CommitBatch` (one Postgres transaction, checkpoint = final round). Live mode uses batch size 1.
5. **Validation** – Round match, non-empty block hash (BH over header), previous-block linkage against the last committed hash. Raw msgpack is preserved.
6. **Live follow** – At tip, `WaitForBlockAfter`; same validation + commit path (batch size 1).

## Schema

See [migrations/001_initial.sql](migrations/001_initial.sql):

- `blocks` – round, hashes, timestamp, txn_count, `raw_block`
- `transactions` – round, txid, sender, type, `raw_transaction`
- `sync_state` – `last_processed_round`

## Metrics

| Metric | Meaning |
|---|---|
| `voi_follower_current_round` | Next commit cursor |
| `voi_follower_target_round` | Network tip |
| `voi_follower_catchup_lag` | `target - current` |
| `voi_follower_last_processed_round` | Durable checkpoint |
| `voi_follower_blocks_processed_total` | Counter |
| `voi_follower_blocks_per_second` | Recent throughput |
| `voi_follower_fetch_latency_seconds` | Fetch+decode histogram |
| `voi_follower_commit_latency_seconds` | Commit histogram |
| `voi_follower_workers_busy` / `_total` | Worker utilization |
| `voi_follower_inflight_rounds` | Bounded window occupancy |
| `voi_follower_errors_total` | Errors |
| `conduit_*` (plugin binary) | Adapter delivery / lag — see phase 7 docs |

## Benchmark

```bash
docker compose up -d postgres
export DATABASE_URL=postgres://follower:follower@localhost:5432/voi_follower?sslmode=disable

# Fetch-only + UNNEST vs COPY sink compare
COUNT=900 WORKERS=32 MODE=sink-compare ./scripts/phase4-sink-compare.sh

# Single e2e run:
go run ./cmd/bench -mode e2e -count 900 -workers 32 -batch 10 -insert-mode unnest -reset
```

Reports start/end round, elapsed time, blocks/sec, and commit latency (avg/p50/p95).

### Phase 2 results (remote Nodely, durable `synchronous_commit`)

| Batch | Blocks/sec | Notes |
|------:|-----------:|-------|
| fetch-only | ~125–245 | Public algod ceiling |
| **50** | **~42–45** | Fetch-bound against Nodely |

### Phase 3 — local Voi algod

See [docs/phase3-local-node.md](docs/phase3-local-node.md).

| Mode | Nodely | Local algod |
|---|---:|---:|
| Fetch-only (best workers) | ~245 | **~2,300–2,900** |
| E2E batch=10 | ~39 | **~540** (peak) |
| E2E batch=50 | ~42 | ~237 |

**Conclusion:** remote e2e ~45 blk/s was acquisition-bound. Local acquisition is ~20× faster; Postgres then becomes the next limiter (~hundreds of blk/s).

### Phase 4 — Postgres sink

See [docs/phase4-postgres-sink.md](docs/phase4-postgres-sink.md).

| Mode | Blocks/sec |
|---|---:|
| Local fetch-only | **~2,780** |
| E2E UNNEST batch=10 (durable) | **~650–750** |
| E2E COPY batch=10 | ~330–380 |

**Defaults:** `PG_INSERT_MODE=unnest`, `COMMIT_BATCH_SIZE=10`. COPY staging remains available but is slower for this payload. Durable Postgres is still ~3.5–4× below local acquisition.

### Phase 5 — block stream + archive

See [docs/phase5-block-stream-archive.md](docs/phase5-block-stream-archive.md).

| Pipeline | Blocks/sec |
|---|---:|
| Fetch only | ~2,000–2,780 |
| PostgreSQL | ~270–750 |
| Archive (fsync) | ~350 |
| Archive + PostgreSQL | ~240 |

```bash
# Capture to archive + Postgres
ARCHIVE_ENABLED=true ARCHIVE_PATH=./archive docker compose up --build

# Offline replay (no Voi contact)
go run ./cmd/replay -archive ./archive -start N -end M -sink postgres -database "$DATABASE_URL"

# Pipeline bake-off
COUNT=900 ./scripts/phase5-pipelines.sh
```

### Phase 7 — Conduit consumer adapter

See [docs/phase7-conduit-adapter.md](docs/phase7-conduit-adapter.md).

```bash
# Build custom Conduit binary (separate module; Conduit stays optional)
cd plugins/conduit && go build -o ../../bin/conduit ./cmd/conduit
./bin/conduit list   # includes voi_archive

# Offline: archive → Conduit → file_writer (no Voi network)
ARCHIVE_ONLY=1 ARCHIVE_PATH=./archive START=N END=M \
  VOI_ALGOD_URL=http://127.0.0.1:4001 ./scripts/phase7-demo.sh

# Or live: local Voi → follower archive → Conduit
VOI_ALGOD_URL=http://127.0.0.1:4001 \
VOI_ALGOD_TOKEN=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  COUNT=20 ./scripts/phase7-demo.sh
```

Standalone paths still work without Conduit: Postgres, Archive, MultiSink, and archive replay.

### Phase 8 — Bootstrap & long-range history

See [docs/phase8-bootstrap-history.md](docs/phase8-bootstrap-history.md).

```bash
# Offline historical bootstrap (no Voi)
go run ./cmd/replay -archive ./archive -start N -end M -sink postgres -database "$DATABASE_URL"

# Archive → live handoff → sink
go run ./cmd/bootstrap -archive ./archive -start N -end TIP \
  -sink postgres -database "$DATABASE_URL" -algod "$VOI_ALGOD_URL"

# Move / verify / compare archives
go run ./cmd/archive-export -archive ./archive -out ./bundle -start N -end M
go run ./cmd/archive-import -in ./bundle -archive ./archive-new
go run ./cmd/archive-compare -a ./archive -b ./archive-new -start N -end M
go run ./cmd/verify -archive ./archive
go run ./cmd/archive-prune -archive ./archive -keep-rounds 500000
```

### Phase 10 — Reference consumer & ecosystem validation

See [docs/phase10-ecosystem-validation.md](docs/phase10-ecosystem-validation.md).

```bash
# Offline ecosystem check (archive must exist)
ARCHIVE_PATH=./archive START=1 END=100 ./scripts/phase10-ecosystem.sh

# Public consumer API (experimental)
go doc github.com/NautilusOSS/voi-fast-follower/pkg/consumer

# Integration tests
go test ./pkg/consumer/... -v -count=1
```

```bash
docker compose up -d postgres voi-node
# After catchup (see docs/phase3-local-node.md):
export VOI_ALGOD_URL=http://127.0.0.1:4001
export VOI_ALGOD_TOKEN=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
./scripts/phase3-local.sh
COUNT=900 ./scripts/phase4-sink-compare.sh
```

## Tests

```bash
go test ./...

# Conduit plugin module (optional dependency)
cd plugins/conduit && go test ./...

# Live algod decode check
VOI_INTEGRATION=1 go test ./internal/block ./internal/voi -v

# Postgres idempotency (compose postgres up)
DATABASE_URL=postgres://follower:follower@localhost:5432/voi_follower?sslmode=disable \
  go test ./internal/storage -v
```

Follower unit tests cover sequential ingestion, out-of-order fetch, ordered commits, missing-round retry, duplicates, checkpoint restart, commit failure retry, chain linkage, worker concurrency, and bounded buffering.

## Project layout

```text
cmd/follower/main.go
cmd/bench/main.go
cmd/replay/main.go        # archive → sink (offline)
cmd/bootstrap/main.go     # archive → live handoff → sink
cmd/verify/main.go        # archive integrity
cmd/archive-info/main.go  # archive metadata
cmd/archive-export|import|compare|prune/
examples/block-consumer/   # reference external consumer (pkg/consumer only)
pkg/consumer/              # experimental public consumer API
internal/config/
internal/voi/             # GetBlock, WaitForBlockAfter
internal/block/           # canonical Block + msgpack decode
internal/follower/        # worker pool + ordered stream
internal/stream/          # RoundSource, HandoffSource, Cursor
internal/conduit/         # translate + delivery (no Conduit module dep)
internal/storage/         # BlockSink, Postgres, Archive, MultiSink
internal/health/          # /healthz /readyz state
internal/metrics/
plugins/conduit/          # optional Conduit binary + voi_archive importer
migrations/
docs/stream-contract.md
docs/phase7-conduit-adapter.md
docs/phase8-bootstrap-history.md
docs/phase9-production.md
docs/phase10-ecosystem-validation.md
deploy/docker-entrypoint.sh
docker-compose.prod.yml
scripts/phase5-pipelines.sh
scripts/phase7-demo.sh
scripts/phase9-*.sh
scripts/phase10-ecosystem.sh
```

## Research notes (Phase 1)

| Topic | Conclusion |
|---|---|
| Authoritative node | [VoiNetwork/go-algorand](https://github.com/VoiNetwork/go-algorand) (`AVAIL` channel) |
| SDK | [`github.com/algorand/go-algorand-sdk/v2`](https://github.com/algorand/go-algorand-sdk) — Voi speaks standard algod v2 |
| Block API | `GET /v2/blocks/{round}?format=msgpack` (`BlockRaw`); no multi-round batch API |
| Throughput | Concurrent prefetch + ordered commit beats sequential HTTP |
| Catchpoints | Bootstrap the **node**, not this follower process |

Genesis ID observed on mainnet API: `voimain-v1.0`.

## Non-goals (Phase 1)

ARC-200/72 decoding, DEX/NFT indexing, explorer APIs, GraphQL, Supabase, Kafka, direct ledger DB access, BlockService RPC, forking Conduit, making Conduit mandatory, snapshot generation, exactly-once delivery.
