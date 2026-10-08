# Voi Fast Follower

Proof-of-concept **block ingestion primitive** for the Voi network. It sits between a Voi algod endpoint and downstream consumers (Conduit, indexers, analytics) and focuses on one job:

> Provide reliable Voi blocks quickly, starting from a recent round—not from genesis.

This is **not** an indexer. Raw blocks/transactions are persisted; application decoding (ARC-200, DEX events, etc.) belongs downstream.

```text
Voi Network (algod)
        │
        ▼
 concurrent BlockRaw fetches  (worker pool)
        │
        ▼
 ordered commit               (gap-free)
        │
        ▼
 durable checkpoint           (last_processed_round)
        │
        ▼
 PostgresBlockSink  ←── BlockSink interface (future: Kafka, NATS, Conduit, …)
```

## Features (Phase 1)

- Configurable algod URL/token (no hard-coded endpoints; no node internals)
- `GetBlock` / `WaitForBlockAfter` over standard algod v2 msgpack `BlockRaw`
- Concurrent fetch worker pool (default **32**) with bounded `FETCH_WINDOW`
- Strictly ordered commits; out-of-order fetches are buffered until the gap fills
- **Batched catch-up commits** (`COMMIT_BATCH_SIZE`, default **50**) via `BatchBlockSink`
- Live follow forces batch size **1** for low latency
- Durable checkpoint advanced atomically with each (batch) commit
- Idempotent writes (`ON CONFLICT DO NOTHING`)
- Live follow via `wait-for-block-after` using the same commit path
- Prometheus metrics on `:9090/metrics`
- Docker Compose: `follower` + `postgres`

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
| `COMMIT_BATCH_SIZE` | Contiguous blocks per catch-up commit (default 50) |
| `COMMIT_FLUSH_INTERVAL` | Partial-batch flush while catching up (default 200ms) |
| `DATABASE_URL` | Postgres DSN |
| `LOG_LEVEL` | `debug` / `info` / `warn` / `error` |
| `METRICS_ADDR` | Metrics listen address (default `:9090`) |

See [config.example.yaml](config.example.yaml). Legacy `PREFETCH_WORKERS` / `PREFETCH_BUFFER` env vars still work.

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

## Benchmark

```bash
docker compose up -d postgres
export DATABASE_URL=postgres://follower:follower@localhost:5432/voi_follower?sslmode=disable

# Fetch-only + COMMIT_BATCH_SIZE sweep (1/10/50/100/500) over 1000 rounds
COUNT=1000 WORKERS=32 MODE=both ./scripts/bench.sh

# Single e2e run:
go run ./cmd/bench -mode e2e -count 1000 -workers 32 -batch 50 -reset
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

```bash
docker compose up -d postgres voi-node
# After catchup (see docs/phase3-local-node.md):
export VOI_ALGOD_URL=http://127.0.0.1:4001
export VOI_ALGOD_TOKEN=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
./scripts/phase3-local.sh
```

## Tests

```bash
go test ./...

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
internal/config/
internal/voi/          # GetBlock, WaitForBlockAfter
internal/block/        # msgpack decode + hash
internal/follower/     # worker pool + ordered commit
internal/storage/      # BlockSink + PostgresBlockSink
internal/metrics/
migrations/
scripts/bench.sh
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

ARC-200/72 decoding, DEX/NFT indexing, explorer APIs, GraphQL, Supabase, Kafka, direct ledger DB access, BlockService RPC, Conduit integration, snapshot generation.
