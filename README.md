# Voi Fast Follower

Proof-of-concept **block ingestion primitive** for the Voi network. It sits between a Voi algod endpoint and downstream consumers (Conduit, indexers, analytics) and focuses on one job:

> Provide reliable Voi blocks quickly, starting from a recent round—not from genesis.

This is **not** an indexer. Raw blocks/transactions are persisted; application decoding (ARC-200, DEX events, etc.) belongs downstream.

```text
Voi Network (algod)
        │
        ▼
 Voi Fast Follower  ──► PostgreSQL (PostgresBlockSink)
        │
        └── BlockSink interface (future: Kafka, NATS, Conduit, …)
```

## Features (Phase 1)

- Configurable algod URL/token (no hard-coded endpoints)
- `start_round: latest` or an explicit round
- Sequential, gap-free ingestion with concurrent prefetch
- Durable checkpoint (`last_processed_round`) committed with each block
- Idempotent writes (`ON CONFLICT DO NOTHING`)
- Node outage retry with exponential backoff
- Prometheus metrics on `:9090/metrics`
- Docker Compose: `follower` + `postgres`

## Quick start

### Docker Compose

```bash
# Optional: catch up the last 200 rounds instead of tip-only
export VOI_START_ROUND=$(($(curl -fsS https://mainnet-api.voi.nodely.dev/v2/status | python3 -c 'import sys,json;print(json.load(sys.stdin)["last-round"])') - 200))

docker compose up --build
```

Environment variables:

| Variable | Description |
|---|---|
| `VOI_ALGOD_URL` | Algod base URL (example: `https://mainnet-api.voi.nodely.dev`) |
| `VOI_ALGOD_TOKEN` | API token if required |
| `VOI_START_ROUND` | `latest` or integer round |
| `VOI_SYNC_MODE` | `fast` (concurrent prefetch) |
| `DATABASE_URL` | Postgres DSN |
| `POLL_INTERVAL` | Fallback poll when waiting at tip |
| `PREFETCH_WORKERS` | Concurrent block fetch workers (default 16) |
| `METRICS_ADDR` | Metrics listen address (default `:9090`) |

See [config.example.yaml](config.example.yaml).

### Local binary

```bash
# Start Postgres (compose service only)
docker compose up -d postgres

export VOI_ALGOD_URL=https://mainnet-api.voi.nodely.dev
export DATABASE_URL=postgres://follower:follower@localhost:5432/voi_follower?sslmode=disable
export VOI_START_ROUND=latest   # or an explicit round for catch-up demos

go run ./cmd/follower -config config.example.yaml -migrations migrations
```

Metrics: `curl localhost:9090/metrics`  
Health: `curl localhost:9090/healthz`  
Explorer UI: [http://localhost:9090/](http://localhost:9090/) (read-only demo over ingested blocks)

## How it works

1. **Checkpoint** – If `sync_state.last_processed_round` exists, resume at `N+1`. Otherwise use `start_round` (`latest` → current tip).
2. **Prefetch** – Fetch a window of rounds concurrently (`PREFETCH_WORKERS`).
3. **Ordered commit** – Persist block + txs + checkpoint in one Postgres transaction, in strict round order (never skip).
4. **Live follow** – At tip, use `GET /v2/status/wait-for-block-after/{round}`; same ingestion path as catch-up.

### Correctness guarantees

- No silent round skips
- At-least-once ingestion with idempotent inserts
- Restart resumes from last safely committed round
- Optional chain-link check: `previous_block_hash` vs stored prior `block_hash`

## Schema

See [migrations/001_initial.sql](migrations/001_initial.sql):

- `blocks` – round, hashes, timestamp, txn_count, `raw_block`
- `transactions` – round, txid, sender, type, `raw_transaction`
- `sync_state` – `last_processed_round`

## Metrics

| Metric | Meaning |
|---|---|
| `voi_follower_current_round` | Commit cursor |
| `voi_follower_target_round` | Network tip |
| `voi_follower_catchup_lag` | `target - current` |
| `voi_follower_last_processed_round` | Durable checkpoint |
| `voi_follower_blocks_processed_total` | Counter |
| `voi_follower_blocks_per_second` | Recent throughput |
| `voi_follower_errors_total` | Errors |

## Benchmark

```bash
# Postgres must be up. RESET_DB=1 clears the range before the run.
docker compose up -d postgres
export DATABASE_URL=postgres://follower:follower@localhost:5432/voi_follower?sslmode=disable
export VOI_START_ROUND=  # leave empty to default tip-500 in the script
RESET_DB=1 ./scripts/bench.sh
```

The script reports start/end round, elapsed time, average and peak blocks/sec. Capture CPU/RAM with `docker stats` or Activity Monitor during the run.

## Research notes (Phase 1)

| Topic | Conclusion |
|---|---|
| Authoritative node | [VoiNetwork/go-algorand](https://github.com/VoiNetwork/go-algorand) (`AVAIL` channel); Docker `ghcr.io/voinetwork/voi-node` |
| SDK | [`github.com/algorand/go-algorand-sdk/v2`](https://github.com/algorand/go-algorand-sdk) — Voi speaks standard algod v2 |
| Block API | `GET /v2/blocks/{round}?format=msgpack` (`BlockRaw`); no multi-round batch API |
| Throughput | Concurrent prefetch + ordered commit is the practical win over sequential HTTP |
| Follower node vs this app | Conduit needs follow-mode + state deltas. This PoC only needs `GetBlock` (works on public/archival APIs) |
| Catchpoints | Bootstrap the **node**, not this follower process |
| Mimir | Nautilus NFT Navigator indexer (`mainnet-idx.nautilus.sh`) — downstream consumer, not coupled here |
| Conduit | Treat as a future **consumer**. Official Voi example: [voi-examples/indexer](https://github.com/VoiNetwork/voi-examples/tree/main/indexer) with `VOINETWORK_PROFILE=conduit` |

Genesis ID observed on mainnet API: `voimain-v1.0`.

## Project layout

```text
cmd/follower/main.go
internal/config/
internal/voi/
internal/block/
internal/follower/
internal/storage/     # BlockSink + PostgresBlockSink
internal/metrics/
migrations/
scripts/bench.sh
Dockerfile
docker-compose.yml
```

## Tests

```bash
go test ./...

# Live algod decode check
VOI_INTEGRATION=1 go test ./internal/block -v

# Postgres idempotency (compose postgres up)
DATABASE_URL=postgres://follower:follower@localhost:5432/voi_follower?sslmode=disable \
  go test ./internal/storage -v
```

## Future (not Phase 1)

- Official follower snapshots (`voi-follower-snapshot-N`)
- Additional `BlockSink` implementations (Kafka, NATS, object storage)
- Optional co-located local-ledger read path
- Conduit integration validation (consumer of this stream or of algod)

## Non-goals

Full explorer, REST/GraphQL API, ARC-200/72 indexers, DEX/NFT business logic, Supabase, consensus node, Conduit fork.
