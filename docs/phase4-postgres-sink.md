# Phase 4 — PostgreSQL Sink Optimization

Phase 3 showed local Voi acquisition (~2–2.9k blk/s) far exceeds durable Postgres e2e (~540 blk/s peak). Phase 4 optimized the sink without changing the follower correctness model.

## Architecture

```text
ordered block batch
       ↓
Postgres transaction  (synchronous_commit=on by default)
       ↓
UNNEST  ──or──  COPY → session temp stage_* → INSERT ON CONFLICT
       ↓
checkpoint UPSERT  (same transaction)
       ↓
COMMIT
```

The follower still only sees `BlockSink` / `BatchBlockSink`. Insert strategy is selected via `PG_INSERT_MODE` / `database.insert_mode`:

| Mode | Hot path |
|---|---|
| **`unnest` (default)** | 1× `INSERT…SELECT UNNEST` blocks + 1× txs + checkpoint |
| **`copy`** | `COPY` into session temp `stage_*` (prepared once per pool connection, `ON COMMIT DELETE ROWS`) → `INSERT…ON CONFLICT` + checkpoint |

Both keep **checkpoint + block/tx rows in one durable transaction**. Retries use `ON CONFLICT DO NOTHING`.

### Baseline work per batch (UNNEST)

| Item | Count |
|---|---|
| SQL statements | 3–4 (`BEGIN` + 1–2 inserts + checkpoint + `COMMIT`) |
| Block rows | = batch size |
| Tx rows | Σ txns in batch |
| Indexes | `blocks(round)` PK; `transactions(txid)` UNIQUE; `idx_transactions_round` |
| FKs | `transactions.round → blocks.round` |
| Pool | `MaxConns ≥ 4`; single commit goroutine |

Phase timings (`BEGIN` / writes / checkpoint / `COMMIT`) are recorded on each successful `CommitBatch`.

## Benchmark methodology

- Local `xarmian/voinode` on `:4001` (non-archival lookback ≈ 1k rounds → **900-round** windows)
- Postgres 16, `synchronous_commit=on`
- 32 fetch workers
- Contiguous tip-relative ranges; DB truncated between runs
- `COMMIT_FLUSH_INTERVAL=200ms` for operating-point runs (live-like catch-up)
- Full-batch compare mode disables time flush (`sink-compare`) so batch size is isolated

```bash
docker compose up -d postgres voi-node
docker compose stop follower   # avoid contending on the same DB
export VOI_ALGOD_URL=http://127.0.0.1:4001
export VOI_ALGOD_TOKEN=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
export DATABASE_URL=postgres://follower:follower@localhost:5432/voi_follower?sslmode=disable
COUNT=900 MODE=sink-compare ./scripts/phase4-sink-compare.sh
```

## Bottleneck analysis

Commit-phase timings show **writes dominate**; `COMMIT` fsync is typically ~1–3 ms. Larger batches inflate per-commit write time and, when flush is disabled, stall the ordered pipeline waiting to fill the batch — **larger is not faster**.

`COPY` pays an extra round trip per table (`COPY` + `INSERT` from stage) versus a single `UNNEST` insert. For Voi block payloads at practical batch sizes, that RTT cost outweighs binary `COPY` benefits.

## UNNEST vs COPY (local, sync commit, 32 workers, 900 blocks)

### Operating point (`flush=200ms`)

| Implementation | Batch | Blocks/sec | Avg Commit | p50 | p95 | Write avg |
|---|---:|---:|---:|---:|---:|---:|
| **UNNEST** | **10** | **~652–752** | ~7–9 ms | ~6–7 ms | ~14–24 ms | ~5–6 ms |
| UNNEST | 50 | ~225 | ~13 ms | ~5 ms | ~41 ms | ~9 ms |
| COPY | 10 | ~326–384 | ~18–22 ms | ~12–15 ms | ~43–46 ms | ~13–17 ms |
| COPY | 25 | ~392 | ~15 ms | ~13 ms | ~30 ms | ~10 ms |
| COPY | 50 | ~213 | ~18 ms | ~8 ms | ~56 ms | ~13 ms |

### Full-batch isolate (`flush=1h`, sink-compare)

| Implementation | Batch | Blocks/sec | Avg Commit | p50 | p95 |
|---|---:|---:|---:|---:|---:|
| UNNEST | 10 | 390 | 17 ms | 14 ms | 33 ms |
| UNNEST | 50 | 159 | 31 ms | 7 ms | 140 ms |
| UNNEST | 100 | 123 | 15 ms | 5 ms | 66 ms |
| UNNEST | 250 | 68 | 28 ms | 6 ms | 24 ms |
| UNNEST | 500 | 37 | 8 ms | 5 ms | 12 ms |
| COPY | 10 | 115 | 73 ms | 17 ms | 94 ms |
| COPY | 50 | 190 | 22 ms | 9 ms | 84 ms |
| COPY | 100 | 82 | 47 ms | 10 ms | 201 ms |
| COPY | 250 | 89 | 12 ms | 7 ms | 16 ms |
| COPY | 500 | 34 | 15 ms | 7 ms | 33 ms |

Peak heap delta during compare runs stayed on the order of a few MiB (bounded buffer + batch payloads).

## Selected defaults

| Setting | Value | Why |
|---|---|---|
| `PG_INSERT_MODE` | **`unnest`** | Higher durable throughput than COPY for this schema/payload |
| `COMMIT_BATCH_SIZE` | **`10`** | Best practical e2e blk/s with low p95 |
| `PG_ASYNC_COMMIT` | **off** | Durability; async did not help overall throughput |

Live mode remains **batch size 1**.

## Async-commit experiment (opt-in only)

`PG_ASYNC_COMMIT=true` sets `SET LOCAL synchronous_commit=off` inside each batch TX.

| Mode | Batch | Blocks/sec | pg_commit avg |
|---|---:|---:|---:|
| UNNEST sync | 10 | ~654 | ~0.9 ms |
| UNNEST async | 10 | ~637 | ~0.4 ms |

Faster durable commit did not raise end-to-end rate — **write SQL still dominates**. Keep async disabled by default.

## Durability & crash recovery

Injected fault points (`before_begin`, `during_writes`, `during_checkpoint`, `before_commit`) leave:

```text
checkpoint == last fully committed round
restart_round = checkpoint + 1
```

`after_commit` returns an error (lost ACK) but data + checkpoint are durable; idempotent retry is safe. Covered by `TestCrashPointsRollback` and `TestCrashRecoveryBatchSemantics` for both insert modes. `TestInsertModesEquivalence` checks identical DB state for the same fixture under UNNEST and COPY.

## How close to the local acquisition ceiling?

| Layer | Blocks/sec |
|---|---:|
| Local fetch-only (32 workers) | **~2,780** |
| Phase 3 e2e peak | ~540 |
| **Phase 4 e2e (UNNEST, batch=10)** | **~650–750** |
| Gap to acquisition | still **~3.5–4×** |

Durable Postgres persistence is closer, but **still the limiter**. COPY was not the win; smaller UNNEST batches were.

## Recommendation for Phase 5

Stop micro-optimizing insert syntax. Remaining options that preserve the correctness model:

1. **Schema / payload trim** — store less raw data, or compress `raw_block` if consumers allow
2. **Postgres tuning** — dedicated volume, `shared_buffers`, checkpoint settings (still `synchronous_commit=on`)
3. **Optional parallel sinks** only if a second consumer can accept the same ordered stream without weakening the primary checkpoint
4. Do **not** introduce Kafka/Redis/S3/async-default durability for the primary path

Phase 5 should pick one concrete consumer path (e.g. Conduit handoff or a thin query API) rather than further bulk-insert experiments unless profiling shows a new hot spot.
