# Phase 3 — Local Voi Node Performance

## Local node configuration

| Item | Value |
|---|---|
| Image | `xarmian/voinode:latest` (arm64) |
| Base | `algorand/algod` 4.0.2 (`rel/stable`, commit `6b940281`) |
| Genesis | `voimain-v1.0` |
| Catchup | Fast catchpoint `23360000#MAPK26MLGABPZKT5JQOOSCY7PSX7FTDTPXVASMFEM5C6MZ3QUEPQ` from Nodely |
| API | `http://127.0.0.1:4001` → container `:8080` |
| Token | `aaaaaaaa…` (64 a’s) via `TOKEN` env |
| Host | Apple M1 Pro, 10 cores, 32 GB RAM, APFS SSD |
| Docker | Docker Desktop 29.8.1 on aarch64 |
| Compose | `docker-compose.yml` service `voi-node` |

### Why not `ghcr.io/voinetwork/voi-node`?

Official `voi-node` is **amd64-only**. Under QEMU on Apple Silicon it crashes at startup (`runtime: lfstack.push invalid packing`). `xarmian/voinode` provides a native arm64 Voi mainnet genesis image.

### Lookback caveat

Non-archival local node retains roughly **~1000 recent blocks**. Benchmarks must use rounds near the local tip. Longer history requires `Archival: true` (not enabled for this phase).

---

## Fetch-only (same 800 rounds, workers sweep)

| Endpoint | Workers | Blocks | Elapsed | Blocks/sec | p50 | p95 |
|---|---:|---:|---:|---:|---:|---:|
| Nodely | 8 | 800 | 23.5s | 34 | 212ms | 398ms |
| Nodely | 16 | 800 | 12.8s | 63 | 217ms | 452ms |
| Nodely | 32 | 800 | 6.3s | **126** | 216ms | 383ms |
| Nodely | 64 | 800 | 3.3s | **244** | 224ms | 422ms |
| Local | 8 | 800 | 0.79s | **1019** | 5.4ms | 22ms |
| Local | 16 | 800 | 0.35s | **2295** | 6.0ms | 15ms |
| Local | 32 | 800 | 0.28–0.48s | **1679–2904** | 9–15ms | 24–47ms |
| Local | 64 | 800 | 0.36–0.74s | **1083–2213** | 26–42ms | 53–212ms |

**Local fetch ceiling ≈ 2.0–2.9k blk/s** (best around 16–32 workers).  
**Remote fetch ceiling ≈ 125–245 blk/s**.

---

## End-to-end PostgreSQL (`synchronous_commit=on`)

| Endpoint | Batch | Blocks/sec | Avg Commit | p95 Commit |
|---|---:|---:|---:|---:|
| Nodely | 1 | 41 | 7.1ms | 19ms |
| Nodely | 10 | 39 | 28ms | 25ms |
| Nodely | 50 | **42** | 44ms | 120ms |
| Nodely | 100 | 35 | 115ms | 432ms |
| Local | 1 | 123 | 6.3ms | 14ms |
| Local | **10** | **542** | 10.6ms | 22ms |
| Local | 50 | 237 | 9.0ms | 27ms |
| Local | 100 | 111 | 16ms | 73ms |

---

## Sustained catch-up

| Run | Rounds | Blocks/sec | Peak RSS | Notes |
|---|---:|---:|---:|---|
| Local e2e batch=10 | 900 | 88 | ~30 MB | Tip advancing; lookback churn during run |
| Nodely e2e batch=50 | **10,000** | 67 | ~44 MB | Checkpoint contiguous; no errors |

Buffer depth stays bounded by `FETCH_WINDOW`. Peak follower RSS stayed modest (&lt;50 MB).

---

## Crash / restart (batched commits)

Postgres integration test `TestCrashRecoveryBatchSemantics`:

- failed/non-contiguous batch → checkpoint unchanged  
- successful batch → checkpoint = final round  
- resume = checkpoint + 1  
- duplicate batch retry → idempotent, no duplicate rows  
- previous-block hashes retained  

---

## Ceilings

```text
Nodely fetch       ≈ 125–245 blk/s
Local fetch        ≈ 2000–2900 blk/s

Nodely PostgreSQL  ≈ 40–45 blk/s   (fetch-bound)
Local PostgreSQL   ≈ 120–540 blk/s (batch=10 peak ~540)
```

---

## Bottleneck decision → **Case A (with nuance)**

Local acquisition is **dramatically faster** than Nodely (~20×).  
Against a local node, PostgreSQL is no longer stuck at ~45 blk/s — batched commits reach **hundreds of blk/s**.

Nodely end-to-end ~45 blk/s was primarily an **acquisition / RTT ceiling**, not a hard Postgres limit.

With local algod, persistence becomes the next limiter (fetch 2k+ vs e2e peak ~540).

### Recommended Phase 4

Optimize the Postgres sink path while keeping durability:

1. ~~`COPY` / staging temp tables~~ — done in Phase 4; UNNEST batch=10 won (~650–750 blk/s). See [phase4-postgres-sink.md](phase4-postgres-sink.md). 
2. Reduce per-batch SQL round-trips further  
3. Measure commit fsync vs statement time  
4. Keep ordered, gap-free, atomic checkpoint semantics  

Do **not** yet add Kafka, ledger-DB reads, or async/unsafe commit as the default.
