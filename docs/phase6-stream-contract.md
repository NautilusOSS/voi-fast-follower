# Phase 6 — Stream Contract & Production Hardening

Phase 5 delivered archive + MultiSink. Phase 6 makes the primitive **stable and
operationally trustworthy** for external consumers (including a future Conduit adapter).

## Deliverables

1. **[Stream contract](stream-contract.md)** — ordering, continuity, at-least-once, restart, failure
2. **Concurrent MultiSink** — independent sinks commit in parallel; checkpoint remains `min`
3. **`cmd/verify`** — offline archive verification
4. **`cmd/archive-info`** — lightweight archive metadata
5. **Health** — `/healthz`, `/readyz` with modes: startup / catching_up / live / degraded / failed
6. **Graceful shutdown** — stop fetches, best-effort drain, commit timeout via `context.WithoutCancel`
7. **Recovery & soak tests** — segment rotation, beyond-checkpoint bytes, catch-up→live→restart
8. **Long-range multi-segment archive test** (3500 synthetic rounds)

## MultiSink failure model

```text
batch ──┬──► Postgres  ──┐
        └──► Archive   ──┴──► all succeed → follower advances
                              any fail   → retry (idempotent)
```

Composite checkpoint:

```text
min(successfully durable sink checkpoints)
```

No distributed transactions. Concurrent execution is an optimization only.

## Archive tooling

```bash
go run ./cmd/verify -archive ./archive
go run ./cmd/archive-info -archive ./archive
```

Verify checks segment headers, framing, gaps, decode/hash linkage, and checkpoint consistency
without contacting Voi.

### Future retention (not implemented)

Documented only: prune whole segments whose `end_round < checkpoint - retention_window`,
never delete the segment containing the checkpoint, and never rewrite history.

## Health modes

| Mode | Meaning | `/readyz` |
|---|---|---|
| `startup` | Not yet progressing | 503 |
| `catching_up` | Healthy, behind tip | 200 |
| `live` | Near tip | 200 |
| `degraded` | Sink errors / retries | 200 |
| `failed` | Cannot continue | 503 |

## Graceful shutdown

On SIGINT/SIGTERM:

1. Context cancels → stop dispatching new fetches
2. Drain already-buffered contiguous rounds (bounded)
3. In-flight `CommitBatch` uses a timeout detached from cancel
4. Incomplete work does **not** advance the checkpoint
5. HTTP server and sinks close

## Regression posture

Phase 6 does not change the acquisition path. Expected baselines remain:

| Pipeline | Blocks/sec |
|---|---:|
| Local fetch | ~2,000–2,800 |
| Durable Postgres | ~650–750 peak |
| Archive | ~350 |

Correctness/ops changes should not introduce a major regression; run
`COUNT=900 ./scripts/phase5-pipelines.sh` after upgrades.

## Consumer integration

```text
Implement storage.BatchBlockSink (or BlockSink)
Wire via BuildSinks / MultiSink
Rely on docs/stream-contract.md — not on fetch internals
```

Replay path is equivalent:

```text
Archive → cmd/replay → same BlockSink
```

## Non-goals (unchanged)

Kafka, Redis, NATS, ledger DB reads, Conduit, ARC processors, object storage,
distributed checkpoint protocols, exactly-once claims, automatic pruning.
