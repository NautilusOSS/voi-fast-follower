# Phase 5 — Raw Block Stream and Archive

Phase 4 optimized the Postgres sink (~650–750 blk/s durable). Phase 5 turns the follower into a **reusable, gap-free raw block delivery layer**: acquisition and ordering stay in the follower; persistence is pluggable via sinks.

## Architecture

```text
Voi algod
   ↓
fetch workers (bounded window)
   ↓
validation (round, hash, prev-hash linkage)
   ↓
ordered block buffer
   ↓
canonical Block stream
   ↓
        ┌─────────────┴─────────────┐
        ↓                           ↓
   PostgresBlockSink          ArchiveSink
        └─────────────┬─────────────┘
                      ↓
                 MultiSink (optional)
                      ↓
              durable checkpoint
           (min across required sinks)
```

```text
Archive  ──replay──►  BlockSink (Postgres / Archive / both)
                         ↑
                   no network I/O
```

## Block representation

`internal/block.Block` is the canonical stream element:

| Field | Role |
|---|---|
| `Round` | Contiguous ordering key |
| `BlockHash` / `PreviousBlockHash` | Chain linkage (BH / branch) |
| `Timestamp`, `TxnCount` | Lightweight metadata |
| `Raw` | **Original algod msgpack `BlockRaw` bytes** (never discarded) |
| `Transactions[]` | Optional decoded view; each keeps its own `Raw` |

Downstream sinks must not need to re-fetch from Voi when `Raw` is present.

## Sink abstraction

```go
type BlockSink interface {
    Commit(ctx context.Context, block Block) error
    LastProcessedRound(ctx) (round uint64, ok bool, err error)
    MaxBlockRound(ctx) (uint64, bool, error)
    BlockHash(ctx, round uint64) (hash string, ok bool, err error)
    Close() error
}

type BatchBlockSink interface {
    BlockSink
    CommitBatch(ctx context.Context, blocks []Block) error
}
```

- **Postgres** — UNNEST/COPY batch inserts + `sync_state` checkpoint (Phase 4).
- **Archive** — segment files + `checkpoint` file (this phase).
- **MultiSink** — fans out to N sinks; **all must succeed** before the follower treats the batch as committed.

The follower package never imports Postgres types.

## Archive format

Chosen: **fixed-size round segments** (default 1000 rounds), not one-file-per-block.

```text
archive/
  checkpoint                 # decimal text: last durable round
  segments/
    000000000000001000.seg   # rounds [1000, 1999]
    000000000000002000.seg
```

### Segment layout (little-endian)

```text
Header (20 bytes):
  magic       "VFFSEG01"     (8)
  version     u32 = 1        (4)
  start_round u64            (8)

Record (repeated):
  round       u64
  hash_len    u16 + hash bytes
  prev_len    u16 + prev-hash bytes
  raw_len     u32 + raw BlockRaw bytes
```

Each record is enough to verify `N+1` follows `N` and `hash(N) == previous_hash(N+1)` after `DecodeRaw`.

Gaps are rejected: after checkpoint `C`, the next written round must be `C+1`.

## Crash semantics

With `DisableSync=false` (default):

1. Append record bytes to segment file(s)
2. `File.Sync()` each dirty segment
3. Write `checkpoint.tmp` → Sync → rename to `checkpoint` → Sync archive directory when possible

**The checkpoint file is authoritative.** On open, segments are reconciled: bytes past the checkpoint (and truncated/corrupt trailers) are discarded. A crash between step 2 and 3 therefore does **not** advance the durable cursor.

Idempotent retries of an already-checkpointed batch are no-ops.

## Checkpoint semantics (multi-sink)

```text
checkpoint = highest round durably accepted by ALL required sinks
```

`MultiSink.LastProcessedRound` returns the **minimum** of per-sink checkpoints. If any sink has no checkpoint, the composite reports none.

Failure policy (v1): any sink error fails the batch. Sinks must be idempotent so a partial fan-out (A succeeded, B failed) is safe to retry.

Restart: `resume_round = checkpoint + 1` (unchanged follower rule).

## Replay

```bash
go run ./cmd/replay \
  -archive ./archive \
  -start 23369178 \
  -end 23369228 \
  -sink postgres \
  -database "$DATABASE_URL"
```

Replay:

1. Reads segment records in round order
2. Validates stored hashes / linkage
3. Reconstructs `Block` (full `DecodeRaw` when payload is real BlockRaw)
4. Commits through the same `BlockSink` stack
5. **Does not contact Voi**

Demonstrated path:

```text
Voi → Follower → Archive → replay → PostgreSQL
```

## Configuration

| Variable | Meaning |
|---|---|
| `DATABASE_URL` | Enable Postgres when non-empty (unless `PG_ENABLED=false`) |
| `ARCHIVE_PATH` | Enable archive (also sets enabled) |
| `ARCHIVE_ENABLED` | Explicit on/off |
| `ARCHIVE_SEGMENT_SIZE` | Rounds per `.seg` (default 1000) |

At least one sink is required. Explorer HTTP API is registered only when Postgres is enabled.

## Benchmarks (local algod, 32 workers, batch=10, sync commit / fsync)

900-round tip window (non-archival lookback ≈ 1k):

| Pipeline | Blocks/sec | Notes |
|---|---:|---|
| Fetch only | ~1,980–2,780 | Acquisition baseline (run variance) |
| PostgreSQL | ~270–750 | Phase 4 durable UNNEST; run-dependent |
| Archive | ~350 | fsync per batch; simple segment append |
| Archive + PostgreSQL | ~240 | MultiSink = sum of sink costs |

### Interpretation

- Archive is **not** at the acquisition ceiling: durable `fsync` per batch dominates.
- Archive alone can beat a cold/contended Postgres run, but does not replace Phase 4’s best Postgres numbers.
- Dual-sink is limited by the slower of the two (plus fan-out).
- Replay into Postgres from archive reached ~600 blk/s on a 51-block sample (no network fetch).

```bash
COUNT=900 ./scripts/phase5-pipelines.sh
```

## Metrics

Existing buffer/commit gauges plus:

| Metric | Meaning |
|---|---|
| `voi_follower_archive_bytes_written_total` | Bytes appended |
| `voi_follower_archive_errors_total` | Archive errors |
| `voi_follower_ordered_buffer_depth` | Backpressure signal |
| `voi_follower_checkpoint_lag` | Tip vs durable cursor |

Slow archive/Postgres naturally fills `FETCH_WINDOW` and stalls workers — bounded by design.

## Future Conduit integration

Not implemented in Phase 5. Intended shape:

```text
Voi → Fast Follower → ordered raw Block stream → Conduit adapter → Conduit
```

The follower will **not** grow ARC-200/72 processors or indexing logic. A future adapter should consume `Block.Raw` (or archive replay) and present Conduit’s expected block import surface.

## Known limitations

- Archive throughput is fsync-bound; no group-commit / io_uring yet
- MultiSink is sequential (postgres then archive), not parallel
- Segment format is purpose-built (simple, documented) — not tar/SQLite
- Explorer UI still requires Postgres
- Local non-archival algod limits long-range benches (~1k rounds)

## Success criteria

1. Canonical ordered `Block` stream with preserved `Raw` — **yes**
2. Acquisition independent of Postgres — **yes** (`BuildSinks` / archive-only)
3. Durable archive + crash reconcile — **yes**
4. Replay without Voi — **yes** (`cmd/replay`)
5. Archive → Postgres equivalence tests — **yes**
6. Multi-sink all-must-succeed checkpoint — **yes**
7. Bounded backpressure — **yes** (existing fetch window)
