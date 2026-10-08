# Phase 10 — Reference Consumer & Ecosystem Validation

Phase 10 validates **voi-fast-follower** from the perspective of an external consumer/operator. The follower is a reusable infrastructure primitive: deploy once, consume history and live blocks independently, restart consumers without rewinding acquisition.

## Architectural statement

**Voi Fast Follower is a durable, ordered raw block source that decouples Voi chain synchronization from downstream consumers.**

```text
                    Voi
                     ↓
                 Voi node
                     ↓
              Fast Follower
                     ↓
          canonical raw block stream
               ↓             ↓
            Archive       PostgreSQL (optional sink)
               ↓
         Consumers (reference, Conduit, custom)
```

---

## 1. Public consumer API (`pkg/consumer`)

**Stability: experimental (v0).** Pin a git tag for production integrations until promoted to stable.

| Symbol | Status | Role |
|--------|--------|------|
| `Block` | experimental | Canonical raw block; `Raw` is a caller-owned copy |
| `Source` | experimental | `Get(round)`, `Checkpoint()` |
| `Archive` | experimental | Filesystem archive reader (no Voi contact) |
| `Handoff` | experimental | Archive → live algod with boundary linkage check |
| `Cursor` / `Process` | experimental | Ordered sequential consumption |
| `CheckpointFile` | experimental | **Consumer cursor** (independent from follower) |
| `Range` / `ArchiveRange` | experimental | One-shot historical walks |
| `OpenAlgod` | experimental | Optional live segment after archive tip |

**Internal (not exported):** `internal/follower`, fetch workers, ordered buffer, sink batching.

### Ordering

- Strict round order when using `Cursor` or `Process`.
- Gaps surface as `ErrGap` (integrity failure).

### Raw bytes

- `Block.Raw` is algod `BlockRaw` msgpack. Each `Get` returns a **copy**; safe to retain after the call returns.

### Errors

| Error | Meaning | Action |
|-------|---------|--------|
| `ErrNotYetAvailable` | Round may appear later (growing archive, live tip pending) | Poll / wait / retry |
| `ErrNotFound` | Permanently unavailable (pruned, never captured) | Backfill from cold bundle or adjust start round |
| `ErrGap` | Missing round in ordered stream | Investigate archive integrity |
| `ErrCorrupt` | Validation / decode failure | Run `verify`, restore from backup |
| `RoundError` | Wraps any of the above with `Round` | Log and branch on `errors.Is` |

Temporary I/O failures propagate as standard Go errors; retry with backoff.

### Historical availability

- **Full history:** any round `[first, checkpoint]` is served from archive.
- **Rolling retention:** rounds below `keep-from` return `ErrNotFound` — never a silent partial range.
- **Partitioned / cold:** import bundle with `archive-import`, then consume — same API.

### Live behavior

- **Archive follow:** `Archive.GetWait` polls until the follower durable checkpoint advances (readers reload checkpoint from disk).
- **Archive → live:** `Handoff` serves archive through its checkpoint, verifies linkage at `checkpoint+1`, then live algod.
- **Follower never rewinds** for a slow consumer.

---

## 2. Consumer-independent checkpointing

```text
Follower durable checkpoint: 10,000,000
Consumer checkpoint:            9,999,500
```

The consumer resumes at **9,999,501** from archive/replay. The follower continues acquiring forward. No coordination file is shared.

Demonstrated in `pkg/consumer/ecosystem_test.go` (`TestConsumerIndependentCheckpoint`).

Consumer checkpoint file format: single decimal round + newline (last **fully processed** round). Checkpoint advances only after `OnBlock` succeeds → **at-least-once** safe on restart (may re-deliver the last round after crash).

---

## 3. Consumer lag (decoupling)

```text
Follower tip ─────────────────────────►
Consumer A: live (at tip)
Consumer B: 5,000 rounds behind (archive replay)
Consumer C: historical range N..M (batch job)
```

The follower does **not** slow down because B or C lag, provided required history exists in the archive (retention permitting). The durable archive is the fan-out buffer — not a message broker.

---

## 4. Independent consumer restart

```text
Follower running → Consumer processes N blocks → crash → restart → resume checkpoint+1
```

- Duplicate delivery of the last committed round is expected; handlers must be idempotent.
- No skipped rounds when consuming sequentially with `Process`.

Test: `TestConsumerRestartAtLeastOnce`, `TestTwoIndependentConsumers`.

---

## 5. Historical bootstrap without Voi

```text
Archive → reference consumer
```

No algod, no network. Use:

```bash
go run ./examples/block-consumer \
  --archive ./archive --start 1000000 --end 1001000 \
  --checkpoint ./consumer.cp
```

Or `consumer.ArchiveRange` in Go. Test: `TestHistoricalWithoutVoi`.

---

## 6. Historical → live transition

```text
archive history → catch up → archive tip → Handoff → live algod → new blocks
```

Same boundary semantics as Phase 8 (`HandoffSource`). No direct historical Voi queries. Linkage verified at `archive_checkpoint+1`.

```bash
go run ./examples/block-consumer \
  --archive ./archive --follow \
  --algod http://127.0.0.1:4001 --token "$VOI_ALGOD_TOKEN" \
  --checkpoint ./consumer.cp
```

Test: `TestHistoricalToLiveHandoff`.

---

## 7. Conduit validation (independent consumer)

```text
Voi → Fast Follower → Archive → Conduit → Postgres
```

Conduit is an **optional** downstream consumer via `voi_archive` importer (`plugins/conduit/`). Restart Conduit independently; the follower and archive continue. A consumer outage does **not** corrupt canonical history.

```bash
cd plugins/conduit && go build -o ../../bin/conduit ./cmd/conduit
ARCHIVE_ONLY=1 ARCHIVE_PATH=./archive START=N END=M ./scripts/phase7-demo.sh
```

See [phase7-conduit-adapter.md](phase7-conduit-adapter.md).

---

## 8. PostgreSQL sink + independent replay

```text
Fast Follower → Postgres sink        (optional indexing path)
Fast Follower → Archive → replay consumer   (parallel, canonical)
```

Postgres is one sink among many (`MultiSink`). The canonical stream for external consumers is the **archive filesystem** (or replay from it). Both paths coexist; neither is required for the other.

---

## 9. Retention behavior

| Strategy | Consumer request below floor | Behavior |
|----------|------------------------------|----------|
| Full history | old round | `Get` succeeds |
| Rolling (`archive-prune`) | pruned round | `ErrNotFound` |
| Partitioned | round in cold bundle only | `ErrNotFound` until `archive-import` |

Never silently return an incomplete range. `Range` stops on first error.

Test: `TestPrunedRangeReturnsNotFound`.

---

## 10. Consumer / operator errors

| Scenario | Expected behavior |
|----------|-------------------|
| Malformed request (start > end) | Immediate error from `Range` / CLI |
| Unavailable round (not yet) | `ErrNotYetAvailable`; poll in follow mode |
| Unavailable round (pruned) | `ErrNotFound` |
| Corrupt archive | `verify` fails; consumer may get `ErrCorrupt` |
| Consumer cancel (SIGINT) | Clean exit; checkpoint at last successful block |
| Sink failure (Postgres) | Follower checkpoint blocked; archive may still advance if in MultiSink min semantics |
| Follower restart | Archive checkpoint monotonic; consumers resume independently |
| Archive restart | Stateless read; reopen `OpenArchive` |

---

## 11. Security boundary

| Surface | Default | Notes |
|---------|---------|-------|
| Archive data | **Filesystem only** | No HTTP archive API; mount volume or copy bundle |
| Metrics / health | `:9090` | Operational plane; bind `127.0.0.1` in prod compose |
| Algod | Outbound from follower | Consumer live handoff uses configured algod URL |
| Authentication | **None built-in** | Do not expose archive dir or metrics beyond localhost without network ACL / reverse proxy |

Trust model: anyone with read access to the archive directory can read chain history. Treat archive like database storage.

---

## 12. Performance characterization (baseline)

Numbers from prior phase benchmarks on documented hardware (local algod unless noted). **Not optimized in Phase 10** — establish expectations only.

| Path | Throughput (blocks/sec) | Bottleneck |
|------|-------------------------|------------|
| Live acquisition (local algod) | ~2,000–2,900 | HTTP / node |
| Live acquisition (remote Nodely) | ~125–245 | Network |
| Archive replay (fsync off, test) | ~350+ | Disk |
| Archive + PostgreSQL e2e | ~240–750 | Postgres insert |
| Conduit offline import | Similar to archive iterate | Disk + translate |
| Reference consumer (`Process`) | Archive-bound | Same as replay read path |

The reference consumer adds negligible overhead vs direct `ArchiveRange` (one decode + callback per block).

Measure locally:

```bash
go test ./pkg/consumer -run TestHistoricalWithoutVoi -bench=. -benchtime=3s  # add bench if needed
go run ./cmd/replay -archive ./archive -start N -end M -sink /dev/null
```

---

## 13. API stability classification

| Package / area | Classification |
|----------------|----------------|
| `pkg/consumer` | **experimental** |
| `cmd/*` CLI flags | **stable** (operational contract) |
| Archive on-disk format | **stable** (versioned header) |
| `internal/*` | **internal** |
| `plugins/conduit` | **experimental** optional module |
| `storage.BlockSink` | **stable** for in-repo sinks; not the external consumer API |

Promote `pkg/consumer` to stable after one release cycle without breaking changes.

---

## 14. Integration test

`pkg/consumer/ecosystem_test.go` exercises:

1. Archive → consumer (independent checkpoint)
2. Consumer restart / at-least-once
3. Two independent consumers on one archive
4. Historical without Voi
5. Historical → live handoff
6. Growing archive follow (reader sees external writes)
7. Pruned range → `ErrNotFound`

Run:

```bash
go test ./pkg/consumer/... -v -count=1
```

Optional live stack:

```bash
./scripts/phase10-ecosystem.sh
```

---

## 15. Reference consumer

Minimal CLI at `examples/block-consumer/` using **only** `pkg/consumer`:

```bash
# Historical
go run ./examples/block-consumer \
  --archive ./archive --start 1000000 --end 1000100 \
  --checkpoint ./consumer.cp

# Follow growing archive (follower still running)
go run ./examples/block-consumer \
  --archive ./archive --follow --checkpoint ./consumer.cp

# Historical → live
go run ./examples/block-consumer \
  --archive ./archive --follow \
  --algod http://127.0.0.1:4001 --token "$VOI_ALGOD_TOKEN" \
  --checkpoint ./consumer.cp
```

---

## Success criteria (Phase 10)

- [x] External-style reference consumer (`examples/block-consumer`)
- [x] Public API boundary (`pkg/consumer`, experimental)
- [x] Independent consumer checkpoints
- [x] Multiple independent consumers
- [x] Historical consumption without Voi
- [x] Historical → live transition
- [x] Safe consumer restart (at-least-once)
- [x] Conduit as independent consumer (documented / Phase 7)
- [x] Explicit retention behavior
- [x] Security boundary documented
- [x] Developer workflow in README
- [x] No new messaging infrastructure
