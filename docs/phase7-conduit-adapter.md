# Phase 7 — Conduit Consumer Adapter

**Architectural statement:** Voi Fast Follower provides the canonical synchronization and delivery layer; Conduit is a downstream consumer.

This document records the Conduit import contract (researched against Conduit **v1.10.0**) and the adapter design used in this repository.

---

## 1. Conduit importer contract (exact)

### Importer interface

```go
// github.com/algorand/conduit/conduit/plugins/importers
type Importer interface {
    plugins.Plugin // Metadata, Init, Close

    GetGenesis() (*sdk.Genesis, error)
    GetBlock(rnd uint64) (data.BlockData, error)
}
```

Optional hooks (same repo):

| Hook | Purpose |
|---|---|
| `conduit.RoundRequestor` | Override pipeline start round from plugin config |
| `conduit.Completed` / `OnComplete(BlockData)` | Called after a round is fully exported (algod follower uses this for `SetSyncRound`) |
| `conduit.PluginMetrics` / `ProvideMetrics(subsystem)` | Register Prometheus collectors |

Conduit is a **pull** pipeline: the framework calls `GetBlock(round)` for each round in order, starting from the exporter/pipeline checkpoint (`InitProvider.NextDBRound()`). There is **no push importer API**.

### Block representation (`data.BlockData`)

```go
type BlockData struct {
    BlockHeader sdk.BlockHeader           // required for round / hash linkage
    Payset      []sdk.SignedTxnInBlock    // transactions carried by the block
    Delta       *sdk.LedgerStateDelta     // optional account/state changes
    Certificate *map[string]interface{}   // optional cert map from BlockRaw
}
```

Helpers: `Round()` → `uint64(BlockHeader.Round)`; `Empty()` → `len(Payset)==0`.

### How the stock algod importer builds `BlockData`

1. Wait until the node tip ≥ requested round (`StatusAfterBlock`).
2. `BlockRaw(rnd)` → msgpack `models.BlockResponse`.
3. Map:
   - `BlockHeader` ← `tmpBlk.Block.BlockHeader`
   - `Payset` ← `tmpBlk.Block.Payset`
   - `Certificate` ← `tmpBlk.Cert`
4. **Follower mode only:** also fetch `/v2/deltas/{rnd}` into `Delta` (except round 0).
5. OnComplete (follower mode): `SetSyncRound(round+1)` so the follower node retains deltas.

### How `file_reader` works (offline precedent)

- Init reads a directory of previously written Conduit block files.
- `GetGenesis` loads `genesis.json`.
- `GetBlock(rnd)` decodes one file per round — pure pull, no network.

This is the closest built-in analogue to an archive-backed importer.

### Ordering expectations

- Conduit advances rounds **sequentially**: N, then N+1, …
- The importer must return the block for the **exact** requested round.
- Skipping or returning the wrong round breaks the pipeline (algod defines `SyncError` for tip/round mismatch).

### Error handling

- Any `GetBlock` error stops that round; Conduit does **not** advance its durable pipeline cursor past a failed export.
- Retries re-call `GetBlock` for the same round → **at-least-once** delivery into processors/exporters.
- Exactly-once is **not** provided by Conduit or by this adapter.

### Checkpointing behavior

| Cursor | Owner | Meaning |
|---|---|---|
| Fast Follower sink checkpoint | Follower + required sinks | Highest round durably accepted by the follower’s sinks |
| Conduit pipeline / exporter DB round | Conduit | Next round Conduit will request (`NextDBRound`) |

These are **independent**. The adapter must not write the follower checkpoint, and must not treat Conduit’s cursor as the follower’s source of truth.

### Follower vs archival algod mode

| Mode | Blocks | Deltas | Sync round |
|---|---|---|---|
| `archival` | BlockRaw | none | n/a |
| `follower` | BlockRaw + `/v2/deltas` | required | OnComplete advances |

Many Indexer-style exporters expect deltas in follower mode. An archive of BlockRaw **cannot** synthesize `LedgerStateDelta`.

### Minimum data for typical processors/exporters

| Field | Required for | Available from Fast Follower archive |
|---|---|---|
| `BlockHeader` (round, branch/prev, timestamp, …) | all | yes (from Raw) |
| `Payset` | txn processors / most exporters | yes (from Raw) |
| `Certificate` | some exporters / audit | yes when present in Raw |
| `Delta` | Indexer postgres / account state exporters | **no** (not stored) |

**Implication:** Fast Follower → Conduit is suitable for **block/transaction pipelines** (file_writer, custom processors, txn-centric exporters). It is **not** a drop-in replacement for algod **follower mode** when the exporter requires ledger deltas.

### External plugins

Conduit does not load shared-object plugins. Custom importers are enabled by a **custom binary** that blank-imports the plugin package so `init()` calls `importers.Register(...)`.

---

## 2. Adapter placement (this repo)

```text
Voi algod
    ↓
voi-fast-follower          ← acquisition, order, validate, checkpoint, backpressure
    ↓
ordered canonical Block    ← Raw BlockRaw msgpack preserved (lossless)
    ↓
ArchiveSink (recommended)  ← durable ordered store
    ↓
Conduit importer plugin    ← translate + GetBlock pull  (plugins/conduit)
    ↓
Conduit processors/exporters
```

The adapter **does not**:

- run catch-up workers
- maintain a second fetch window
- query arbitrary historical rounds from algod
- reimplement hash validation (archive already verified on write/read)
- maintain a competing chain checkpoint for the follower

### Why pull-from-archive (not a BlockSink push)

Conduit’s only import surface is `GetBlock(rnd)`. A `BlockSink` that tried to “push into Conduit” would still need an in-process Conduit pipeline and would couple the follower process to Conduit. Keeping Conduit optional requires the adapter to live in a **separate module/binary** that reads the follower’s durable stream (archive).

Conceptual stream boundary (core module, no Conduit dependency):

```go
type RoundSource interface {
    Get(ctx context.Context, round uint64) (block.Block, error)
    Checkpoint(ctx context.Context) (round uint64, ok bool, err error)
}
```

`Next`-style cursors are built on top of `RoundSource` for sequential consumers that are not Conduit.

---

## 3. Translation

Canonical `block.Block` remains lossless (`Raw` always retained).

Translation (non-destructive decode):

1. `msgpack.Decode(Raw)` → `models.BlockResponse` (same shape as algod BlockRaw).
2. Emit Conduit fields: header, payset, certificate.
3. `Delta` is left `nil`.
4. Verify decoded round / hashes match `Block.Round` / `BlockHash` / `PreviousBlockHash` when those fields are set.

Core package: `internal/conduit` (SDK types only).  
Plugin maps those fields into `data.BlockData`.

---

## 4. Checkpoint ownership model (restart)

Preferred model:

```text
Fast Follower checkpoint
        =
highest block durably accepted by required sink(s)

Conduit checkpoint
        =
highest round fully processed by Conduit’s exporter
```

### Recovery matrix

| Archive CP | Conduit next | Behavior |
|---|---|---|
| 1000 | 991 | Importer serves 991…1000 from archive (no Voi network) |
| 1000 | 1001 | Live follow: wait until archive advances, then serve |
| 990 | 1000 | **Misconfiguration** — Conduit is ahead of the sync layer; document/fail clearly |

Restart semantics:

1. Follower resumes at `follower_cp + 1` and appends to the archive.
2. Conduit resumes at its own `NextDBRound` and re-requests that round via `GetBlock`.
3. Duplicate `GetBlock` for an already-exported round only happens if Conduit itself retries before advancing — safe under at-least-once; exporters must remain idempotent.

**Do not** set `follower_cp = conduit_cp`. If Conduit lags (1000 vs 990), that is expected and recovers by archive replay.

Operational rule: when using the Conduit adapter, make the **archive a required sink** so every round Conduit might request is durable offline.

---

## 5. Failure behavior

| Scenario | Expected |
|---|---|
| `GetBlock` / translate fails | Error returned; round not delivered; Conduit does not advance |
| Conduit processor/exporter fails after successful `GetBlock` | Conduit does not advance; later retry → `GetBlock` again (at-least-once) |
| Adapter / Conduit process restarts | Resume from Conduit’s durable cursor; blocks come from archive |
| Duplicate delivery | Safe if downstream exporters are idempotent; **not** exactly-once |

---

## 6. Archive → Conduit replay (no Voi network)

```text
Archive  →  RoundSource.Get  →  translate  →  Conduit GetBlock  →  exporter
```

Requirements: archive root + `genesis.json` for the network. Deterministic fixture ranges are covered by unit tests with synthetic BlockRaw payloads.

---

## 7. Direct Conduit sync vs Fast Follower → Conduit

| Dimension | Direct Conduit algod importer | Fast Follower → archive → Conduit |
|---|---|---|
| Startup / catch-up | Conduit + algod (catchpoint / wait) | Follower catch-up + archive; Conduit reads local store |
| Network requests | Per-round BlockRaw (+ deltas in follower mode) from algod | Follower amortizes concurrent fetch; Conduit is local I/O |
| Checkpoint | Conduit-only (plus algod sync round in follower mode) | Split: follower durability + Conduit processing cursor |
| Restart | Re-hit algod for missing rounds | Archive replay without Voi |
| Implementation complexity | One binary, stock plugins | Follower + custom Conduit binary |
| Multi-sink | Single Conduit pipeline | Archive + Postgres + Conduit consumers in parallel |
| Ledger deltas | Available in follower mode | **Not available** from archive |
| Indexing logic | In Conduit | Stays in Conduit (follower remains sync-only) |

---

## 8. Optional dependency

| Path | Depends on `github.com/algorand/conduit`? |
|---|---|
| `cmd/follower`, sinks, archive, replay | **No** |
| `internal/stream`, `internal/conduit` translate | **No** |
| `plugins/conduit` module + `cmd/conduit` | **Yes** (separate `go.mod`) |

Core builds and tests must pass without downloading Conduit.

---

## 9. Consumer-facing API (pkg/block?)

`internal/block.Block` is sufficient for in-repo sinks. **Not promoted to `pkg/block` yet.**

If an external consumer needs a stable API later:

| Topic | Expectation |
|---|---|
| Type stability | Treat `internal/` as unstable; prefer archive format + stream contract docs |
| Raw bytes | Always present; algod BlockRaw msgpack; copy on decode |
| Byte slice ownership | Callers own returned `Raw`; do not mutate shared buffers |
| Ordering | Strict contiguous rounds; see [stream-contract.md](stream-contract.md) |
| Errors | Gaps / decode failures are hard errors — never skip |

Premature `pkg/` export is avoided until a real external consumer needs it.

---

## 10. Metrics (adapter-specific)

Exposed by the Conduit plugin (not the core follower):

| Metric | Meaning |
|---|---|
| `conduit_blocks_delivered_total` | Successful `GetBlock` translations returned to Conduit |
| `conduit_delivery_errors_total` | Failed `GetBlock` / wait / translate |
| `conduit_delivery_latency_seconds` | Time inside importer `GetBlock` |
| `conduit_lag` | `archive_checkpoint - requested_round` (negative while waiting) |

Lag localization:

| Signal | Layer |
|---|---|
| `voi_follower_catchup_lag` / fetch BPS | Voi acquisition |
| `voi_follower_ordered_buffer_depth` | Follower ordering |
| `conduit_lag` / delivery latency | Conduit waiting on archive or translate |
| Exporter DB / Conduit pipeline metrics | Downstream persistence |

---

## 11. Package layout

```text
internal/stream/          RoundSource + archive-backed source + Next cursor
internal/conduit/         Translate Block → Conduit field bundle (no conduit module)
plugins/conduit/          Separate Go module: importer plugin + custom conduit binary
docs/phase7-conduit-adapter.md
```

Plugin name: `voi_archive`.
