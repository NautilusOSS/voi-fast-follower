# Phase 8 — Bootstrap, Long-Range History & Retention

**Architectural result:** Voi provides the live chain. Fast Follower provides synchronization. Archive provides durable history. Consumers can bootstrap from history and follow the live stream.

```text
                    Voi Network
                         ↓
                    local algod
                         ↓
                  Fast Follower
                         ↓
                 durable archive
                   ↓           ↓
                live          replay
              consumers       consumers
```

---

## 1. Archive as historical source

The segment archive is a durable, ordered, gap-free sequence of canonical raw blocks (`BlockRaw` msgpack).

Consumers request ranges without contacting Voi:

```bash
go run ./cmd/replay -archive ./archive -start N -end M -sink postgres -database "$DATABASE_URL"
go run ./cmd/verify -archive ./archive -start N -end M
```

Programmatic boundary (`internal/stream`):

| Type | Role |
|---|---|
| `RoundSource` | `Get(round)` + `Checkpoint` — no Voi historical catch-up |
| `ArchiveSource` | Archive-backed `RoundSource` |
| `Range` / iterate | Contiguous `[start,end]` without loading the whole range into memory |
| `HandoffSource` | Archive history then live algod with verified boundary |

Segment format is unchanged (`VFFSEG01`). On open/commit the archive builds a lightweight
in-memory segment index (sorted filename/header start rounds) so `GetBlock` picks the
correct file in O(log segments) even when `SegmentSize` differs across processes. Within a
segment, records are still scanned linearly (default 1000 rounds) — adequate for bootstrap
and sparse random lookup without SQLite.

---

## 2. Bootstrap workflow

Consumer needs `10_000_000 → tip` while local algod only exposes a recent lookback:

```text
archive  →  historical range through archive tip
local algod  →  archive_tip+1 → network tip
```

Handoff must be contiguous:

```text
archive ends at N
live begins at N+1
hash(N) == previous_block_hash(N+1)
```

Never skip a round at the boundary. Prefer archive when both sources can serve the same round (deterministic bootstrap).

### Operator procedure

1. Run Fast Follower with archive as a **required** sink so history accumulates while the node tip advances.
2. Optionally copy/export archive segments to bootstrap other hosts (`archive-export` / `archive-import`).
3. Verify: `go run ./cmd/verify -archive ./archive`
4. Bootstrap sink from archive only (no Voi history):
   ```bash
   go run ./cmd/replay -archive ./archive -start START -end ARCHIVE_TIP -sink postgres ...
   ```
5. Continue live with follower (or `cmd/bootstrap` handoff) so the sink advances `ARCHIVE_TIP+1 → tip`.
6. Resume always: `sink_checkpoint + 1`.

---

## 3. Composite source (archive + live)

```text
HandoffSource
  ├─ HistoricalSource = ArchiveSource   (rounds ≤ archive checkpoint)
  └─ LiveSource       = algod BlockRaw  (rounds ≥ archive checkpoint + 1)
```

Phases (operational):

| Phase | Meaning |
|---|---|
| `historical` | Serving from archive |
| `handoff` | First live round after archive tip; linkage verified |
| `live` | Serving from algod past the handoff |
| `waiting` | Requested round not yet available (archive tip behind / live not ready) |
| `failed` | Gap, hash break, or hard error |

`internal/stream` does **not** import Postgres, Conduit, or a concrete Voi package beyond a small `LiveFetcher` interface.

---

## 4. Archive ↔ live handoff rules

1. Read historical rounds from archive through checkpoint `N`.
2. Request `N+1` from live.
3. Verify `block(N).BlockHash == block(N+1).PreviousBlockHash`.
4. Continue live `N+2…tip` with the same linkage checks against the previously delivered block.
5. At-least-once: re-delivering `N` or `N+1` after restart is safe for idempotent sinks; never skip.

Restart during handoff: resume at `durable_consumer_checkpoint + 1`. If that round is still in the archive, serve archive; if it is the first live round, re-fetch live and re-verify linkage.

---

## 5. Retention semantics (policies)

Not mutually exclusive with export to cold storage later. No cloud object store in Phase 8.

### Full history

Keep all segments indefinitely. Default for a sync archive that feeds many bootstraps.

### Rolling history

Keep the last **N** rounds (or last **K** segments). Prune only prefixes that are below `checkpoint - N + 1`. Checkpoint is unchanged; first available round advances. Use when disk is bounded and cold copies exist elsewhere.

```bash
go run ./cmd/archive-prune -archive ./archive -keep-rounds 500000
```

### Partitioned history

- **Active/local:** recent segments on fast disk (follower host).
- **Long-term:** exported segment bundles on separate storage.
  Import reconstitutes a contiguous archive before bootstrap. Future backends should still expose the same segment directory + checkpoint contract.

---

## 6. Archive export / import

```bash
go run ./cmd/archive-export -archive ./archive -out ./bundle -start N -end M
go run ./cmd/archive-import -in ./bundle -archive ./archive-new
```

Rules:

- Preserve segment bytes and checkpoint metadata for the exported range.
- Verify hashes/linkage on import.
- Refuse to silently overwrite a non-empty destination with conflicting history.

---

## 7. Archive determinism

Two archives built from the same canonical blocks are **semantically** equivalent when, for a round range:

- same rounds present (contiguous)
- same block hashes
- same previous hashes
- same raw payloads

Filesystem metadata is ignored.

```bash
go run ./cmd/archive-compare -a ./archive1 -b ./archive2 -start N -end M
```

---

## 8. Conduit bootstrap

Conduit must not query old Voi rounds during historical bootstrap.

```text
Archive
  ↓  voi_archive (offline or follow)
Conduit historical processing
  ↓  archive tip
Follower continues appending live blocks to the same archive
  ↓  voi_archive follow polls archive tip
Conduit live processing
```

Recommended:

1. Follower: required archive sink catching up / live.
2. Conduit: `importer.name: voi_archive`, `mode: follow`, `archive_path` shared with follower.
3. Conduit starts at its exporter cursor; historical rounds come from archive only.
4. After archive tip, follow mode waits for new segments (live path) — no algod lookback needed inside Conduit.

`mode: bootstrap` is accepted as an alias of `follow`.

---

## 9. PostgreSQL bootstrap

```bash
go run ./cmd/replay -archive ./archive -start N -end M -sink postgres -database "$DATABASE_URL"
```

Compare:

| Path | Voi latency |
|---|---|
| Voi → follower → PostgreSQL | Yes (acquisition) |
| Archive → replay → PostgreSQL | No |

Replay throughput is dominated by sink durability (UNNEST/fsync), not HTTP.

Then run the follower (or `cmd/bootstrap`) from `M+1` so Postgres continues live.

---

## 10. Resume matrix

| Situation | Behavior |
|---|---|
| Archive CP behind sink CP | Misconfiguration if sink was fed from this archive; do not skip. Reconcile manually. |
| Sink CP behind archive CP | Replay/bootstrap `sink+1 … archive_cp`, then live |
| Archive tip behind consumer request | Consumer waits (`waiting`); never skip |
| Archive + live overlap | Prefer archive; duplicates safe under at-least-once |
| Restart during handoff | `resume = checkpoint + 1`; re-verify linkage |

---

## 11. Metrics

| Metric | Meaning |
|---|---|
| `voi_follower_bootstrap_phase` | 0 historical / 1 handoff / 2 live / 3 waiting / 4 failed |
| `voi_follower_archive_replay_blocks_total` | Blocks read from archive in bootstrap/replay |
| `voi_follower_archive_replay_blocks_per_second` | Recent archive read throughput |
| `voi_follower_handoff_round` | Archive tip at last successful handoff |
| Existing `voi_follower_catchup_lag` | Live acquisition lag |

---

## 12. Performance notes

Do not weaken durability. Historical replay should make **network** non-dominant for bootstrap.

Measure (local): archive iterate BPS, archive→Postgres, archive→Conduit file_writer, handoff latency (time from last archive round to first live accept), tip lag after handoff.

Segment index: O(log segments) file selection + linear scan within a segment (default 1000 rounds). Adequate for sequential bootstrap and sparse random lookup; no SQLite.

---

## 13. Limitations

- Local non-archival algod still cannot backfill history the archive never captured.
- Archive does not store ledger deltas (Conduit follower-mode exporters that need deltas remain unsupported from archive alone).
- Rolling prune is irreversible without an exported copy.
- Exactly-once is not claimed.

---

## 14. Tooling

| Command | Purpose |
|---|---|
| `cmd/archive-info` | Metadata |
| `cmd/verify` | Integrity |
| `cmd/replay` | Archive → sink |
| `cmd/bootstrap` | Archive → live handoff → sink |
| `cmd/archive-export` / `archive-import` | Move segment bundles |
| `cmd/archive-compare` | Semantic equality |
| `cmd/archive-prune` | Rolling retention |
