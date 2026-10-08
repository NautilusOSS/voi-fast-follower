# Phase 9 — Reference Deployment & Production Validation

**Positioning:** Voi Fast Follower is production-grade infrastructure for acquiring, durably retaining, and delivering Voi's raw block stream to independent downstream consumers.

```text
                 Voi Network
                      ↓
                  Voi node
                      ↓
              Voi Fast Follower
                      ↓
              durable raw stream
                 ↓       ↓
            PostgreSQL  Archive
                           ↓
                        Replay
                           ↓
                  Conduit / consumers
```

---

## 1. Reference architecture

Minimal production topology (Docker Compose):

```text
┌─────────────────────────────┐
│ Voi node (optional local)   │
│ localhost algod :4001       │
└──────────────┬──────────────┘
               ↓
┌─────────────────────────────┐
│ voi-fast-follower           │
│ fetch → validate → sinks    │
│   ├─ Archive (required)     │
│   └─ PostgreSQL             │
│ :9090 metrics/health        │
└─────────────────────────────┘
```

Conduit is **not** in the default stack. Point an optional Conduit binary at the archive (`voi_archive` importer) when needed.

Compose files:

| File | Purpose |
|---|---|
| `docker-compose.yml` | Dev / local (optional remote algod; archive off by default) |
| `docker-compose.prod.yml` | Reference production (archive + Postgres required sinks) |

```bash
# Production-style reference
cp .env.example .env   # edit secrets
docker compose -f docker-compose.prod.yml up --build -d
```

---

## 2. Configuration

### Concise env table

| Variable | Group | Default | Class | Notes |
|---|---|---|---|---|
| `VOI_ALGOD_URL` | Required | — | production-safe | Algod base URL |
| `VOI_ALGOD_TOKEN` | Required* | `""` | production-safe | *if node requires it |
| `VOI_START_ROUND` / `START_ROUND` | Required | `latest` | production-safe | Ignored after checkpoint exists |
| `DATABASE_URL` | Persistence | — | production-safe | Postgres DSN |
| `ARCHIVE_ENABLED` | Persistence | prod: `true` | production-safe | Keep archive as required sink |
| `ARCHIVE_PATH` | Persistence | `/var/lib/voi-fast-follower/archive` | production-safe | Durable history |
| `ARCHIVE_SEGMENT_SIZE` | Persistence | `1000` | production-safe | Rounds per segment file |
| `WORKERS` | Performance | `32` | production-safe | Fetch concurrency |
| `FETCH_WINDOW` | Performance | `max(workers*2, batch)` | production-safe | Bounded in-flight rounds |
| `COMMIT_BATCH_SIZE` | Performance | `10` | production-safe | Catch-up only; **live = 1** |
| `COMMIT_FLUSH_INTERVAL` | Performance | `200ms` | production-safe | Partial catch-up flush |
| `PG_INSERT_MODE` | Performance | `unnest` | production-safe | `copy` = alternate |
| `PG_ASYNC_COMMIT` | Performance | `false` | **benchmark/dev only** | Never enable in prod |
| `POLL_INTERVAL` | Performance | `100ms` | production-safe | Tip poll |
| `LOG_LEVEL` | Operations | `info` | production-safe | `debug` for troubleshooting |
| `METRICS_ADDR` | Operations | `:9090` | production-safe | Bind metrics/health |
| `SHUTDOWN_TIMEOUT` | Operations | `10s` | production-safe | Graceful HTTP stop |
| `MIGRATIONS_PATH` | Operations | `/app/migrations` | production-safe | Container path |

Legacy aliases still accepted: `PREFETCH_WORKERS`, `PREFETCH_BUFFER`. Prefer `WORKERS` / `FETCH_WINDOW`.

### Production defaults (verified)

| Setting | Value | Why |
|---|---|---|
| Postgres `synchronous_commit` | **ON** (default) | Durability |
| `PG_ASYNC_COMMIT` | **false** | Experiment only |
| Live commit batch | **1** | Low latency; code-enforced |
| Catch-up batch | **10** | Throughput without unbounded memory |
| Fetch window | **bounded** | Backpressure |
| Archive fsync | **enabled** | Crash-safe segments |
| Archive in prod compose | **required sink** | Historical source of truth |

---

## 3. Storage layout

Recommended host / volume layout:

```text
/var/lib/voi-fast-follower/
    archive/                 # durable history (MUST back up)
        checkpoint
        segments/*.seg
    # Postgres data lives in the Postgres volume, not here
```

Compose named volumes map to the same logical roles:

| Path / volume | Kind | Backup? |
|---|---|---|
| `archive/` (`follower-data`) | **Durable history** | **Yes — primary** |
| Postgres data volume | Durable consumer/index state | Yes if you rely on SQL consumers |
| Algod data volume | Node ledger (outside follower) | Node ops concern |
| Metrics / logs | Operational metadata | Optional |

**Rebuildable / non-authoritative for history:** explorer UI cache, process memory buffers, Prometheus scrapes.

**Must back up for block history:** the archive directory (checkpoint + segments). A DB dump alone does **not** replace the raw archive.

---

## 4. Backup / restore

Archive is the durable historical source.

```bash
# Backup (consistent enough for crash-reconciled archives: stop follower OR accept
# at-least-once retry after restore)
./scripts/phase9-backup-restore.sh backup

# Restore into a fresh directory + verify + optional replay smoke
./scripts/phase9-backup-restore.sh restore ./restore-test
```

Manual equivalent:

```bash
go run ./cmd/archive-export -archive "$ARCHIVE_PATH" -out ./bundle -start FIRST -end LAST
# or: tar/rsync of archive/ after stopping the follower

go run ./cmd/archive-import -in ./bundle -archive /var/lib/voi-fast-follower/archive
go run ./cmd/verify -archive /var/lib/voi-fast-follower/archive
go run ./cmd/replay -archive ... -start N -end M -sink postgres ...
```

Restore must never silently overwrite a non-empty destination with conflicting history (`archive-import` / `archive-export` enforce empty dest).

---

## 5. Health & readiness

| Endpoint | OK when | 503 when |
|---|---|---|
| `/healthz` | process alive (not `failed`) | `failed` |
| `/readyz` | `catching_up` or `live` | `startup`, `degraded`, `failed` |

State machine:

```text
startup → catching_up → live
                ↓
            degraded  (sink error; checkpoint does not advance)
                ↓
              live    (after sink recovers)
                ↓
             failed   (unrecoverable)
```

**Readiness fails on:** startup, active sink error (`degraded`), failed mode.  
**Catch-up is ready** so load balancers can keep the instance in rotation while filling lag.

---

## 6. Metrics (operator dashboard)

Primary signals (no high-cardinality labels such as round IDs):

| Metric | Meaning |
|---|---|
| `voi_follower_target_round` | Network tip |
| `voi_follower_current_round` | Next commit cursor |
| `voi_follower_last_processed_round` | Durable checkpoint |
| `voi_follower_catchup_lag` / `checkpoint_lag` | Tip lag |
| `voi_follower_fetch_blocks_per_second` | Acquisition |
| `voi_follower_commit_blocks_per_second` | Sink throughput |
| `voi_follower_archive_bytes_written_total` | Archive growth |
| `voi_follower_ordered_buffer_depth` | Backpressure / buffer |
| `voi_follower_sink_errors_total{sink=…}` | Sink failures |
| `voi_follower_archive_errors_total` | Archive failures |
| `voi_follower_bootstrap_phase` | historical/handoff/live/waiting/failed |
| `/readyz` JSON `mode` | Health state |

### Alerting recommendations

| Alert | Condition |
|---|---|
| Follower stalled | `last_processed_round` unchanged for **X minutes** while tip advances |
| High lag | `catchup_lag` > threshold for **Y minutes** |
| Sink failure | `sink_errors_total` increasing / `/readyz` 503 with `degraded` |
| Archive failure | `archive_errors_total` increasing or write stalled |
| Disk pressure | **Filesystem** monitoring on archive + Postgres volumes (not in-process) |

---

## 7. Failure recovery

| Failure | Expected behavior |
|---|---|
| Process kill mid catch-up | Restart → `checkpoint + 1`; no skipped rounds |
| Postgres down | Sink errors; checkpoint frozen; buffers bounded; `/readyz` 503; resume when DB returns |
| Archive FS error | Equivalent sink failure / degraded |
| Algod down | Retries; no skips; lag grows; resume from checkpoint |
| Full compose restart | Volumes preserve archive + Postgres; resume `cp+1` |

Inject locally:

```bash
./scripts/phase9-failure-inject.sh
```

---

## 8. Upgrade / restart

```text
version N → checkpoint durable → replace binary/container → version N+1 → resume cp+1
```

Compatible upgrades **must not** require deleting archive, checkpoint, or Postgres unless a release notes a breaking migration.

Archive format: magic `VFFSEG01`, version `u32=1`. Older segments remain readable; never silently reinterpret bytes. Future format changes require a new magic/version and an explicit migration tool.

---

## 9. Security notes (lightweight)

| Topic | Guidance |
|---|---|
| Algod token / DB URL | Env or secrets file; not committed; avoid logging secrets |
| Metrics / health | Bind to private network; put reverse-proxy ACLs in front if exposed |
| Explorer API | Same listener as metrics when Postgres enabled — restrict network |
| Filesystem | Archive dir owned by service user; not world-writable |
| Container | Runs as non-root in production image; no privileged flag |
| Malformed blocks | Decode/validation errors abort the batch; no silent skip |
| Auth in follower | **Not** built-in; rely on network controls |

---

## 10. Operational commands

| Intent | Command |
|---|---|
| start | `docker compose -f docker-compose.prod.yml up -d` |
| status | `curl -s localhost:9090/readyz \| jq` |
| verify | `go run ./cmd/verify -archive $ARCHIVE_PATH` |
| backup | `./scripts/phase9-backup-restore.sh backup` |
| restore | `./scripts/phase9-backup-restore.sh restore DIR` |
| replay | `go run ./cmd/replay -archive … -start N -end M -sink postgres …` |
| prune | `go run ./cmd/archive-prune -archive … -keep-rounds N` |
| bootstrap | `go run ./cmd/bootstrap -archive … -algod …` |
| info / compare | `archive-info`, `archive-compare`, `archive-export`, `archive-import` |

---

## 11. Live + historical consumers

**Property:** consumers are replaceable without reacquiring the chain from Voi.

```text
Follower continues live → archive grows
Consumer A: replay archive historically → catch up → follow (Conduit follow / bootstrap)
Consumer B: Postgres sink or independent replay
```

Historical replay reads the archive; it must not pause follower acquisition (separate processes).

---

## 12. CI / release

```bash
git checkout <tag>
docker build -t voi-fast-follower:<tag> .
# or
make docker
```

CI (`.github/workflows/ci.yml`): unit/integration tests, `-race` on selected packages, build all `cmd/…`, optional `plugins/conduit` build.

---

## 13. Performance regression baseline

Record with local algod + production defaults (`PG_ASYNC_COMMIT=false`, archive fsync on):

```bash
./scripts/phase9-regression.sh
```

Captures fetch-only, archive, Postgres, archive+Postgres. Store results under `docs/baselines/` when run. Do not weaken durability to chase numbers.

---

## 14. Known limitations

- Non-archival algod lookback still limits **acquisition** of deep history the archive never captured.
- Archive has no ledger deltas (Conduit follower-mode delta exporters unsupported from archive alone).
- Exactly-once is not claimed.
- No automatic distributed failover / cloud backup orchestration.
- Metrics/health share one listener with optional explorer — lock down the network.

---

## 15. Soak / long-duration

```bash
./scripts/phase9-soak.sh   # default several hours; override HOURS=
```

Watch: blocks processed, lag, RSS, FD count (optional), archive/Postgres growth, error counters, reconnect after injected blips.
