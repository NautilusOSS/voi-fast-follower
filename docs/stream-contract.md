# Voi Fast Follower — Block Stream Contract

This document defines the **delivery contract** of the canonical ordered block stream.
Downstream consumers (Postgres, archive, Conduit `voi_archive` importer) can rely on these
guarantees without understanding how blocks are fetched. See [phase7-conduit-adapter.md](phase7-conduit-adapter.md).

## What the follower owns

| Concern | Owner |
|---|---|
| Acquisition from algod | Follower |
| Contiguous ordering | Follower |
| Hash / prev-hash validation | Follower |
| Bounded backpressure | Follower |
| Durable checkpoint cursor | Follower + sinks (composite = min) |
| Persistence / durability | Sink |
| Idempotent writes | Sink |

## Ordering

Blocks are delivered **strictly in round order**:

```text
N, N+1, N+2, …
```

No sink `Commit` / `CommitBatch` call will present round `N+2` before `N+1` has been
successfully accepted for the durable cursor (or offered in the same contiguous batch
as `N+1`).

Within a batch, rounds are contiguous and increasing by exactly one.

## Continuity

The follower **never intentionally skips a round**. A gap is an error:

- Fetch gaps are buffered until filled (or retried).
- Validation failures (hash / linkage) abort the batch.
- Archive appends reject non-contiguous writes relative to the checkpoint.

## Delivery semantics

**At-least-once ordered delivery with idempotent sink semantics.**

| Claim | Status |
|---|---|
| At-least-once | **Yes** — retries after sink / process failure may re-offer a batch |
| Exactly-once | **No** — not claimed |
| Effectively-once persistence | **Yes, when sinks are idempotent** (`ON CONFLICT DO NOTHING`, archive checkpoint skips) |

Consumers that are not idempotent must not be used as required sinks.

## Restart

After a durable checkpoint `C`:

```text
resume = C + 1
```

The follower will not ask sinks to accept rounds `≤ C` except as idempotent retries.

## Failure

A sink failure **prevents the durable cursor from advancing** past the failed batch:

1. `CommitBatch` returns an error to the follower.
2. The follower re-queues the batch and retries.
3. Composite `LastProcessedRound` is `min` across required sinks — a sink that did not
   accept the batch keeps the composite cursor behind.

Partial fan-out (one MultiSink member succeeded, another failed) is safe **only**
because sinks must tolerate idempotent retry.

## Batching

Catch-up may deliver multiple contiguous rounds in one `CommitBatch`.
Live follow uses batch size **1** for low latency.

## Canonical block

Each delivered unit is `block.Block`:

- `Round`, `BlockHash`, `PreviousBlockHash`
- `Raw` — original algod msgpack `BlockRaw` (never discarded)
- Optional decoded `Transactions` (each with its own `Raw`)

Live delivery and archive replay must present equivalent fields for the same round.

## Out of scope

Indexing, ARC-200/72 decoding, Conduit processors, application transforms, and
exactly-once distributed transactions are **not** part of this contract.
