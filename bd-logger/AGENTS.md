# bd-logger - Agent Guidelines

This document covers design decisions and behavioral invariants for `bd-logger` that are not
obvious from reading the code alone.

## State Snapshot Uploads

### Why State Snapshots Exist

A snapshot file contains N state entries, each carrying its own original write timestamp. The
snapshot's filename timestamp is the *rotation* timestamp — the moment the journal was compacted
— which is always **after** the log timestamps it covers. The server reconstructs which state
was active at log time T by replaying the per-entry timestamps within the snapshot (entries with
write timestamp ≤ T), not by comparing T against the snapshot's rotation timestamp. Logs and
state travel separately — the logger just needs to ensure the relevant snapshot files are
uploaded so the server has them available when it processes those logs.

### Architecture: Handle + Worker

State upload coordination is split into two types:

- **`StateUploadHandle`** — a cheap, `Arc`-cloneable coalescing handle. Each buffer uploader holds one. When
  a batch is about to be flushed, the uploader calls
  `handle.notify_upload_needed(batch_oldest_micros, batch_newest_micros)` in a fire-and-forget
  manner. The call merges the range into shared pending state protected by a mutex, then
  best-effort nudges the worker via a bounded wake channel; it never blocks the log upload path.

- **`StateUploadWorker`** — a single background task that owns all snapshot creation and upload
  logic. Because exactly one task processes requests, deduplication and cooldown enforcement are
  centralized. On each wakeup (or retry tick), the worker drains/coalesces shared pending state,
  then processes the widest pending range before deciding whether to act.

The handle and worker are created together via `StateUploadHandle::new`.

### Upload Decision Logic

When the worker receives a batch's timestamp range `[oldest, newest]`, it evaluates in order:

1. **Snapshot files exist** in `{state_store_path}/snapshots/` → upload snapshots whose filename
   timestamp is within the current pending log range `[oldest, newest]` (oldest-first).
2. **No in-range snapshots found** → decide whether to create one on-demand via
   `state_store.rotate_journal()`, subject to cooldown and the in-process
   `last_change_at_rotation` optimization (skip if no changes since last worker-triggered
   rotation).

Snapshot discovery runs before on-demand creation checks, so persisted pending coverage can match
existing on-disk snapshots across restarts.

### Snapshot Cooldown

Typically snapshots are created in response to the state journal filling up due to state updates, but
but when logs are streamed we may need to periodically create snapshots in order to upload state changes.
The state uploader may trigger manual snapshot creation by calling into the state store.

Creating a snapshot on every batch flush during high-volume streaming is wasteful. The worker
tracks `last_snapshot_creation_micros` and will not create a new snapshot if one was created
within `snapshot_creation_interval_micros` (a runtime-configurable value). During cooldown, the
worker defers on-demand creation and keeps pending work for retry.

### Snapshot Move Semantics

State snapshot uploads are enqueued via `enqueue_upload(UploadSource::Path(...))`: the snapshot
file is moved
(renamed) from `state/snapshots/` into `bd-artifact-upload`'s `report_uploads/` directory. This
means:

- No re-copy/re-checksum pass is required for snapshot files (they are already zlib compressed).
- Once the enqueue ack succeeds, the file has left `state/snapshots/`, so the worker will not
  re-upload it.
- If enqueue fails, the file remains in `state/snapshots/`, so the next retry still sees it.

Upload selection is range-based over file presence; there is no separate uploaded watermark state.

### Pending Range Durability

The worker persists pending coverage to key-value storage (`state_upload.pending_range.1`) whenever
it drains/merges producer requests, and clears it after successful processing. On startup, it reads
this key and immediately processes recovered pending work before entering the normal wake loop.

During successful upload progress, the worker tightens pending coverage by advancing
`pending_range.oldest_micros` after each snapshot enqueue persistence ack. This narrowed coverage is
persisted immediately so restart resumes with the same tighter lower bound.

### Retention Ownership

Retention is split by responsibility:

- **Buffer consumer retention handles** are the source of truth for logs that may still be uploaded
  in the future.
- **State upload worker retention handle** protects only the uploader's current pending coverage.

The worker sets its retention handle from `pending_range.oldest_micros` while pending work exists
and uses `RETENTION_NONE` when pending work is empty.

### BatchBuilder Timestamp Tracking

`BatchBuilder` (in `consumer.rs`) tracks `oldest_micros` and `newest_micros` incrementally as
logs are added via `add_log`. This avoids a second scan of the batch at flush time. Both fields
are reset to `None` by `take()` when the batch is consumed. Callers must read `timestamp_range()`
*before* calling `take()` — `take()` resets the fields.

The three flush paths that interact with state uploads are:
- `ContinuousBufferUploader::flush_current_batch`
- `StreamedBufferUpload::start`
- `CompleteBufferUpload::flush_batch`

All three follow the same pattern: read `timestamp_range()`, call `notify_upload_needed` if a
range is available, then call `take()` to produce the log batch.

### Wake Channel Backpressure

The wake channel has capacity 1. If wake signaling is saturated,
`notify_upload_needed` still records the requested range in shared pending state and returns. A missed wake does not lose coverage; the worker will observe pending state on the next wake/timer cycle, and version tracking forces immediate reprocessing when producers update pending state while
the worker is active.

### Key Invariants

- Snapshot uploads are considered confirmed once they are successfully enqueued to the
  `bd-artifact-upload` queue (which persists them to disk and retries the network upload). If
  enqueue fails, the source file is still present in `state/snapshots/` and the next batch will
  retry.
- Snapshot creation and upload progress logic run in the single worker task. Producer-side range
  coalescing is concurrent but synchronized via a mutex-backed accumulator.
