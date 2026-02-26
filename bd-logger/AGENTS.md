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

The handle and worker are created together via `StateUploadHandle::new`, which also restores
previously-persisted upload coverage from `bd-key-value`.

### Upload Decision Logic

When the worker receives a batch's timestamp range `[oldest, newest]`, it evaluates in order:

1. **No state changes ever recorded** (`last_change_micros == 0`) → skip. Nothing to upload.
2. **Coverage already sufficient** (`uploaded_through >= batch_oldest`) → skip. The server
   already has state that covers this batch.
3. **No new changes since last upload** (`last_change <= uploaded_through`) → skip. State hasn't
   changed since we last uploaded.
4. **Existing snapshots cover the gap** → upload those snapshot files. Snapshots are found by
   scanning `{state_store_path}/snapshots/` for files whose parsed timestamp falls in
   `(uploaded_through, batch_newest_micros]`.
5. **No existing snapshots cover the gap** → create one on-demand via
   `state_store.rotate_journal()`, subject to a cooldown (see below).

### Snapshot Cooldown

Creating a snapshot on every batch flush during high-volume streaming is wasteful. The worker
tracks `last_snapshot_creation_micros` and will not create a new snapshot if one was created
within `snapshot_creation_interval_micros` (a runtime-configurable value). During cooldown, the
worker defers on-demand creation and keeps pending work for retry.

### Coverage Persistence and Retention

`uploaded_through_micros` — the watermark of what has been confirmed uploaded — is persisted via
`bd-key-value` under the key `state_upload.uploaded_through.1`. It is loaded on startup and used
immediately, so the worker never re-uploads state snapshots that were confirmed in a previous
process run.

The watermark is also fed into the `RetentionHandle` from `bd-resilient-kv`, which prevents the
snapshot retention cleanup from deleting any snapshot that is still needed. This prevents a race
where cleanup removes a snapshot before the logger has had a chance to upload it.

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

The wake channel has capacity `UPLOAD_CHANNEL_CAPACITY` (8). If wake signaling is saturated,
`notify_upload_needed` still records the requested range in shared pending state and returns. A
missed wake does not lose coverage; the worker will observe pending state on the next wake/timer
cycle, and version tracking forces immediate reprocessing when producers update pending state while
the worker is active.

### Key Invariants

- `uploaded_through_micros` is monotonically non-decreasing. It is only advanced via
  `fetch_max`, never set to a smaller value.
- Snapshot uploads are considered confirmed once they are successfully enqueued to the
  `bd-artifact-upload` queue (which persists them to disk and retries the network upload). If the
  enqueue fails, the watermark stays put and the next batch will retry.
- Snapshot creation and upload progress logic run in the single worker task. Producer-side range
  coalescing is concurrent but synchronized via a mutex-backed accumulator.
