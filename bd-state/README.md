# bd-state

`bd-state` is the runtime state layer used by the logger pipeline. It wraps
`bd-resilient-kv::VersionedKVStore` and adds:

- process-lifecycle semantics (capture previous process state, then clear selected scopes),
- change tracking (`last_change_micros`) for upload decisions,
- a simple async API for reads/writes,
- explicit snapshot rotation entry points used by `bd-logger`.

This document focuses on how state is recorded, persisted, and uploaded with logs.

## Related references

- Journal/file format: `bd-resilient-kv/VERSIONED_FORMAT.md`
- Logger upload invariants: `bd-logger/AGENTS.md`
- Upload coordinator implementation: `bd-logger/src/state_upload.rs`

## 1) State model and process lifecycle

State is namespaced by `Scope` (`FeatureFlagExposure`, `GlobalState`, `System`) and keyed by
string. Values are protobuf `StateValue`s.

On persistent startup (`Store::persistent`):

1. `VersionedKVStore` is opened and replayed into an in-memory `ScopedMaps` cache.
2. That cache is cloned into `previous_state` (for crash/context reporting).
3. `FeatureFlagExposure` and `GlobalState` are cleared for the new process.
4. `System` scope is not blanket-cleared by this startup path.

If persistent initialization fails, `persistent_or_fallback` falls back to in-memory storage and
returns `fallback_occurred = true`.

## 2) How writes are recorded

`Store` delegates to `VersionedKVStore` and tracks effective state mutations:

- `insert(scope, key, value)`:
  - no-op if new value equals existing value,
  - otherwise appends a journal entry and updates cache.
- `remove(scope, key)`:
  - appends a tombstone entry (empty `StateValue`) if key exists.
- `extend(scope, entries)`:
  - batch write path in underlying store; currently treated as changed when non-empty.
- `clear(scope)`:
  - iterates keys in that scope and removes each one.

For real mutations, `Store` updates `last_change_micros` using `fetch_max`, so the timestamp is
monotonic non-decreasing even with concurrent writers.

## 3) On-disk persistence format and layout

When persistent mode is enabled, files live under the configured state directory.

Active journal:

- `state.jrn.<generation>` (e.g. `state.jrn.0`)
- memory-mapped for fast append and replay
- not compressed

Archived snapshots (on rotation):

- `snapshots/state.jrn.g<generation>.t<rotation_timestamp>.zz`
- zlib-compressed archived journal of the old generation

Journal entry framing and semantics are defined in `VERSIONED_FORMAT.md`:

- each entry stores scope + key + timestamp + protobuf payload + CRC,
- timestamp is per-entry write time (microseconds),
- tombstones represent deletions,
- compacted journals preserve original entry timestamps.

Important timestamp nuance: snapshot filename timestamp is the **rotation time marker** for that
archived file, while each entry inside the file keeps its own original write timestamp.

## 4) Rotation, compaction, and retention

Rotation can happen automatically (high-water mark/capacity pressure) or manually
(`Store::rotate_journal` from `bd-state`):

1. create a new active generation,
2. rewrite compacted live state into the new journal (preserving entry timestamps),
3. archive+compress the old generation to `snapshots/*.zz`,
4. run snapshot cleanup policy,
5. delete the old uncompressed generation file.

Snapshot creation is retention-aware:

- controlled by `RetentionRegistry` and `state.max_snapshot_count`,
- snapshotting is disabled if max snapshot count is `0`,
- if no retention handle requests data, rotation may skip snapshot creation,
- cleanup removes snapshots older than required retention and enforces max count safety cap.

## 5) How snapshots are uploaded with logs

State snapshots and logs are separate artifact streams; coordination lives in `bd-logger`.

### Producer side (log batch flush paths)

`BatchBuilder` tracks `(oldest_micros, newest_micros)` incrementally while logs are added.
Before consuming a batch (`take()`), uploader paths call:

`StateUploadHandle::notify_upload_needed(oldest, newest)`

This currently happens in:

- continuous flush (`flush_current_batch`),
- trigger/complete flush (`flush_batch`).

`StreamedBufferUpload::start` currently has a TODO for state upload integration.

`notify_upload_needed` is non-blocking:

- merges ranges in a shared accumulator,
- best-effort wakes a single worker via a capacity-1 channel,
- does not block log upload path.

### Worker side (`StateUploadWorker`)

Single background worker owns all decisions and retries:

1. drains/coalesces pending range, persists it to key-value key `state_upload.pending_range.1`,
2. computes preflight decision:
   - if `last_change_micros == 0`: skip,
   - else if snapshots exist in requested range: upload them oldest-first,
   - else create an on-demand snapshot via `state_store.rotate_journal()` (subject to cooldown),
3. enqueues each snapshot through `bd-artifact-upload` as `UploadSource::Path(...)`,
4. waits for persistence ack from artifact queue.

Queue semantics are move-based:

- success: snapshot file is moved out of `{state_store_path}/snapshots/` into artifact upload
  storage,
- enqueue failure: source file remains in `{state_store_path}/snapshots/`, so later retries can
  re-attempt.

The worker keeps pending coverage on backpressure/errors and retries periodically; pending range is
recovered on startup so upload intent survives process restart.

## 6) Why this matches server-side state hydration

The server reconstructs active state for log time `T` from per-entry timestamps in snapshots
(entries with write timestamp `<= T`), not by snapshot filename time alone. The client therefore
only needs to ensure relevant snapshot files are eventually uploaded along with log traffic.
