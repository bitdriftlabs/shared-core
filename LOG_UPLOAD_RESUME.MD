# Log Upload Resume

This document summarizes the handwritten branch changes in `shared-core` relative to `origin/main` for the log upload / remote flush / restart-persistence work.

Generated `bd-proto` output is intentionally excluded from this review. The branch also includes an `api` submodule bump, but this document focuses on the handwritten Rust and proto source changes that affect behavior.

## Branch Summary

Primary branch goal:

- Make remote `FlushBuffers` behave like workflow-triggered `ActionFlushBuffers`.
- Add remote flush streaming parity.
- Start a durable logger-owned persistence path for trigger uploads across restart.

Main handwritten files changed:

- `bd-api/src/api.rs`
- `bd-api/src/api_test.rs`
- `bd-api/src/lib.rs`
- `bd-logger/src/async_log_buffer_test.rs`
- `bd-logger/src/builder.rs`
- `bd-logger/src/consumer.rs`
- `bd-logger/src/consumer_test.rs`
- `bd-logger/src/flush_registry.rs`
- `bd-logger/src/flush_registry_test.rs`
- `bd-logger/src/lib.rs`
- `bd-logger/src/log_replay.rs`
- `bd-logger/src/logging_state.rs`
- `bd-logger/src/test/logger_integration.rs`
- `bd-test-helpers/src/test_api_server.rs`
- `bd-workflows/src/actions_flush_buffers.rs`
- `bd-workflows/src/config.rs`
- `bd-workflows/src/engine.rs`
- `api/src/bitdrift_public/protobuf/client/v1/api.proto`

## What Has Been Fixed

### 1. Remote flush parity with workflow flushes

Remote `FlushBuffers` now forwards the same information the workflow path needs:

- `bd-api` now carries a stable trigger source for each upload via `TriggerUploadSource`.
- Remote API-triggered flushes now forward optional streaming config instead of dropping it.
- Empty remote `buffer_id_list` is now treated as "all eligible trigger buffers" inside the logger upload manager, matching workflow behavior.

Relevant files:

- `bd-api/src/lib.rs`
- `bd-api/src/api.rs`
- `bd-logger/src/consumer.rs`
- `api/src/bitdrift_public/protobuf/client/v1/api.proto`

### 2. Remote streaming behavior now uses workflow machinery

Remote flush streaming is no longer a separate side path. Instead:

- the logger upload manager emits a `RemoteFlushStreamingRequest`
- `log_replay` forwards it into `WorkflowsEngine::start_remote_flush_streaming`
- the workflow engine creates the same kind of streaming reroute action used by workflow-triggered flushes

This keeps remote streaming semantics aligned with workflow streaming semantics.

Relevant files:

- `bd-logger/src/consumer.rs`
- `bd-logger/src/log_replay.rs`
- `bd-workflows/src/actions_flush_buffers.rs`
- `bd-workflows/src/engine.rs`

### 3. Duplicate reroute extension was reduced

Remote streaming activation now checks for an equivalent active reroute and skips starting a duplicate one. The equivalence intentionally ignores `max_logs_count` and only compares session plus source/destination buffer sets, to avoid prolonging reroute state without changing emission targets.

Relevant files:

- `bd-workflows/src/actions_flush_buffers.rs`
- `bd-workflows/src/engine.rs`
- `bd-logger/src/test/logger_integration.rs`

### 4. Durable trigger-upload registry added in logger

A new logger-local persistence layer was added:

- `bd-logger/src/flush_registry.rs` stores pending trigger uploads in the key-value store.
- Records currently include:
  - logical upload ID
  - source kind (`WorkflowAction`, `ExplicitSessionCapture`, `RemoteCommand`)
  - trigger buffer IDs
  - `has_streaming` boolean
  - lifecycle (`ReadyToUpload`, `Uploading`)

This is the first durable slice toward restart recovery.

### 5. Startup replay exists for `ReadyToUpload`

When trigger buffers are registered, the upload manager now checks the persisted registry and replays entries whose lifecycle is still `ReadyToUpload`.

This means restart recovery now works for the narrow case where:

- the logical flush was persisted
- the upload had not yet crossed the first batch boundary
- the trigger buffer still exists on startup

Relevant files:

- `bd-logger/src/consumer.rs`
- `bd-logger/src/consumer_test.rs`

## Validation Added On This Branch

### API-level coverage

- `flush_buffers_response_forwards_streaming_to_trigger_upload`

### Logger unit coverage

- `trigger_upload_with_empty_buffer_list_flushes_all_trigger_buffers`
- `trigger_upload_is_persisted_until_completion`
- `persisted_trigger_upload_replays_after_restart`
- `uploading_trigger_upload_is_not_replayed_after_restart`

### Logger integration coverage

- `remote_buffer_upload`
- `remote_buffer_upload_with_streaming_matches_workflow_streaming_behavior`
- `remote_buffer_upload_with_streaming_does_not_extend_active_workflow_streaming`

### Registry coverage

- `upsert_replaces_existing_upload_with_same_id`
- `remove_clears_matching_upload`
- `mark_uploading_updates_lifecycle_without_replacing_other_fields`

Focused validation run during review:

```text
cargo nextest run -p bd-logger -E 'test(mark_uploading_updates_lifecycle_without_replacing_other_fields) | test(trigger_upload_is_persisted_until_completion) | test(persisted_trigger_upload_replays_after_restart) | test(uploading_trigger_upload_is_not_replayed_after_restart) | test(upsert_replaces_existing_upload_with_same_id) | test(remove_clears_matching_upload)'
```

## Review Findings Against This Branch

### 1. High: persisted trigger-upload records can be clobbered by concurrent updates

The new registry methods are all read-modify-write over one shared serialized list with no in-process synchronization:

- `upsert()` reads the whole list and writes the whole list back
- `remove()` reads the whole list and writes the whole list back
- `mark_uploading()` reads the whole list and writes the whole list back

Relevant code:

- [bd-logger/src/flush_registry.rs](bd-logger/src/flush_registry.rs#L186)
- [bd-logger/src/flush_registry.rs](bd-logger/src/flush_registry.rs#L192)
- [bd-logger/src/flush_registry.rs](bd-logger/src/flush_registry.rs#L195)
- [bd-logger/src/consumer.rs](bd-logger/src/consumer.rs#L282)

Why this matters:

- Trigger uploads are executed in spawned tasks.
- Multiple uploads can update persistence concurrently.
- Two independent logical uploads can race and overwrite each other’s lifecycle or even drop one record entirely.

This must be fixed before treating the registry as a reliable source of truth.

Recommended fix direction:

- serialize registry mutations behind a process-local mutex, or
- move to one durable key per logical flush ID instead of one shared list value.

### 2. High: `Uploading` is currently an unrecoverable dead-end state

The branch now marks a persisted entry as `Uploading` at the first batch boundary, but restart recovery explicitly skips everything that is not `ReadyToUpload`.

Relevant code:

- [bd-logger/src/consumer.rs](bd-logger/src/consumer.rs#L481)
- [bd-logger/src/consumer.rs](bd-logger/src/consumer.rs#L993)
- [bd-logger/src/service.rs](bd-logger/src/service.rs#L151)
- [bd-logger/src/service.rs](bd-logger/src/service.rs#L191)

Why this matters:

- `mark_uploading()` happens before the upload service call completes.
- The upload service still waits for the tracked upload response path.
- If the process dies after lifecycle transition but before durable post-drain acceptance is guaranteed, recovery skips the entry forever.

The branch documents this gap in tests by intentionally not replaying `Uploading`, but that means restart is still not reliable once a trigger upload has started draining into upload work.

## Remaining Work

### A. Define a real durable boundary after drain begins

The branch still needs a correct resume boundary for uploads that have crossed the first batch boundary.

Current state:

- `ReadyToUpload` can be replayed.
- `Uploading` is intentionally not replayed.

Required next step:

- persist per-buffer progress or a durable enqueue boundary, so an in-flight/drained upload can be resumed or safely reissued without duplication.

### B. Persist actual streaming recovery state, not just `has_streaming`

The persisted record only stores a boolean `has_streaming` and recovery recreates uploads with `streaming = None`.

Relevant code:

- [bd-logger/src/flush_registry.rs](bd-logger/src/flush_registry.rs#L74)
- [bd-logger/src/consumer.rs](bd-logger/src/consumer.rs#L228)
- [bd-logger/src/consumer.rs](bd-logger/src/consumer.rs#L498)
- [bd-logger/src/log_replay.rs](bd-logger/src/log_replay.rs#L600)

Consequences:

- after restart, a ready-to-upload flush can be replayed
- but any associated streaming configuration is lost
- remote/workflow streaming restart semantics are therefore still incomplete

### C. Replace runtime-only workflow flush completion tracking

The workflow engine still relies on `pending_buffer_flushes`, which is an in-memory map of oneshot receivers.

Relevant code:

- [bd-workflows/src/engine.rs](bd-workflows/src/engine.rs#L93)
- [bd-workflows/src/engine.rs](bd-workflows/src/engine.rs#L187)
- [bd-workflows/src/engine.rs](bd-workflows/src/engine.rs#L587)
- [bd-workflows/src/engine.rs](bd-workflows/src/engine.rs#L876)

Consequences:

- persisted streaming actions still do not have durable flush-completion state
- restored streaming actions can still terminate incorrectly or lose their relationship to the underlying flush

### D. Add buffer-config filtering and missing-buffer cleanup policy to persistence

Current recovery only skips entries if a referenced buffer is missing at the moment the event is handled.

It does not yet:

- partially prune invalid buffer IDs from a persisted logical flush
- rewrite the persisted record after pruning
- explicitly drop the record when no buffers remain

### E. Remote logical IDs are still locally generated

Remote flushes currently use client-generated IDs, which is enough for in-process tracking but not enough for strong cross-reconnect correlation with server intent.

Relevant code:

- [bd-api/src/api.rs](bd-api/src/api.rs#L1102)

This is acceptable for the current slice, but it is still a limitation for long-term recovery semantics.

## Recommended Next Implementation Order

1. Fix registry mutation correctness first.
2. Introduce a durable per-buffer progress or durable post-drain enqueue boundary.
3. Persist real streaming recovery state, not just a boolean.
4. Replace `pending_buffer_flushes` with durable completion state or a wrapper backed by durable state.
5. Add restart integration coverage for workflow-triggered and remote-triggered streaming continuation.

## Bottom Line

What is genuinely better on this branch:

- remote flush parity is much better
- remote streaming now uses workflow machinery
- empty remote buffer lists now behave correctly
- there is now a real persistent registry and a narrow startup replay path for `ReadyToUpload`

What is not yet reliable enough to stop thinking about:

- concurrent persistence updates
- restart after the first batch has started uploading
- restart of streaming behavior
- durable workflow flush completion state

If resuming this work later, start in `bd-logger/src/flush_registry.rs`, `bd-logger/src/consumer.rs`, and `bd-workflows/src/engine.rs`.