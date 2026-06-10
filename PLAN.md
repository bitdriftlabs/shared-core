# Reliable Flush Resume Plan

## Goal

Fully complete reliable flush resume across restarts by replacing the current ad hoc trigger-upload registry with a file-backed protobuf persistence layer, then using that durable state to recover trigger uploads and streaming only after the buffer-locking and cursor-advance semantics are made crash-safe.

The design split for this work is deliberate:

1. Make persistence itself correct and race-free.
2. Make post-drain resume correct.
3. Reconnect workflow and remote streaming completion to durable flush state.

## Non-Goals

- Do not continue iterating on the current manual string encoding in `bd-logger/src/flush_registry.rs`.
- Do not move this persistence into workflow state or `bd_state`.
- Do not treat replay-from-the-beginning as an acceptable substitute for correct resume once a trigger upload has started consuming data.

## Current Branch State

The current branch already fixes several prerequisite behaviors:

- Remote `FlushBuffers` now forwards streaming and matches workflow-triggered streaming behavior.
- An empty remote `buffer_id_list` now means all eligible trigger buffers, matching workflow semantics.
- Equivalent remote and workflow reroutes are deduped without depending on `max_logs_count`.
- Logger-owned persistence exists for pending trigger uploads, with a file-backed protobuf snapshot
  and startup replay for `ReadyToUpload` entries.
- The current persistence split between `ReadyToUpload` and `Uploading` makes the remaining gap explicit.

The remaining work is to replace the persistence substrate, remove concurrency hazards, and define a crash-safe recovery model for uploads that have already started draining data.

## Milestone 1: Replace the Trigger Upload Registry

Status: Implemented.

Rewrite `bd-logger/src/flush_registry.rs` to use repo-native file-backed protobuf persistence.

### Requirements

- Replace manual encoding and decoding with typed protobuf serialization.
- Use a versioned on-disk file under the SDK directory, not `bd_key_value::Store`.
- Follow the persistence style used by `bd-workflows` state storage and `bd-client-stats` file-backed persistence.
- Make mutation atomic from the perspective of concurrent logger tasks.
- Ensure schema evolution is explicit and safe.

### Deliverables

- A new persisted protobuf schema for trigger upload recovery state.
- A file-backed registry owner abstraction with internal synchronization.
- Removal of the current shared-list string rewrite approach.
- Narrow unit tests for load, save, replace, prune, and unknown-field compatibility.

### Exit Criteria

- Concurrent registry updates cannot clobber each other.
- Registry contents survive process restart through a file-backed protobuf snapshot.
- The old manual encoding path is fully removed.

### Implementation Notes

- Landed as a versioned file-backed protobuf snapshot owned by `PendingTriggerUploadsStore`.
- Registry mutation is now serialized through one in-process synchronized owner instead of shared
  read-modify-write over `bd_key_value`.
- Because this feature is still new and unreleased, later milestones may directly change the
  snapshot protobuf shape as needed. Do not preserve transitional fields just for branch-local
  compatibility; reorder field IDs, rename fields, or remove obsolete fields when that produces the
  clearest steady-state schema.

## Milestone 2: Define the Durable Flush Model

Before adding more recovery behavior, define the persisted model completely enough to support full resume.

### Requirements

Persist the minimum information needed to recover one logical flush operation correctly:

- Logical flush ID.
- Source kind.
- Session ID at trigger time.
- Trigger buffer IDs.
- Durable streaming configuration snapshot, not just a boolean.
- Lifecycle stage.
- Per-buffer progress metadata.

### Lifecycle Work

Replace the current coarse lifecycle with explicit crash-boundary states, but do not assume a
single global `Enqueued` phase exists in the current design. The trigger buffer is uploaded in
chunks, and the current logger path moves directly from reading the trigger buffer into repeated
upload calls. That means the durable model has to be chunk-aware and boundary-based rather than
inventing a queue stage that does not yet exist.

A likely near-term shape is:

- `ReadyToUpload`
- `UploadingFromBuffer`
- `Completed`
- `Failed`

An additional post-buffer state should only be introduced if the architecture later gains a real
durable handoff boundary after buffer drain. If that happens, the state name and semantics should
be defined in terms of that concrete mechanism rather than a generic `Enqueued` label.

The exact names can change, but the model must distinguish:

- Work that has not yet consumed buffer data.
- Work that is actively reading and uploading one or more chunks from the trigger buffer.
- Work whose resume point is determined by persisted per-buffer progress metadata, not just a
  coarse lifecycle string.
- Work that has crossed into a durable post-buffer boundary, but only if such a boundary is made
  concrete in a later design.
- Terminal success and terminal failure.

The persisted model should therefore define lifecycle and progress together:

- The lifecycle says which crash boundary the flush is in.
- The progress metadata says, per buffer, how far chunked upload has advanced and what resume point
  remains valid.
- A lifecycle state alone is not sufficient once chunked upload has started.

Because this work is still defining the feature rather than evolving a released format, adjust the
snapshot protobuf directly when a later milestone needs a better structure. Do not add compatibility
only fields or fallback decoding unless the feature has actually shipped and needs on-disk upgrade
support.

### Exit Criteria

- The persisted record contains enough information to recreate remote and workflow streaming intent.
- The lifecycle model matches real crash boundaries in the current chunked upload path instead of
  implementation convenience.
- The design eliminates the current `Uploading` dead-end.
- The plan does not rely on a vague `Enqueued` state unless a real durable post-buffer handoff is
  explicitly introduced and defined.

## Milestone 3: Analyze Trigger Buffer Resume Semantics

Status: Implemented.

Analyze and harden the underlying buffer semantics before implementing resume for started uploads.

### Questions To Resolve

- Exactly when is the exclusive trigger-upload lock acquired?
- What writes are blocked once that lock is held?
- Can startup recovery re-lock the buffer before normal producers resume writing?
- If not, what ordering or API changes are needed to close that race?
- What is the authoritative resume primitive: consumer cursor, raw buffer offset, or another marker?
- When is buffer progress advanced relative to upload handoff and ack?
- What happens if the persisted position is no longer valid because the buffer wrapped or changed while the process was down?

### Required Code Analysis

- `bd-buffer/src/ring_buffer.rs`
- `bd-buffer/src/buffer/common_ring_buffer.rs`
- `bd-buffer/src/buffer/non_volatile_ring_buffer.rs`
- The trigger-upload creation and completion path in `bd-logger/src/consumer.rs`

### Exit Criteria

- The recovery design names one authoritative persisted resume point.
- Startup lock ordering is either proven safe or explicitly redesigned.
- Offset invalidation cases are handled intentionally, not implicitly.

### Findings

- Trigger-upload exclusivity is established in `bd_buffer::RingBuffer::new_consumer()`, which
  acquires the aggregate buffer lock before registering the one-off consumer. For aggregate trigger
  buffers, that lock targets the volatile producer side and prevents new reservations.
- Once the lock is held, `new_consumer()` waits for outstanding volatile reservations to drain and
  then flushes volatile data into the non-volatile buffer before registering the upload consumer.
  This means the actual upload drains a stable non-volatile snapshot after the producer side has
  been frozen.
- Startup recovery currently re-locks trigger buffers before normal producers resume writing.
  `LoggerUpdate::apply_configuration()` awaits `buffer_manager.update_from_config()`, and that call
  does not return until `TriggerBufferCreated` has been processed by `BufferUploadManager`. The
  upload manager immediately attempts persisted trigger-upload recovery in that event path, while
  `BufferProducers` are only rebuilt after the await returns.
- The authoritative persisted resume primitive is not an existing cursor consumer. Overwrite-mode
  trigger buffers explicitly reject cursor consumers today, and the persisted non-volatile
  `next_read_start` is destructive global buffer state rather than an isolated per-upload resume
  token.
- Current per-buffer progress counters are descriptive only. They identify that upload crossed into
  `UploadingFromBuffer`, but they do not yet define a crash-safe resume point inside the buffer.
- Offset invalidation remains unresolved and must be addressed by the milestone 4 design. Because
  trigger buffers operate in overwrite mode, any future persisted in-buffer position must define how
  recovery detects wraparound or stale offsets after restart.

### Outcome

- No structural buffer code change is required to complete Milestone 3 itself.
- The startup lock ordering question is resolved in favor of the current design.
- Milestone 4 must introduce a new authoritative persisted resume primitive or a real durable
  post-buffer handoff boundary; it cannot reuse the existing cursor-consumer persistence approach.

## Milestone 4: Add Crash-Safe Progress Persistence For Started Uploads

Status: Implemented.

Once the buffer semantics are settled, persist progress at the exact boundary where replay-from-the-start becomes incorrect.

### Requirements

- Persist progress before the system crosses from replay-safe into replay-incorrect territory.
- Either:
  - persist per-buffer consumer progress directly, or
  - define a real durable post-buffer handoff boundary after which recovery resumes from a durable
    artifact instead of the trigger buffer.
- Replace the current restart behavior that skips `Uploading` work.
- Ensure recovery after a crash in the middle of an upload is deterministic and does not duplicate logs.

Do not introduce an `Enqueued` lifecycle phase in this milestone unless that phase corresponds to a
concrete durable artifact or queue with well-defined recovery semantics.

### Exit Criteria

- Started trigger uploads can recover from the correct point after restart.
- The system no longer permanently abandons in-flight uploads after a crash.
- Duplicate emission is prevented by design, not best-effort heuristics.

### Implementation Notes

- Started trigger uploads now spill through a durable per-buffer artifact file under logger state
  instead of relying on in-memory batch state after the trigger buffer begins draining.
- The trigger-drain path now rebuilds full upload batches in memory, persists one whole queued batch
  at a time, and only then bulk-advances the trigger-buffer read pointers for that batch. This
  keeps the buffer copy authoritative until the durable handoff exists without paying one file
  write per log.
- Each persisted artifact can hold both one queued batch and a concrete persisted `inflight_batch`
  carrying the exact upload UUID and log payload for the next remote upload attempt.
- When recovery sees an `inflight_batch`, it resends that exact batch with the same upload UUID.
  This matches the existing upload protocol's UUID reuse semantics and closes the previous
  crash-after-ack gap without inventing a second local progress cursor.
- Trigger-upload recovery no longer skips started work. It now replays both `ReadyToUpload` and
  started non-terminal states, only skipping terminal `Completed` and `Failed` entries.
- To close the crash window between artifact persistence and buffer advancement, recovery
  reconciles any queued batch against the current oldest unread trigger-buffer records and drops a
  queued batch that is still represented by the buffer. That makes the buffer copy the source of
  truth until the batch has actually left the trigger buffer.
- The persisted lifecycle now includes `UploadingFromArtifact`, which is the concrete durable
  post-buffer handoff boundary Milestone 2 left intentionally undefined.

## Milestone 5: Reconnect Streaming To Durable Flush Completion

Make workflow and remote streaming restoration depend on durable flush state instead of runtime-only completion channels.

Status: Implemented.

- Added a shared `FlushCompletionTracker` in `bd-workflows` and wired it through `bd-logger` so
  the workflow engine can distinguish a genuinely completed flush from a restart that merely lost
  the in-memory `oneshot` receiver.
- The tracker is hydrated from the persisted trigger-upload registry during logger startup and kept
  up to date as trigger uploads are scheduled and completed.
- Restart coverage now verifies both workflow-triggered and remote streaming actions keep rerouting
  past their log-count threshold until the durable flush is marked complete.

### Requirements

- Replace or wrap `pending_buffer_flushes` in `bd-workflows/src/engine.rs` so restart does not lose completion tracking.
- Restore remote and workflow streaming actions from durable flush state using persisted flush IDs and persisted streaming config.
- Preserve the current overlap dedupe behavior for equivalent reroutes.
- Ensure restored streaming actions do not terminate early just because runtime-only receivers are missing after restart.

### Exit Criteria

- Streaming behavior after restart matches in-process behavior.
- Remote and workflow streaming remain linked to the recovered flush lifecycle.
- Restored actions do not extend, duplicate, or prematurely terminate reroutes.

## Milestone 6: Handle Config Drift And Missing Buffers

Startup recovery must reconcile persisted work against the current trigger-buffer configuration.

### Requirements

- Reconcile each persisted flush against currently registered trigger buffers.
- If some buffers are missing, prune and rewrite the record instead of silently skipping forever.
- Drop the full record only when no eligible buffers remain.
- Distinguish ordinary completion from abandonment due to missing buffers.
- Treat session changes as non-fatal; recovery should continue using persisted trigger context even if the live session has moved on.

### Exit Criteria

- Recovery behaves deterministically when config changes across restart.
- Persisted state does not become immortal because one referenced buffer disappeared.
- Operational logs clearly distinguish completion, pruning, and abandonment.

## Milestone 7: Verification And Regression Coverage

Add restart-focused coverage for the new persistence and recovery model.

### Unit Coverage

- Registry serialization and deserialization.
- Versioning and unknown-field handling.
- Synchronization and concurrent mutation.
- Lifecycle transitions.
- Pruning and corruption handling.

### Buffer And Consumer Coverage

- Restart before buffer re-lock.
- Restart after lock acquisition.
- Restart after partial drain.
- Restart after durable enqueue or ack boundary.
- Invalid or stale offset handling.

### Integration Coverage

- Workflow-triggered flush resume.
- Remote-triggered flush resume.
- Streaming continuation across restart.
- Restored streaming remaining linked to durable flush completion.
- No premature streaming completion when runtime-only receivers are absent.

### Exit Criteria

- The chosen recovery model is covered end to end.
- Crash windows discussed in this plan have direct regression tests.
- The restart path is validated for both workflow and remote flush triggers.

## Implementation Notes

### Preferred Persistence Pattern

The registry rewrite should follow a hybrid of the existing shared-core patterns:

- Use a versioned protobuf snapshot like `WorkflowsState`.
- Use a file-backed manager abstraction like `bd-client-stats`.
- Keep ownership local to logger code.
- Centralize writes through a synchronized owner instead of allowing free-form read-modify-write from multiple tasks.

### Core Design Constraints

- Persistence must be inspectable and evolvable.
- Mutation must be race-free.
- Recovery state must be rich enough to recreate streaming, not just upload intent.
- Resume correctness depends on exact buffer advancement semantics, so that analysis cannot be skipped.
- The final design must recover from the correct post-crash point rather than replaying from the beginning or dropping started work.

## Suggested Execution Order

1. Replace the registry substrate and schema.
2. Define the durable lifecycle and record shape.
3. Complete the buffer lock and offset analysis.
4. Implement crash-safe progress persistence for started uploads.
5. Reconnect streaming restoration to durable completion.
6. Handle config drift and buffer disappearance.
7. Land restart-focused verification.

## Files To Drive The Work

- `LOG_UPLOAD_RESUME.MD`
- `bd-logger/src/flush_registry.rs`
- `bd-logger/src/consumer.rs`
- `bd-logger/src/builder.rs`
- `bd-logger/src/log_replay.rs`
- `bd-logger/src/logging_state.rs`
- `bd-logger/src/service.rs`
- `bd-buffer/src/ring_buffer.rs`
- `bd-buffer/src/buffer/common_ring_buffer.rs`
- `bd-buffer/src/buffer/non_volatile_ring_buffer.rs`
- `bd-workflows/src/engine.rs`
- `bd-workflows/src/actions_flush_buffers.rs`
- `bd-client-stats/src/file_manager.rs`
- `bd-logger/src/consumer_test.rs`
- `bd-logger/src/test/logger_integration.rs`

## Verification Plan

During implementation, use focused validation at each milestone:

1. Check editor diagnostics on touched files before and after each milestone.
2. For the registry rewrite, add targeted tests and run narrow `cargo nextest run -p bd-logger` coverage.
3. For buffer resume semantics, add narrow buffer and consumer restart tests before wider integration coverage.
4. For workflow and streaming restoration, run targeted `bd-workflows` and `bd-logger` nextest slices.
5. Before considering the project complete, run `cargo clippy -p bd-logger -p bd-workflows --bins --examples --tests -- --no-deps` and a workspace diagnostics sweep.
