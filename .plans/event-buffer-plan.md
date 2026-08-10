# Logger Reliability Improvements: EventBuffer Plan

This document is intentionally layered. Read the plan and roadmap first; the rest is the detailed
reference for implementation and review. The detail preserves the important locking, ordering, and
failure semantics without requiring every reader to understand them before they can comment on the
overall direction.

## Plan at a glance

`EventBuffer` becomes the single bounded ingress for logger work that needs an ordering relation
with logs: log entries, selected state operations, and flush barriers. It replaces the current log
and state ingress channels while retaining one async consumer.

- The producer path only captures inputs and admits an entry. It never waits for disk I/O, provider
  calls while holding the buffer lock, or downstream workflow processing.
- Five fixed retention tiers decide what is discarded under memory pressure; admission IDs preserve
  global FIFO delivery across those tiers. Previous-process logs have a temporary startup lane so
  they can replay before current-process work without a drain-time queue partition.
- `bd_session` remains responsible for session state and persistence. EventBuffer uses a small,
  protected fence to wait on the existing session coalescer before a still-current session's work
  reaches downstream durable buffers. A newer session supersedes an older fence rather than
  writing stale session state.
- The buffer begins with a 10 MiB total in-memory limit and a 1 MiB ordinary-log sublimit. Both
  become runtime-configurable after configuration arrives; configuration changes apply on the next
  admission rather than walking the queue immediately.
- Startup replay remains unchanged until the final milestone. The final gate uses a 500 ms
  strawman base delay plus up to 1 s for a crash hint, both runtime-configurable.

## Milestone roadmap

1. **Shared platform mutex.** Introduce the caller-thread mutex adapter; use it for `bd_session`.
2. **Session persistence.** Replace ALB-carried session writes with a coalescing `bd_session`
   flusher and the atomic fence handshake.
3. **EventBuffer.** Build and test the bounded five-queue component without changing logger ingress.
4. **Ingress migration.** Route logger/state inputs through EventBuffer while preserving
   `PreConfigBuffer` startup behavior.
5. **Startup replay.** Replace `PreConfigBuffer` with the delayed replay gate and prior-process
   ordering lane.

The milestone checklists appear after the detailed design reference. Each milestone is independently
shippable: it changes only the behavior named in that milestone and keeps later behavior unchanged.

## Detailed design reference

This section is the canonical source for behavior and invariants. The milestone checklists below
name implementation work without restating these rules unless a milestone changes them.

### Goals

- Prefer important logs when bounded memory requires loss.
- Preserve workflow ordering during startup, including prior-session crash logs.
- Make every normal admission decision observable and priority-aware.
- Capture provider metadata close to the original `Logger.log` call.

The ring buffer and upload pipeline are out of scope.

### Architecture

```text
LoggerHandle / state APIs
  -> synchronous EventBuffer admission
  -> Notify
  -> AsyncLogBuffer task
  -> PreConfigBuffer or workflow engine
```

`EventBuffer` replaces the log and state ingress channels and `OrderedReceiver`. It is shared by
synchronous producers and has one asynchronous consumer. Milestone 1 introduces one shared
target-selected `PlatformMutex` available to every platform-facing caller-thread lock. `bd_session`
adopts it first, and EventBuffer reuses it in Milestone 3. The producer path uses its
`EventBufferMutex` only for short, non-awaiting buffer operations. Every implementation releases
the lock before awaits, provider calls, callbacks, or pipeline invocation. The existing logger
continues to use its current channels and `PreConfigBuffer` until milestone 4; milestone 5 moves
startup responsibility into EventBuffer.

The buffer lock is the ordering point. No producer-visible sequence number is needed: an internal
monotonic admission ID preserves FIFO delivery across retained entries and breaks equal-priority
eviction ties. The ID is an implementation detail; evicted IDs simply leave gaps and do not need
explicit drop-range records.

EventBuffer lifecycle and replay gating are independent state machines. Its lifecycle is
`Accepting` until shutdown changes it to `Closed`; closed handles reject new work. Separately,
Milestone 5's drain gate is `Holding` or `Open`. A holding gate still accepts and accounts for
entries—it merely withholds consumer delivery. This avoids using "closed" to mean both normal
startup buffering and terminal shutdown.

### Control-flow ownership

EventBuffer is the ordered data-plane ingress, not a general control bus. The migration retains
the following ownership boundaries.

| Flow | Owner and transport | Why it does or does not enter EventBuffer |
| --- | --- | --- |
| Logs, feature-flag exposure, post-startup memory pressure/entity-ID persistence, `FlushState`, and session-persistence fences | EventBuffer entries | These are ordered workflow, state-store, or barrier inputs. `FlushState`, `Block::Yes` logs, and session-persistence fences are protected entries. |
| Logger `setField` / `removeField` | Synchronous EventBuffer-owned `Arc` field map with per-log snapshots | These mutate admission metadata rather than producing a replayable workflow input. |
| Session creation/rotation, durable persistence, and session flush | `bd_session` generation-based coalescing flusher | Session mutation remains under the strategy's existing mutex. A single flusher persists only the latest generation and has no per-operation FIFO command queue. EventBuffer carries immutable session snapshots and protected conditional fence tokens; the consumer asks `bd_session` to wait, outside the EventBuffer lock. |
| Config updates and configuration readiness | Existing config-update path directly to the consumer | Applying config can build/replace downstream pipeline state and has no producer admission order. In Milestone 5, readiness starts the runtime-configured replay-delay timer; it never consumes EventBuffer capacity. |
| Crash-report processing request | Existing report-processing request path directly to the consumer, then one EventBuffer batch admission | The request triggers potentially expensive report discovery and parsing. Only the resulting replayable crash logs enter EventBuffer, preserving their batch source order and taking one well-defined admission boundary relative to concurrent producers. |
| `CrashPending` | Direct drain-gate extension hint | It changes gate policy, not workflow state; it can extend a held window but cannot itself deliver, reorder, or consume buffer capacity. |
| Shutdown, `Notify`, timers, SDK lifecycle/status, sleep-mode watch, and tracing flag | Direct lifecycle/scheduling primitives | These change consumer scheduling or local observable state; they are not retained replayable work. Shutdown closes EventBuffer rather than queuing a terminal entry. |
| Downstream stats, upload, buffer-flush, and workflow side effects | Existing consumer-owned downstream channels | These are consequences of a processed ordered entry, not new logger ingress. Do not feed them back through EventBuffer. |

The existing configuration and crash-request channels therefore remain purpose-specific control
paths. Session persistence remains a coalesced `bd_session` mechanism rather than a second
general-purpose logger control queue. Its small, protected EventBuffer fence token is the one
exception because it establishes the required ordering before session-bearing work reaches the
consumer; the token never performs I/O while EventBuffer is locked.

### Metadata and state handling

- `LoggerHandle::log` captures the provider timestamp and provider fields inline, outside the
  EventBuffer lock and while actually holding `with_thread_local_logger_guard`. The existing
  admission-only guard is not sufficient once provider code runs on the caller thread.
- It then locks EventBuffer, clones the current `Arc<LoggerFieldMap>` pointer, and admits a
  `CapturedLog` containing both snapshots. A same-thread `setField(); log()` is therefore
  reflected in the captured log; concurrent callers are ordered by buffer admission.
- EventBuffer owns a mutex-protected `Arc<LoggerFieldMap>`. Log admission clones that `Arc`, not
  the map, so queued logging never pays a field-map copy. Each admitted log therefore retains the
  exact field map visible at admission. `setField` and `removeField` are the less-frequent writers:
  after validating the proposed byte/count change, they update the map without changing any
  snapshot already held by a log. They may use `Arc::make_mut` when EventBuffer is the sole owner
  to avoid that update's map clone; this is an update-path optimization, not the reason for the
  design. These APIs are not downstream workflow events.
- Refactor `MetadataCollector` into provider-snapshot capture and background normalization. The
  async consumer merges captured provider data, caller fields, and the captured logger field map,
  then performs global-state tracking, state-store updates, and replay. It must not resolve a
  mutable current session for an already-admitted log.
- Prior-run logs keep their existing special path: do not capture current-process provider data;
  finalize them against prior global state on the background task. Preserve the existing
  `PreviousRunSessionID` `_logged_at` behavior explicitly: it currently uses the timestamp
  provider only, while `occurred_at` comes from the crash report.
- State/control operations that affect workflows or persistence remain protected EventBuffer
  entries. `Block::Yes` logs are also protected: blocking is an explicit reliability request, not
  merely a consumer-completion mechanism. A session-persistence fence is a protected ordering token
  whose wait remains owned by `bd_session`; crash-pending and shutdown remain direct control
  signals, not buffer entries.
- `set_feature_flag_exposure` resolves its session ID through `bd_session`, then captures provider
  and logger-field snapshots plus an admission timestamp before EventBuffer admission; provider
  capture uses the same held thread-local guard as logs. The consumer uses those immutable inputs
  for state-store insertion, global-state tracking, and workflow replay rather than querying the
  provider or session strategy later. Any implicit-session callback is dispatched after this state
  operation is admitted or terminally dropped, using the same rule as logs.

Provider capture is intentionally outside the EventBuffer lock, so each operation has two defined
cut points: provider time/fields and session ID are captured at the original API call, while the
logger-field map and FIFO position are captured at EventBuffer lock admission. A concurrent
`setField` that wins the EventBuffer lock before a log is admitted is therefore reflected in that
log; one that loses is not. Session mutations are serialized by `bd_session`'s existing state
mutex; persistence is coalesced separately.
This is the linearization rule for concurrent callers, and is preferable to holding the EventBuffer
mutex across arbitrary platform provider code.

Provider calls now become part of normal `Logger.log` latency. Before the migration, add temporary
duration/failure instrumentation to the current async calls; retain it through the move and measure
edge lock wait/hold time. This is also a provider-threading migration: today providers are called
serially on the async task; after the move they may be called concurrently on arbitrary application
threads. This contract is approved: platform implementations must preserve their required
thread-affinity, synchronization, and reentrant-logging behavior under concurrent inline capture.

### Session transitions

`bd_session` retains ownership of session durability and its existing mutex-protected in-memory
state. EventBuffer never owns a persistence handle or performs I/O under its lock. It does carry a
small protected `SessionPersistenceFence` token so consumer delivery can wait for `bd_session` at
the precise point where a session-bearing entry would otherwise be written downstream.

Milestone 2 replaces snapshot-carrying `PreparedSessionOperation` persistence with a
generation-based coalescing flusher inside `bd_session`:

- A session mutation applies under the existing strategy mutex, increments a monotonic dirty
  generation, and returns the new/current session ID immediately. Creating a new session also
  increments a monotonic session epoch and stores it to
  `pending_session_fence_epoch` with release ordering before releasing the strategy mutex. A
  resolved current-session snapshot contains the immutable `(session_id, session_epoch)` needed by
  EventBuffer; ordinary activity updates may advance the dirty generation without creating a new
  epoch. Deferred callbacks are recorded against the mutation generation rather than handing a
  cloned `LoadedState` to a logger queue.
- A persistence request marks the strategy dirty and wakes one flusher. The flusher snapshots the
  latest in-memory state and generation, persists it, then checks whether a newer generation was
  created while the write was in flight. If so, it persists the newest snapshot again. Only one
  flusher writes at a time, so an older asynchronous write cannot be the final durable state. A
  successful write clears `pending_session_fence_epoch` only with a compare-and-exchange for the
  epoch it wrote, so it cannot erase a newer session's pending fence.
- There is no FIFO command queue and no persistence-before-return requirement for
  `session_id`/`start_new_session`: a successfully applied mutation becomes current immediately;
  persistence is best effort and measured. This intentionally adopts the non-blocking session
  creation decision in this plan.
- Beginning in Milestone 4, EventBuffer loads `pending_session_fence_epoch` with acquire ordering
  while it is already holding its normal admission lock. If it equals the captured source epoch
  and that epoch has no fence already enqueued, EventBuffer atomically places one protected
  `SessionPersistenceFence { session_id, session_epoch }` immediately before the source. If the
  atomic contains a later epoch, the captured source is already superseded and receives no stale
  fence; if it is empty, its session is already durable. EventBuffer tracks only its highest
  enqueued fence epoch for deduplication. The fence and source either both fit or the source is
  terminally rejected; a full protected budget never permits a session-bearing entry to leap over
  its required fence.
- On delivery, the consumer invokes
  `bd_session::wait_for_current_persistence(session_id, session_epoch)` outside the EventBuffer
  lock. This is a coalescer waiter, not a second persistence path: it checks whether the target is
  current and already durable, otherwise registers a waiter for the existing coalescer and may send
  an idempotent wake. It does not perform a write itself. If C has superseded B before the
  coalescer selects B, B's waiter completes as `superseded` without writing B. A write failure is
  recorded and completes the fence as a best-effort failure so the consumer does not stall
  indefinitely; subsequent B entries retain their captured session ID but may proceed without a
  durable session state in that failure case.
- A successful fence delays downstream work behind it, not the caller that rotated or logged. It
  therefore preserves the normal logging tail while deliberately allowing persistence latency to
  reduce consumer throughput and create ordinary bounded-buffer pressure. It strengthens the usual
  case: while an epoch remains current, no work carrying that session reaches downstream durable
  buffers before a state containing the session ID has been written.
- A deferred callback runs inline after the strategy mutex is released and without waiting for its
  persistence generation. Direct `session_id` and `start_new_session` calls invoke it before
  returning on their initiating thread. Before Milestone 4, automatic rotations continue to invoke
  it on the consumer task that resolved the session. After Milestone 4, a log or feature-flag
  rotation holds its callback until that source reaches its terminal admission outcome, then invokes
  it on the admitting caller thread after the EventBuffer lock is released. Callback-generated work
  therefore follows the source operation, including when the source is dropped before admission.
  Benchmark whether the Rust or platform layer performs the final platform-callback invocation;
  prefer the platform layer if it avoids an extra FFI crossing without regressing callback latency
  or reentrancy behavior. This choice changes no callback timing, thread, or ordering contract.
- `FlushState(Block::Yes)` captures the current session generation after draining earlier
  EventBuffer work and waits for the coalescer to *complete* a write covering that generation
  before the existing session/store/workflow flush completes. A write of a newer generation also
  covers an earlier one. This is the explicit durability hedge for callers that can afford to
  block; normal log/session calls remain non-blocking. A failed write completes the flush wait with
  a measured best-effort failure rather than falsely claiming durability or waiting forever.
  Shutdown remains best effort and does not turn pending persistence into EventBuffer entries.

The accepted crash window is an in-memory transition from durable session A to session B followed
by process death before B's fence has completed. Entries still retained by EventBuffer are lost
with the process, so the recovered durable work will normally be only the crash artifact. On
restart, it is intentionally attributed to A—the last durable previous-process session—rather
than inventing an undurable B. Grouping that crash with A is preferable to adding disk I/O to the
normal logging tail. The fence removes the usual case of a still-current B log reaching a
downstream durable buffer first. The remaining best-effort gaps are a failed B write or a B fence
that is superseded after transition to C; neither causes a stale B write. A blocking flush remains
the opt-in hedge for callers that need to narrow those gaps further.

Pure reads such as `previous_process_session_id` remain direct. In Milestone 4, current-process
logs and feature flags resolve their immutable session snapshot from the in-memory strategy before
provider capture and store it in their EventBuffer payload. A later EventBuffer rejection never
rolls back the already-current session.

The workflow engine still receives no new session-control event. Its existing session-transition
behavior remains driven by the captured session ID on replayed logs.

### EventBuffer behavior

EventBuffer owns priority-aware retained-entry storage, byte accounting, its `Arc` field map with
per-entry snapshots, and a `Notify` for its consumer. Retention priority chooses *what may be evicted*; it must not
change the delivery order. Whatever representation is selected, these invariants are fixed:

- Every entry receives an increasing admission ID while the EventBuffer mutex is held. The consumer
  delivers the retained entry with the lowest applicable delivery key, so all retained entries are
  replayed in global admission order rather than in priority order.
- Retention tier and process source are independent fields. The tier controls admission and
  eviction; `CurrentProcess` versus `PreviousProcess` controls only the startup-gate delivery
  route.
  An eligible previous-process log uses a startup-only delivery lane in the protected category;
  it is charged to the shared total budget, is not another retention tier, and does not require
  rearranging retained entries at drain time.
- Pressure may evict only a strictly lower retention priority than the incoming entry. It always
  takes the lowest eligible priority first and the newest entry within that priority, preserving
  the oldest retained equal-priority entry. Protected entries are not eviction victims.
- Every entry is charged a conservative fixed bookkeeping overhead in addition to its payload.
  This gives even zero-payload control entries a nonzero cost and bounds live entry count plus
  metadata. Callbacks are collected while locked and invoked only after unlocking.
- Admission reserves any required container capacity before evicting retained entries. Allocation
  failure rejects the incoming entry and leaves retained entries untouched. Capacity remains
  available for amortized producer latency and is never synchronously shrunk on the hot path.

#### Fixed priority queue design

The priority taxonomy is intentionally fixed and small for the initial EventBuffer: five retention
tiers—one protected tier plus four evictable tiers. The protected tier has a normal lane and a
startup-only previous-process lane, so the implementation uses six `VecDeque`s during startup.
This is a delivery-layout detail, not a sixth retention priority. The taxonomy uses the full
public `LogType` surface but does not multiply every type by every level. The logger's five levels
are trace, debug, info, warn, and error; a future numeric level higher than error maps to the error
tier.

| Retention tier | Mapping | Rationale |
| --- | --- | --- |
| Protected | State/control entries, `Block::Yes` logs, all `LIFECYCLE` logs, and every previous-process log | These entries carry ordering, durability, barrier, or startup-recovery semantics. They are bounded only by `total_limit` and never priority-evicted. |
| Error | Any non-protected log at `ERROR` or a higher forward-compatible level | Preserve failures regardless of whether the source is application, UX, device, or SDK instrumentation. |
| Warning | Any remaining non-protected log at `WARN` | A warning is operationally meaningful, but an error may evict it under pressure. |
| Primary signal | `NORMAL`, `VIEW`, `UX`, `SPAN`, and `DEVICE` at `INFO` | These are application, user-interaction, network-request, and device-state signals most likely to explain product behavior. |
| Diagnostic | `RESOURCE`, `REPLAY`, and `INTERNAL_SDK` at `INFO`; every non-protected `DEBUG` or `TRACE` log; unknown types or unrecognized levels that are neither warning nor error | This contains high-volume telemetry and debug detail while keeping the mapping safe when a new enum value arrives. |

`LIFECYCLE` remains protected because it carries startup, app-update, and crash-report paths in the
current logger. `VIEW` and `UX` are included even though the Rust logger currently does not emit
them: they are public payload types and platform callers may use them. Severity is deliberately a
stronger signal than type for unprotected `WARN`/`ERROR` logs, while type differentiates `INFO`.
No producer selects a queue directly; one EventBuffer mapping function owns the table and records
the fallback mapping for unknown type/level combinations.

An admitted entry therefore carries three separate values: `(retention_tier, source,
admission_id)`. Routing under the EventBuffer mutex is:

```text
PreviousProcess while the gate is Holding  -> startup_previous lane (protected; total-limit charged)
everything else                            -> normal lane for retention_tier
```

While the startup gate is holding, no entry drains. Gate release only flips the gate state—there
is no stable partition or queue walk. `next_batch` drains `startup_previous` in FIFO order until
empty, then merges the fronts of the five normal retention lanes by admission ID. A previous-process
log admitted after the seal routes to the normal protected lane, so it never reorders live
current-process work. Because admission and gate sealing use the same mutex, a concurrent producer
is unambiguously either before or after the ordering window.

Store one normal `VecDeque<Entry>` per retention tier, plus the startup-only
`startup_previous` deque. An eligible previous-process entry appends to that lane while the gate
is holding; every other entry appends to its tier's normal lane. Insertion is O(1) amortized. On
pressure, admission scans the bounded evictable-tier array from lowest eligible priority and
removes entries from each normal queue's tail until enough bytes are released. This makes victim
selection and physical removal O(1) per evicted entry, with no middle removal, tombstones, or
compaction.

To preserve EventBuffer's single ordering domain, `next_batch` first drains the non-empty
`startup_previous` lane, then does *not* drain one tier at a time: it examines the front entry of
every non-empty normal tier and removes the one with the lowest admission ID. Since each normal
per-tier deque is FIFO, a minimum over their fronts is the globally oldest normal retained entry.
With five fixed tiers this performs at most five front comparisons per normal delivered entry: fixed
O(1) work, rather than a queue-dependent scheduler or a need to track dropped sequence ranges. The
startup lane makes the one-time prior-process prefix O(1) to activate at gate release, without
partitioning or rebuilding a global queue.

Advantages: simple ownership and memory accounting; contiguous storage and good cache locality;
constant-time eviction; and a direct expression of the product's small, fixed taxonomy. Costs:
the number of tiers is part of the implementation contract; every dequeue compares a fixed set of
fronts; and future arbitrary numeric priorities would either grow that set or need a different
representation.

EventBuffer has two runtime-configurable **in-memory** byte limits, neither of which preallocates
memory or includes the ring buffer, disk buffer, or upload pipeline. It starts with bootstrap
defaults because it can accept work before the first runtime configuration arrives:

- `total_limit`: a hard limit over every retained EventBuffer entry. Its bootstrap default is
  10 MiB, derived from today's 10 MiB state/control channel capacity.
- `log_limit`: a 1 MiB sub-limit over ordinary evictable (non-control) log entries. Its bootstrap
  default is today's 1 MiB log-channel capacity. Protected state/control entries and protected
  logs bypass this sub-limit but remain charged to `total_limit`.

These defaults are not added together: EventBuffer begins with a 10 MiB total cap, of which at
most 1 MiB may be ordinary evictable logs. The corresponding runtime settings replace both values
as one pending generation when configuration becomes available. The generation becomes effective
atomically at the beginning of the next EventBuffer admission. Until then, bootstrap limits remain
fully active; EventBuffer never has an unbounded pre-configuration period merely because
configuration is expected later.

The configuration-owning async consumer watches those runtime settings. On a change, it calls a
short `EventBuffer::set_pending_limits(log_limit, total_limit)` operation under the same
EventBuffer mutex used by admission. It records the pair without walking or evicting queued
entries. The next admission, while holding that mutex, applies the latest pending pair and any
required eviction *before* judging the incoming entry. Admission therefore sees one coherent
configuration generation and needs no independent atomics; separate atomics for the two values
could expose a torn configuration while capacity accounting is being updated.

If no entry is admitted after a configuration change, the current retained bytes are left alone and
may drain naturally under the preceding effective limit. On the next admission, a raised limit
becomes usable; a lowered limit evicts eligible ordinary entries where possible before that entry
is considered. EventBuffer never retroactively evicts protected entries: if retained protected
bytes alone exceed a newly lowered `total_limit`, record that explicit over-limit state and reject
new admissions until draining brings usage below the configured cap.

On ordinary-log admission, EventBuffer first makes room within `log_limit` by evicting lower
priority evictable logs. It drops the incoming log when it cannot displace a retained log; equal
priority retains the older entry. It then checks `total_limit`, using the same eviction policy if
additional evictable log bytes must be released. A protected entry bypasses `log_limit` and may
evict any evictable log to fit `total_limit`; it is rejected only if the total limit is occupied
entirely by protected entries. A normal log larger than `log_limit`, or any entry larger than
`total_limit`, is rejected and measured. There is no scheduler-dependent soft overflow.

Priority policy is the fixed tier mapping above. The protected tier bypasses `log_limit`; the four
evictable tiers share it. A protected admission may release bytes from any evictable tier, again
starting with the lowest tier and newest equal-priority entry.

#### Protected and control capacity semantics

Protected does **not** mean “cannot be dropped under any circumstance,” and control entries do
not have a separate reserved byte limit. Protected entries—including state/control operations,
`FlushState`, `Block::Yes` logs, session-persistence fences, lifecycle logs, and eligible
previous-process logs—share the one `total_limit` with every other retained entry. They bypass the
`log_limit` and can displace evictable logs, but they cannot exceed `total_limit`.

An already admitted protected entry is never evicted to admit another entry. A new protected entry
is instead terminally rejected if it is oversized, EventBuffer cannot reserve its required memory,
the lifecycle is closed, or the retained buffer is full of protected entries. On shutdown, even
admitted protected entries are removed as explicit shutdown drops. Thus the contract is
**non-evictable after admission, not delivery-guaranteed**. Rejection and shutdown are measured;
any associated blocking completion resolves exactly once.

#### Shutdown and terminal outcomes

Shutdown first changes EventBuffer's lifecycle to `Closed` under the lock, then wakes the consumer.
The consumer may finish an already detached batch, but it does not begin a new batch after its
shutdown branch wins. All still-retained entries are removed under the lock, recorded as shutdown
drops, and have any completion resolved after the lock is released. This preserves the current
best-effort shutdown behavior while preventing waiters from timing out solely because their sender
was stranded in the buffer.

`Block::Yes` and blocking `FlushState` preserve their current public meaning: the caller waits for
a terminal outcome, not a guarantee that the log was delivered or that every downstream operation
succeeded. A blocking log bypasses `log_limit` and is never a priority-eviction victim, while it
still counts against `total_limit`; it can be explicitly rejected if the protected portion has
filled that hard limit. The completion payload remains unit; protected-budget rejection,
provider-capture failure, processing completion, and shutdown all resolve it exactly once. The
distinguishing information is emitted through outcome metrics and internal diagnostics, not a new
public API result.

### ALB migration audit

The existing async log buffer is more than a log receiver. Milestone 4 must replace the following
behaviors deliberately rather than moving only `LoggerHandle::log`.

#### Inputs and state

- **Normal and helper logs:** Route every path—including `RESOURCE`, `REPLAY`, `LIFECYCLE`, and
  `INTERNAL_SDK` helpers—through the common admission flow below. Helpers only construct their
  existing type-specific fields; they never bypass EventBuffer.
- **Field changes:** `AddLogField` and `RemoveLogField` synchronously update EventBuffer's `Arc`
  field map using the collector's existing custom-key validation. Logs clone the `Arc` at admission;
  updates may use `Arc::make_mut` when uniquely owned. They do not create workflow entries.
- **Feature flags:** Keep them as protected ordered state operations. Capture their session,
  provider, and field snapshots at admission; insert a matching session fence first. While
  `PreConfigBuffer` remains in Milestone 4, its pending feature-flag item carries the same immutable
  inputs so replay never rereads mutable provider or session state.
- **Memory pressure and opaque entities:** Preserve ordered durable writes. Before state-store
  readiness, coalesce opaque-entity updates in an EventBuffer-owned pending slot. Builder takes the
  newest value, persists it, updates the public watch only on success, and marks the store ready.
  Afterward, each update is protected and updates the watch only after admission. Memory pressure
  retains its existing prior-run initialization behavior.

#### Session and blocking operations

- **Session APIs:** Milestone 2 replaces ALB's `PersistPreparedSession` entry with `bd_session`'s
  generation-based coalescing persistence. Mutation becomes current immediately, publishes its
  pending epoch atomically, and wakes persistence without blocking the caller. In Milestone 4, a
  matching immutable session snapshot places at most one protected conditional fence ahead of
  downstream work; that fence waits on `bd_session`, not a queue-owned persistence entry.
- **`FlushState` and `Block::Yes`:** Both are protected, bypass `log_limit`, and remain bounded by
  `total_limit`; an all-protected full buffer explicitly rejects them. A blocking flush drains
  earlier admitted work, captures the session generation, and waits for a completed persistence
  write before the existing stats, buffer, session, and workflow flushes. Blocking flushes and logs
  are ordered barriers; `FlushState(Block::No)` stays protected but does not change startup timing.
  Failures are measured best-effort outcomes. Every blocking completion resolves exactly once on
  processing, rejection, persistence failure, or shutdown.

#### Startup and consumer-owned work

- **Crash reports:** Keep report scanning outside EventBuffer. Admit parsed reports as one ordered
  batch, with per-report admission outcomes rather than all-or-nothing success. Current-run reports
  use captured provider, field, session, and fence context. Previous-run reports remain protected,
  use persisted prior state and session when available, and preserve the timestamp-provider
  `_logged_at` behavior. `CrashPending` remains an out-of-band gate-extension signal.
- **Configuration:** Keep updates outside EventBuffer: they have no producer admission order and
  may perform pipeline construction. Readiness is the explicit startup-gate release condition.
- **Workflow-injected logs and interceptors:** Keep both on the single consumer. Generated logs
  inherit immutable source context rather than re-entering edge admission. Interceptors—including
  internal counters, aggregation, network-quality decoration, device matching, and screenshot
  effects—remain outside the EventBuffer lock to preserve their serialized side effects.

#### Normal and helper-produced log flow

Every `LoggerHandle::log` path—including resource utilization, session replay, SDK start, app
update, and internal SDK helpers—calls one `EventBuffer::admit_log` API. Helpers construct their
existing message, fields, and `LogType`; priority follows from that type and level inside
EventBuffer rather than from a helper-specific queue path, except that `Block::Yes` promotes the
log to the protected class.

1. For a current-process log, `bd_session` resolves an immutable `(session_id, session_epoch)`
   snapshot and schedules any required persistence. A resolution failure is a terminal log drop; a
   successful implicit rotation records a deferred callback for post-admission dispatch without
   waiting for persistence. The caller then holds
   `with_thread_local_logger_guard` and captures provider timestamp and fields outside the
   EventBuffer lock. A provider failure is also a terminal drop; both outcomes record their
   respective metrics and resolve any `Block::Yes` completion without entering EventBuffer.
2. `PreviousRunSessionID` logs skip current-process session, provider, and logger-field capture.
   They retain their raw fields and override for the existing previous-global-state consumer path.
   Normal logs and `OccurredAt` logs proceed with their captured provider data; the latter retains
   its supplied occurrence timestamp.
3. EventBuffer acquires its lock and clones the current `Arc<LoggerFieldMap>` pointer. The log entry
   retains the original `LogLine` message, fields, matching fields, override, `CaptureSession`,
   provider snapshot, field-map snapshot, immutable session snapshot, and optional completion
   handle. It applies the pending-session-fence handshake defined in [Session transitions](#session-transitions):
   a matching pending epoch inserts a protected fence before the log, a later epoch means this
   source was superseded, and an empty value means its session is already durable. A rejected log
   does not roll back or otherwise alter an already-current session transition.
4. Admission applies the total/log limits and priority eviction policy. When a fence is present,
   it and the log are one source operation. A `Block::Yes` log is protected, so it bypasses
   `log_limit` and cannot be evicted; it is rejected only if it cannot fit the remaining
   `total_limit` after evictable entries have been displaced. Rejection or eviction resolves the
   entry's completion with a terminal drop outcome after releasing the lock. Successful admission
   schedules `Notify`; it does not wait for the background consumer. In either terminal source
   outcome, dispatch any deferred implicit-session callback only after the lock is released, so
   callback-originated logging follows this source operation.
5. The consumer processes a `SessionPersistenceFence` before any following work for that epoch. It
   waits outside the EventBuffer lock through the coalescer as specified in
   [Session transitions](#session-transitions). It then removes a log in FIFO order, runs the
   existing interceptors, and normalizes the original fields using the captured provider and
   logger-field snapshots. It uses the captured session ID rather than querying mutable session
   state. For `OccurredAt`, it emits the supplied timestamp and attaches captured provider time as
   `_logged_at`; the previous-run branch retains its existing prior-global-state and `_logged_at`
   semantics. It then follows the existing replay, buffer-writing, `CaptureSession`, and blocking-
   flush path. A successfully processed blocking log resolves its completion exactly once after
   that path finishes.

`CapturedLog` sizing includes provider snapshots, the immutable session snapshot, and completion state. The
logger-field `Arc` is deliberately not charged once per retained log: EventBuffer instead enforces
the aggregate logger-field byte/count limit at `setField` time. Older field-map snapshots retained by
queued logs are accepted as auxiliary memory, not a reason to evict or reject otherwise valid
events. Their worst case is bounded by the configured map limit times the bounded number of
retained map versions. Record live bytes, distinct-snapshot count, and rejected field mutations;
if that overhead proves material, replace the map representation with structural sharing rather
than coupling it to log-priority eviction.

The consumer exposes `EventBuffer::next_batch(max_entries)` as one branch of the existing async
`select!`. That future first registers `Notify::notified()`, then checks and takes a bounded FIFO
batch under the lock; registering before the check prevents a missed wakeup. It releases the lock
before interceptors, normalization, persistence, and replay. If entries remain after a batch, the
next call is immediately ready; control returns to `select!` between batches so configuration,
crash-report processing, the pipeline, timers, resource utilization, replay recording, events,
and shutdown retain fair progress. No code awaits, runs callbacks, parses reports, updates config,
or flushes while holding the lock.

## Milestone implementation details

### Milestone 1: shared platform-facing mutex

- Introduce a shared synchronous `PlatformMutex<T>` and RAII guard for platform-facing,
  caller-thread state. Its iOS implementation wraps `os_unfair_lock`; its Linux implementation
  wraps `parking_lot::Mutex`. The adapter never provides an async guard and documents that awaits,
  provider calls, callbacks, and pipeline work must occur after the guard is released.
- Migrate `bd_session`'s in-memory strategy state to `PlatformMutex` first, preserving its current
  lock scope and callback-after-unlock contract. This establishes the target-specific behavior
  before changing session persistence semantics.
- Add target-appropriate unit and concurrency coverage for mutual exclusion, guard release, and
  callback reentrancy. No EventBuffer, session-persistence, ALB, or startup-ordering semantics
  change in this milestone.

### Milestone 2: generation-based session persistence

- Refactor `bd_session` so a mutation increments an in-memory persistence generation and no
  longer hands a cloned state snapshot to `PersistPreparedSession`. Remove that ALB state variant.
- Add one coalescing persistence flusher: snapshot the latest state/generation, persist, and loop
  if a newer generation appeared. It has a single in-flight write, bounded state, and a wakeup—not
  a FIFO queue of prepared operations or per-operation responses.
- Add the `wait_for_current_persistence(session_id, session_epoch)` coalescer waiter specified in
  [Session transitions](#session-transitions). It performs no I/O itself. Keep it in `bd_session`;
  Milestone 2 does not yet enqueue it through ALB.
- Make `session_id` and `start_new_session` non-blocking with respect to persistence. They return
  after their in-memory mutation has succeeded, record durable write failures, and use temporary
  attempt-latency instrumentation. Deferred callbacks run inline after the strategy mutex is
  released rather than waiting for that attempt; preserve the existing initiating-thread delivery
  for direct calls and consumer-task delivery for automatic rotations while ALB remains in place.
  Preserve pure previous-process lookup behavior. Benchmark Rust- versus platform-layer final callback
  invocation, preferring the platform layer if it removes an FFI crossing without affecting the
  callback contract.
- Preserve current automatic-rotation *detection* while ALB remains in place. The consumer still
  resolves sessions at its current processing point; it merely wakes the coalescing flusher rather
  than awaiting a persisted snapshot. `FlushState(Block::Yes)` captures a generation and waits for
  its completed write outcome.
- Add a durable session-write failure metric. Generation, coalescing, completed-write latency, and
  callback-outcome instrumentation are temporary migration diagnostics. Test concurrent
  `session_id`/`start_new_session` mutations, inline callback-after-unlock behavior, a write
  completing after newer mutations, last-activity persistence, automatic rotation, blocking
  flush-through-generation success/failure, and shutdown. No EventBuffer, provider, log-admission,
  or startup-replay semantics change in this milestone.

### Milestone 3: EventBuffer state machine

- Implement EventBuffer as an unused logger-internal component with the full entry model required
  by this plan: captured logs (including protected `Block::Yes` logs), protected state/control
  entries including `SessionPersistenceFence`, completion handles, and closed/shutdown state.
- Implement the five fixed priority queues, dual-limit admission, global FIFO delivery by admission
  ID, tail eviction from lower tiers, protected-entry handling, fallible container growth, and
  terminal completion on rejection, eviction, or close.
- Implement the atomic fence-plus-source admission handshake from [Session transitions](#session-transitions),
  including the pending-epoch load and fence deduplication. A rejected fence rejects its source;
  an admitted fence remains protected even if its source is later evicted.
- Implement the mutex-protected `Arc<LoggerFieldMap>` snapshot model, with its independent
  aggregate byte/count limit and field validation. Log admission clones only the `Arc`; field
  updates may use `Arc::make_mut` as a unique-owner optimization. Snapshot telemetry is temporary
  validation instrumentation.
- Implement `next_batch(max_entries)` with the lost-wakeup-safe `Notify` protocol and bounded
  batches. Test it independently from the async logger's `select!` loop.
- Add focused unit and concurrency tests for all capacity, priority, ordering, field-map snapshots,
  completion, close, and notification invariants. Verify that logging clones only the `Arc`, and
  that a subsequent field update leaves an admitted log's snapshot unchanged. Exercise both
  `Arc::make_mut` paths as an update-path optimization. This remains an unused component milestone;
  logger ingress behavior is unchanged.

### Milestone 4: logger ingress migration

- Construct EventBuffer with the logger and replace the ALB log/state channels and
  `OrderedReceiver` with its synchronous handle and `next_batch` branch in the existing async
  `select!` consumer.
- Move provider snapshot capture to `LoggerHandle`, move logger-managed fields into EventBuffer,
  and split metadata normalization from provider capture. Provider execution on concurrent caller
  threads is an approved contract; retain temporary migration instrumentation to detect regressions
  before broad rollout.
- Move current log and feature-flag session resolution from the consumer to the logger edge using
  the Milestone-2 in-memory `bd_session` API. Capture the returned `(session_id, session_epoch)`
  before provider capture and EventBuffer admission, then apply the session-fence handshake. Invoke
  implicit log/state callbacks on the admitting caller thread only after the source reaches a
  terminal admission/drop outcome; do not wait for persistence.
- Migrate every ALB state and internal-ingress path in the audit above, including feature-flag
  metadata/session capture, opaque-entity startup recovery, crash-report batches, interceptor
  placement, generated-log context, and flush/blocking completion semantics.
- Preserve the current `PreConfigBuffer` and its immediate replay on initial configuration.
  Consequently, Milestone 4 has up to the 10 MiB bootstrap EventBuffer total allowance plus the
  existing 1 MiB startup buffer allowance while configuration is unavailable. Bootstrap EventBuffer
  limits remain active during that period even though the initial configuration will later make a
  runtime pair pending for the next admission. Overflow in `PreConfigBuffer` retains the current
  FIFO behavior and metrics; priority-aware startup retention arrives in milestone 5.
- Ship the small production metric set below. During development, enable the temporary
  instrumentation below to compare provider, lock, consumer, session, and callback behavior before
  selecting the final implementation, while preserving the agreed timing and ordering semantics.

### Milestone 5: soft startup replay gate

After Milestone 4 is stable, replace `PreConfigBuffer` with EventBuffer startup buffering and add
the soft drain gate below. This delivers delayed replay and crash-log reordering without coupling
those startup semantics to the ingress migration.

EventBuffer starts with its drain gate `Holding`. Once configuration has created the processing
pipeline, it reads the replay-delay runtime configuration and starts the base replay timer. The
strawman default is a 500 ms configuration-relative base delay. The gate opens only after that
deadline has passed. A platform crash-pending hint while the gate is holding can add up to a
further 1 s crash delay, for a 1.5 s maximum under the strawman. Both the base delay and maximum
crash delay are runtime-configurable. Holding continues to capture and prioritize events but does
not deliver them.

Before configuration is ready, `CrashPending` is retained as a pending extension hint and a
high-watermark crossing is retained as an early-release request; neither can deliver work without
a pipeline. At configuration readiness, apply the pending hint to the runtime-configured deadline,
record the current `(log_limit, total_limit)` pair through `EventBuffer::set_pending_limits`, and
retain the same runtime watch for later limit changes. The pair becomes effective on the next
EventBuffer admission. If the buffer is already at the high watermark calculated from the runtime
`total_limit`, release immediately with reason `high_watermark`; otherwise arm the configured
timer.

Configuration construction, including restoration of already-persisted workflow actions, remains
outside the EventBuffer ordering domain and keeps its current startup behavior while the gate is
holding. `InitLifecycle::LogProcessingStarted` and the SDK "running" status move to the first
gate release, immediately before the first EventBuffer batch is delivered; creating the pipeline
alone is not reported as log processing.

Removing `PreConfigBuffer` at this point means startup events are retained in their original
EventBuffer representation, so the same priority/eviction policy applies before and after
configuration is ready.

#### Previous-session replay ordering

`CapturedLog` carries two independent classifications: its retention priority and its source
(`CurrentProcess` or `PreviousProcess`). A previous-process log is eligible for special ordering
only when EventBuffer admits it while the startup gate is holding. The eligibility bit is captured
on the entry; it is not inferred later from the source alone.

An eligible previous-process entry enters the protected `startup_previous` FIFO lane at admission;
every other entry enters its normal retention lane. Gate release only changes the gate to `Open`—it
does not partition, move, clone, or scan queued entries. The consumer drains the startup lane first
in its original FIFO order, then resumes normal admission-ID merge delivery across the five normal
lanes.

Entries admitted after release always use normal retention lanes, including a late previous-process
crash log, which uses the normal protected lane. The gate never reopens, so that log keeps its high
retention priority but is not reordered ahead of current-session work. This avoids both a
drain-time O(n) partition and retroactively changing workflow order after current-session events
have started flowing.

The gate is soft: admission of a protected event at or above an 80%-of-`total_limit` high
watermark opens it early with reason `high_watermark`. Low-priority traffic alone does not shorten
the startup window. If the consumer still cannot catch up, the normal hard-cap eviction policy
applies; priority-event loss is measured rather than exceeding capacity.

`CrashPending` may extend the deadline only while the gate is holding. A high-watermark release, a
flush barrier, or normal timer release seals the ordering window; later hints and late
previous-process logs cannot reopen it. `start_new_session` remains an in-memory session mutation;
without a following EventBuffer entry it has no replay-ordering effect.

An admitted `FlushState(Block::Yes)` or blocking log is also a gate barrier: it seals the gate,
drains the already-admitted startup-previous lane first, then drains through its ordered position
before its completion resolves. It does not bypass older work. An admission-rejected or
provider-capture-rejected blocking operation resolves immediately as a terminal drop and cannot
act as a barrier. `FlushState(Block::No)` stays behind the gate, matching its existing
fire-and-forget behavior. Neither admitted blocking operation may remain pending solely because
the soft startup delay has not elapsed.

## Observability and validation

- Preserve the existing log enqueue success/full/closed metrics for continuity until the channel
  path is removed; add equivalent state metrics during the transition.

Production rollout metrics are intentionally small and use only bounded, coarse dimensions:

- Count EventBuffer entry outcomes as admitted, evicted, rejected-full, rejected-oversized, or
  closed. Do not break these down by event kind, log type, level, exact tier, or completion outcome.
- Export queued bytes and entries split only into protected and evictable categories, plus the age
  of the oldest retained entry to detect an unhealthy consumer.
- Count drain-gate openings by timer, crash hint, high watermark, or barrier, and record gate-hold
  duration.
- Keep durable session-write failures as a `bd_session` metric rather than an EventBuffer metric.

Development instrumentation is temporary: it must be feature-gated or sampled, have a stated
removal point before broad rollout, and not become a permanent metric merely because it aided
implementation. It includes per-lane depth/bytes/evictions; EventBuffer lock wait/hold time;
provider and callback duration/failure; consumer batch length, notification-to-dequeue time, and
`select!` branch service time; field-map snapshot bytes/version count and field-limit rejection;
pending/effective limit generations and shrink results; startup-lane/reorder counts; and detailed
session generation/coalescing/write timing. These measurements support benchmarks and migration
validation, not steady-state dashboards.
- In milestone 3, test the priority mapping exhaustively across every public `LogType` and level,
  including unknown forward-compatible values; bootstrap dual-limit admission before configuration;
  pending runtime-limit updates that raise and lower each cap; application of the pair before the
  next admission; protected-over-limit behavior after a shrink; priority eviction; global FIFO
  delivery across tiers; newest-first eviction within an equal-priority tier; protected-entry
  behavior; release/acquire pending-epoch publication, atomic fence/source admission, and fence
  deduplication; oversized input; field-map snapshots and limits; lifecycle close; completion on
  rejection/eviction/close; and Notify wake/drain races. Exercise every tier boundary and
  allocation failure plus callback-reentrancy coverage.
- In milestone 4, add old-log / session-start / new-log attribution, automatic session rotation,
  concurrent session-start/log admission, edge-time in-memory session resolution, explicit- and
  implicit-rotation callback dispatch/order (including callback-after-source admission and no
  persistence wait), fence-before-delivery for a current epoch, persistence failure best-effort
  progress, B-to-C fence supersession with no stale B write, already-durable and coalescer-waiter
  fence outcomes, both Rust- and platform-layer callback invocation paths, feature-flag state replay with captured metadata,
  opaque-entity pre-store coalescing/admission/recovery, memory-pressure persistence, flush
  ordering, both crash-report paths, provider reentrancy/threading, generated-log session
  inheritance, and bounded-batch fairness with a continuously non-empty EventBuffer.
- In milestone 5, add prior-run metadata behavior, gate timer and crash extension,
  high-watermark early replay, barrier release, previous-process startup-lane FIFO delivery, late
  previous-process no-reorder behavior, blocking-flush gate activation, nonblocking-flush gate
  retention, and
  PreConfigBuffer-to-EventBuffer migration coverage.
- Add contention benchmarks covering concurrent logging, field updates, deliberately slow
  providers, fixed-tier dequeue selection, tail eviction, and startup-lane drain as part of
  milestone 4. Verify the crate with `cargo nextest run -p bd-logger`.

## Decision record

- The plan deliberately avoids an extra ingress task and the residual blind-drop path of a bounded
  Tokio ingress channel.
- It deliberately accepts that provider execution can add synchronous caller latency, subject to
  measured provider and lock-tail latency.
- Session persistence remains owned by `bd_session`. It applies a session mutation immediately
  under its own mutex and coalesces durable writes by generation; logs and state operations carry
  immutable session snapshots. EventBuffer admission never changes or rolls back session state, but
  its protected conditional fence delays downstream work until the current session epoch is written,
  superseded, or has a measured best-effort failure.
- EventBuffer is one ordering domain for producer data and state, not for configuration. Existing
  configuration control-plane timing remains out of band and is made explicit through the startup
  gate.
- The bootstrap limits are a 10 MiB total budget and a 1 MiB evictable-log budget. They derive
  from today's separate state/control and non-control channel capacities, but EventBuffer applies
  them as one total cap with a log sublimit rather than adding them or reserving bytes per priority
  queue. Both limits are runtime-configurable after configuration arrives.
- Milestones 1 through 4 preserve current `PreConfigBuffer` startup behavior; milestone 5 is the
  only milestone that changes initialization replay and workflow ordering.
- Milestone 5 introduces runtime settings for the 500 ms strawman base replay delay, 1 s maximum
  crash-hint extension, `log_limit`, and `total_limit`. The 1 MiB log sublimit, 10 MiB total limit,
  and 80% high watermark are bootstrap defaults before the first configuration arrives; later
  runtime updates become effective on the next EventBuffer admission using the limit-shrink
  behavior specified above.
- Provider-capture failures preserve today's best-effort public behavior: the affected log is
  dropped, its blocking completion becomes terminal, and diagnostics remain metrics/logging rather
  than a new caller-visible error. Reentrant logging during provider capture remains rejected by
  the thread-local guard.
- Current-process crash reports use admission-time provider, logger-field, and session snapshots;
  prior-process reports use prior global state and the prior-process session ID when available.
  This is the attribution rule rather than an unresolved choice.
- An all-protected full EventBuffer rejects the incoming protected entry. It never exceeds
  `total_limit`, parks a pending admission, or relies on Tokio scheduling for room; rejection is
  explicit and measured.
- The initial `Arc<LoggerFieldMap>` design makes log admission clone only the `Arc` and accepts
  older snapshots as bounded auxiliary memory. Field updates may use `Arc::make_mut` to avoid a
  clone when uniquely owned. Structural sharing is needed only if telemetry shows that retained
  snapshots are material; it is not a prerequisite for the EventBuffer migration.
- Use the shared `PlatformMutex` for platform-facing caller-thread locks: `bd_session` adopts it in
  Milestone 1 and EventBuffer reuses it in Milestone 3. Its target bindings and lock contract are
  defined in Milestone 1.
- Session callbacks no longer wait for persistence. They run inline after the session mutex is
  released; direct session APIs retain their initiating-thread delivery, while Milestone 4
  log/feature-flag rotations invoke their callback after the source's terminal admission outcome.
  Benchmark the final callback invocation layer, with the platform layer preferred when it avoids
  an extra FFI crossing without changing callback semantics.

### Rejected alternative: fork the previous-process workflow engine

We considered taking a copy of the persisted workflow-engine state at startup and replaying
previous-process logs against an isolated, in-memory historical engine while current-process logs
continued immediately through the live engine. This would prevent a late previous-process log
from directly resetting or advancing the live engine's state.

We are not selecting this as a replacement for the soft replay gate. A process boundary is not
necessarily a session boundary: a prior-process log and a current-process log may belong to the
same session. In that case the historical and live engines would advance independently, and there
is no general correct merge for workflow runs, extraction state, tracing, generated logs, or
triggered actions. The prior log must still be processed before the current work in the live
workflow ordering domain.

The alternative also requires a fork of both the persisted workflow state and the exact workflow
configuration that produced it; the current state snapshot does not contain workflow definitions.
It would require a new action policy as well: workflow processing currently emits metrics,
flush/streaming intents, Sankey work, screenshots, and injected logs while advancing state. A
historical engine would either suppress those effects—changing crash-log workflow behavior—or need
per-effect idempotence and cross-process ownership rules. Those costs are disproportionate to the
cases where process and session boundaries happen to differ.

An isolated historical engine remains a possible future policy for *late* previous-process logs
after the startup window has sealed, if product decides they must not affect current workflows. It
does not remove the need for the bounded replay gate that establishes the initial ordering.

## Remaining review gates

| Topic | Locked direction | Remaining action |
| --- | --- | --- |
| Startup reordering | Only previous-process logs admitted before gate sealing are reordered ahead of current-process entries. Late previous-process logs retain high eviction priority but are never reordered. | Validate the strawman delay values and select the high-watermark threshold. |
| Priority representation | Five fixed `VecDeque` tiers use admission-ID merge delivery. | Confirm the five-tier mapping with product/workflow owners and benchmark the fixed-tier design before Milestone 3 implementation. |

## Possible user-exposed priority levels

This is a possible future API, not part of the initial EventBuffer migration. If applications need
to influence retention, expose a coarse `RetentionHint` rather than the five internal tiers or an
arbitrary numeric priority:

| Public hint | Effective retention tier | Limits of the hint |
| --- | --- | --- |
| `Background` | Diagnostic | It may lower ordinary `INFO` application/user logs, but it cannot demote an automatic `WARN` or `ERROR`. |
| `Default` | The normal type-and-level mapping | This preserves the behavior specified in the five-tier table. |
| `Elevated` | At least Primary signal | It may promote an ordinary log above Diagnostic, but it cannot outrank Warning, Error, or Protected. |

The effective tier is calculated at admission from the automatic type/level mapping plus this
bounded hint. The hint changes only local EventBuffer eviction eligibility: it never changes log
level, delivery order, workflow order, upload order, or downstream persistence. `Protected` is not
publicly selectable, `Elevated` entries still count against both applicable byte limits and remain
evictable, and an application that marks all logs elevated simply makes those logs compete with one
another.

Five internal tiers are sufficient for this coarse public API because every hint maps to an existing
tier. If product instead requires an arbitrary numeric value with meaningful distinctions between
values, fixed lanes would only provide undocumented bucketing. That would be a separate future
design decision, not an extension of this EventBuffer plan.
