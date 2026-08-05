# Logger Reliability Improvements: EventBuffer Plan

This plan introduces a single, mutex-backed `EventBuffer` at the logger edge. It is delivered in
four milestones: first make session persistence generation-based and independently durable,
then build EventBuffer itself, then wire EventBuffer into logger ingress while retaining the
current `PreConfigBuffer`, and finally replace startup buffering with EventBuffer's delayed soft
replay gate.

## Goals

- Prefer important logs when bounded memory requires loss.
- Preserve workflow ordering during startup, including prior-session crash logs.
- Make every normal admission decision observable and priority-aware.
- Capture provider metadata close to the original `Logger.log` call.

The ring buffer and upload pipeline are out of scope.

## Architecture

```text
LoggerHandle / state APIs
  -> synchronous EventBuffer admission
  -> Notify
  -> AsyncLogBuffer task
  -> PreConfigBuffer or workflow engine
```

`EventBuffer` replaces the log and state ingress channels and `OrderedReceiver`. It is shared by
synchronous producers and has one asynchronous consumer. The producer path uses
`parking_lot::Mutex` only for short, non-awaiting buffer operations; the consumer releases the
lock before awaiting work or invoking the pipeline. The existing logger continues to use its
current channels and `PreConfigBuffer` until milestone 3; milestone 4 moves startup responsibility
into EventBuffer.

The buffer lock is the ordering point. No producer-visible sequence number is needed: events are
delivered in lock-admission order, and an internal monotonic insertion ID only breaks priority
ties and preserves oldest-retained behavior.

EventBuffer lifecycle and replay gating are independent state machines. Its lifecycle is
`Accepting` until shutdown changes it to `Closed`; closed handles reject new work. Separately,
Milestone 4's drain gate is `Holding` or `Open`. A holding gate still accepts and accounts for
entries—it merely withholds consumer delivery. This avoids using "closed" to mean both normal
startup buffering and terminal shutdown.

## Control-flow ownership

EventBuffer is the ordered data-plane ingress, not a general control bus. The migration retains
the following ownership boundaries.

| Flow | Owner and transport | Why it does or does not enter EventBuffer |
| --- | --- | --- |
| Logs, feature-flag exposure, post-startup memory pressure/entity-ID persistence, and `FlushState` | EventBuffer entries | These are ordered workflow, state-store, or barrier inputs. `FlushState` and `Block::Yes` logs are protected entries. |
| Logger `setField` / `removeField` | Synchronous EventBuffer-owned COW map | These mutate admission metadata rather than producing a replayable workflow input. |
| Session creation/rotation, durable persistence, and session flush | `bd_session` generation-based coalescing flusher | Session mutation remains under the strategy's existing mutex. A single flusher persists only the latest generation and has no per-operation FIFO command queue. EventBuffer receives only the resolved immutable session ID. |
| Config updates and configuration readiness | Existing config-update path directly to the consumer | Applying config can build/replace downstream pipeline state and has no producer admission order. In Milestone 4, readiness starts the runtime-configured replay-delay timer; it never consumes EventBuffer capacity. |
| Crash-report processing request | Existing report-processing request path directly to the consumer, then one EventBuffer batch admission | The request triggers potentially expensive report discovery and parsing. Only the resulting replayable crash logs enter EventBuffer, preserving their batch source order and taking one well-defined admission boundary relative to concurrent producers. |
| `CrashPending` | Direct drain-gate extension hint | It changes gate policy, not workflow state; it can extend a held window but cannot itself deliver, reorder, or consume buffer capacity. |
| Shutdown, `Notify`, timers, SDK lifecycle/status, sleep-mode watch, and tracing flag | Direct lifecycle/scheduling primitives | These change consumer scheduling or local observable state; they are not retained replayable work. Shutdown closes EventBuffer rather than queuing a terminal entry. |
| Downstream stats, upload, buffer-flush, and workflow side effects | Existing consumer-owned downstream channels | These are consequences of a processed ordered entry, not new logger ingress. Do not feed them back through EventBuffer. |

The existing configuration and crash-request channels therefore remain purpose-specific control
paths. Session persistence adds only a coalesced wakeup/flush mechanism inside `bd_session`, not a
second general-purpose logger control queue: a flow belongs in EventBuffer when it needs ordering
with replayable inputs, and otherwise stays with the owner above.

## Metadata and state handling

- `LoggerHandle::log` captures the provider timestamp and provider fields inline, outside the
  EventBuffer lock and while actually holding `with_thread_local_logger_guard`. The existing
  admission-only guard is not sufficient once provider code runs on the caller thread.
- It then locks EventBuffer, snapshots its copy-on-write `setField` map, and admits a
  `CapturedLog` containing both snapshots. A same-thread `setField(); log()` is therefore
  reflected in the captured log; concurrent callers are ordered by buffer admission.
- `setField` and `removeField` update EventBuffer's copy-on-write field map synchronously. They
  are not downstream workflow events. EventBuffer enforces a separate configured aggregate
  logger-field byte/count limit: a mutation that would exceed it is rejected and leaves the
  current map unchanged. Log calls only clone an `Arc` snapshot.
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
  merely a consumer-completion mechanism. Dedicated session persistence belongs to `bd_session`;
  crash-pending and shutdown remain direct control signals, not buffer entries.
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

Provider calls now become part of normal `Logger.log` latency. Before the migration, add duration
and failure telemetry to the current async calls; retain that telemetry after the move and measure
edge lock wait/hold time. A slow-provider result is the decision point for retaining the older
ingress-task design instead. This is also a provider-threading migration: today providers are
called serially on the async task; after the move they may be called concurrently on arbitrary
application threads. Certify the platform implementations' thread-affinity, synchronization, and
reentrant-logging behavior before enabling inline capture.

## Session transitions

Session durability remains outside EventBuffer. `bd_session` keeps ownership of its existing
mutex-protected in-memory state; EventBuffer never holds a session strategy handle, persistence
entry, callback, or session-persistence byte charge.

Milestone 1 replaces snapshot-carrying `PreparedSessionOperation` persistence with a
generation-based coalescing flusher inside `bd_session`:

- A session mutation applies under the existing strategy mutex, increments a monotonic dirty
  generation, and returns the new/current session ID immediately. It records any deferred callback
  against that generation, rather than handing a cloned `LoadedState` to a logger queue.
- A persistence request marks the strategy dirty and wakes one flusher. The flusher snapshots the
  latest in-memory state and generation, persists it, then checks whether a newer generation was
  created while the write was in flight. If so, it persists the newest snapshot again. Only one
  flusher writes at a time, so an older asynchronous write cannot be the final durable state.
- There is no FIFO command queue and no persistence-before-return requirement for
  `session_id`/`start_new_session`: a successfully applied mutation becomes current immediately;
  persistence is best effort and measured. This intentionally adopts the non-blocking session
  creation decision in this plan.
- A callback is dispatched only after the flusher has attempted persistence through its generation,
  outside the strategy mutex, through a platform-provided `SessionCallbackDispatcher`. This is an
  intentional change from the current originating/consumer-thread delivery and must be approved as
  part of Milestone 1. After Milestone 3, an implicit log/feature-flag callback additionally waits
  for its source admission outcome before dispatching.
- `FlushState(Block::Yes)` captures the current session generation after draining earlier
  EventBuffer work and waits until persistence has attempted that generation before the existing
  session/store/workflow flush completes. Shutdown remains best effort and does not turn pending
  persistence into EventBuffer entries.

Pure reads such as `previous_process_session_id` remain direct. In Milestone 3, current-process
logs and feature flags resolve their ID from the in-memory strategy before provider capture and
store that immutable ID in their EventBuffer payload. A later EventBuffer rejection never rolls
back the already-current session.

The workflow engine still receives no new session-control event. Its existing session transition
behavior is driven by the captured session ID on the first log or session-bearing state operation
it processes.

## EventBuffer behavior

EventBuffer owns a `VecDeque<Entry>` in admission order, byte accounting, the copy-on-write field
map, and a `Notify` for its consumer. The deque is both the FIFO delivery order and the source
scanned for priority eviction.

### Queue representation and growth

Each admission receives a monotonic insertion ID and is appended directly to `VecDeque<Entry>`.
On pressure, EventBuffer scans the deque to select strictly lower-priority victims. It evicts
lowest-priority candidates first and, within a victim priority, newer entries first; this preserves
the oldest retained entry for an equal-priority admission.

- Eviction physically removes selected entries with `VecDeque::remove`, in descending index order
  so earlier selected indexes remain valid. There are no tombstones, stale index records, or
  compaction passes in this version.
- `next_batch` drains with `pop_front`, so delivery remains ordinary FIFO over the retained
  entries.
- Every entry is charged a conservative fixed bookkeeping overhead in addition to its payload.
  This gives even zero-payload control entries a nonzero cost and bounds both live entry count and
  queue metadata for a full buffer.
- The deque grows only on successful admission using fallible reservation before any existing
  entry is evicted. Allocation failure rejects the incoming entry and leaves retained entries
  untouched. Capacity is retained for amortized producer latency and is not synchronously shrunk
  on the hot path.
- Completion callbacks are collected while the lock is held and invoked only after it is released.
  Admission, eviction, and callback code therefore cannot re-enter one another while holding
  EventBuffer state.

EventBuffer has two configured byte limits, neither of which preallocates memory:

- `total_limit`: a hard limit over every retained EventBuffer entry. Its initial value is 11 MiB,
  preserving the current 1 MiB log-channel plus 10 MiB state-channel allowance.
- `log_limit`: a 1 MiB sub-limit over evictable log entries only. Protected state/control entries
  and protected logs bypass this sub-limit but remain charged to `total_limit`.

On ordinary-log admission, EventBuffer first makes room within `log_limit` by evicting lower
priority evictable logs. It drops the incoming log when it cannot displace a retained log; equal
priority retains the older entry. It then checks `total_limit`, using the same eviction policy if
additional evictable log bytes must be released. A protected entry bypasses `log_limit` and may
evict any evictable log to fit `total_limit`; it is rejected only if the total limit is occupied
entirely by protected entries. A normal log larger than `log_limit`, or any entry larger than
`total_limit`, is rejected and measured. There is no scheduler-dependent soft overflow.

Priority policy:

1. Prior-run logs, lifecycle logs, `Block::Yes` logs, and state/control events are protected and
   replay-capable.
2. `NORMAL` and `DEVICE` logs form the higher evictable class.
3. `SPAN`, `REPLAY`, `INTERNAL_SDK`, and `RESOURCE` form the lower evictable class.
4. Within either log class, severity is error, warn, info, debug, then trace.

### Shutdown and terminal outcomes

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

## ALB migration audit

The existing async log buffer is more than a log receiver. Milestone 3 must replace the following
behaviors deliberately rather than moving only `LoggerHandle::log`.

| Current ALB input or behavior | EventBuffer migration requirement |
| --- | --- |
| Normal logs and helper-produced `RESOURCE`, `REPLAY`, `LIFECYCLE`, and `INTERNAL_SDK` logs | Route every path through the concrete admission/consumer flow below; helpers only construct their existing type-specific fields and never bypass EventBuffer. |
| `AddLogField` / `RemoveLogField` | Update EventBuffer's copy-on-write map synchronously after the same custom-key validation the collector performs today. Do not emit a downstream workflow entry. |
| Feature-flag exposure | Keep it a protected ordered state operation. Capture both its session ID and the provider/field snapshots needed by `handle_state_insert` at admission; otherwise it would still call providers later and observe a different session/global context. While Milestone 3 still uses `PreConfigBuffer`, extend its pending feature-flag item to carry these captured inputs and replay that immutable payload after config. |
| Memory-pressure and opaque-entity updates | Preserve their ordered durable state-store writes. Before the state store is initialized, coalesce opaque-entity updates in an EventBuffer-owned pending slot rather than enqueueing individual state entries. Builder atomically takes that latest value, persists it, updates the public watch only on successful persistence, and marks the store ready; no stale startup entries remain to overwrite it later. After readiness, each update is a protected entry and updates the watch only after admission. Memory-pressure remains an ordered protected persistence entry and retains its existing prior-run initialization path. |
| `session_id` and `start_new_session` | In Milestone 1, remove ALB's `PersistPreparedSession` state entry and use `bd_session`'s generation-based coalesced persistence. A mutation becomes current immediately and wakes persistence without blocking the caller. In Milestone 3, EventBuffer receives only the immutable session ID already resolved from that in-memory state; it contains no session-persistence entry. |
| `FlushState` and `Block::Yes` logs | Classify every `FlushState` as protected, and classify every `Block::Yes` log as a protected log. They bypass `log_limit`, are never evicted for priority, and remain bounded by `total_limit`; an all-protected full buffer explicitly rejects the incoming operation and resolves its completion. `FlushState(Block::Yes)` and blocking logs are ordered barriers. A blocking flush drains all earlier admitted EventBuffer work, captures and waits for the current session persistence generation, then runs the existing stats, buffer, session, and workflow flushes. `FlushState(Block::No)` remains protected but does not change startup timing. Every blocking completion must resolve exactly once on processing, protected-budget rejection, admission failure, or shutdown so callers never wait forever. |
| Crash-report requests and the crash-monitor callback | Keep report scanning outside EventBuffer. Once a scan returns, take one EventBuffer lock and admit its reports as an ordered batch, applying normal priority eviction per report without producer interleaving. A batch is an atomic producer-order boundary, not all-or-nothing: every report gets its own admission result and metric, so a full protected budget can retain an ordered prefix and explicitly reject the remainder without soft overflow. Current-run reports capture current provider/field/session context at that admission. Previous-run reports are protected, use persisted prior global state and the prior-process session ID; if none exists, use the normal current-session preparation path and record the fallback. They do not capture current field-provider fields, but preserve the existing timestamp-provider use for `_logged_at`. The crash-monitor callback uses the same current-run admission helper. `CrashPending` remains an out-of-band gate-extension signal. |
| Config updates | Keep these control-plane messages outside EventBuffer. They have no producer admission order today and may perform expensive pipeline setup; configuration readiness is the explicit gate-release condition. |
| Workflow-injected logs | Keep them within the consumer's current processing transaction rather than re-admitting them at the edge. They must inherit immutable source context—at minimum the source session ID—so a later edge session transition cannot relabel generated logs. |
| Interceptors | Keep all interceptors on the single consumer and outside the EventBuffer lock. This includes internal-report counters, HTTP/battery aggregation, network-quality decoration, device matching fields, and the screenshot-ready side effect; moving them would change their serialized state and side effects. |

### Normal and helper-produced log flow

Every `LoggerHandle::log` path—including resource utilization, session replay, SDK start, app
update, and internal SDK helpers—calls one `EventBuffer::admit_log` API. Helpers construct their
existing message, fields, and `LogType`; priority follows from that type and level inside
EventBuffer rather than from a helper-specific queue path, except that `Block::Yes` promotes the
log to the protected class.

1. For a current-process log, `bd_session` resolves the current session ID and schedules any
   required persistence. A resolution failure is a terminal log drop; a successful implicit
   rotation records a deferred callback for post-admission dispatch once persistence is attempted.
   The caller then holds
   `with_thread_local_logger_guard` and captures provider timestamp and fields outside the
   EventBuffer lock. A provider failure is also a terminal drop; both outcomes record their
   respective metrics and resolve any `Block::Yes` completion without entering EventBuffer.
2. `PreviousRunSessionID` logs skip current-process session, provider, and logger-field capture.
   They retain their raw fields and override for the existing previous-global-state consumer path.
   Normal logs and `OccurredAt` logs proceed with their captured provider data; the latter retains
   its supplied occurrence timestamp.
3. EventBuffer acquires its lock and snapshots the COW logger-field map. The log entry retains the
   original `LogLine` message, fields, matching fields, override, `CaptureSession`, provider
   snapshot, field-map snapshot, session ID, and optional completion handle. A rejected log does
   not roll back or otherwise alter an already-persisted session transition.
4. Admission applies the total/log limits and priority eviction policy. A `Block::Yes` log is
   protected, so it bypasses `log_limit` and cannot be evicted; it is rejected only if it cannot
   fit the remaining `total_limit` after evictable entries have been displaced. Rejection or
   eviction resolves the entry's completion with a terminal drop outcome after releasing the lock.
   Successful admission schedules `Notify`; it does not wait for the background consumer. In
   either admission outcome, dispatch any deferred implicit-session callback only after the lock is
   released, so callback-originated logging follows this source operation.
5. The consumer removes the entry in FIFO order, runs the existing interceptors, then normalizes
   the original fields using the captured provider and logger-field snapshots. It uses the captured
   session ID rather than querying mutable session state. For `OccurredAt`, it emits the supplied
   timestamp and attaches captured provider time as `_logged_at`; the previous-run branch retains
   its existing prior-global-state and `_logged_at` semantics. It then follows the existing replay,
   buffer-writing, `CaptureSession`, and blocking-flush path. A successfully processed blocking log
   resolves its completion exactly once after that path finishes.

`CapturedLog` sizing includes provider snapshots, the session ID, and completion state. The
logger-field `Arc` is deliberately not charged once per retained log: EventBuffer instead enforces
the aggregate logger-field byte/count limit at `setField` time. Old COW snapshots retained by
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

## Milestone 1: generation-based session persistence

- Refactor `bd_session` so a mutation increments an in-memory persistence generation and no
  longer hands a cloned state snapshot to `PersistPreparedSession`. Remove that ALB state variant.
- Add one coalescing persistence flusher: snapshot the latest state/generation, persist, and loop
  if a newer generation appeared. It has a single in-flight write, bounded state, and a wakeup—not
  a FIFO queue of prepared operations or per-operation responses.
- Make `session_id` and `start_new_session` non-blocking with respect to persistence. They return
  after their in-memory mutation has succeeded and record failure/latency telemetry from the later
  attempt. Deferred callbacks use `SessionCallbackDispatcher` after that attempt rather than the
  current initiating/consumer thread. Preserve pure previous-process lookup behavior.
- Preserve current automatic-rotation *detection* while ALB remains in place. The consumer still
  resolves sessions at its current processing point; it merely wakes the coalescing flusher rather
  than awaiting a persisted snapshot. `FlushState(Block::Yes)` captures a generation and waits
  until it has been attempted.
- Add generation, coalescing, attempt/failure, and callback-outcome telemetry. Test concurrent
  `session_id`/`start_new_session` mutations, a write completing after newer mutations,
  last-activity persistence, automatic rotation, flush-through-generation, shutdown, and callback
  dispatch. No EventBuffer, provider, log-admission, or startup-replay semantics change in this
  milestone.

## Milestone 2: EventBuffer state machine

- Implement EventBuffer as an unused logger-internal component with the full entry model required
  by this plan: captured logs (including protected `Block::Yes` logs), protected state/control
  entries, completion handles, and
  closed/shutdown state.
- Implement dual-limit admission, priority classification, FIFO delivery, bounded deque scans and
  physical eviction, insertion-ID tie breaking, protected-entry handling, fallible container
  growth, and terminal completion on rejection, eviction, or close.
- Implement the copy-on-write logger-field map with its independent aggregate byte/count limit,
  field validation, and snapshot telemetry.
- Implement `next_batch(max_entries)` with the lost-wakeup-safe `Notify` protocol and bounded
  batches. Test it independently from the async logger's `select!` loop.
- Add focused unit and concurrency tests for all capacity, priority, ordering, COW, completion,
  close, and notification invariants. This remains an unused component milestone; logger ingress
  behavior is unchanged.

## Milestone 3: logger ingress migration

- Construct EventBuffer with the logger and replace the ALB log/state channels and
  `OrderedReceiver` with its synchronous handle and `next_batch` branch in the existing async
  `select!` consumer.
- Move provider snapshot capture to `LoggerHandle`, move logger-managed fields into EventBuffer,
  and split metadata normalization from provider capture. Enable this only after platform-provider
  threading certification and its telemetry are in place.
- Move current log and feature-flag session resolution from the consumer to the logger edge using
  the Milestone-1 in-memory `bd_session` API. Capture the returned session ID before provider capture and
  EventBuffer admission. Implicit log/state rotations dispatch callbacks only after source
  admission/drop as well as their persistence attempt.
- Migrate every ALB state and internal-ingress path in the audit table, including feature-flag
  metadata/session capture, opaque-entity startup recovery, crash-report batches, interceptor
  placement, generated-log context, and flush/blocking completion semantics.
- Preserve the current `PreConfigBuffer` and its immediate replay on initial configuration.
  Consequently, Milestone 3 has up to the 11 MiB EventBuffer allowance plus the existing 1 MiB
  startup buffer allowance while configuration is unavailable. Overflow in that startup buffer
  retains the current FIFO behavior and metrics; priority-aware startup retention arrives in
  milestone 4.
- Ship integration telemetry for EventBuffer admission, eviction, provider latency, lock latency,
  consumer service time, session-persistence generation/coalescing/attempt time, and `select!`
  fairness. Use
  production measurements to validate the synchronous provider and locking cost before changing
  startup semantics.

## Milestone 4: soft startup replay gate

After Milestone 3 is stable, replace `PreConfigBuffer` with EventBuffer startup buffering and add
the soft drain gate below. This delivers delayed replay and crash-log reordering without coupling
those startup semantics to the ingress migration.

EventBuffer starts with its drain gate `Holding`. Once configuration has created the processing
pipeline, it reads the replay-delay runtime configuration and starts the base replay timer. The
gate opens only after that configuration-relative deadline has passed. A platform crash-pending
hint can extend the deadline while the gate is still holding, subject to the configured extension
limit. Holding continues to capture and prioritize events but does not deliver them.

Before configuration is ready, `CrashPending` is retained as a pending extension hint and a
high-watermark crossing is retained as an early-release request; neither can deliver work without
a pipeline. At configuration readiness, apply the pending hint to the runtime-configured deadline.
If the buffer is already at the high watermark, release immediately with reason
`high_watermark`; otherwise arm the configured timer.

Configuration construction, including restoration of already-persisted workflow actions, remains
outside the EventBuffer ordering domain and keeps its current startup behavior while the gate is
holding. `InitLifecycle::LogProcessingStarted` and the SDK "running" status move to the first
gate release, immediately before the first EventBuffer batch is delivered; creating the pipeline
alone is not reported as log processing.

Removing `PreConfigBuffer` at this point means startup events are retained in their original
EventBuffer representation, so the same priority/eviction policy applies before and after
configuration is ready.

### Previous-session replay ordering

`CapturedLog` carries two independent classifications: its retention priority and its source
(`CurrentProcess` or `PreviousProcess`). A previous-process log is eligible for special ordering
only when EventBuffer admits it while the startup gate is holding. The eligibility bit is captured
on the entry; it is not inferred later from the source alone.

When the gate releases, EventBuffer takes its deque under the lock, partitions the already-admitted
entries into reorderable previous-process logs and everything else, then restores the deque as:

```text
previous-process logs (their original FIFO order)
-> all other retained entries (their original FIFO order)
```

The same lock transition changes the gate to `Open`. Entries admitted afterwards always append to
the back, including a late previous-process crash log. The gate never reopens, so a crash report
that arrives later in the session keeps its higher retention priority but is not reordered ahead of
current-session work. This avoids retroactively changing workflow order after current-session
events have started flowing.

Only prior-process logs move during this partition. Current state/control entries, fields,
session-bearing entries, and flush barriers stay in their original FIFO relation. The one-time
partition moves entries without cloning payloads; it is an intentional startup-only lock hold and
is instrumented separately.

The gate is soft: admission of a protected event at or above an 80%-of-`total_limit` high
watermark opens it early with reason `high_watermark`. Low-priority traffic alone does not shorten
the startup window. If the consumer still cannot catch up, the normal hard-cap eviction policy
applies; priority-event loss is measured rather than exceeding capacity.

`CrashPending` may extend the deadline only while the gate is holding. A high-watermark release,
a current-process session change after the gate has observed its first current-process session ID,
a flush barrier, or normal timer release seals the ordering window; later hints and late
previous-process logs cannot reopen it.

The first current-process session-bearing entry establishes the gate's current-session baseline. A
later current-process entry whose captured session ID differs from that baseline is a replay
barrier: if admitted while holding, the buffer drains through that entry after partitioning
eligible previous-process logs first. This prevents retained previous-session logs from being
finalized under the newer session. `start_new_session` itself remains an in-memory session mutation; with no
following EventBuffer entry it has no replay-ordering effect.

An admitted `FlushState(Block::Yes)` or blocking log is also a gate barrier: it seals the gate,
partitions eligible previous-process logs first, then drains through its ordered position before
its completion resolves. It does not bypass older work. An admission-rejected or
provider-capture-rejected blocking operation resolves immediately as a terminal drop and cannot
act as a barrier. `FlushState(Block::No)` stays behind the gate, matching its existing
fire-and-forget behavior. Neither admitted blocking operation may remain pending solely because
the soft startup delay has not elapsed.

## Observability and validation

- Preserve the existing log enqueue success/full/closed metrics for continuity until the channel
  path is removed; add equivalent state metrics during the transition.
- Record EventBuffer admission, eviction, incoming drop, protected rejection, oversized rejection,
  queued bytes by total/log/protected category, high-watermark replay, scheduled replay, crash-hint replay,
  and time spent behind the drain gate. Break these down by event kind, log type, level, and
  completion outcome. Record aggregate logger-field bytes/count, COW snapshot bytes/version
  count, and field-limit rejections separately from event-buffer eviction. Record startup-window
  sealing reason, reordered previous-process count, late previous-process count, and partition
  duration separately from ordinary dequeue latency.
- Record provider duration/failure and EventBuffer lock wait/hold histograms before and after the
  inline-provider migration, including state-operation snapshots. Record consumer batch length,
  time between notification and dequeue, and service time for each external `select!` branch.
- In milestone 2, test dual-limit admission, priority eviction, FIFO retention, equal-priority
  oldest retention, protected-entry behavior, oversized input, COW field snapshots and limits,
  lifecycle close, completion on rejection/eviction/close, and Notify wake/drain races. Add
  repeated arbitrary eviction tests that verify descending-index removal, plus allocation-failure
  and callback-reentrancy coverage.
- In milestone 3, add old-log / session-start / new-log attribution, automatic session rotation,
  concurrent session-start/log admission, edge-time in-memory session resolution, explicit- and
  implicit-rotation callback dispatch/order, feature-flag state replay with captured metadata,
  opaque-entity pre-store coalescing/admission/recovery, memory-pressure persistence, flush
  ordering, both crash-report paths, provider reentrancy/threading, generated-log session
  inheritance, and bounded-batch fairness with a continuously non-empty EventBuffer.
- In milestone 4, add prior-run metadata behavior, gate timer and crash extension,
  high-watermark early replay, barrier release, previous-process FIFO partitioning, late
  previous-process no-reorder behavior, captured-session-change gate activation,
  blocking-flush gate activation, nonblocking-flush gate retention, and
  PreConfigBuffer-to-EventBuffer migration coverage.
- Add contention benchmarks covering concurrent logging, field updates, and deliberately slow
  providers as part of milestone 3. Verify the crate with `cargo nextest run -p bd-logger`.

## Decision record

- The plan deliberately avoids an extra ingress task and the residual blind-drop path of a bounded
  Tokio ingress channel.
- It deliberately accepts that provider execution can add synchronous caller latency, subject to
  measured provider and lock-tail latency.
- Session persistence is deliberately outside EventBuffer. `bd_session` applies a session mutation
  immediately under its own mutex and coalesces durable writes by generation; logs and state
  operations carry the resulting immutable session ID. EventBuffer admission never changes,
  persists, or rolls back session state.
- EventBuffer is one ordering domain for producer data and state, not for configuration. Existing
  configuration control-plane timing remains out of band and is made explicit through the startup
  gate.
- The initial limits are an 11 MiB total budget and a 1 MiB evictable-log budget, preserving the
  current separate log and state allowances without requiring separate ingress queues. Reducing
  either is a later product decision informed by telemetry.
- Milestones 1 through 3 preserve current `PreConfigBuffer` startup behavior; milestone 4 is the
  only milestone that changes initialization replay and workflow ordering.
- The current 1 MiB log limit, 10 MiB state limit, and 80% high watermark are initial defaults.
  Runtime configurability can be added after telemetry establishes safe bootstrap and live-resize
  semantics.
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
- The initial COW implementation accepts old map snapshots as bounded auxiliary memory. It will be
  replaced with structural sharing only if the specified telemetry shows that retained versions are
  material; it is not a prerequisite for the EventBuffer migration.

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

## Open questions for team review

| Topic | Assumption in this plan | Decision needed |
| --- | --- | --- |
| Provider execution contract | Providers run inline on arbitrary application threads and may run concurrently. Provider time, fields, and failure are observed at `LoggerHandle` admission rather than later on the async task. | Are platform providers thread-safe, thread-affinity-free, and fast enough for this to be an SDK contract? If not, retain the ingress-task design or define a provider execution boundary. |
| `setField` contract | Field changes take effect at synchronous EventBuffer admission. The aggregate logger-field map has a configured byte/count limit, and an over-limit mutation is rejected without changing current fields. | Is immediate same-thread `setField(); log()` behavior desired on every platform, and how should callers observe a rejected field mutation? |
| Session callback contract | `bd_session` records an implicit log/state rotation against its persistence generation. Callbacks use `SessionCallbackDispatcher` after the generation has been attempted; after Milestone 3 implicit callbacks also wait for source admission/drop. | Is moving callbacks away from the current originating/consumer thread acceptable, which platform dispatcher provides the required affinity, and is source-admission-before-callback ordering acceptable for activity-session integrations? |
| Startup reordering | Only previous-process logs admitted before gate sealing are reordered ahead of current-process entries. Late previous-process logs retain high eviction priority but are never reordered. | What base delay, maximum crash-hint extension, and high-watermark threshold provide the desired crash coverage without delaying normal startup too much? |
| Startup capacity transition | In Milestone 3, EventBuffer has an 11 MiB hard total while the retained 1 MiB `PreConfigBuffer` may also hold work. Milestone 4 currently removes that second stage and keeps an 11 MiB EventBuffer. | Should Milestone 4 raise `total_limit` to 12 MiB to preserve the current worst-case startup retention budget, or is the intentional 1 MiB reduction acceptable once the duplicate staging buffer is gone? |
| Priority policy | Previous-process, lifecycle, `Block::Yes`, and state/control entries are protected; other log types and levels follow the proposed eviction ranking. | Confirm the taxonomy with product/workflow owners, including whether any customer log classes need to be promoted or demoted. |
