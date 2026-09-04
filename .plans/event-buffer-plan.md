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
- Three fixed retention lanes decide what is discarded under memory pressure; admission IDs preserve
  global FIFO delivery across those lanes. Previous-process logs have a temporary startup lane so
  they can replay before current-process work without a drain-time queue partition.
- `bd_session` remains responsible for session state and best-effort persistence. EventBuffer
  captures immutable session IDs at admission but does not coordinate persistence with each log.
  A blocking `FlushState` waits for persistence work pending at its barrier.
- The buffer begins with separate bootstrap log and overall in-memory budgets. Both become
  runtime-configurable after configuration arrives; configuration changes apply on the next
  admission rather than walking the queue immediately.
- Startup replay remains unchanged until the final milestone. The final gate uses a construction-time
  enum: no delay for `NoPriorCrash`, 1 s for `MayHavePriorCrash`, and 50 ms for `Unknown`.
  The latter two total delays are independently runtime-configurable.

## Milestone roadmap

1. [x] **Shared platform mutex.** Introduce the caller-thread mutex adapter; use it for `bd_session`.
2. [x] **Session persistence.** Replace ALB-carried session writes with a coalescing `bd_session`
   flusher.
3. [x] **EventBuffer.** Build and test the bounded three-queue component without changing logger ingress.
4. [x] **Ingress migration.** Route logger/state inputs through EventBuffer while preserving
   `PreConfigBuffer` startup behavior.
5. [x] **Startup replay.** Replace `PreConfigBuffer` with the delayed replay gate and prior-process
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

**EventBuffer entries.** Logs, feature-flag exposure, post-startup memory-pressure and entity-ID
persistence, and `FlushState` are ordered workflow, state-store, or barrier inputs. `FlushState`
is protected. Crash reports are discovered and parsed on the existing direct request path, then
admitted as one EventBuffer batch so their result has one ordered boundary with concurrent
producers.

**ALB-owned local state.** `setField` and `removeField` become ordered `LoggerControl` entries,
which ALB applies to its one mutable field map before it processes later ingress events. They do not
capture field-map snapshots or produce replayable workflow input. Keeping this accumulation in ALB
means frequent updates retain one current map rather than creating queued copies. Session creation,
rotation, and persistence remain in `bd_session`: its coalescing flusher writes only the newest
state, and EventBuffer carries only immutable session IDs. A blocking flush is the sole operation
that waits for that flusher.

**Direct control paths.** Configuration stays on its existing path because applying it can build or
replace pipeline state and has no producer-admission order. Report-processing requests do not change
the construction-time replay classification or deadline. Shutdown, `Notify`, timers, lifecycle/status, the sleep-mode watch, and the tracing
flag are scheduling or local-state signals, not retained work.

**Consumer-owned downstream work.** Stats, upload, buffer flushes, and workflow side effects stay
on their existing downstream channels. They are consequences of an entry, never new logger ingress.

### Metadata and state handling

- `LoggerHandle::log` captures the provider timestamp and provider fields inline, outside the
  EventBuffer lock and while actually holding `with_thread_local_logger_guard`. The existing
  admission-only guard is not sufficient once provider code runs on the caller thread.
- It then locks EventBuffer and admits a `LoggerIngressEvent` containing the immutable provider
  and session snapshots. `setField` and `removeField` are separate FIFO `LoggerControl` entries;
  ALB applies every earlier field update before it normalizes a later ingress event.
- ALB owns one mutable `LoggerFieldMap` and applies field controls on its serialized consumer path.
  No ingress event retains a logger-field snapshot, so frequent field updates do not create queued
  map versions. These APIs are ordered local-state operations, not downstream workflow events.
- Refactor `MetadataCollector` into provider-snapshot capture and background normalization. The
  async consumer merges captured provider data, caller fields, and its current ALB-owned logger
  field map, then performs global-state tracking, state-store updates, and replay. It must not
  resolve a mutable current session for an already-admitted log.
- Prior-run logs keep their existing special path: do not capture current-process provider data;
  capture only the timestamp-provider result in the `logged_at` field of
  `EventContext::PreviousProcess` when they enter EventBuffer, then finalize them against prior
  global state on the background task.
  `PreviousRunSessionID` continues to take `occurred_at` from the crash report, while `_logged_at`
  is the pinned timestamp-provider value.
- State/control operations that affect workflows or persistence remain protected EventBuffer
  entries. Report processing and shutdown remain direct control signals, not buffer entries.
- `set_feature_flag_exposure` resolves its session ID through `bd_session`, then captures provider
  data plus an admission timestamp before EventBuffer admission; provider capture uses the same
  held thread-local guard as logs. The consumer combines those immutable inputs with the current
  ALB field map for state-store insertion, global-state tracking, and workflow replay rather than
  querying the provider or session strategy later. Any implicit-session callback is dispatched
  after this state operation is admitted or terminally dropped, using the same rule as logs.

Provider capture is intentionally outside the EventBuffer lock. Provider time/fields and session ID
are captured at the original API call; FIFO admission orders a log against field-control entries.
ALB applies every earlier control before normalizing that log, while a later control affects only
later ingress events. This is the linearization rule for concurrent callers, and is preferable to
holding the EventBuffer mutex across arbitrary platform provider code.

Provider calls now become part of normal `Logger.log` latency. Before the migration, add temporary
duration/failure instrumentation to the current async calls; retain it through the move and measure
edge lock wait/hold time. This is also a provider-threading migration: today providers are called
serially on the async task; after the move they may be called concurrently on arbitrary application
threads. This contract is approved: platform implementations must preserve their required
thread-affinity, synchronization, and reentrant-logging behavior under concurrent inline capture.

### Session persistence

`bd_session` owns mutable session state and persistence. It changes state under its existing mutex,
returns the current session ID immediately, and has one coalescing background flusher that persists
the newest state. EventBuffer neither owns persistence nor contains persistence entries.

At admission, current-process logs and feature flags capture that immutable session ID; later
processing never rereads mutable session state. Persistence is best effort: a log may be delivered
before a newly created session is durable. If a process dies in that window, the recovered crash
work can be attributed to the last durable session. That tradeoff avoids disk I/O on the logging
path.

`FlushState(Block::Yes)` is the opt-in durability hedge. After earlier EventBuffer work drains, it
waits for persistence work pending at its barrier, then continues with the existing flush path. A
write failure completes that wait as a measured best-effort failure, rather than blocking forever.
Normal logs, session APIs, callbacks, and shutdown never wait for session persistence.

### EventBuffer behavior

EventBuffer owns priority-aware retained-entry storage, byte accounting, and a `Notify` for its
consumer. ALB owns the mutable logger-field map. Retention priority chooses *what may be evicted*;
it never changes delivery order. Whatever representation is selected, these invariants are fixed:

- Every entry receives an increasing admission ID while the EventBuffer mutex is held. The consumer
  delivers the retained entry with the lowest applicable delivery key, so all retained entries are
  replayed in global admission order rather than in priority order.
- Retention lane controls admission and eviction. `PreviousProcess` logs are always Protected;
  process source then controls their startup-gate delivery route. An eligible previous-process log
  uses a startup-only delivery lane in the protected category; it is charged to the shared overall
  budget, is not another retention lane, and does not require rearranging retained entries at drain
  time.
- Pressure may evict only a strictly lower retention priority than the incoming entry. It always
  takes the lowest eligible priority first and the newest entry within that priority, preserving
  the oldest retained equal-priority entry. Protected entries are not eviction victims.
- Every entry is charged a conservative fixed bookkeeping overhead in addition to its payload.
  This gives even zero-payload control entries a nonzero cost and bounds live entry count plus
  metadata. Rejection, eviction, and shutdown drop entries while holding the EventBuffer mutex.
  Today's terminal completion is a Tokio oneshot sender: closing it wakes the receiver but does
  not directly poll that receiver's continuation. A resumed task may contend for the mutex, but it
  cannot reenter through the receiver continuation while the drop is in progress. Do not add
  direct callbacks or user-defined destruction to entries without first introducing an explicit
  post-unlock handoff.
- Admission fallibly reserves required incoming-lane container capacity before evicting retained
  entries. Allocation failure rejects the incoming entry and leaves retained entries untouched.
  Capacity remains available for amortized producer latency and is never synchronously shrunk on
  the hot path.

#### Fixed priority queue design

The priority taxonomy is intentionally fixed and small for the initial EventBuffer: three retention
lanes—one protected lane plus two evictable lanes. The protected lane has a normal lane and a
startup-only previous-process lane, so the implementation uses four `VecDeque`s during startup.
This is a delivery-layout detail, not a fourth retention lane. Automatic placement for normal logs
depends on level, with two type carveouts: all `LIFECYCLE` and `DEVICE` logs are Protected. Trace
and debug map to Low; info, warn, and error map to High. A future level higher than error maps to
High unless it is `LIFECYCLE` or `DEVICE`.

| Retention lane | Mapping | Rationale |
| --- | --- | --- |
| Protected | State/control entries, every previous-process log, and all `LIFECYCLE` and `DEVICE` logs | These entries carry ordering, barrier, startup-recovery, lifecycle, or device-state semantics. They are bounded only by the overall budget and never priority-evicted. |
| High | Any non-protected `INFO`, `WARN`, `ERROR`, or higher forward-compatible level | Retain ordinary operational and application logs. |
| Low (diagnostic) | Any non-protected `TRACE` or `DEBUG` log | This is the diagnostic lane. It contains only lower-severity diagnostic detail. |

No producer selects a queue directly; one EventBuffer mapping function owns this table. Apart from
the `LIFECYCLE` and `DEVICE` carveouts, log type has no effect. Within a lane, the oldest retained
equal-priority entries win over newer ones under pressure. An unrecognized future level at or above
`INFO` maps to High; one below `INFO` maps to Low. The two type carveouts always map to Protected.

Later changes may introduce additional lanes if we want to add more granularity here, which won't materially change the design.

An admitted entry therefore carries three separate values: `(retention_lane, source,
admission_id)`. Routing under the EventBuffer mutex is:

```text
PreviousProcess while the gate is Holding  -> startup_previous lane (protected; overall-budget charged)
everything else                            -> normal lane for retention_lane
```

While the startup gate is holding, no entry drains. Gate release only flips the gate state—there
is no stable partition or queue walk. `next_batch` drains `startup_previous` in FIFO order until
empty, then merges the fronts of the three normal retention lanes by admission ID. A previous-process
log admitted after the seal routes to the normal protected lane, so it never reorders live
current-process work. Because admission and gate sealing use the same mutex, a concurrent producer
is unambiguously either before or after the ordering window.

Store one normal `VecDeque<Entry>` per retention lane, plus the startup-only
`startup_previous` deque. An eligible previous-process entry appends to that lane while the gate
is holding; every other entry appends to its lane's normal queue. Insertion is O(1) amortized. On
pressure, admission scans the bounded evictable-lane array from lowest eligible priority and
removes entries from each normal queue's tail until enough bytes are released. This makes victim
selection and physical removal O(1) per evicted entry, with no middle removal, tombstones, or
compaction. In other words, equal-priority eviction pops from the back: older retained entries
win over newer ones.

To preserve EventBuffer's single ordering domain, `next_batch` first drains the non-empty
`startup_previous` lane, then does *not* drain one lane at a time: it examines the front entry of
every non-empty normal lane and removes the one with the lowest admission ID. Since each normal
per-lane deque is FIFO, a minimum over their fronts is the globally oldest normal retained entry.
With three fixed lanes this performs at most three front comparisons per normal delivered entry:
fixed O(1) work, rather than a queue-dependent scheduler or a need to track dropped sequence ranges. The
startup lane makes the one-time prior-process prefix O(1) to activate at gate release, without
partitioning or rebuilding a global queue.

Advantages: simple ownership and memory accounting; contiguous storage and good cache locality;
constant-time eviction; and a direct expression of the product's small, fixed taxonomy. Costs:
the number of lanes is part of the implementation contract; every dequeue compares a fixed set of
fronts; and future arbitrary numeric priorities would either grow that set or need a different
representation.

EventBuffer has two runtime-configurable **in-memory** byte budgets, neither of which preallocates
memory or includes the ring buffer, disk buffer, or upload pipeline. It starts with bootstrap
defaults because it can accept work before the first runtime configuration arrives:

- `total_limit` (the **overall budget**) is a hard limit over every retained EventBuffer entry.
  Its bootstrap default is 10 MiB, derived from today's state/control channel capacity.
- `log_limit` (the **log budget**) is a 1 MiB sub-limit over ordinary evictable (non-control) log
  entries. Its bootstrap default is today's 1 MiB log-channel capacity. Protected state/control
  entries and protected logs bypass this sub-limit but remain charged to the overall budget.

The log budget lives *within*, not in addition to, the overall budget. The corresponding runtime
settings replace both values as one pending generation when configuration becomes available. The
generation becomes effective atomically at the beginning of the next EventBuffer admission. Until
then, bootstrap budgets remain fully active; EventBuffer never has an unbounded pre-configuration
period merely because configuration is expected later.

The configuration-owning async consumer watches those runtime settings. On a change, it calls a
short `EventBuffer::set_pending_limits(log_limit, total_limit)` operation under the same
EventBuffer mutex used by admission. It records the pair without walking or evicting queued
entries. The next admission, while holding that mutex, applies the latest pending pair and any
required eviction *before* judging the incoming entry. Admission therefore sees one coherent
configuration generation and needs no independent atomics; separate atomics for the two values
could expose a torn configuration while capacity accounting is being updated.

If no entry is admitted after a configuration change, the current retained bytes are left alone and
may drain naturally under the preceding effective budget. On the next admission, a raised budget
becomes usable; a lowered budget evicts eligible ordinary entries where possible before that entry
is considered. EventBuffer never retroactively evicts protected entries: if retained protected
bytes alone exceed a newly lowered overall budget, record that explicit over-limit state and reject
new admissions until draining brings usage below the configured cap.

On ordinary-log admission, EventBuffer first makes room within `log_limit` by evicting lower
priority evictable logs. It drops the incoming log when it cannot displace a retained log; equal
priority retains the older entry. It then checks the overall budget, using the same eviction policy
if additional evictable log bytes must be released. A protected entry bypasses `log_limit` and may
evict any evictable log to fit the overall budget; it is rejected only if that budget is occupied
entirely by protected entries. A normal log larger than the log budget, or any entry larger than
the overall budget, is rejected and measured. There is no scheduler-dependent soft overflow.

Priority policy is the fixed lane mapping above. The protected lane bypasses `log_limit`; the two
evictable lanes share it. A protected admission may release bytes from any evictable lane, again
starting with the lowest lane and newest equal-priority entry.

#### Protected and control capacity semantics

Protected does **not** mean “cannot be dropped under any circumstance,” and control entries do
not have a separate reserved byte limit. Protected entries—including state/control operations,
`FlushState`, lifecycle logs, and eligible previous-process logs—share the one
overall budget with every other retained entry. They bypass the log budget and can displace
evictable logs, but they cannot exceed the overall budget.

An already admitted protected entry is never evicted to admit another entry. A new protected entry
is instead terminally rejected if it is oversized, EventBuffer cannot reserve its required memory,
the lifecycle is closed, or the retained buffer is full of protected entries. On shutdown, even
admitted protected entries are removed as explicit shutdown drops. Thus the contract is
**non-evictable after admission, not delivery-guaranteed**. Rejection and shutdown are measured;
any associated completion resolves exactly once.

#### Shutdown and terminal outcomes

Shutdown first changes EventBuffer's lifecycle to `Closed` under the lock, then wakes the consumer.
The consumer may finish an already detached batch, but it does not begin a new batch after its
shutdown branch wins. All still-retained entries are removed under the lock, recorded as shutdown
drops, and have any completion resolved after the lock is released. This preserves the current
best-effort shutdown behavior while preventing waiters from timing out solely because their sender
was stranded in the buffer.

`FlushState(Block::Yes)` preserves its current public meaning: the caller waits for a terminal
outcome, not a guarantee that every downstream operation succeeded. The completion payload remains
unit; protected-budget rejection, processing completion, and shutdown all resolve it exactly once.
The distinguishing information is emitted through outcome metrics and internal diagnostics, not a
new public API result.

### ALB migration audit

The existing async log buffer is more than a log receiver. Milestone 4 must replace the following
behaviors deliberately rather than moving only `LoggerHandle::log`.

#### Inputs and state

- **Normal and helper logs:** Route every path—including `RESOURCE`, `REPLAY`, `LIFECYCLE`, and
  `INTERNAL_SDK` helpers—through the common admission flow below. Helpers only construct their
  existing type-specific fields; they never bypass EventBuffer.
- **Field changes:** `AddLogField` and `RemoveLogField` are protected `LoggerControl` entries.
  ALB validates and applies them to its single mutable field map in FIFO order with ingress events.
  They do not create workflow entries or per-log snapshots.
- **Feature flags:** Keep them as protected ordered state operations. Capture their session and
  provider data at admission; ALB supplies the current logger fields in FIFO order. While
  `PreConfigBuffer` remains in Milestone 4, its pending feature-flag item carries the same immutable
  inputs so replay never rereads mutable provider or session state.
- **Memory pressure and opaque entities:** Preserve ordered durable writes. Before state-store
  readiness, coalesce opaque-entity updates in an EventBuffer-owned pending slot. Builder takes the
  newest value, persists it, updates the public watch only on success, and marks the store ready.
  Afterward, each update is protected and updates the watch only after admission. Memory pressure
  retains its existing prior-run initialization behavior.

#### Blocking operations

- **`FlushState`:** Flush controls are protected, bypass the log budget, and remain bounded by the
  overall budget; an all-protected full buffer explicitly rejects them. A blocking flush drains
  earlier admitted work and waits for persistence pending at its barrier before the existing stats,
  buffer, session, and workflow flushes. `FlushState(Block::No)` stays protected but does not
  change startup timing. Failures are measured best-effort outcomes. Every blocking flush
  completion resolves exactly once on processing, rejection, persistence failure, or shutdown.

#### Startup and consumer-owned work

- **Crash reports:** Keep report scanning outside EventBuffer. Admit parsed reports as one ordered
  batch, with per-report admission outcomes rather than all-or-nothing success. Current-run reports
  use captured provider, field, and session context. Previous-run reports remain protected, use
  persisted prior state and session when available, and pin their timestamp-provider `_logged_at`
  value at EventBuffer admission. Report processing does not extend the replay delay.
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
EventBuffer rather than from a helper-specific queue path.

1. For a current-process log, `bd_session` resolves an immutable session ID. Any required
  persistence remains background work. A resolution failure is a terminal log drop. A successful
  implicit rotation records a deferred callback for post-admission dispatch without waiting for
  persistence. The caller then holds
   `with_thread_local_logger_guard` and captures provider timestamp and fields outside the
   EventBuffer lock. A provider failure is also a terminal drop; both outcomes record their
   respective metrics before returning to the caller.
2. `PreviousRunSessionID` logs skip current-process session and provider-field capture. They retain
   their raw fields and override, and capture only the timestamp-provider output as `logged_at`,
   for the existing previous-global-state consumer path.
   Normal logs and `OccurredAt` logs proceed with their captured provider data; the latter retains
   its supplied occurrence timestamp.
3. EventBuffer acquires its lock and admits the log entry with its original `LogLine` message,
   fields, matching fields, override, `CaptureSession`, provider snapshot, immutable session ID,
   and optional completion handle. Earlier `LoggerControl` field updates precede it in the same
   FIFO stream.
4. Admission applies the log and overall budgets plus the priority eviction policy. Rejection or
   eviction terminates any associated completion after releasing the lock. Successful admission
   schedules `Notify`; it does not wait for the background consumer. In either terminal source
   outcome, dispatch any deferred implicit-session callback only after the lock is released, so
   callback-originated logging follows this source operation.
5. The consumer removes a log in FIFO order, runs the existing interceptors, and normalizes the
   original fields using the captured provider data and ALB's current logger field map. It uses the
   captured session ID. For `OccurredAt`, it emits the supplied timestamp and attaches captured
   provider time as `_logged_at`; the previous-run branch uses its pinned `logged_at` value with
   prior global state. It then follows the existing replay, buffer-writing, `CaptureSession`, and
   flush path.

`LoggerIngressEvent` sizing includes provider snapshots, the immutable session ID, and completion
state. It excludes logger-managed fields, which ALB retains once in its current field map. Field-map
limits and rejected mutations remain ALB concerns; they do not affect EventBuffer admission or
retention accounting.

The consumer exposes `EventBuffer::next_batch(max_entries)` as one branch of the existing async
`select!`. That future first registers `Notify::notified()`, then checks and takes a bounded FIFO
batch under the lock; registering before the check prevents a missed wakeup. It releases the lock
before interceptors, normalization, persistence, and replay. If entries remain after a batch, the
next call is immediately ready; control returns to `select!` between batches so configuration,
crash-report processing, the pipeline, timers, resource utilization, replay recording, events,
and shutdown retain fair progress. No code awaits, runs callbacks, parses reports, updates config,
or flushes while holding the lock.

## Milestone implementation details

### Milestone 1: shared platform-facing mutex — complete

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

### Milestone 2: generation-based session persistence — complete

- Refactor `bd_session` so a mutation increments an in-memory persistence generation and no
  longer hands a cloned state snapshot to `PersistPreparedSession`. Remove that ALB state variant.
- Add one coalescing persistence flusher: snapshot the latest state/generation, persist, and loop
  if a newer generation appeared. It has a single in-flight write, bounded state, and a wakeup—not
  a FIFO queue of prepared operations or per-operation responses.
- Keep session APIs, callbacks, and automatic rotation non-blocking; preserve their existing
  callback contract. A blocking flush waits for pending persistence as defined above.
- Add a durable session-write failure metric and focused coalescing, callback, blocking-flush, and
  shutdown coverage. No EventBuffer, provider, log-admission, or startup-replay semantics change
  in this milestone.

### Milestone 3: EventBuffer state machine — complete

- Implement EventBuffer as an unused logger-internal component with the full entry model required
  by this plan: `LoggerIngressEvent`, protected `LoggerControl` entries, completion handles, and
  closed/shutdown state.
- Implement the three fixed priority lanes, dual-budget admission, global FIFO delivery by admission
  ID, tail eviction from lower lanes, protected-entry handling, fallible container growth, and
  terminal completion on rejection, eviction, or close.
- Implement `next_batch(max_entries)` with the lost-wakeup-safe `Notify` protocol and bounded
  batches. Test it independently from the async logger's `select!` loop.
- Add focused unit and concurrency tests for all capacity, priority, ordering, completion, close,
  and notification invariants. This remains an unused component milestone; logger ingress behavior
  is unchanged.

### Milestone 4: logger ingress migration

- Construct EventBuffer with the logger and replace the ALB log/state channels and
  `OrderedReceiver` with its synchronous handle and `next_batch` branch in the existing async
  `select!` consumer.
- Move provider snapshot capture to `LoggerHandle`, retain logger-managed field accumulation in ALB,
  and split metadata normalization from provider capture. Field mutations become ordered ALB
  controls and logs use the field map ALB has applied at their FIFO position. Provider execution on
  concurrent caller threads is an approved contract; retain temporary migration instrumentation to
  detect regressions before broad rollout.
- Move current log and feature-flag session resolution from the consumer to the logger edge using
  the Milestone-2 in-memory `bd_session` API. Capture the returned session ID before provider
  capture and EventBuffer admission. Invoke implicit log/state callbacks on the admitting caller
  thread only after the source reaches a
  terminal admission/drop outcome; do not wait for persistence.
- Migrate every ALB state and internal-ingress path in the audit above, including feature-flag
  metadata/session capture, opaque-entity startup recovery, crash-report batches, interceptor
  placement, generated-log context, and flush completion semantics.
- Preserve the current `PreConfigBuffer` and its immediate replay on initial configuration.
  Consequently, Milestone 4 retains the bootstrap EventBuffer budgets plus the existing startup
  buffer allowance while configuration is unavailable. Bootstrap EventBuffer budgets remain active
  during that period even though the initial configuration will later make a
  runtime pair pending for the next admission. Overflow in `PreConfigBuffer` retains the current
  FIFO behavior and metrics; priority-aware startup retention arrives in milestone 5.
- Ship the small production metric set below. During development, enable the temporary
  instrumentation below to compare provider, lock, consumer, and callback behavior before
  selecting the final implementation, while preserving the agreed timing and ordering semantics.

### Milestone 5: soft startup replay gate

After Milestone 4 is stable, replace `PreConfigBuffer` with EventBuffer startup buffering and add
the soft drain gate below. This delivers delayed replay and crash-log reordering without coupling
those startup semantics to the ingress migration.

EventBuffer starts with its drain gate `Holding`. The platform supplies an immutable
`StartupReplayEligibility` when constructing the logger: `NoPriorCrash` skips the replay timer,
`MayHavePriorCrash` selects a 1 s total delay, and `Unknown` selects a 50 ms total delay. Existing
callers default to `Unknown`. Both nonzero categories have independent runtime flags; their delays
are alternatives, never additive. The timer starts when ALB starts, before configuration readiness.
Holding continues to capture and prioritize events but does not deliver them.

The pipeline must be ready before any release, even when there is no replay delay. While the gate
is holding, runtime updates can extend the selected deadline relative to ALB startup, including
after the original timer elapsed while waiting for configuration. They cannot shorten the deadline,
and updates to the other category have no effect. ALB rereads runtime values before timer-driven
release so a concurrent increase cannot release ingress at the old deadline. Report-processing
requests never alter the classification or extend the deadline.

Before configuration is ready, a high-watermark crossing is retained as an early-release request;
it cannot deliver work without a pipeline. At configuration readiness, refresh the selected delay,
record the current `(log_limit, total_limit)` pair through `EventBuffer::set_pending_limits`, and
retain the same runtime watch for later budget changes. The pair becomes effective on the next
EventBuffer admission. If the buffer is already at the high watermark calculated from the runtime
overall budget, release immediately with reason `high_watermark`; otherwise wait for any remaining
selected delay.
`FlushState` during this hard pre-configuration gate completes immediately as a no-op and is not
queued: with no pipeline, there is no retained downstream work for it to flush. EventBuffer
linearizes that decision against ALB marking the pipeline ready, so later flushes use normal
ordered-barrier behavior.

Configuration construction, including restoration of already-persisted workflow actions, remains
outside the EventBuffer ordering domain and keeps its current startup behavior while the gate is
holding. `InitLifecycle::LogProcessingStarted` and the SDK "running" status move to the first
gate release, immediately before the first EventBuffer batch is delivered; creating the pipeline
alone is not reported as log processing.

Removing `PreConfigBuffer` at this point means startup events are retained in their original
EventBuffer representation, so the same priority/eviction policy applies before and after
configuration is ready.

#### Previous-process replay ordering

`LoggerIngressEvent` carries two independent classifications: its retention priority and its source
(`CurrentProcess` or `PreviousProcess`). A previous-process log is eligible for special ordering
only when EventBuffer admits it while the startup gate is holding. The eligibility bit is captured
on the entry; it is not inferred later from the source alone.

An eligible previous-process entry enters the protected `startup_previous` FIFO lane at admission;
every other entry enters its normal retention lane. Gate release only changes the gate to `Open`—it
does not partition, move, clone, or scan queued entries. The consumer drains the startup lane first
in its original FIFO order, then resumes normal admission-ID merge delivery across the three normal
lanes.

Entries admitted after release always use normal retention lanes, including a late previous-process
crash log, which uses the normal protected lane. The gate never reopens, so that log keeps its
protected retention but is not reordered ahead of current-process work. This avoids both a
drain-time O(n) partition and retroactively changing workflow order after current-process events
have started flowing.

The gate is soft: a protected event that brings the buffer to at least 80% of the overall budget
opens it early with reason `high_watermark`. Low-priority traffic alone does not shorten
the startup window. If the consumer still cannot catch up, the normal hard-cap eviction policy
applies; priority-event loss is measured rather than exceeding capacity.

A high-watermark release, a flush barrier, or normal timer release seals the ordering window;
later runtime updates and late previous-process logs cannot reopen it. Arbitrarily stale crash
reports have no reliable prior-session association; their presence cannot override the platform's
construction-time classification.

An admitted `FlushState(Block::Yes)` after the hard pre-configuration gate is also a gate barrier:
it seals the gate, drains the
already-admitted startup-previous lane first, then drains through its ordered position before its
completion resolves. It does not bypass older work. An admission-rejected blocking flush resolves
immediately as a terminal drop and cannot act as a barrier. `FlushState(Block::No)` stays behind
the gate, matching its existing fire-and-forget behavior. An admitted blocking flush may not remain
pending solely because the soft startup delay has not elapsed.

## Observability and validation

Production metrics must stay off the successful producer fast path. The rollout set is deliberately
small:

- Count EventBuffer loss only: evictions, full rejections, and oversized rejections. Use a single
  bounded `reason` dimension; do not count successful admissions, split by lane, or label by event
  kind, log type, level, or completion outcome. Closed-buffer rejections are ordinary shutdown and
  are not a rollout health signal.
- Once per ALB start, count the selected replay eligibility. Once per gate release, count the
  release reason (`no_prior_crash`, timer, high watermark, or barrier) and record gate-hold
  duration. This exposes classification rollout, early releases, and unexpectedly long successful
  gates without a per-entry cost.
- Keep durable-write failures with the persistence owner rather than as an EventBuffer metric.

We intentionally do not export queue depth, queued bytes, oldest-entry age, lock timing, or
successful-admission counters. Those are high-frequency gauges or require additional state and can
be enabled temporarily during a targeted investigation rather than becoming permanent client
telemetry.

Development instrumentation is temporary: it must be feature-gated or sampled, have a stated
removal point before broad rollout, and not become a permanent metric merely because it aided
implementation. It includes per-lane depth/bytes/evictions; EventBuffer lock wait/hold time;
provider and callback duration/failure; consumer batch length, notification-to-dequeue time, and
`select!` branch service time; ALB field-map bytes and field-limit rejection; pending/effective
budget generations and shrink results; and startup-lane/reorder counts. These measurements support
benchmarks and migration validation, not steady-state dashboards.
- In milestone 3, test the priority mapping exhaustively across every public `LogType` and level,
  including unknown forward-compatible values; bootstrap dual-budget admission before configuration;
  pending runtime-budget updates that raise and lower each cap; application of the pair before the
  next admission; protected-over-budget behavior after a shrink; priority eviction; global FIFO
  delivery across lanes; newest-first eviction within an equal-priority lane; protected-entry
  behavior; oversized input; lifecycle close; completion on rejection/eviction/close; and Notify
  wake/drain races. Exercise every lane boundary and allocation failure plus callback-reentrancy
  coverage.
- In milestone 4, add admission-time identity attribution, callback dispatch/order, best-effort
  persistence failure progress, blocking-flush persistence success and failure, feature-flag state
  replay with captured metadata,
  opaque-entity pre-store coalescing/admission/recovery, memory-pressure persistence, flush
  ordering, both crash-report paths, provider reentrancy/threading, generated-log session
  inheritance, and bounded-batch fairness with a continuously non-empty EventBuffer.
- In milestone 5, add prior-run metadata behavior, classification-selected timers, independent runtime
  updates, report requests that leave the selected delay unchanged,
  high-watermark early replay, barrier release, previous-process startup-lane FIFO delivery, late
  previous-process no-reorder behavior, blocking-flush gate activation, nonblocking-flush gate
  retention, and
  PreConfigBuffer-to-EventBuffer migration coverage.
- Add contention benchmarks covering concurrent logging, field updates, deliberately slow
  providers, fixed-lane dequeue selection, tail eviction, and startup-lane drain as part of
  milestone 4. Verify the crate with `cargo nextest run -p bd-logger`.

## Decision record

- The plan deliberately avoids an extra ingress task and the residual blind-drop path of a bounded
  Tokio ingress channel.
- It deliberately accepts that provider execution can add synchronous caller latency, subject to
  measured provider and lock-tail latency.
- EventBuffer is one ordering domain for producer data and state, not for configuration. Existing
  configuration control-plane timing remains out of band and is made explicit through the startup
  gate.
- The bootstrap log and overall budgets derive from today's separate non-control and state/control
  channel capacities. The log budget is a sublimit within the overall budget, not an additional
  allocation or a reservation per priority lane. Both budgets are runtime-configurable after
  configuration arrives.
- Milestones 1 through 4 preserve current `PreConfigBuffer` startup behavior; milestone 5 is the
  only milestone that changes initialization replay and workflow ordering.
- Milestone 5 introduces runtime settings for the 50 ms uncertain-run replay delay, 1 s known-fatal
  replay delay, `log_limit`, and `total_limit`. A positive no-prior-crash classification skips the
  replay delay. The bootstrap budgets and 80% high
  watermark remain active before the first configuration arrives; later runtime updates become
  effective on the next EventBuffer admission using the budget-shrink behavior specified above.
- Provider-capture failures preserve today's best-effort public behavior: the affected log is
  dropped, diagnostics remain metrics/logging rather than a new caller-visible error, and reentrant
  logging during provider capture remains rejected by the thread-local guard.
- Current-process crash reports use admission-time provider and session snapshots; ALB supplies the
  current logger fields in FIFO order. Prior-process reports use prior global state and the
  prior-process session ID when available. This is the attribution rule rather than an unresolved
  choice.
- An all-protected full EventBuffer rejects the incoming protected entry. It never exceeds the
  overall budget, parks a pending admission, or relies on Tokio scheduling for room; rejection is
  explicit and measured.
- ALB owns one mutable `LoggerFieldMap`; ordered `LoggerControl` entries apply field changes before
  later ingress events are normalized. This avoids retaining field-map snapshots in queued entries
  and keeps field-map limits separate from EventBuffer retention.
- Use the shared `PlatformMutex` for platform-facing caller-thread locks: `bd_session` adopts it in
  Milestone 1 and EventBuffer reuses it in Milestone 3. Its target bindings and lock contract are
  defined in Milestone 1.

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

- **Startup reordering.** Only previous-process logs admitted before gate sealing are reordered
  ahead of current-process entries. Late previous-process logs remain protected but are never
  reordered. Validate the strawman delay values and select the high-watermark threshold.
- **Priority representation.** Three fixed `VecDeque` lanes use admission-ID merge delivery.
  Confirm the three-lane mapping with product/workflow owners and benchmark it before Milestone 3.

## Possible user-exposed priority levels

This is a possible future API, not part of the initial EventBuffer migration. If applications need
to influence retention, expose a coarse `RetentionHint` rather than the three internal lanes or an
arbitrary numeric priority:

| Public hint | Effective retention lane | Limits of the hint |
| --- | --- | --- |
| `Background` | Low | It may lower an ordinary `INFO` log, but it cannot demote an automatic `WARN` or `ERROR`. |
| `Default` | The normal mapping | This preserves the behavior specified in the three-lane table. |
| `Elevated` | High | It may promote an ordinary log above Low, but it cannot outrank Protected. |

The effective lane is calculated at admission from the automatic type/level mapping plus this
bounded hint. The hint changes only local EventBuffer eviction eligibility: it never changes log
level, delivery order, workflow order, upload order, or downstream persistence. `Protected` is not
publicly selectable, `Elevated` entries still count against both applicable byte limits and remain
evictable, and an application that marks all logs elevated simply makes those logs compete with one
another.

Three internal lanes are sufficient for this coarse public API because every hint maps to an existing
lane. If product instead requires an arbitrary numeric value with meaningful distinctions between
values, fixed lanes would only provide undocumented bucketing. That would be a separate future
design decision, not an extension of this EventBuffer plan.
