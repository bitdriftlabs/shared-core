# Linked Android crash-profile contracts

[`CrashProfileMetadata`](../../api/src/bitdrift_public/protobuf/client/v1/api.proto) contains only
the artifact IDs of the associated crash reports. It is carried on the existing artifact intent,
upload and [durable index](../../api/src/bitdrift_public/protobuf/client/v1/artifact.proto).

## Identity and matching

- A report ID is its artifact ID, also used by `_crash_artifact_id`. It is not an issue grouping key
  or source filename. The SDK records the ID returned by the existing enqueue path in an in-RAM
  candidate set, seeded at startup from still-pending index entries. Queue retries keep that ID. No
  report FlatBuffer schema changes are required.
- A profile ID is its outer artifact ID. Every submitted `report_ids` entry selects an explicit
  parent; the backend never searches for a different parent.
- PID/time matching stays on the device. Eligible Android reports carry an encoded
  `CrashProfileSource`
  ([artifact.proto](../../api/src/bitdrift_public/protobuf/client/v1/artifact.proto): run UUID, PID,
  boot count, process start and a paired startup clock sample) in the report's typed
  `crash_profile_source` FlatBuffer field; kind comes from the report type. After a restart the SDK
  re-reads still-queued report payloads. Nothing else is transmitted, and the backend ignores the
  field.
- Matching uses source-process context, not a window around the report event. A recovered ANR's
  `ApplicationExitInfo` timestamp supplies a death bound, with paired-clock skew and millisecond
  rounding tolerance, so later reuse of its numeric PID cannot attach an unrelated profile when
  optional trace process-start metadata is absent. A live Java OOM timestamp is not a known process
  death and does not impose that bound. Distinct overlapping runs compatible with the trace's
  process-start tolerance are rejected as ambiguous; multiple reports from one run remain eligible.
  Known limitation: OOM traces may omit process-start
  metadata, so unbounded OOM candidates cannot exclude later PID reuse unless another candidate
  exposes it. Recovered OOMs need trustworthy process-exit evidence; using exception time as death
  would reject valid heap capture that continues afterward.
- The selective wire reader streams zlib `compressed_packets` into the parser without retaining
  either the encoded field or expanded heap/event bodies. It verifies the complete zlib stream,
  checksum, declared protobuf message lengths and absence of trailing bytes; nested compressed
  packets remain unsupported. Repeated singular defaults and ftrace/compact-scheduler messages
  merge within a packet before clocks and timestamp deltas are applied. Unrelated
  unusable incremental packets are skipped, but loss still invalidates sequence defaults and any
  selected clock, heap or process evidence requiring that state fails closed. Conflicting
  process/thread ownership is rejected regardless of metadata order, including process IDs known
  only through thread ownership records.
- Reuse existing artifact fields for capture time, MIME type, encoding and captured metadata,
  including the receiving process's build at first adoption. Keep IDs, bytes and lookup build stable
  across retries. Scope and authorization checks remain server responsibilities.

## Persistence and delivery

`bd-artifact-upload::Client::enqueue_crash_profile` queues a profile exactly like a command
attachment (ZLIB storage, `attachment.max_bytes`, ownership of an absolute source path) with the
receiving build's static metadata and the `crash_profile` report IDs, and returns its artifact ID.
Attribution resolves relative source paths once before queueing, so matching, persistence and source
cleanup refer to the same file. That metadata is retained in the index and forwarded unchanged to
intent and upload. Existing persistence notifications, retries and ACKs are unchanged.
`ReportAttribution` is the platform hook: the logger seeds it from the pending index before the
uploader starts, and the crash monitor adds each enqueued report that carries source facts.
Readiness and successful report/profile ACKs do not remove candidates: several later profiles may
reuse one canonical report ID until process exit. Persistence rejection retires the owned profile
source through the ordinary attachment path without removing its report candidate. Shutdown can
cancel the attribution worker even while it awaits a persistence acknowledgement; an upload already
accepted by the uploader keeps its ordinary ownership and delivery semantics.

Profiles use ordinary artifact intent decisions and upload ACKs. A successful ACK covers the whole
submitted artifact and its report links after raw ownership and durable work scheduling; processing
remains asynchronous. Failed submissions retry through the normal upload path.

Additional links identify retained server bytes by the same scoped profile artifact ID. Missing or
expired content fails normally rather than resurrecting it. There are no content receipts, embedded
hashes, custom ACKs or feature-protocol versions. Local storage keeps its existing
compression/checksum handling.

## Diagnosing trace clock failures

The reader orders paired REALTIME/BOOTTIME snapshots by BOOTTIME and selects the mapping preceding
the first typed process observation (or the earliest snapshot if observations precede all snapshots).
It validates that mapping and every subsequent mapping through the last observation: their
`REALTIME - BOOTTIME` offsets must agree within one second. Historical corrections before that
window and corrections after it do not invalidate the profile. A correction inside the window
still fails closed; this reader does not implement piecewise attribution across clock jumps.
The latest applicable mapping supplies both report alignment and captured wall time.

An `unstable trace clocks` error happens before report matching and includes total/applicable
snapshot counts, the activity window, selected mapping, minimum/maximum offsets, spread, tolerance,
and both extrema's raw clock readings and one-based snapshot indexes. Debug logging emits the same
bounded summary for stable traces. The reader retains scalar clock pairs, not snapshot payloads;
it never logs the full history or profile contents.

In debug builds (`cfg(debug_assertions)`), with `RUST_LOG=bd=debug` (or logging enabled for
`bd_artifact_upload::crash_profile_attribution`), attribution errors retain the raw profile beside
the rejected source as `bitdrift-attribution-failed-anr.perfetto-trace` or
`bitdrift-attribution-failed-oom.perfetto-trace`. Each new failure atomically replaces the previous
file of that kind. Retention code is compiled out of release builds. Without debug logging the
rejected source is deleted as before. These names
are not pending-profile names: the SDK does not resubmit or upload them. On Android they remain
inside the private `files/bitdrift_capture/crash_profiles/` directory, count toward its existing
storage budget, and are subject to its age cleanup. Remove diagnostic files after collecting them.

Copy the failing trace locally and independently decode its clock snapshots with Perfetto tooling.
Compare the raw clock pairs and their offsets with the error before changing the tolerance or
matching behavior. Real clock adjustments can violate this reader's single-offset assumption even
when Perfetto can process the trace. Keep traces private: they can contain sensitive app data.

Artifact uploader lifecycle logs include `artifact_id` and `type` so report, profile, and state
snapshot uploads can be distinguished. File-upload logs also show payload size/encoding and linked
report count; intent decisions, discards and retries identify the same artifact and type.

## Milestones and generation

M1 supplies the additive contracts and metadata relay. M2 adds server admission; M3 adds the
reader/matcher and M4 the Android integration, gated by explicit SDK-start
`Configuration.crashProfilingMode` opt-in (default `null`/disabled), fatal issue reporting and
`client_feature.android.crash_profiles.{anr,oom}` (enabled by default within the local opt-in). The
app grants the SDK exclusive ownership of enabled trigger types and the ANR process-state summary;
on-demand profiling is unaffected. Use existing SDK/config compatibility checks and feature gates
for rollout and rollback.

Regenerate shared-core bindings with `make -C shared-core protos`; the monorepo generation command
is `just protos`. The report schema gained the additive `crash_profile_source:[ubyte]` field;
regenerate Capture SDK Kotlin bindings with its pinned flatc version. Tests cover legacy indexes,
metadata enqueue/persistence/restart, queue-size accounting and profile metadata relay using
ordinary intent decisions and ACKs. Host lifecycle tests connect the real crash monitor, Android
attribution adapter and uploader through a test-only Bazel feature variant, including post-ACK
candidate reuse, late current-process report dispatch after empty startup recovery, attachment-limit
and filesystem persistence rejection, and deterministic shutdown.
