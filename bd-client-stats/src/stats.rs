// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./stats_test.rs"]
mod stats_test;

use crate::file_manager::{FileManager, PendingUpload, StatsPipelineAnalyticsReport};
#[cfg(feature = "logger-cli-observer")]
use crate::observer::{
  ObservedMetric,
  ObservedMetricValue,
  SnapshotObservation,
  UploadAckObservation,
  UploadAttemptObservation,
  with_observer,
};
use crate::{FlushEpoch, FlushTrigger, Stats};
use analytics::StatsPipelineAnalyticsReport as HandshakeStatsPipelineAnalyticsReport;
use async_trait::async_trait;
use bd_api::DataUpload;
use bd_api::api::StatsHandshakeExtension;
use bd_api::upload::{TrackedStatsUploadRequest, UploadResponse};
use bd_client_common::maybe_await;
use bd_client_stats_store::{Collector, Histogram, MetricData, MetricsByNameCore};
use bd_error_reporter::reporter::handle_unexpected;
use bd_proto::protos::client::api::handshake_request::analytics;
use bd_proto::protos::client::api::stats_upload_request::snapshot::Snapshot_type;
use bd_proto::protos::client::api::stats_upload_request::{
  Snapshot as StatsSnapshot,
  UploadReason,
};
use bd_proto::protos::client::api::{
  HandshakeRequest,
  StatsUploadRequest,
  debug_data_request,
  handshake_response,
};
#[cfg(feature = "logger-cli-observer")]
use bd_proto::protos::client::metric::metric::Data as ProtoMetricData;
use bd_proto::protos::client::metric::metric::Metric_name_type;
use bd_proto::protos::client::metric::{Metric as ProtoMetric, MetricsList};
use bd_shutdown::ComponentShutdown;
use bd_stats_common::{Counter, MetricType, NameType};
use bd_time::{OffsetDateTimeExt, TimeDurationExt, TimeProvider};
use bd_workflow_stats::workflow::{WorkflowDebugStateKey, WorkflowDebugTransitionType};
use debug_data_request::workflow_transition_debug_data::Transition_type;
use debug_data_request::{WorkflowDebugData, WorkflowTransitionDebugData};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use itertools::Itertools;
use sha2::{Digest, Sha256};
#[cfg(test)]
use stats_test::{TestHooks, TestHooksReceiver};
use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Sleep;

type UploadFuture =
  Pin<Box<dyn std::future::Future<Output = (Option<UploadResponse>, UploadContext)> + Send + Sync>>;

const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PeriodicAction {
  Flush,
  Upload,
}

// Completion ownership after a request has been accepted by the data-upload channel. The context
// carries the per-request gate that must be released once its response receiver resolves.
enum UploadContext {
  Periodic(PendingUploadMetadata),
  Flush(PendingUploadMetadata),
  Startup(PendingUploadMetadata),
}

// Completion ownership before a request is accepted by the data-upload channel. In this state the
// source files are already claimed, but no StateTracker owns the response receiver yet.
enum PendingUploadContext {
  Periodic,
  Flush,
}

// Identifies the files covered by a request. FileManager uses these IDs to retain the claim until
// a transport response either completes the upload or abandons it for a later retry.
struct PendingUploadMetadata {
  source_file_ids: Vec<String>,
}

// A fully constructed request that has not necessarily entered the shared upload channel. Keeping
// the request and response receiver together prevents the claimed files from becoming detached
// from their eventual completion path while the channel is backpressured.
struct PreparedUpload {
  data_upload: DataUpload,
  response_rx: oneshot::Receiver<UploadResponse>,
  metadata: PendingUploadMetadata,
}

// The single upload that Flusher may retain locally while the shared upload channel is full. One
// retained request bounds memory and preserves the existing one-upload-at-a-time policy.
struct PendingDataUpload {
  prepared_upload: PreparedUpload,
  context: PendingUploadContext,
}

// Result of attempting the synchronous channel handoff. Deferred work remains in
// `pending_data_upload` and is dispatched by the main select loop once capacity becomes available.
enum UploadDispatch {
  Dispatched,
  Deferred,
  Closed,
}

#[derive(Clone, Copy)]
enum DiskFlushOutcome {
  Deferred,
  Durable,
  Failed,
}

// State for the leading/trailing disk-flush debounce window. A caller that arrives during an open
// window records intent here; the deadline then makes one trailing disk write and resumes the
// deferred request and periodic-upload work against that durable state.
struct DiskFlushDebounce {
  deadline: Option<Pin<Box<Sleep>>>,
  flush_pending: bool,
  periodic_upload_pending: bool,
}

//
// HandshakeStats
//

/// Stats handshake behavior that shares the flusher's persisted upload state.
pub struct HandshakeStats {
  file_manager: Arc<FileManager>,
  time_provider: Arc<dyn TimeProvider>,
  // The API task uses this to transfer API-originated upload response receivers to Flusher. A
  // receiver cannot be created at initialization: it is paired with the sender registered in a
  // particular stream's StateTracker and carries that request's claimed source files.
  api_upload_completion_tx: mpsc::Sender<(oneshot::Receiver<UploadResponse>, Vec<String>)>,
}

impl HandshakeStats {
  pub fn new(
    file_manager: Arc<FileManager>,
    time_provider: Arc<dyn TimeProvider>,
    api_upload_completion_tx: mpsc::Sender<(oneshot::Receiver<UploadResponse>, Vec<String>)>,
  ) -> Self {
    Self {
      file_manager,
      time_provider,
      api_upload_completion_tx,
    }
  }

  fn handshake_stats_pipeline_analytics(
    report: StatsPipelineAnalyticsReport,
  ) -> HandshakeStatsPipelineAnalyticsReport {
    let analytics = report.analytics;
    HandshakeStatsPipelineAnalyticsReport {
      report_id: report.report_id,
      analytics: Some(analytics).into(),
      ..Default::default()
    }
  }

  async fn register_api_upload_completion(
    &self,
    source_file_ids: Vec<String>,
    response_rx: oneshot::Receiver<UploadResponse>,
  ) -> anyhow::Result<()> {
    if let Err(error) = self
      .api_upload_completion_tx
      .send((response_rx, source_file_ids))
      .await
    {
      // Flusher is the sole owner of completion processing. If it has stopped, no task can
      // observe the receiver closing when the stream StateTracker drops, so release this claim
      // immediately instead of leaving the source files in flight.
      self.file_manager.release_pending_upload(&error.0.1).await?;
      anyhow::bail!("stats flusher shut down before API upload completion was registered");
    }

    Ok(())
  }
}

#[async_trait]
impl StatsHandshakeExtension for HandshakeStats {
  async fn prepare_stats_handshake(
    &self,
    handshake: &mut HandshakeRequest,
  ) -> anyhow::Result<Option<TrackedStatsUploadRequest>> {
    if let Some(report) = self
      .file_manager
      .prepare_stats_pipeline_analytics_report()
      .await?
      && let Some(analytics) = handshake.analytics.as_mut()
    {
      analytics.stats_pipeline = Some(Self::handshake_stats_pipeline_analytics(report)).into();
    }

    // Claim the current persisted batch before constructing the request. The claim is later
    // transferred to Flusher through `register_api_upload_completion`, which owns ACK handling
    // for both API and locally initiated uploads.
    let Some(PendingUpload {
      mut request,
      source_file_ids,
    }) = self.file_manager.get_or_create_pending_upload(true).await?
    else {
      return Ok(None);
    };

    let upload_uuid = batch_transport_uuid(&source_file_ids);
    request.upload_uuid.clone_from(&upload_uuid);
    request.sent_at = self.time_provider.now().into_proto();
    request.upload_reason = UploadReason::UPLOAD_REASON_HANDSHAKE.into();
    observe_upload_attempt(&request, UploadReason::UPLOAD_REASON_HANDSHAKE);
    let (startup_stats_upload, response_rx) = TrackedStatsUploadRequest::new(upload_uuid, request);
    self
      .register_api_upload_completion(source_file_ids.clone(), response_rx)
      .await?;
    if let Err(error) = self
      .file_manager
      .record_pending_upload_attempt(&source_file_ids)
      .await
    {
      log::debug!("failed to persist handshake stats upload attempt: {error}");
    }

    Ok(Some(startup_stats_upload))
  }

  async fn process_stats_pipeline_analytics_ack(
    &self,
    analytics_ack: &handshake_response::AnalyticsAck,
  ) {
    if let Err(error) = self
      .file_manager
      .acknowledge_stats_pipeline_analytics_report(&analytics_ack.report_id)
      .await
    {
      log::debug!("failed to acknowledge handshake stats analytics: {error}");
    }
  }
}

impl UploadContext {
  const fn name(&self) -> &'static str {
    match self {
      Self::Periodic(..) => "periodic",
      Self::Flush(..) => "explicit flush",
      Self::Startup(..) => "handshake",
    }
  }

  // Exposes the file claim independently from the request type so the common completion path can
  // finish or release files before it performs request-type-specific bookkeeping.
  const fn metadata(&self) -> &PendingUploadMetadata {
    match self {
      Self::Periodic(metadata) | Self::Flush(metadata) | Self::Startup(metadata) => metadata,
    }
  }
}

impl PendingUploadContext {
  const fn name(&self) -> &'static str {
    match self {
      Self::Periodic => "periodic",
      Self::Flush => "explicit flush",
    }
  }

  // Once a retained request gets a channel permit, move it into the response-driven completion
  // state while preserving whether it was periodic work or an explicit caller request.
  fn with_metadata(self, metadata: PendingUploadMetadata) -> UploadContext {
    match self {
      Self::Periodic => UploadContext::Periodic(metadata),
      Self::Flush => UploadContext::Flush(metadata),
    }
  }
}

fn batch_transport_uuid(source_file_ids: &[String]) -> String {
  if source_file_ids.len() == 1 {
    return source_file_ids[0].clone();
  }

  // Retries must use the same transport UUID for the same logical batch, but the UUID also needs
  // to change when the batch composition changes. A stable SHA-256 hex digest of the ordered
  // source file IDs gives us both properties across retries and restarts, and `upload_uuid` is
  // only treated as an opaque string in this path.
  let digest = Sha256::digest(format!("stats-batch:{}", source_file_ids.join(",")));
  let mut encoded = String::with_capacity(digest.len() * 2);
  for byte in digest {
    encoded.push(HEX_DIGITS[usize::from(byte >> 4)] as char);
    encoded.push(HEX_DIGITS[usize::from(byte & 0x0f)] as char);
  }
  encoded
}

#[async_trait]
pub trait PeriodicSchedule: Send + Sync {
  async fn next_action(&mut self) -> PeriodicAction;
}

//
// RuntimePeriodicSchedule
//

// Tracks a single periodic upload cycle. `next_flush_at` is omitted when the effective flush
// cadence has collapsed to the upload cadence, which means the next interesting event is the
// upload boundary itself.
struct RuntimePeriodicScheduleState {
  upload_interval: Duration,
  effective_flush_interval: Duration,
  next_flush_at: Option<OffsetDateTime>,
  next_upload_at: OffsetDateTime,
  first_upload_pending: bool,
}

// Couples the stats flush and upload timers into one schedule driven by the active upload
// interval. The scheduler always reasons about one upload cycle at a time and may insert one or
// more intermediate flush deadlines before the next upload deadline when the configured flush
// interval is a clean divisor of the active upload interval.
//
// Startup is deterministic: the first cycle uses its dedicated interval without jitter. Later
// cycles use the active recurring upload interval. We only re-jitter recurring upload deadlines
// when that active interval changes at runtime, which keeps reconnect storms from synchronizing.
pub struct RuntimePeriodicSchedule {
  flush_interval: watch::Receiver<Duration>,
  live_upload_interval: watch::Receiver<Duration>,
  sleep_upload_interval: watch::Receiver<Duration>,
  first_upload_interval: watch::Receiver<Duration>,
  sleep_mode_active: watch::Receiver<bool>,
  time_provider: Arc<dyn TimeProvider>,
  state: Option<RuntimePeriodicScheduleState>,
}

impl RuntimePeriodicSchedule {
  #[must_use]
  pub fn new(
    flush_interval: watch::Receiver<Duration>,
    live_upload_interval: watch::Receiver<Duration>,
    sleep_upload_interval: watch::Receiver<Duration>,
    first_upload_interval: watch::Receiver<Duration>,
    sleep_mode_active: watch::Receiver<bool>,
    time_provider: Arc<dyn TimeProvider>,
  ) -> Self {
    Self {
      flush_interval,
      live_upload_interval,
      sleep_upload_interval,
      first_upload_interval,
      sleep_mode_active,
      time_provider,
      state: None,
    }
  }

  // Snap the current runtime inputs into a fresh cycle. This is called at startup and whenever a
  // watched runtime value changes in a way that should re-plan future deadlines.
  fn rebuild_schedule(&mut self, jitter_upload_deadline: bool, preserve_upload_deadline: bool) {
    let now = self.time_provider.now();

    // Flush cadence is always read directly from runtime, but upload cadence depends on whether
    // we are currently in live mode or sleep mode.
    let flush_interval = *self.flush_interval.borrow_and_update();
    let upload_interval = self.active_upload_interval();
    let first_upload_pending = self.first_upload_pending();
    let scheduled_upload_interval = if first_upload_pending {
      *self.first_upload_interval.borrow_and_update()
    } else {
      upload_interval
    };
    let effective_flush_interval =
      effective_flush_interval(flush_interval, scheduled_upload_interval);

    // An invalid flush cadence does not stop scheduling; it simply means we only flush at the
    // upload boundary for this cycle.
    if effective_flush_interval != flush_interval {
      log::debug!(
        "stats disk flush interval {flush_interval} does not cleanly divide active upload \
         interval {scheduled_upload_interval}; falling back to upload cadence"
      );
    }

    // We only jitter when the upload interval itself changes. The steady-state schedule remains
    // deterministic, but a runtime cadence change still gets a one-time spread to avoid herding.
    //
    // A flush-only config change should not move an already scheduled upload deadline. If the
    // active upload cadence is unchanged and the previously scheduled upload is still in the
    // future, preserve that absolute boundary and only recompute how many flushes can fit before
    // it. Without this, a flush-interval tweak would effectively restart the upload timer from
    // "now", which is not the behavior we want.
    let preserved_upload_at = (preserve_upload_deadline && !jitter_upload_deadline)
      .then(|| {
        self.state.as_ref().and_then(|state| {
          (state.first_upload_pending == first_upload_pending
            && (first_upload_pending || state.upload_interval == upload_interval)
            && state.next_upload_at > now)
            .then_some(state.next_upload_at)
        })
      })
      .flatten();

    // If we kept the previous upload deadline, derive the remaining delay from that fixed point.
    // Otherwise schedule a brand new upload boundary, with optional jitter when the upload cadence
    // itself changed.
    let next_upload_delay = preserved_upload_at.map_or_else(
      || {
        if jitter_upload_deadline {
          scheduled_upload_interval
            .jittered()
            .try_into()
            .unwrap_or(scheduled_upload_interval)
        } else {
          scheduled_upload_interval
        }
      },
      |next_upload_at| next_upload_at - now,
    );

    // `next_upload_at` is either the preserved absolute deadline from the prior cycle or a newly
    // computed deadline for the rebuilt cycle.
    let next_upload_at = preserved_upload_at.unwrap_or(now + next_upload_delay);

    // Only schedule an intermediate flush when there is actually time for one before the upload
    // deadline. Otherwise the upload tick will perform the flush itself.
    let next_flush_at =
      (effective_flush_interval < next_upload_delay).then_some(now + effective_flush_interval);

    self.state = Some(RuntimePeriodicScheduleState {
      upload_interval,
      effective_flush_interval,
      next_flush_at,
      next_upload_at,
      first_upload_pending,
    });
  }

  // Reads the currently active upload interval and consumes any pending watch updates on that
  // active source. This keeps later comparisons against `state.upload_interval` honest.
  fn active_upload_interval(&mut self) -> Duration {
    if *self.sleep_mode_active.borrow() {
      *self.sleep_upload_interval.borrow_and_update()
    } else {
      *self.live_upload_interval.borrow_and_update()
    }
  }

  fn current_active_upload_interval(&self) -> Duration {
    if *self.sleep_mode_active.borrow() {
      *self.sleep_upload_interval.borrow()
    } else {
      *self.live_upload_interval.borrow()
    }
  }

  fn first_upload_pending(&self) -> bool {
    self
      .state
      .as_ref()
      .is_none_or(|state| state.first_upload_pending)
  }

  // Replans after a recurring upload cadence or sleep-mode update. The initial upload has its
  // own deadline, so these updates only take effect once it has completed.
  fn rebuild_for_recurring_upload_interval_change(&mut self) {
    if self.first_upload_pending() {
      return;
    }

    let active_upload_interval = self.current_active_upload_interval();
    let upload_interval_changed = self
      .state
      .as_ref()
      .is_some_and(|state| state.upload_interval != active_upload_interval);
    self.rebuild_schedule(upload_interval_changed, true);
  }

  fn update_after_action(&mut self, action: PeriodicAction) {
    match action {
      PeriodicAction::Flush => self.update_after_flush(),
      PeriodicAction::Upload => self.update_after_upload(),
    }
  }

  fn update_after_flush(&mut self) {
    let Some(state) = self.state.as_mut() else {
      return;
    };

    let Some(next_flush_at) = state.next_flush_at else {
      return;
    };

    // Keep generating intermediate flushes until the next one would land on or after the upload
    // deadline. At that point the upload tick owns the final flush for the cycle.
    let candidate = next_flush_at + state.effective_flush_interval;
    state.next_flush_at = (candidate < state.next_upload_at).then_some(candidate);
  }

  // Upload always performs a flush first, so the next cycle only needs intermediate flushes.
  fn update_after_upload(&mut self) {
    let now = self.time_provider.now();
    let upload_interval = self.active_upload_interval();
    let flush_interval = *self.flush_interval.borrow_and_update();
    let effective_flush_interval = effective_flush_interval(flush_interval, upload_interval);
    let Some(state) = self.state.as_mut() else {
      return;
    };

    state.first_upload_pending = false;
    state.upload_interval = upload_interval;
    state.effective_flush_interval = effective_flush_interval;
    state.next_upload_at = now + state.upload_interval;
    state.next_flush_at = (state.effective_flush_interval < state.upload_interval)
      .then_some(now + state.effective_flush_interval);
  }
}

#[async_trait]
impl PeriodicSchedule for RuntimePeriodicSchedule {
  async fn next_action(&mut self) -> PeriodicAction {
    loop {
      // Lazily build the first cycle so construction stays cheap and so startup uses the latest
      // runtime values at the first point we actually need to schedule work.
      if self.state.is_none() {
        self.rebuild_schedule(false, false);
      }

      let now = self.time_provider.now();
      let (next_flush_at, next_upload_at) = {
        let Some(state) = self.state.as_ref() else {
          continue;
        };
        (state.next_flush_at, state.next_upload_at)
      };

      // If either deadline is already in the past, return immediately instead of sleeping. Flush
      // wins ties so we preserve the invariant that an upload cycle always flushes first.
      let next_action = match next_flush_at {
        Some(next_flush_at) if next_flush_at <= now => Some(PeriodicAction::Flush),
        _ if next_upload_at <= now => Some(PeriodicAction::Upload),
        _ => None,
      };

      if let Some(next_action) = next_action {
        self.update_after_action(next_action);
        return next_action;
      }

      // Otherwise wait for whichever deadline arrives first, but keep listening for runtime
      // updates so we can rebuild the schedule without waiting for the old deadline to expire.
      let next_deadline = next_flush_at.map_or(next_upload_at, |next_flush_at| {
        next_flush_at.min(next_upload_at)
      });
      let sleep_duration = next_deadline - now;
      let time_provider = self.time_provider.clone();
      let sleep = time_provider.sleep(sleep_duration);
      tokio::pin!(sleep);

      // Clone the receivers used in the select so each branch can take ownership of the updated
      // receiver and store it back onto `self` before rebuilding the schedule.
      let mut flush_interval = self.flush_interval.clone();
      let mut live_upload_interval = self.live_upload_interval.clone();
      let mut sleep_upload_interval = self.sleep_upload_interval.clone();
      let mut first_upload_interval = self.first_upload_interval.clone();
      let mut sleep_mode_active = self.sleep_mode_active.clone();

      tokio::select! {
        // The current schedule reached its next deadline. Loop around to return the due action.
        () = &mut sleep => {},
        changed = flush_interval.changed() => {
          if changed.is_err() {
            continue;
          }

          // A flush-only cadence change should not add jitter to the upload deadline; it only
          // changes how many intermediate flushes fit before the same upload boundary.
          self.flush_interval = flush_interval;
          self.rebuild_schedule(false, true);
        },
        changed = live_upload_interval.changed() => {
          if changed.is_err() {
            continue;
          }
          self.live_upload_interval = live_upload_interval;
          self.rebuild_for_recurring_upload_interval_change();
        },
        changed = sleep_upload_interval.changed() => {
          if changed.is_err() {
            continue;
          }
          self.sleep_upload_interval = sleep_upload_interval;
          self.rebuild_for_recurring_upload_interval_change();
        },
        changed = first_upload_interval.changed() => {
          if changed.is_err() {
            continue;
          }
          self.first_upload_interval = first_upload_interval;

          // The first-delay setting is relevant only while the initial upload is pending. A
          // change then deliberately starts that one deadline over using the new duration.
          if self.first_upload_pending() {
            self.rebuild_schedule(false, false);
          }
        },
        changed = sleep_mode_active.changed() => {
          if changed.is_err() {
            continue;
          }
          self.sleep_mode_active = sleep_mode_active;
          self.rebuild_for_recurring_upload_interval_change();
        },
      }
    }
  }
}

// Only flush on a separate cadence when the configured flush interval cleanly partitions the
// upload interval. Otherwise the scheduler collapses flushes to the upload boundary so the two
// periodic loops cannot drift apart.
fn effective_flush_interval(
  configured_flush_interval: Duration,
  upload_interval: Duration,
) -> Duration {
  let configured_flush_millis: i128 = configured_flush_interval.whole_milliseconds();
  let upload_millis: i128 = upload_interval.whole_milliseconds();

  if configured_flush_millis <= 0
    || configured_flush_millis > upload_millis
    || upload_millis % configured_flush_millis != 0
  {
    upload_interval
  } else {
    configured_flush_interval
  }
}

//
// Flusher
//

/// Responsible for periodically flushing the stats store to a locally aggregated file.
pub struct Flusher {
  stats: Arc<Stats>,
  shutdown: ComponentShutdown,
  periodic_schedule: Box<dyn PeriodicSchedule>,
  flush_rx: tokio::sync::mpsc::Receiver<()>,
  flush_trigger: FlushTrigger,
  flush_time_histogram: Histogram,
  data_flush_tx: mpsc::Sender<DataUpload>,
  // API-originated response receivers originate in HandshakeStats, which is held by Api. Flusher
  // owns the completion state machine for periodic, explicit, and API-originated uploads, so
  // receivers are handed back here before they are polled.
  api_upload_completion_rx: mpsc::Receiver<(oneshot::Receiver<UploadResponse>, Vec<String>)>,
  file_manager: Arc<FileManager>,
  // Every request admitted to the transport has a response receiver here. This must be a set:
  // periodic and explicit uploads have independent in-flight gates and may overlap, while an API
  // handshake upload may be registered while either is awaiting an ACK. `uploads` is the only
  // place that resolves claimed persisted files after transport handoff.
  uploads: FuturesUnordered<UploadFuture>,
  // Unlike admitted uploads, only one request may wait for the shared channel. A full channel
  // must not block this task from persisting stats, accepting flush requests, observing shutdown,
  // or handling existing completions, so retain one request and wait for its permit as a select
  // arm instead.
  pending_data_upload: Option<PendingDataUpload>,
  // Holds requests and periodic-upload intent coalesced by the fixed disk debounce window.
  disk_flush: DiskFlushDebounce,
  // These gates distinguish an admitted upload from a merely persisted snapshot. They prevent
  // duplicate upload attempts without delaying local durability for later flush callers.
  periodic_in_flight: bool,
  flush_in_flight: bool,
  // This uses system time to allow integration tests to work. It should really use monotonic time.
  last_flush_upload_time: Option<time::OffsetDateTime>,
  time_provider: Arc<dyn TimeProvider>,
  minimum_upload_interval:
    bd_runtime::runtime::DurationWatch<bd_runtime::runtime::stats::MinimumUploadIntervalFlag>,
  disk_flush_debounce:
    bd_runtime::runtime::DurationWatch<bd_runtime::runtime::stats::DiskFlushDebounceFlag>,

  #[cfg(test)]
  test_hooks: TestHooks,
}

impl Flusher {
  pub fn new(
    stats: Arc<Stats>,
    shutdown: ComponentShutdown,
    periodic_schedule: Box<dyn PeriodicSchedule>,
    flush_rx: tokio::sync::mpsc::Receiver<()>,
    flush_time_histogram: Histogram,
    data_flush_tx: mpsc::Sender<DataUpload>,
    file_manager: Arc<FileManager>,
    time_provider: Arc<dyn TimeProvider>,
    minimum_upload_interval: bd_runtime::runtime::DurationWatch<
      bd_runtime::runtime::stats::MinimumUploadIntervalFlag,
    >,
    disk_flush_debounce: bd_runtime::runtime::DurationWatch<
      bd_runtime::runtime::stats::DiskFlushDebounceFlag,
    >,
    api_upload_completion_rx: mpsc::Receiver<(oneshot::Receiver<UploadResponse>, Vec<String>)>,
    flush_trigger: FlushTrigger,
  ) -> Self {
    Self {
      stats,
      shutdown,
      periodic_schedule,
      flush_rx,
      flush_trigger,
      flush_time_histogram,
      data_flush_tx,
      api_upload_completion_rx,
      file_manager,
      uploads: FuturesUnordered::new(),
      pending_data_upload: None,
      disk_flush: DiskFlushDebounce {
        deadline: None,
        flush_pending: false,
        periodic_upload_pending: false,
      },
      periodic_in_flight: false,
      flush_in_flight: false,
      last_flush_upload_time: None,
      time_provider,
      minimum_upload_interval,
      disk_flush_debounce,

      #[cfg(test)]
      test_hooks: TestHooks::default(),
    }
  }

  #[cfg(test)]
  pub const fn test_hooks(&mut self) -> TestHooksReceiver {
    self.test_hooks.receiver.take().unwrap()
  }

  fn should_skip_upload(&self) -> bool {
    // Minimum interval is intentionally applied only to upload dispatch. Disk persistence always
    // continues so a throttled caller still leaves its latest metrics durable for a later retry.
    self.last_flush_upload_time.is_some_and(|last_upload| {
      let now = self.time_provider.now();
      let elapsed = (now - last_upload).unsigned_abs();
      let min_interval = self.minimum_upload_interval.read().unsigned_abs();
      elapsed < min_interval
    })
  }

  pub async fn periodic_flush(mut self) {
    // All asynchronous transitions of the flusher are driven from one select loop. In particular,
    // do not await channel capacity inside an event handler: that would stop this receiver from
    // processing explicit flushes while another data-upload producer occupies the shared channel.
    loop {
      tokio::select! {
        // A retained request owns a FileManager claim but no transport receiver. Wait for capacity
        // alongside every other event, then atomically transfer that ownership to `uploads`.
        permit = self.data_flush_tx.clone().reserve_owned(), if self
          .pending_data_upload
          .is_some() => {
          match permit {
            Ok(permit) => self.dispatch_pending_upload(permit).await,
            Err(_) => {
              let () = self.abandon_pending_upload().await;
            },
          }
        },
        Some(()) = self.flush_rx.recv() => {
          self.handle_flush_request().await;
        },
        // The deadline is present only during a debounce window. Its handler makes a trailing
        // write before releasing request completions or starting a deferred periodic upload.
        () = maybe_await(&mut self.disk_flush.deadline) => {
          self.handle_disk_flush_deadline().await;
        },
        () = self.shutdown.cancelled() => {
          self.flush_trigger.fail_open_epoch();
          return;
        },
        action = self.periodic_schedule.next_action() => {
          match action {
            PeriodicAction::Flush => {
              let _ = self.flush_to_disk_with_debounce().await;
            },
            PeriodicAction::Upload => self.handle_periodic_upload_tick().await,
          }
        },
        Some((upload_response, context)) = self.uploads.next() => {
          self.handle_upload_completion(upload_response, context).await;
        },
        Some((response_rx, source_file_ids)) = self.api_upload_completion_rx.recv() => {
          // Register the handshake upload in the same completion path as all other persisted
          // uploads. A dropped StateTracker makes this receiver resolve to None, which follows
          // the existing abandon-without-ACK branch in handle_upload_completion.
          log::debug!(
            "registered handshake stats upload completion for {} source files",
            source_file_ids.len()
          );
          self.push_upload_future(
            response_rx,
            UploadContext::Startup(PendingUploadMetadata { source_file_ids }),
          );
        },
      };
    }
  }

  async fn handle_periodic_upload_tick(&mut self) {
    // An upload must include everything collected before its tick. If a debounce window is open,
    // wait for the trailing write rather than preparing a request from stale on-disk state.
    match self.flush_to_disk_with_debounce().await {
      DiskFlushOutcome::Durable => self.handle_upload_tick().await,
      DiskFlushOutcome::Deferred => {
        log::debug!("deferring periodic stats upload until debounced disk flush completes");
        self.disk_flush.periodic_upload_pending = true;
      },
      DiskFlushOutcome::Failed => {
        log::debug!("skipping periodic stats upload because its disk flush failed");
      },
    }
  }

  async fn handle_upload_tick(&mut self) {
    // A periodic upload cannot overtake either an already admitted periodic request or one waiting
    // for channel capacity. Explicit flush uploads use their own gate and remain independent.
    if self.periodic_in_flight || self.pending_data_upload.is_some() {
      log::debug!("skipping periodic stats upload: another periodic or deferred upload is active");
      return;
    }

    if self.should_skip_upload() {
      log::debug!("skipping periodic stats upload: minimum upload interval has not elapsed");
      return;
    }

    if let Some(prepared_upload) = self
      .upload_from_disk(false, UploadReason::UPLOAD_REASON_PERIODIC)
      .await
    {
      self.periodic_in_flight = true;
      let _ = self
        .dispatch_prepared_upload(prepared_upload, PendingUploadContext::Periodic)
        .await;
    }
  }

  async fn handle_flush_request(&mut self) {
    // All callers have already joined the trigger's open epoch. A write atomically rotates that
    // epoch before I/O, so callers that arrive during the write wait for the next physical write.
    let _ = self.flush_to_disk_with_debounce().await;
  }

  async fn handle_flush_upload(&mut self) {
    // Coalesce upload requests onto the existing upload; the persisted snapshot remains eligible
    // for a future attempt if transport handoff or ACK later fails.
    if self.flush_in_flight || self.pending_data_upload.is_some() {
      log::debug!(
        "skipping explicit stats upload: another flush upload is active; stats are durable"
      );
      return;
    }

    if self.should_skip_upload() {
      log::debug!("skipping explicit stats upload: minimum upload interval has not elapsed");
      return;
    }

    if let Some(prepared_upload) = self
      .upload_from_disk(false, UploadReason::UPLOAD_REASON_EVENT_TRIGGERED)
      .await
    {
      self.last_flush_upload_time = Some(self.time_provider.now());
      self.flush_in_flight = true;
      match self
        .dispatch_prepared_upload(prepared_upload, PendingUploadContext::Flush)
        .await
      {
        UploadDispatch::Dispatched | UploadDispatch::Deferred => {},
        UploadDispatch::Closed => {
          // `abandon_pending_upload` has released the file claim and completed the caller.
          self.flush_in_flight = false;
        },
      }
    }
  }

  async fn flush_to_disk_with_debounce(&mut self) -> DiskFlushOutcome {
    if self.disk_flush.deadline.is_some() {
      // Do not reset the deadline: this is a fixed window, not a quiet-period debounce.
      log::debug!("coalescing stats disk flush into active debounce window");
      self.disk_flush.flush_pending = true;
      return DiskFlushOutcome::Deferred;
    }

    let epoch = self.flush_trigger.begin_disk_flush();
    let disk_flush_succeeded = self.flush_to_disk().await;
    self.complete_flush_epoch(epoch, disk_flush_succeeded).await;
    self.start_disk_flush_debounce_window();
    if disk_flush_succeeded {
      DiskFlushOutcome::Durable
    } else {
      DiskFlushOutcome::Failed
    }
  }

  async fn complete_flush_epoch(&mut self, epoch: FlushEpoch, disk_flush_succeeded: bool) {
    if !disk_flush_succeeded {
      epoch.fail();
      return;
    }

    let do_upload = epoch.do_upload();
    if do_upload {
      // This only prepares a request and attempts a nonblocking handoff. It never waits for
      // channel capacity or an ACK, so durable completion remains independent of transport.
      self.handle_flush_upload().await;
    }
    epoch.complete_durable();
  }

  fn start_disk_flush_debounce_window(&mut self) {
    // Each actual disk write starts a fresh window. A trailing write may therefore start one more
    // window even when no caller is currently waiting, which bounds write frequency under load.
    let debounce = self.disk_flush_debounce.read().unsigned_abs();
    self.disk_flush.deadline = Some(Box::pin(tokio::time::sleep(debounce)));
    log::debug!("started stats disk flush debounce window: duration={debounce:?}");
  }

  async fn handle_disk_flush_deadline(&mut self) {
    // Clear the old deadline before awaiting I/O so any event that arrives during the trailing
    // write is treated as the leading write of the next window rather than joining the old one.
    self.disk_flush.deadline = None;
    if !self.disk_flush.flush_pending {
      log::debug!("stats disk flush debounce window closed without a trailing flush");
      return;
    }

    log::debug!(
      "running debounced trailing stats disk flush: periodic_upload_pending={}",
      self.disk_flush.periodic_upload_pending
    );
    self.disk_flush.flush_pending = false;
    let disk_flush_outcome = self.flush_to_disk_with_debounce().await;

    if self.disk_flush.periodic_upload_pending {
      // The periodic tick was deferred solely to wait for the trailing write. It can now prepare
      // an upload from the latest persisted state and still obey normal in-flight/throttle gates.
      self.disk_flush.periodic_upload_pending = false;
      if matches!(disk_flush_outcome, DiskFlushOutcome::Durable) {
        self.handle_upload_tick().await;
      }
    }
  }

  async fn handle_upload_completion(
    &mut self,
    upload_response: Option<UploadResponse>,
    context: UploadContext,
  ) {
    // A missing response means the upload's StateTracker sender was dropped before the server
    // acknowledged it, normally because the mux stream closed or reconnected. The source files
    // remain claimed while the tracker is active, so release the claim without recording an ACK
    // outcome; otherwise this batch remains permanently in flight and cannot be retried.
    let Some(upload_response) = upload_response else {
      let source_file_ids = context.metadata().source_file_ids.clone();
      log::debug!(
        "{} stats upload tracker closed before an ACK; releasing {} source files",
        context.name(),
        source_file_ids.len()
      );
      if let Err(error) = self
        .file_manager
        .release_pending_upload(&source_file_ids)
        .await
      {
        log::debug!("failed to abandon stats upload without an ACK: {error}");
      }

      match context {
        UploadContext::Periodic(..) => self.periodic_in_flight = false,
        UploadContext::Flush(..) => {
          self.flush_in_flight = false;
        },
        UploadContext::Startup(..) => {},
      }
      #[cfg(test)]
      self
        .test_hooks
        .sender
        .upload_complete_tx
        .send(())
        .await
        .unwrap();
      return;
    };

    log::debug!(
      "{} stats upload completed: uuid={}, success={}, source_files={}",
      context.name(),
      upload_response.uuid,
      upload_response.success,
      context.metadata().source_file_ids.len()
    );

    if matches!(context, UploadContext::Flush(..)) && !upload_response.success {
      // Clear the flush upload gate on failure so a later background or explicit flush can retry.
      self.last_flush_upload_time = None;
    }

    self
      .process_pending_upload_completion(&upload_response, context.metadata())
      .await;

    #[cfg(test)]
    self
      .test_hooks
      .sender
      .upload_complete_tx
      .send(())
      .await
      .unwrap();

    // FileManager consumes the response before any request-specific bookkeeping. That ordering
    // releases successful source files (or preserves failed ones for retry) even if later work
    // such as periodic backlog draining begins another upload immediately.
    match context {
      UploadContext::Periodic(..) => {
        if upload_response.success {
          if let Some(prepared_upload) = self
            .upload_from_disk(true, UploadReason::UPLOAD_REASON_PERIODIC)
            .await
          {
            // Startup sends one capped batch per handshake. After a periodic upload succeeds,
            // continue draining remaining old snapshots so a persisted backlog does not wait for
            // another handshake or periodic interval. These uploads bypass the minimum interval.
            let _ = self
              .dispatch_prepared_upload(prepared_upload, PendingUploadContext::Periodic)
              .await;
          } else {
            self.periodic_in_flight = false;
          }
        } else {
          self.periodic_in_flight = false;
        }
      },
      UploadContext::Flush(..) => {
        self.flush_in_flight = false;
      },
      UploadContext::Startup(..) => {},
    }
  }

  fn push_upload_future(&self, rx: oneshot::Receiver<UploadResponse>, context: UploadContext) {
    // Normalizing the receiver into this future makes all request sources share the same `None`
    // (transport dropped) and `Some(response)` completion handling in the main loop.
    self
      .uploads
      .push(Box::pin(async move { (rx.await.ok(), context) }));
  }

  // Merges a delta snapshot to disk. This contains the difference in metrics since the last time
  // stats were flushed to disk.
  async fn merge_delta_snapshot_to_disk(
    &self,
    delta_snapshot: SnapshotHelper,
  ) -> anyhow::Result<()> {
    // Use either the snapshot cached to disk or a new one that records starting point of this
    // aggregation window.
    log::debug!("starting merge of delta snapshot to disk");
    let mut handle = self.file_manager.get_or_create_snapshot().await?;
    let mut new_or_existing_snapshot =
      SnapshotHelper::new(handle.snapshot(), self.stats.collector.limit());

    for ((metric_type, name), metrics) in delta_snapshot.metrics {
      for (labels, metric) in metrics {
        let Some(cached_metric) = new_or_existing_snapshot.mut_metric(metric_type, &name, &labels)
        else {
          log::trace!("adding new metric to snapshot: {}{labels:?}", name.as_str());
          new_or_existing_snapshot.add_metric(name.clone(), labels, metric);
          continue;
        };

        // If the metric already exists in the cached snapshot, sum the values together.
        match (&metric, &cached_metric) {
          (MetricData::Counter(c), MetricData::Counter(cached_counter)) => {
            log::trace!(
              "merging counter {}{labels:?} with value {}",
              name.as_str(),
              c.get()
            );
            cached_counter.inc_by(c.get());
          },
          (MetricData::Histogram(h), MetricData::Histogram(cached_histogram)) => {
            log::trace!("merging histogram {}{labels:?}", name.as_str());
            cached_histogram.merge_from(h)?;
          },
          _ => {
            // We don't support metrics changing type ever, so do nothing but record an error so we
            // know if this happens.
            handle_unexpected::<(), anyhow::Error>(
              Err(anyhow::anyhow!("metrics inconsistency")),
              "stats merging",
            );
          },
        }
      }
    }

    for (name, count) in delta_snapshot.overflows {
      new_or_existing_snapshot
        .overflows
        .entry(name)
        .and_modify(|e| *e += count)
        .or_insert(count);
    }

    for (workflow_id, debug_data) in delta_snapshot.workflow_debug_data {
      log::trace!("merging workflow debug data for {workflow_id}");
      let existing = new_or_existing_snapshot
        .workflow_debug_data
        .entry(workflow_id)
        .or_default();
      if let Some(start_reset) = debug_data.start_reset.into_option() {
        existing
          .start_reset
          .mut_or_insert_default()
          .transition_count += start_reset.transition_count;
      }
      for (state_id, state_data) in debug_data.states {
        log::trace!("merging workflow debug state for {state_id}");
        let existing_state = existing.states.entry(state_id).or_default();
        for transition in state_data.transitions {
          log::trace!(
            "merging workflow debug transition for {:?}",
            transition.transition_type
          );
          if let Some(transition_type) = &transition.transition_type {
            if let Some(existing_transition) = existing_state
              .transitions
              .iter_mut()
              .find(|t| t.transition_type == Some(transition_type.clone()))
            {
              existing_transition.transition_count += transition.transition_count;
            } else {
              existing_state.transitions.push(transition);
            }
          }
        }
      }
    }

    // If there are no metrics, overflow counts, or workflow debug state in the snapshot after
    // merging in the latest delta, skip writing the aggregated snapshot to prevent empty uploads.
    if new_or_existing_snapshot.metrics.is_empty()
      && new_or_existing_snapshot.overflows.is_empty()
      && new_or_existing_snapshot.workflow_debug_data.is_empty()
    {
      self.file_manager.remove_empty_snapshot().await?;
      return Ok(());
    }

    // Write the updated snapshot back to disk. This will either be read back up on the next
    // iteration of this task or converted into an upload payload by the upload task.
    log::debug!(
      "updating aggregated snapshot file with {} metrics, {} overflowed IDs, and {} workflow \
       debug entries",
      new_or_existing_snapshot.metrics.len(),
      new_or_existing_snapshot.overflows.len(),
      new_or_existing_snapshot.workflow_debug_data.len()
    );

    // This might fail due to us being out of space or other I/O errors.
    // TODO(snowp): Consider how we might record stats for this - if stats flushing is broken we
    // might not be able to propagate the stats values.
    self
      .file_manager
      .write_snapshot(handle, new_or_existing_snapshot.into_proto()?)
      .await
  }

  async fn flush_to_disk(&self) -> bool {
    log::debug!("flushing collected stats to disk");
    let _timer = self.flush_time_histogram.start_timer();
    // To support flushing stats between multiple process lifetimes, we go through a few steps to
    // apply the diff to the disk-cached snapshot:
    // 1. Gather the current set of delta metrics from the stats registry and convert this into a
    //    StatsSnapshot. This is referred to as the delta snapshot.
    // 2. Attempt to write the new delta snapshot to disk.
    let delta_snapshot = self.create_delta_snapshot();

    #[cfg(feature = "logger-cli-observer")]
    with_observer(|observer| {
      let metrics = snapshot_action_metrics(&delta_snapshot);
      if !metrics.is_empty() {
        observer.on_snapshot(SnapshotObservation { metrics });
      }
    });

    // Because we have snapped deltas out of the collectors, if we fail to write to disk we will
    // lose the stats. Given that we will lose the stats anyway if the process terminates, this
    // seems not completely terrible. If we want to slightly improve this in the future we could
    // decide to re-merge the deltas back into the collectors if we fail to write to disk.
    let succeeded = match self.merge_delta_snapshot_to_disk(delta_snapshot).await {
      Ok(()) => true,
      Err(error) => {
        handle_unexpected::<(), anyhow::Error>(Err(error), "writing stats to disk");
        false
      },
    };

    #[cfg(test)]
    self
      .test_hooks
      .sender
      .flush_complete_tx
      .send(())
      .await
      .unwrap();

    succeeded
  }

  fn create_delta_snapshot(&self) -> SnapshotHelper {
    let mut snapshot = SnapshotHelper::new(None, self.stats.collector.limit());
    Self::snap_collector_to_snapshot(&self.stats.collector, &mut snapshot);
    snapshot.overflows = std::mem::take(&mut self.stats.overflows.lock());

    let workflow_debug_data = self.stats.take_workflow_debug_data();
    let mut snapshot_workflow_debug_data: HashMap<String, WorkflowDebugData> = HashMap::new();
    for (key, count) in workflow_debug_data {
      let workflow_entry = snapshot_workflow_debug_data
        .entry(key.workflow_id)
        .or_default();

      match key.state_key {
        WorkflowDebugStateKey::StartOrReset => {
          workflow_entry
            .start_reset
            .mut_or_insert_default()
            .transition_count = count;
        },
        WorkflowDebugStateKey::StateTransition {
          state_id,
          transition_type,
        } => {
          workflow_entry
            .states
            .entry(state_id)
            .or_default()
            .transitions
            .push(WorkflowTransitionDebugData {
              transition_type: Some(match transition_type {
                WorkflowDebugTransitionType::Normal(index) => {
                  Transition_type::TransitionIndex(index.try_into().unwrap_or(0))
                },
                WorkflowDebugTransitionType::Timeout => Transition_type::TimeoutTransition(true),
              }),
              transition_count: count,
              ..Default::default()
            });
        },
      }
    }
    snapshot.workflow_debug_data = snapshot_workflow_debug_data;

    snapshot
  }

  fn snap_collector_to_snapshot(collector: &Collector, snapshot: &mut SnapshotHelper) {
    // During iteration if a metric has data, we retain it, since it is likely to be used again.
    // If there is no data we drop it if there are no outstanding references. This iteration
    // occurs under the collector lock so it is serialized with respect to new fetches.
    collector.retain(|name, labels, metric| {
      metric.snap().map_or_else(
        || metric.multiple_references(),
        |metric| {
          snapshot.add_metric(name.clone(), labels.clone(), metric);
          true
        },
      )
    });
  }

  async fn upload_from_disk(
    &self,
    only_if_file_is_old: bool,
    upload_reason: UploadReason,
  ) -> Option<PreparedUpload> {
    async fn inner(
      flusher: &Flusher,
      only_if_file_is_old: bool,
      upload_reason: UploadReason,
    ) -> anyhow::Result<Option<PreparedUpload>> {
      // FileManager atomically chooses and claims a persisted batch. From here on, every exit path
      // must either transfer the claim into an upload future or explicitly release it.
      if let Some(pending_upload) = flusher
        .file_manager
        .get_or_create_pending_upload(only_if_file_is_old)
        .await?
      {
        return Ok(Some(Flusher::prepare_pending_upload(
          pending_upload,
          upload_reason,
        )));
      }
      Ok(None)
    }

    // Note on error handling: while we could probably gracefully handle some of the failing I/O
    // operations, it is likely to result in inaccurate stats (double submission of stats, missing
    // aggregations, etc.), so we bail on failure. As we start seeing this out in the wild we may
    // get a better understanding of why things are failing at which point we can do more targeted
    // error handling.
    log::debug!(
      "preparing stats upload from disk: only_if_file_is_old={only_if_file_is_old}, \
       reason={upload_reason:?}"
    );
    match inner(self, only_if_file_is_old, upload_reason).await {
      Ok(result) => result,
      Err(e) => {
        handle_unexpected::<(), anyhow::Error>(Err(e), "upload from disk");
        None
      },
    }
  }

  // Prepares a persisted stats request for dispatch. The pending-file claim remains held until
  // the request is either handed to the transport or explicitly abandoned.
  fn prepare_pending_upload(
    pending_upload: PendingUpload,
    upload_reason: UploadReason,
  ) -> PreparedUpload {
    let PendingUpload {
      mut request,
      source_file_ids,
    } = pending_upload;
    let transport_uuid = batch_transport_uuid(&source_file_ids);
    request.upload_uuid = transport_uuid;
    request.upload_reason = upload_reason.into();
    let (stats, response_rx) = TrackedStatsUploadRequest::new(request.upload_uuid.clone(), request);

    log::debug!(
      "prepared {upload_reason:?} stats upload: uuid={}, snapshots={}, metrics={}",
      stats.payload.upload_uuid,
      stats.payload.snapshot.len(),
      stats
        .payload
        .snapshot
        .iter()
        .map(|s| s.metrics().metric.len())
        .sum::<usize>(),
    );

    observe_upload_attempt(&stats.payload, upload_reason);

    PreparedUpload {
      data_upload: DataUpload::StatsUpload(stats),
      response_rx,
      metadata: PendingUploadMetadata { source_file_ids },
    }
  }

  async fn dispatch_prepared_upload(
    &mut self,
    prepared_upload: PreparedUpload,
    context: PendingUploadContext,
  ) -> UploadDispatch {
    // Store first so a full channel leaves a recoverable, bounded unit of work. `try_reserve_owned`
    // avoids awaiting capacity here; the select loop handles that wait without losing
    // responsiveness.
    debug_assert!(self.pending_data_upload.is_none());
    let upload_kind = context.name();
    let source_file_count = prepared_upload.metadata.source_file_ids.len();
    self.pending_data_upload = Some(PendingDataUpload {
      prepared_upload,
      context,
    });

    match self.data_flush_tx.clone().try_reserve_owned() {
      Ok(permit) => {
        self.dispatch_pending_upload(permit).await;
        UploadDispatch::Dispatched
      },
      Err(mpsc::error::TrySendError::Full(_)) => {
        log::debug!(
          "deferring {upload_kind} stats upload for {source_file_count} source files: shared \
           data-upload channel is full"
        );
        UploadDispatch::Deferred
      },
      Err(mpsc::error::TrySendError::Closed(_)) => {
        log::debug!(
          "abandoning {upload_kind} stats upload for {source_file_count} source files: shared \
           data-upload channel is closed"
        );
        self.abandon_pending_upload().await;
        UploadDispatch::Closed
      },
    }
  }

  async fn dispatch_pending_upload(&mut self, permit: mpsc::OwnedPermit<DataUpload>) {
    // Taking the retained state and sending through its permit is the ownership transition from
    // local backpressure storage to the transport's StateTracker and response receiver.
    let Some(PendingDataUpload {
      prepared_upload,
      context,
    }) = self.pending_data_upload.take()
    else {
      log::debug!("acquired stats upload channel capacity without a deferred upload");
      return;
    };
    let upload_kind = context.name();
    let source_file_count = prepared_upload.metadata.source_file_ids.len();
    permit.send(prepared_upload.data_upload);
    log::debug!("dispatched {upload_kind} stats upload for {source_file_count} source files");

    // The request is now visible to the transport, so record the attempt before waiting for its
    // response. Failure to record is non-fatal because the claim still protects this batch.
    if let Err(error) = self
      .file_manager
      .record_pending_upload_attempt(&prepared_upload.metadata.source_file_ids)
      .await
    {
      log::debug!("failed to persist stats upload attempt: {error}");
    }

    self.push_upload_future(
      prepared_upload.response_rx,
      context.with_metadata(prepared_upload.metadata),
    );
  }

  async fn abandon_pending_upload(&mut self) {
    // A closed channel means no transport task will ever own the receiver. Release the retained
    // claim here so the same persisted files can be selected by a future flusher after restart.
    let Some(PendingDataUpload {
      prepared_upload,
      context,
    }) = self.pending_data_upload.take()
    else {
      log::debug!("shared data-upload channel closed without a deferred stats upload");
      return;
    };
    let upload_kind = context.name();
    let source_file_count = prepared_upload.metadata.source_file_ids.len();
    log::debug!(
      "releasing deferred {upload_kind} stats upload for {source_file_count} source files"
    );
    if let Err(error) = self
      .file_manager
      .release_pending_upload(&prepared_upload.metadata.source_file_ids)
      .await
    {
      log::debug!("failed to release pending stats upload: {error}");
    }
  }

  async fn process_pending_upload_completion(
    &self,
    upload_response: &UploadResponse,
    metadata: &PendingUploadMetadata,
  ) {
    #[cfg(feature = "logger-cli-observer")]
    with_observer(|observer| {
      observer.on_upload_ack(UploadAckObservation {
        upload_uuid: upload_response.uuid.clone(),
        success: upload_response.success,
      });
    });

    // If this fails we are in a bad state and are likely going to end up double uploading, but
    // there is little we can do about it.
    handle_unexpected(
      self
        .file_manager
        .complete_pending_upload(&metadata.source_file_ids, upload_response.success)
        .await,
      "complete pending upload",
    );
  }
}

//
// SnapshotHelper
//

struct SnapshotHelper {
  // `metrics` mirrors the persisted protobuf shape while retaining native metric values for merge
  // operations. The helper is the boundary between in-memory collector deltas and disk snapshots.
  metrics: MetricsByNameCore<(MetricType, NameType), MetricData>,
  overflows: HashMap<String, u64>,
  limit: Option<u32>,
  workflow_debug_data: HashMap<String, WorkflowDebugData>,
}

#[derive(Default)]
struct MetricsFromSnapshotResult {
  metrics: MetricsByNameCore<(MetricType, NameType), MetricData>,
  overflows: HashMap<String, u64>,
  workflow_debug_data: HashMap<String, WorkflowDebugData>,
}

impl SnapshotHelper {
  fn new(snapshot: Option<StatsSnapshot>, limit: Option<u32>) -> Self {
    let result = Self::metrics_from_snapshot(snapshot).unwrap_or_default();
    Self {
      metrics: result.metrics,
      overflows: result.overflows,
      limit,
      workflow_debug_data: result.workflow_debug_data,
    }
  }

  fn metrics_from_snapshot(snapshot: Option<StatsSnapshot>) -> Option<MetricsFromSnapshotResult> {
    // Corrupt or structurally unexpected snapshots are treated as absent by this conversion. The
    // caller then writes a fresh aggregation rather than merging against mismatched metric data.
    let snapshot = snapshot?;
    let Some(Snapshot_type::Metrics(metrics)) = snapshot.snapshot_type else {
      return None;
    };

    let mut new_metrics: MetricsByNameCore<(MetricType, NameType), MetricData> = HashMap::new();
    for proto_metric in metrics.metric {
      let tags = proto_metric.tags.into_iter().collect();
      if let Some(data) = proto_metric.data
        && let Some(metric) = MetricData::from_proto(data)
      {
        let metric_type = match metric {
          MetricData::Counter(_) => MetricType::Counter,
          MetricData::Histogram(_) => MetricType::Histogram,
        };

        let name = match proto_metric.metric_name_type {
          Some(Metric_name_type::Name(name)) => NameType::Global(name),
          Some(Metric_name_type::MetricId(id)) => NameType::ActionId(id),
          None => continue,
        };

        let existing = new_metrics
          .entry((metric_type, name))
          .or_default()
          .insert(tags, metric);
        debug_assert!(existing.is_none());
      }
    }

    Some(MetricsFromSnapshotResult {
      metrics: new_metrics,
      overflows: snapshot.metric_id_overflows,
      workflow_debug_data: snapshot.workflow_debug_data,
    })
  }

  fn mut_metric(
    &mut self,
    metric_type: MetricType,
    name: &NameType,
    labels: &BTreeMap<String, String>,
  ) -> Option<&mut MetricData> {
    self
      .metrics
      .get_mut(&(metric_type, name.clone()))
      .and_then(|metrics| metrics.get_mut(labels))
  }

  fn add_metric(&mut self, name: NameType, labels: BTreeMap<String, String>, metric: MetricData) {
    // The per-name cardinality limit applies only to workflow/action metrics. Global metrics are
    // not subject to this cap and are keyed separately by their `NameType`.
    let maybe_limit = if matches!(name, NameType::ActionId(..)) {
      self.limit
    } else {
      None
    };

    let by_name = self
      .metrics
      .entry((metric.metric_type(), name.clone()))
      .or_default();
    if let Some(limit) = maybe_limit
      && by_name.len() >= limit as usize
    {
      log::debug!("metric overflow during snapshot insert");
      self
        .overflows
        .entry(name.into_string())
        .and_modify(|e| *e += 1)
        .or_insert(1);
      return;
    }

    let existing = by_name.insert(labels, metric);
    debug_assert!(existing.is_none());
  }

  fn into_proto(self) -> anyhow::Result<StatsSnapshot> {
    // Convert the native merge representation only at the disk/transport boundary so callers do
    // not lose histogram behavior or label ordering while aggregating snapshots.
    let proto_metrics: Vec<ProtoMetric> = self
      .metrics
      .into_iter()
      .flat_map(|(name, metrics)| {
        metrics.into_iter().map(move |(labels, metric)| {
          Ok::<_, anyhow::Error>(ProtoMetric {
            metric_name_type: Some(match name.clone() {
              (_, NameType::Global(name)) => Metric_name_type::Name(name),
              (_, NameType::ActionId(id)) => Metric_name_type::MetricId(id),
            }),
            tags: labels.into_iter().collect(),
            data: Some(metric.to_proto()?),
            ..Default::default()
          })
        })
      })
      .try_collect()?;

    Ok(StatsSnapshot {
      snapshot_type: Some(Snapshot_type::Metrics(MetricsList {
        metric: proto_metrics,
        ..Default::default()
      })),
      metric_id_overflows: self.overflows,
      workflow_debug_data: self.workflow_debug_data,
      ..Default::default()
    })
  }
}

#[cfg(feature = "logger-cli-observer")]
fn observed_metric_value(metric: &MetricData) -> Option<ObservedMetricValue> {
  match metric.to_proto().ok()? {
    ProtoMetricData::Counter(counter) => Some(ObservedMetricValue::Counter(counter.value)),
    ProtoMetricData::InlineHistogramValues(values) => {
      Some(ObservedMetricValue::InlineHistogram(values.values))
    },
    ProtoMetricData::DdsketchHistogram(histogram) => Some(ObservedMetricValue::DdSketchHistogram {
      encoded_len: histogram.serialized.len(),
    }),
  }
}

#[cfg(feature = "logger-cli-observer")]
fn proto_metric_value(data: &ProtoMetricData) -> ObservedMetricValue {
  match data {
    ProtoMetricData::Counter(counter) => ObservedMetricValue::Counter(counter.value),
    ProtoMetricData::InlineHistogramValues(values) => {
      ObservedMetricValue::InlineHistogram(values.values.clone())
    },
    ProtoMetricData::DdsketchHistogram(histogram) => ObservedMetricValue::DdSketchHistogram {
      encoded_len: histogram.serialized.len(),
    },
  }
}

#[cfg(feature = "logger-cli-observer")]
fn snapshot_action_metrics(snapshot: &SnapshotHelper) -> Vec<ObservedMetric> {
  let mut observed_metrics = Vec::new();
  for ((_, name), metrics) in &snapshot.metrics {
    let NameType::ActionId(action_id) = name else {
      continue;
    };

    for (labels, metric) in metrics {
      if let Some(value) = observed_metric_value(metric) {
        observed_metrics.push(ObservedMetric {
          action_id: action_id.clone(),
          labels: labels.clone(),
          value,
        });
      }
    }
  }

  observed_metrics.sort_by(|left, right| {
    left
      .action_id
      .cmp(&right.action_id)
      .then_with(|| left.labels.cmp(&right.labels))
  });
  observed_metrics
}

#[cfg(feature = "logger-cli-observer")]
fn request_action_metrics(request: &StatsUploadRequest) -> Vec<ObservedMetric> {
  let mut observed_metrics = Vec::new();
  for snapshot in &request.snapshot {
    for metric in &snapshot.metrics().metric {
      let Some(Metric_name_type::MetricId(action_id)) = &metric.metric_name_type else {
        continue;
      };

      let Some(data) = &metric.data else {
        continue;
      };

      observed_metrics.push(ObservedMetric {
        action_id: action_id.clone(),
        labels: metric.tags.clone().into_iter().collect(),
        value: proto_metric_value(data),
      });
    }
  }

  observed_metrics.sort_by(|left, right| {
    left
      .action_id
      .cmp(&right.action_id)
      .then_with(|| left.labels.cmp(&right.labels))
  });
  observed_metrics
}

#[cfg(feature = "logger-cli-observer")]
fn observe_upload_attempt(request: &StatsUploadRequest, upload_reason: UploadReason) {
  with_observer(|observer| {
    let metrics = request_action_metrics(request);
    if !metrics.is_empty() {
      observer.on_upload_attempt(UploadAttemptObservation {
        upload_uuid: request.upload_uuid.clone(),
        upload_reason: format!("{upload_reason:?}"),
        metrics,
      });
    }
  });
}

#[cfg(not(feature = "logger-cli-observer"))]
fn observe_upload_attempt(_request: &StatsUploadRequest, _upload_reason: UploadReason) {}
