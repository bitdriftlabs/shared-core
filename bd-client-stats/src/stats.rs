// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./stats_test.rs"]
mod stats_test;

use crate::file_manager::FileManager;
#[cfg(feature = "logger-cli-observer")]
use crate::observer::{
  ObservedMetric,
  ObservedMetricValue,
  SnapshotObservation,
  UploadAckObservation,
  UploadAttemptObservation,
  with_observer,
};
use crate::{FlushTriggerRequest, Stats};
use async_trait::async_trait;
use bd_api::DataUpload;
use bd_api::upload::{TrackedStatsUploadRequest, UploadResponse};
use bd_client_stats_store::{Collector, Histogram, MetricData, MetricsByNameCore};
use bd_error_reporter::reporter::handle_unexpected;
use bd_proto::protos::client::api::stats_upload_request::snapshot::Snapshot_type;
use bd_proto::protos::client::api::stats_upload_request::{
  Snapshot as StatsSnapshot,
  UploadReason,
};
use bd_proto::protos::client::api::{StatsUploadRequest, debug_data_request};
#[cfg(feature = "logger-cli-observer")]
use bd_proto::protos::client::metric::metric::Data as ProtoMetricData;
use bd_proto::protos::client::metric::metric::Metric_name_type;
use bd_proto::protos::client::metric::{Metric as ProtoMetric, MetricsList};
use bd_shutdown::ComponentShutdown;
use bd_stats_common::workflow::{WorkflowDebugStateKey, WorkflowDebugTransitionType};
use bd_stats_common::{MetricType, NameType};
use bd_time::{TimeDurationExt, TimeProvider};
use debug_data_request::workflow_transition_debug_data::Transition_type;
use debug_data_request::{WorkflowDebugData, WorkflowTransitionDebugData};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use itertools::Itertools;
#[cfg(test)]
use stats_test::{TestHooks, TestHooksReceiver};
use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};
use tokio::sync::{mpsc, oneshot, watch};

type UploadFuture =
  Pin<Box<dyn std::future::Future<Output = (UploadResponse, UploadContext)> + Send + Sync>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PeriodicAction {
  Flush,
  Upload,
}

enum UploadContext {
  Periodic,
  Flush(FlushTriggerRequest),
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
}

// Couples the stats flush and upload timers into one schedule driven by the active upload
// interval. The scheduler always reasons about one upload cycle at a time and may insert one or
// more intermediate flush deadlines before the next upload deadline when the configured flush
// interval is a clean divisor of the active upload interval.
//
// Startup is deterministic: the first cycle is scheduled immediately from the current runtime
// values, but without jitter. That means we do not wait for some extra startup-only delay; we
// simply avoid randomizing the initial deadline. We only re-jitter upload deadlines when the
// active upload interval changes at runtime, which keeps reconnect storms from synchronizing.
pub struct RuntimePeriodicSchedule {
  flush_interval: watch::Receiver<Duration>,
  live_upload_interval: watch::Receiver<Duration>,
  sleep_upload_interval: watch::Receiver<Duration>,
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
    sleep_mode_active: watch::Receiver<bool>,
    time_provider: Arc<dyn TimeProvider>,
  ) -> Self {
    Self {
      flush_interval,
      live_upload_interval,
      sleep_upload_interval,
      sleep_mode_active,
      time_provider,
      state: None,
    }
  }

  // Snap the current runtime inputs into a fresh cycle. This is called at startup and whenever a
  // watched runtime value changes in a way that should re-plan future deadlines.
  fn rebuild_schedule(&mut self, jitter_upload_deadline: bool) {
    let now = self.time_provider.now();

    // Flush cadence is always read directly from runtime, but upload cadence depends on whether
    // we are currently in live mode or sleep mode.
    let flush_interval = *self.flush_interval.borrow_and_update();
    let upload_interval = self.active_upload_interval();
    let effective_flush_interval = effective_flush_interval(flush_interval, upload_interval);

    // An invalid flush cadence does not stop scheduling; it simply means we only flush at the
    // upload boundary for this cycle.
    if effective_flush_interval != flush_interval {
      log::debug!(
        "stats disk flush interval {flush_interval} does not cleanly divide active upload \
         interval {upload_interval}; falling back to upload cadence"
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
    let preserved_upload_at = (!jitter_upload_deadline)
      .then(|| {
        self.state.as_ref().and_then(|state| {
          (state.upload_interval == upload_interval && state.next_upload_at > now)
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
          upload_interval
            .jittered()
            .try_into()
            .unwrap_or(upload_interval)
        } else {
          upload_interval
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

  // Advances the in-memory schedule after we decide which action to run. Flushes preserve the
  // current upload boundary, while uploads roll the entire cycle forward.
  fn update_after_action(&mut self, action: PeriodicAction) {
    let now = self.time_provider.now();
    let Some(state) = self.state.as_mut() else {
      return;
    };

    match action {
      PeriodicAction::Flush => {
        let Some(next_flush_at) = state.next_flush_at else {
          return;
        };

        // Keep generating intermediate flushes until the next one would land on or after the
        // upload deadline. At that point the upload tick owns the final flush for the cycle.
        let candidate = next_flush_at + state.effective_flush_interval;
        state.next_flush_at = (candidate < state.next_upload_at).then_some(candidate);
      },
      // Upload always performs a flush first, so the next cycle only needs intermediate flushes.
      PeriodicAction::Upload => {
        state.next_upload_at = now + state.upload_interval;
        state.next_flush_at = (state.effective_flush_interval < state.upload_interval)
          .then_some(now + state.effective_flush_interval);
      },
    }
  }
}

#[async_trait]
impl PeriodicSchedule for RuntimePeriodicSchedule {
  async fn next_action(&mut self) -> PeriodicAction {
    loop {
      // Lazily build the first cycle so construction stays cheap and so startup uses the latest
      // runtime values at the first point we actually need to schedule work.
      if self.state.is_none() {
        self.rebuild_schedule(false);
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
          self.rebuild_schedule(false);
        },
        changed = live_upload_interval.changed() => {
          if changed.is_err() {
            continue;
          }
          self.live_upload_interval = live_upload_interval;

          // Only re-jitter if the active upload cadence actually changed. In sleep mode, a live
          // mode update is just cached state for later.
          let active_upload_interval = if *self.sleep_mode_active.borrow() {
            *self.sleep_upload_interval.borrow()
          } else {
            *self.live_upload_interval.borrow()
          };
          let upload_interval_changed = self
            .state
            .as_ref()
            .is_some_and(|state| state.upload_interval != active_upload_interval);
          self.rebuild_schedule(upload_interval_changed);
        },
        changed = sleep_upload_interval.changed() => {
          if changed.is_err() {
            continue;
          }
          self.sleep_upload_interval = sleep_upload_interval;

          // Symmetric to the live-mode branch: a sleep-mode update only matters immediately when
          // sleep mode is active, otherwise it is just stored until we transition modes.
          let active_upload_interval = if *self.sleep_mode_active.borrow() {
            *self.sleep_upload_interval.borrow()
          } else {
            *self.live_upload_interval.borrow()
          };
          let upload_interval_changed = self
            .state
            .as_ref()
            .is_some_and(|state| state.upload_interval != active_upload_interval);
          self.rebuild_schedule(upload_interval_changed);
        },
        changed = sleep_mode_active.changed() => {
          if changed.is_err() {
            continue;
          }
          self.sleep_mode_active = sleep_mode_active;

          // A mode transition can swap which upload watch is authoritative, so treat it like an
          // upload cadence change when the newly active interval differs from the current cycle.
          let active_upload_interval = if *self.sleep_mode_active.borrow() {
            *self.sleep_upload_interval.borrow()
          } else {
            *self.live_upload_interval.borrow()
          };
          let upload_interval_changed = self
            .state
            .as_ref()
            .is_some_and(|state| state.upload_interval != active_upload_interval);
          self.rebuild_schedule(upload_interval_changed);
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
  flush_rx: tokio::sync::mpsc::Receiver<FlushTriggerRequest>,
  flush_time_histogram: Histogram,
  data_flush_tx: mpsc::Sender<DataUpload>,
  file_manager: Arc<FileManager>,
  uploads: FuturesUnordered<UploadFuture>,
  periodic_in_flight: bool,
  flush_in_flight: bool,
  // This uses system time to allow integration tests to work. It should really use monotonic time.
  last_flush_upload_time: Option<time::OffsetDateTime>,
  time_provider: Arc<dyn TimeProvider>,
  minimum_upload_interval:
    bd_runtime::runtime::DurationWatch<bd_runtime::runtime::stats::MinimumUploadIntervalFlag>,

  #[cfg(test)]
  test_hooks: TestHooks,
}

impl Flusher {
  pub fn new(
    stats: Arc<Stats>,
    shutdown: ComponentShutdown,
    periodic_schedule: Box<dyn PeriodicSchedule>,
    flush_rx: tokio::sync::mpsc::Receiver<FlushTriggerRequest>,
    flush_time_histogram: Histogram,
    data_flush_tx: mpsc::Sender<DataUpload>,
    file_manager: Arc<FileManager>,
    time_provider: Arc<dyn TimeProvider>,
    minimum_upload_interval: bd_runtime::runtime::DurationWatch<
      bd_runtime::runtime::stats::MinimumUploadIntervalFlag,
    >,
  ) -> Self {
    Self {
      stats,
      shutdown,
      periodic_schedule,
      flush_rx,
      flush_time_histogram,
      data_flush_tx,
      file_manager,
      uploads: FuturesUnordered::new(),
      periodic_in_flight: false,
      flush_in_flight: false,
      last_flush_upload_time: None,
      time_provider,
      minimum_upload_interval,

      #[cfg(test)]
      test_hooks: TestHooks::default(),
    }
  }

  #[cfg(test)]
  pub const fn test_hooks(&mut self) -> TestHooksReceiver {
    self.test_hooks.receiver.take().unwrap()
  }

  fn should_skip_upload(&self) -> bool {
    self.last_flush_upload_time.is_some_and(|last_upload| {
      let now = self.time_provider.now();
      let elapsed = (now - last_upload).unsigned_abs();
      let min_interval = self.minimum_upload_interval.read().unsigned_abs();
      elapsed < min_interval
    })
  }

  pub async fn periodic_flush(mut self) {
    self.startup_upload_if_old().await;

    loop {
      tokio::select! {
        Some(request) = self.flush_rx.recv() => {
          self.handle_flush_request(request).await;
        },
        () = self.shutdown.cancelled() => return,
        action = self.periodic_schedule.next_action() => {
          match action {
            PeriodicAction::Flush => self.flush_to_disk().await,
            PeriodicAction::Upload => self.handle_periodic_upload_tick().await,
          }
        },
        Some((upload_response, context)) = self.uploads.next() => {
          self.handle_upload_completion(upload_response, context).await;
        },
      };
    }
  }

  async fn startup_upload_if_old(&mut self) {
    if let Some((uuid, rx)) = self
      .upload_from_disk(true, UploadReason::UPLOAD_REASON_PERIODIC)
      .await
    {
      log::debug!("starting old-file stats upload during startup");
      self.periodic_in_flight = true;
      self.push_upload_future(uuid, rx, UploadContext::Periodic);
    }
  }

  async fn handle_periodic_upload_tick(&mut self) {
    self.flush_to_disk().await;

    self.handle_upload_tick().await;
  }

  async fn handle_upload_tick(&mut self) {
    if self.periodic_in_flight {
      log::debug!("upload already in progress, skipping");
      return;
    }

    if self.should_skip_upload() {
      log::debug!("skipping periodic upload, minimum interval not elapsed");
      return;
    }

    if let Some((uuid, rx)) = self
      .upload_from_disk(false, UploadReason::UPLOAD_REASON_PERIODIC)
      .await
    {
      self.periodic_in_flight = true;
      self.push_upload_future(uuid, rx, UploadContext::Periodic);
    }
  }

  async fn handle_flush_request(&mut self, request: FlushTriggerRequest) {
    // TODO(mattklein123): Currently we just ignore flush requests if one is already in flight.
    // We could consider queueing them up and processing them one after another, but given that
    // flushes are relatively infrequent this seems ok for now.
    if self.flush_in_flight {
      log::debug!("flush already in progress, skipping");
      return;
    }

    log::debug!("received a signal to flush stats to disk");
    self.flush_to_disk().await;
    log::debug!("stats flushed");

    if !request.do_upload {
      if let Some(tx) = request.completion_tx {
        let () = tx.send(());
      }
      return;
    }

    if self.should_skip_upload() {
      log::debug!("skipping flush upload, minimum interval not elapsed");
      if let Some(tx) = request.completion_tx {
        let () = tx.send(());
      }
      return;
    }

    if let Some((uuid, rx)) = self
      .upload_from_disk(false, UploadReason::UPLOAD_REASON_EVENT_TRIGGERED)
      .await
    {
      self.last_flush_upload_time = Some(self.time_provider.now());
      self.flush_in_flight = true;
      self.push_upload_future(uuid, rx, UploadContext::Flush(request));
    } else if let Some(tx) = request.completion_tx {
      let () = tx.send(());
    }
  }

  async fn handle_upload_completion(
    &mut self,
    upload_response: UploadResponse,
    context: UploadContext,
  ) {
    if matches!(context, UploadContext::Flush(_)) && !upload_response.success {
      // Clear the flush upload gate on failure so a later background or explicit flush can retry.
      self.last_flush_upload_time = None;
    }

    self
      .process_pending_upload_completion(&upload_response)
      .await;

    #[cfg(test)]
    self
      .test_hooks
      .sender
      .upload_complete_tx
      .send(())
      .await
      .unwrap();

    match context {
      UploadContext::Periodic => {
        if upload_response.success {
          // During startup or after getting network connectivity back it's possible that we will
          // have a number of pending uploads to process. Go ahead and see if we have an
          // old file at the head of the list which we should upload immediately.
          // TODO(mattklein123): It would be better to batch all of the "old" files into a single
          // upload request. We can do this in the future.
          if let Some((uuid, rx)) = self
            .upload_from_disk(true, UploadReason::UPLOAD_REASON_PERIODIC)
            .await
          {
            // Old file uploads bypass the minimum interval check, so don't consult the flush gate.
            self.push_upload_future(uuid, rx, UploadContext::Periodic);
          } else {
            self.periodic_in_flight = false;
          }
        } else {
          self.periodic_in_flight = false;
        }
      },
      UploadContext::Flush(request) => {
        self.flush_in_flight = false;
        if let Some(tx) = request.completion_tx {
          let () = tx.send(());
        }
      },
    }
  }

  fn push_upload_future(
    &self,
    uuid: String,
    rx: oneshot::Receiver<UploadResponse>,
    context: UploadContext,
  ) {
    self.uploads.push(Box::pin(async move {
      let res = rx.await.unwrap_or(UploadResponse {
        success: false,
        uuid,
      });
      (res, context)
    }));
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
      log::debug!("merging workflow debug data for {workflow_id}");
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
        log::debug!("merging workflow debug state for {state_id}");
        let existing_state = existing.states.entry(state_id).or_default();
        for transition in state_data.transitions {
          log::debug!(
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

    // If there are no metrics or workflow debug state in the snapshot after merging in the latest
    // delta, skip writing the aggregated snapshot to prevent empty stats uploads.
    if new_or_existing_snapshot.metrics.is_empty()
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

  async fn flush_to_disk(&self) {
    log::debug!("processing flush to disk tick");
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
    if let Err(e) = self.merge_delta_snapshot_to_disk(delta_snapshot).await {
      handle_unexpected::<(), anyhow::Error>(Err(e), "writing stats to disk");
    }

    #[cfg(test)]
    self
      .test_hooks
      .sender
      .flush_complete_tx
      .send(())
      .await
      .unwrap();
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
  ) -> Option<(String, oneshot::Receiver<UploadResponse>)> {
    async fn inner(
      flusher: &Flusher,
      only_if_file_is_old: bool,
      upload_reason: UploadReason,
    ) -> anyhow::Result<Option<(String, oneshot::Receiver<UploadResponse>)>> {
      if let Some(pending_upload) = flusher
        .file_manager
        .get_or_create_pending_upload(only_if_file_is_old)
        .await?
      {
        return flusher
          .process_pending_upload(pending_upload, upload_reason)
          .await;
      }
      Ok(None)
    }

    // Note on error handling: while we could probably gracefully handle some of the failing I/O
    // operations, it is likely to result in inaccurate stats (double submission of stats, missing
    // aggregations, etc.), so we bail on failure. As we start seeing this out in the wild we may
    // get a better understanding of why things are failing at which point we can do more targeted
    // error handling.
    log::debug!("processing upload from disk");
    match inner(self, only_if_file_is_old, upload_reason).await {
      Ok(result) => result,
      Err(e) => {
        handle_unexpected::<(), anyhow::Error>(Err(e), "upload from disk");
        None
      },
    }
  }

  // Attempts to upload the provided stats request. Upon success, the file containing the pending
  // request will be deleted.
  async fn process_pending_upload(
    &self,
    mut request: StatsUploadRequest,
    upload_reason: UploadReason,
  ) -> anyhow::Result<Option<(String, oneshot::Receiver<UploadResponse>)>> {
    #[cfg(feature = "logger-cli-observer")]
    let upload_reason_name = format!("{upload_reason:?}");
    request.upload_reason = upload_reason.into();
    let (stats, response_rx) = TrackedStatsUploadRequest::new(request.upload_uuid.clone(), request);

    log::debug!(
      "sending pending flush upload: {} with {} metrics",
      stats.payload.upload_uuid,
      stats
        .payload
        .snapshot
        .iter()
        .map(|s| s.metrics().metric.len())
        .sum::<usize>(),
    );

    #[cfg(feature = "logger-cli-observer")]
    with_observer(|observer| {
      let metrics = request_action_metrics(&stats.payload);
      if !metrics.is_empty() {
        observer.on_upload_attempt(UploadAttemptObservation {
          upload_uuid: stats.payload.upload_uuid.clone(),
          upload_reason: upload_reason_name.clone(),
          metrics,
        });
      }
    });

    let uuid = stats.payload.upload_uuid.clone();
    let tracked_upload = DataUpload::StatsUpload(stats);

    // If this errors out the other end of the channel has closed, indicating that we are shutting
    // down.
    if self.data_flush_tx.send(tracked_upload).await.is_err() {
      return Ok(None);
    }

    Ok(Some((uuid, response_rx)))
  }

  async fn process_pending_upload_completion(&self, upload_response: &UploadResponse) {
    log::debug!("stat upload attempt complete: {upload_response:?}");

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
        .complete_pending_upload(&upload_response.uuid, upload_response.success)
        .await,
      "complete pending upload",
    );
  }
}

//
// SnapshotHelper
//

struct SnapshotHelper {
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
