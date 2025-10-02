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
use crate::{FlushTriggerCompletionSender, Stats};
use async_trait::async_trait;
use bd_api::DataUpload;
use bd_api::upload::{TrackedStatsUploadRequest, UploadResponse};
use bd_client_common::{maybe_await, maybe_await_interval};
use bd_client_stats_store::{Collector, Histogram, MetricData, MetricsByNameCore};
use bd_error_reporter::reporter::handle_unexpected;
use bd_proto::protos::client::api::stats_upload_request::Snapshot as StatsSnapshot;
use bd_proto::protos::client::api::stats_upload_request::snapshot::Snapshot_type;
use bd_proto::protos::client::api::{StatsUploadRequest, debug_data_request};
use bd_proto::protos::client::metric::metric::Metric_name_type;
use bd_proto::protos::client::metric::{Metric as ProtoMetric, MetricsList};
use bd_shutdown::ComponentShutdown;
use bd_stats_common::workflow::{WorkflowDebugStateKey, WorkflowDebugTransitionType};
use bd_stats_common::{MetricType, NameType};
use bd_time::TimeDurationExt;
use debug_data_request::workflow_transition_debug_data::Transition_type;
use debug_data_request::{WorkflowDebugData, WorkflowTransitionDebugData};
use itertools::Itertools;
#[cfg(test)]
use stats_test::{TestHooks, TestHooksReceiver};
use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;
use std::sync::Arc;
use time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::MissedTickBehavior;

//
// Ticker
//

#[async_trait]
pub trait Ticker: Send + Sync {
  async fn tick(&mut self);
}

//
// RuntimeWatchTicker
//

pub struct RuntimeWatchTicker {
  receiver: watch::Receiver<Duration>,
  interval: Option<tokio::time::Interval>,
}

impl RuntimeWatchTicker {
  #[must_use]
  pub const fn new(receiver: watch::Receiver<Duration>) -> Self {
    Self {
      receiver,
      interval: None,
    }
  }
}

#[async_trait]
impl Ticker for RuntimeWatchTicker {
  async fn tick(&mut self) {
    // We use jittered_interval_at() to make sure we stagger the start time to avoid synchronization
    // during mass reconnect.
    if self.interval.is_none() {
      self.interval = Some(
        self
          .receiver
          .borrow_and_update()
          .jittered_interval_at(MissedTickBehavior::Delay),
      );
    }

    loop {
      tokio::select! {
        () = maybe_await_interval(self.interval.as_mut()) => break,
        _ = self.receiver.changed() => {
          self.interval = Some(
            self.receiver.borrow_and_update().jittered_interval_at(MissedTickBehavior::Delay)
          );
        },
      }
    }
  }
}

//
// SleepModeAwareRuntimeWatchTicker
//

pub trait IntervalCreator: Send + Sync {
  fn interval(duration: Duration) -> tokio::time::Interval;
}
pub struct JitteredIntervalCreator;
impl IntervalCreator for JitteredIntervalCreator {
  fn interval(duration: Duration) -> tokio::time::Interval {
    duration.jittered_interval_at(MissedTickBehavior::Delay)
  }
}
pub struct SleepModeAwareRuntimeWatchTicker<T> {
  live_mode_receiver: watch::Receiver<Duration>,
  sleep_mode_receiver: watch::Receiver<Duration>,
  sleep_mode_active: watch::Receiver<bool>,
  interval: Option<tokio::time::Interval>,
  phantom: PhantomData<T>,
}

impl<T: IntervalCreator> SleepModeAwareRuntimeWatchTicker<T> {
  #[must_use]
  pub const fn new(
    live_mode_receiver: watch::Receiver<Duration>,
    sleep_mode_receiver: watch::Receiver<Duration>,
    sleep_mode_active: watch::Receiver<bool>,
  ) -> Self {
    Self {
      live_mode_receiver,
      sleep_mode_receiver,
      sleep_mode_active,
      interval: None,
      phantom: PhantomData,
    }
  }
}

#[async_trait]
impl<T: IntervalCreator> Ticker for SleepModeAwareRuntimeWatchTicker<T> {
  async fn tick(&mut self) {
    loop {
      // Initialize the interval if it doesn't exist
      if self.interval.is_none() {
        // Choose interval duration based on sleep mode status
        let duration = if *self.sleep_mode_active.borrow() {
          log::trace!("sleep mode active, using sleep mode duration");
          *self.sleep_mode_receiver.borrow_and_update()
        } else {
          log::trace!("sleep mode inactive, using live mode duration");
          *self.live_mode_receiver.borrow_and_update()
        };

        self.interval = Some(T::interval(duration));
      }

      tokio::select! {
        () = maybe_await_interval(self.interval.as_mut()) => break,
        _ = self.live_mode_receiver.changed() => {
          if !*self.sleep_mode_active.borrow() {
            // Only update if we're using live mode
            self.interval = None;
          }
        },
        _ = self.sleep_mode_receiver.changed() => {
          if *self.sleep_mode_active.borrow() {
            // Only update if we're using sleep mode
            self.interval = None;
          }
        },
        _ = self.sleep_mode_active.changed() => {
          // TODO(mattklein123): Potentially we should consider firing immediately if we change
          // from sleep mode to live mode, but given that we use a jittered interval it should
          // happen soon enough so seems ok to just let it re-init.
          self.interval = None;
        },
      }
    }
  }
}

//
// Flusher
//

/// Responsible for periodically flushing the stats store to a locally aggregated file.
pub struct Flusher {
  stats: Arc<Stats>,
  shutdown: ComponentShutdown,
  flush_ticker: Box<dyn Ticker>,
  flush_rx: tokio::sync::mpsc::Receiver<FlushTriggerCompletionSender>,
  flush_time_histogram: Histogram,
  upload_ticker: Box<dyn Ticker>,
  data_flush_tx: mpsc::Sender<DataUpload>,
  file_manager: Arc<FileManager>,

  #[cfg(test)]
  test_hooks: TestHooks,
}

impl Flusher {
  pub fn new(
    stats: Arc<Stats>,
    shutdown: ComponentShutdown,
    flush_ticker: Box<dyn Ticker>,
    flush_rx: tokio::sync::mpsc::Receiver<FlushTriggerCompletionSender>,
    flush_time_histogram: Histogram,
    upload_ticker: Box<dyn Ticker>,
    data_flush_tx: mpsc::Sender<DataUpload>,
    file_manager: Arc<FileManager>,
  ) -> Self {
    Self {
      stats,
      shutdown,
      flush_ticker,
      flush_rx,
      flush_time_histogram,
      upload_ticker,
      data_flush_tx,
      file_manager,

      #[cfg(test)]
      test_hooks: TestHooks::default(),
    }
  }

  #[cfg(test)]
  pub const fn test_hooks(&mut self) -> TestHooksReceiver {
    self.test_hooks.receiver.take().unwrap()
  }

  pub async fn periodic_flush(mut self) {
    let mut upload_rx = None;
    loop {
      tokio::select! {
        Some(completion_tx) = self.flush_rx.recv() => {
          log::debug!("received a signal to flush stats to disk");
          self.flush_to_disk().await;
          log::debug!("stats flushed");

          if let Some(completion_tx) = completion_tx {
            completion_tx.send(());
          }
        },
        () = self.shutdown.cancelled() => return,
        () = self.flush_ticker.tick() => self.flush_to_disk().await,
        () = self.upload_ticker.tick() => {
          if upload_rx.is_some() {
            log::debug!("upload already in progress, skipping");
            continue;
          }

          upload_rx = self.upload_from_disk(false).await;
        },
        upload_result = maybe_await(&mut upload_rx) => {
          upload_rx = self.process_pending_upload_completion(
            upload_result.unwrap_or(UploadResponse { success: false, uuid: String::new() })
          ).await;

          #[cfg(test)]
          self.test_hooks.sender.upload_complete_tx.send(()).await.unwrap();
        },
      };
    }
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
    let mut new_or_existing_snapshot = SnapshotHelper::new(handle.snapshot(), self.stats.limit());

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
      existing
        .start_reset
        .mut_or_insert_default()
        .transition_count += debug_data.start_reset.transition_count;
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
    let mut snapshot = SnapshotHelper::new(None, self.stats.limit());
    Self::snap_collector_to_snapshot(&self.stats.collector, &mut snapshot);
    snapshot.overflows = std::mem::take(&mut self.stats.overflows.lock());

    let workflow_debug_data = std::mem::take(&mut *self.stats.workflow_debug_data.lock());
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
            .transition_count += count;
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
  ) -> Option<oneshot::Receiver<UploadResponse>> {
    async fn inner(
      flusher: &Flusher,
      only_if_file_is_old: bool,
    ) -> anyhow::Result<Option<oneshot::Receiver<UploadResponse>>> {
      if let Some(pending_upload) = flusher
        .file_manager
        .get_or_create_pending_upload(only_if_file_is_old)
        .await?
      {
        return flusher.process_pending_upload(pending_upload).await;
      }
      Ok(None)
    }

    // Note on error handling: while we could probably gracefully handle some of the failing I/O
    // operations, it is likely to result in inaccurate stats (double submission of stats, missing
    // aggregations, etc.), so we bail on failure. As we start seeing this out in the wild we may
    // get a better understanding of why things are failing at which point we can do more targeted
    // error handling.
    log::debug!("processing upload from disk");
    match inner(self, only_if_file_is_old).await {
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
    request: StatsUploadRequest,
  ) -> anyhow::Result<Option<oneshot::Receiver<UploadResponse>>> {
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

    let tracked_upload = DataUpload::StatsUpload(stats);

    // If this errors out the other end of the channel has closed, indicating that we are shutting
    // down.
    if self.data_flush_tx.send(tracked_upload).await.is_err() {
      return Ok(None);
    }

    Ok(Some(response_rx))
  }

  async fn process_pending_upload_completion(
    &self,
    upload_response: UploadResponse,
  ) -> Option<oneshot::Receiver<UploadResponse>> {
    log::debug!("stat upload attempt complete: {upload_response:?}");
    if upload_response.success {
      // If this fails we are in a bad state and are likely going to end up double uploading, but
      // there is little we can do about it.
      handle_unexpected(
        self
          .file_manager
          .complete_pending_upload(&upload_response.uuid)
          .await,
        "complete pending upload",
      );
      // During startup or after getting network connectivity back it's possible that we will have
      // a number of pending uploads to process. Go ahead and see if we have an old file at the
      // head of the list which we should upload immediately.
      // TODO(mattklein123): It would be better to batch all of the "old" files into a single
      // upload request. We can do this in the future.
      return self.upload_from_disk(true).await;
    }
    None
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
