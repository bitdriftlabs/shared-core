// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![deny(
  clippy::expect_used,
  clippy::panic,
  clippy::todo,
  clippy::unimplemented,
  clippy::unreachable,
  clippy::unwrap_used
)]

mod file_manager;
#[cfg(feature = "logger-cli-observer")]
pub mod observer;
pub mod stats;
pub mod test;

use crate::stats::{Flusher, HandshakeStats, PeriodicSchedule};
use bd_api::DataUpload;
use bd_client_common::file_system::RealFileSystem;
use bd_client_stats_store::{Collector, Error as StatsError};
use bd_runtime::runtime::ConfigLoader;
use bd_shutdown::ComponentShutdown;
use bd_stats_common::Counter as _;
use bd_time::{SystemTimeProvider, TimeProvider};
use bd_workflow_stats::StatsCollector;
use bd_workflow_stats::workflow::WorkflowDebugKey;
use file_manager::FileManager;
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;

#[cfg(test)]
#[ctor::ctor(unsafe)]
fn test_global_init() {
  bd_test_helpers_core::test_global_init();
}

//
// FlushHandles
//

pub struct FlushHandles {
  pub flusher: Flusher,
  pub flush_trigger: FlushTrigger,
  pub handshake_stats: HandshakeStats,
}

//
// FlushCompletion
//

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FlushEpochOutcome {
  Pending,
  Durable,
  Failed,
  Shutdown,
}

pub struct FlushCompletion {
  outcome_rx: watch::Receiver<FlushEpochOutcome>,
}

impl FlushCompletion {
  pub async fn wait(mut self) -> anyhow::Result<()> {
    loop {
      let outcome = *self.outcome_rx.borrow_and_update();
      match outcome {
        FlushEpochOutcome::Pending => self
          .outcome_rx
          .changed()
          .await
          .map_err(|_| anyhow::anyhow!("stats flusher stopped before flush completed"))?,
        FlushEpochOutcome::Durable => return Ok(()),
        FlushEpochOutcome::Failed => {
          anyhow::bail!("stats flush failed before metrics became durable")
        },
        FlushEpochOutcome::Shutdown => {
          anyhow::bail!("stats flusher shut down before flush completed")
        },
      }
    }
  }

  // Test harnesses use this from synchronous setup code; production callers must await `wait`.
  pub fn blocking_wait_for_test(self) -> anyhow::Result<()> {
    futures::executor::block_on(self.wait())
  }
}

//
// FlushEpoch
//

pub(crate) struct FlushEpoch {
  outcome_tx: watch::Sender<FlushEpochOutcome>,
  do_upload: bool,
}

impl FlushEpoch {
  fn new() -> Self {
    let (outcome_tx, _) = watch::channel(FlushEpochOutcome::Pending);
    Self {
      outcome_tx,
      do_upload: false,
    }
  }

  fn register(&mut self, do_upload: bool) -> FlushCompletion {
    self.do_upload |= do_upload;
    FlushCompletion {
      outcome_rx: self.outcome_tx.subscribe(),
    }
  }

  pub(crate) const fn do_upload(&self) -> bool {
    self.do_upload
  }

  pub(crate) fn complete_durable(self) {
    self.outcome_tx.send_replace(FlushEpochOutcome::Durable);
  }

  pub(crate) fn fail(self) {
    self.outcome_tx.send_replace(FlushEpochOutcome::Failed);
  }
}

//
// FlushTriggerState
//

struct FlushTriggerState {
  open_epoch: FlushEpoch,
  wake_scheduled: bool,
}

//
// FlushTrigger
//

#[derive(Clone)]
pub struct FlushTrigger {
  flush_tx: Sender<()>,
  state: Arc<Mutex<FlushTriggerState>>,
}

impl std::fmt::Debug for FlushTrigger {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    formatter
      .debug_struct("FlushTrigger")
      .finish_non_exhaustive()
  }
}

impl FlushTrigger {
  #[must_use]
  pub fn new() -> (Self, tokio::sync::mpsc::Receiver<()>) {
    let (flush_tx, flush_rx) = tokio::sync::mpsc::channel(1);

    (
      Self {
        flush_tx,
        state: Arc::new(Mutex::new(FlushTriggerState {
          open_epoch: FlushEpoch::new(),
          wake_scheduled: false,
        })),
      },
      flush_rx,
    )
  }

  // Joins the current disk-durability epoch before scheduling work, so callers cannot miss the
  // completion that follows the physical write containing their metrics.
  pub fn flush(&self, do_upload: bool) -> anyhow::Result<FlushCompletion> {
    let mut state = self.state.lock();
    if self.flush_tx.is_closed() {
      anyhow::bail!("stats flusher shut down before flush was requested");
    }

    let completion = state.open_epoch.register(do_upload);
    if state.wake_scheduled {
      return Ok(completion);
    }

    match self.flush_tx.try_send(()) {
      Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Full(())) => {
        state.wake_scheduled = true;
        Ok(completion)
      },
      Err(tokio::sync::mpsc::error::TrySendError::Closed(())) => {
        anyhow::bail!("stats flusher shut down before flush was requested")
      },
    }
  }

  pub fn blocking_flush_for_test(&self, do_upload: bool) -> anyhow::Result<FlushCompletion> {
    self.flush(do_upload)
  }

  pub(crate) fn begin_disk_flush(&self) -> FlushEpoch {
    let mut state = self.state.lock();
    state.wake_scheduled = false;
    std::mem::replace(&mut state.open_epoch, FlushEpoch::new())
  }

  pub(crate) fn fail_open_epoch(&self) {
    let state = self.state.lock();
    state
      .open_epoch
      .outcome_tx
      .send_replace(FlushEpochOutcome::Shutdown);
  }
}

//
// Stats
//

pub struct Stats {
  collector: Collector,
  overflows: Mutex<HashMap<String, u64>>,
  workflow_debug_data: Mutex<HashMap<WorkflowDebugKey, u64>>,
}

impl Stats {
  #[must_use]
  pub fn new(collector: Collector) -> Arc<Self> {
    Arc::new(Self {
      collector,
      overflows: Mutex::default(),
      workflow_debug_data: Mutex::default(),
    })
  }

  /// Creates a flush handle that can be used to periodically flush the stats store.
  pub fn flush_handle(
    self: &Arc<Self>,
    runtime_loader: &Arc<ConfigLoader>,
    periodic_schedule: Box<dyn PeriodicSchedule>,
    shutdown: ComponentShutdown,
    sdk_directory: &Path,
    data_flush_tx: tokio::sync::mpsc::Sender<DataUpload>,
    time_provider: Arc<dyn TimeProvider>,
  ) -> FlushHandles {
    let minimum_upload_interval = runtime_loader.register_duration_watch();
    let disk_flush_debounce = runtime_loader.register_duration_watch();
    let file_manager = Arc::new(FileManager::new(
      Box::new(RealFileSystem::new(sdk_directory.to_path_buf())),
      Arc::new(SystemTimeProvider),
      runtime_loader,
    ));
    self.flush_handle_helper(
      periodic_schedule,
      shutdown,
      data_flush_tx,
      file_manager,
      time_provider,
      minimum_upload_interval,
      disk_flush_debounce,
    )
  }

  fn flush_handle_helper(
    self: &Arc<Self>,
    periodic_schedule: Box<dyn PeriodicSchedule>,
    shutdown: ComponentShutdown,
    data_flush_tx: tokio::sync::mpsc::Sender<DataUpload>,
    fs: Arc<FileManager>,
    time_provider: Arc<dyn TimeProvider>,
    minimum_upload_interval: bd_runtime::runtime::DurationWatch<
      bd_runtime::runtime::stats::MinimumUploadIntervalFlag,
    >,
    disk_flush_debounce: bd_runtime::runtime::DurationWatch<
      bd_runtime::runtime::stats::DiskFlushDebounceFlag,
    >,
  ) -> FlushHandles {
    let flush_time_histogram = self.collector.scope("stats").histogram("flush_time");
    let (flush_trigger, flush_rx) = FlushTrigger::new();
    // HandshakeStats is owned by the API task while Flusher runs in a sibling task. Each
    // API-originated upload creates a fresh tracked request whose sender belongs to the stream
    // StateTracker, but its receiver must be consumed by Flusher to complete or abandon claimed
    // files. This handoff is dynamic because reconnects create new StateTrackers and batches. At
    // most one receiver may wait to be registered; a full channel backpressures handshake
    // preparation until Flusher can take ownership rather than retaining unbounded receivers.
    let (api_upload_completion_tx, api_upload_completion_rx) = tokio::sync::mpsc::channel(1);

    FlushHandles {
      flusher: Flusher::new(
        self.clone(),
        shutdown,
        periodic_schedule,
        flush_rx,
        flush_time_histogram,
        data_flush_tx,
        fs.clone(),
        time_provider.clone(),
        minimum_upload_interval,
        disk_flush_debounce,
        api_upload_completion_rx,
        flush_trigger.clone(),
      ),
      flush_trigger,
      handshake_stats: HandshakeStats::new(fs, time_provider, api_upload_completion_tx),
    }
  }

  fn handle_overflow(&self, id: &str) {
    log::debug!("dynamic metrics overflow");
    self
      .overflows
      .lock()
      .entry(id.to_string())
      .and_modify(|e| *e += 1)
      .or_insert(1);
  }

  pub fn collector(&self) -> &Collector {
    &self.collector
  }

  pub fn take_workflow_debug_data(&self) -> HashMap<WorkflowDebugKey, u64> {
    std::mem::take(&mut *self.workflow_debug_data.lock())
  }
}

impl StatsCollector for Stats {
  type Counter = bd_client_stats_store::Counter;
  type Histogram = bd_client_stats_store::Histogram;

  fn record_workflow_debug_state(&self, state: Vec<WorkflowDebugKey>) {
    log::trace!("recording workflow debug state: {state:?}");
    let mut workflow_debug_data = self.workflow_debug_data.lock();
    for key in state {
      workflow_debug_data
        .entry(key)
        .and_modify(|e| *e += 1)
        .or_insert(1);
    }
  }

  fn record_dynamic_counter(&self, tags: BTreeMap<String, String>, id: &str, value: u64) {
    log::debug!("recording dynamic counter: id={id}, value={value}, tags={tags:?}");
    if let Some(counter) = self.workflow_dynamic_counter(tags, id) {
      counter.inc_by(value);
    }
  }

  fn workflow_dynamic_counter(
    &self,
    tags: BTreeMap<String, String>,
    id: &str,
  ) -> Option<Self::Counter> {
    match self.collector.dynamic_counter(tags, id) {
      Ok(counter) => Some(counter),
      Err(StatsError::Overflow) => {
        self.handle_overflow(id);
        None
      },
    }
  }

  fn workflow_dynamic_histogram(
    &self,
    tags: BTreeMap<String, String>,
    id: &str,
  ) -> Option<Self::Histogram> {
    match self.collector.dynamic_histogram(tags, id) {
      Ok(histogram) => Some(histogram),
      Err(StatsError::Overflow) => {
        self.handle_overflow(id);
        None
      },
    }
  }
}
