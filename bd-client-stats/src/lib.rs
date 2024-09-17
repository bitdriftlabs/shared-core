// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

pub mod stats;

use crate::stats::{Flusher, RealSerializedFileSystem, SerializedFileSystem, Uploader};
use anyhow::anyhow;
use bd_api::DataUpload;
use bd_client_common::error::handle_unexpected;
use bd_client_stats_store::{
  BoundedCollector,
  BoundedScope,
  Collector,
  Counter,
  Error as StatsError,
  Scope,
};
use bd_runtime::runtime::ConfigLoader;
use bd_shutdown::ComponentShutdown;
use bd_time::{SystemTimeProvider, TimeProvider};
use std::collections::BTreeMap;
use std::fmt::Formatter;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

#[cfg(test)]
#[ctor::ctor]
fn test_global_init() {
  bd_test_helpers::test_global_init();
}

//
// FlushHandles
//

pub struct FlushHandles<T: TimeProvider, F: SerializedFileSystem> {
  pub flusher: Flusher<T, F>,
  pub flush_trigger: FlushTrigger,
  pub uploader: Uploader<F>,
}

//
// FlushTriggerCompletionSender
//

type FlushTriggerCompletionSender = Option<bd_completion::Sender<()>>;

//
// FlushTrigger
//

#[derive(Clone, Debug)]
pub struct FlushTrigger {
  flush_tx: Sender<FlushTriggerCompletionSender>,
}

impl FlushTrigger {
  #[must_use]
  pub fn new() -> (
    Self,
    tokio::sync::mpsc::Receiver<FlushTriggerCompletionSender>,
  ) {
    let (flush_tx, flush_rx) = tokio::sync::mpsc::channel::<FlushTriggerCompletionSender>(1);

    (Self { flush_tx }, flush_rx)
  }

  // Signals the SDK to flush stats to disk and waits for the operation to complete before
  // returning.
  pub async fn flush(&self, completion_tx: FlushTriggerCompletionSender) -> anyhow::Result<()> {
    self
      .flush_tx
      .send(completion_tx)
      .await
      .map_err(|e| anyhow::anyhow!("failed to send flush stats trigger: {e}"))
  }
}

//
// DynamicStats
//

/// Manages caching dynamic stat handles in a way that avoids having to know the stat name
/// statically. This is helpful to support stats that are dynamically created as part of
/// remote configuration.
pub struct DynamicStats {
  dynamic_collector: BoundedCollector,
  dynamic_scope: BoundedScope,
  dynamic_stats_overflow: Counter,
}

impl std::fmt::Debug for DynamicStats {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("DynamicStats").finish()
  }
}

impl DynamicStats {
  pub fn new(stats: &Scope, runtime: &bd_runtime::runtime::ConfigLoader) -> Self {
    let dynamic_stats_overflow = stats.scope("stats").counter("dynamic_stats_overflow");

    let max_dynamic_stats =
      bd_runtime::runtime::stats::MaxDynamicCountersFlag::register(runtime).unwrap();
    let dynamic_collector = BoundedCollector::new(Some(max_dynamic_stats.into_inner()));
    let dynamic_scope = dynamic_collector.scope("");

    Self {
      dynamic_collector,
      dynamic_scope,
      dynamic_stats_overflow,
    }
  }

  #[must_use]
  pub const fn collector_for_test(&self) -> &BoundedCollector {
    &self.dynamic_collector
  }

  pub fn record_dynamic_counter(&self, name: &str, tags: BTreeMap<String, String>, value: u64) {
    match self.dynamic_scope.counter_with_labels(name, tags) {
      Ok(counter) => counter.inc_by(value),
      Err(StatsError::ChangedType) => {
        handle_unexpected::<(), anyhow::Error>(
          Err(anyhow!("change in dynamic metric type")),
          "dynamic counter type change",
        );
      },
      Err(StatsError::Overflow) => {
        log::debug!("dynamic metrics overflow");
        self.dynamic_stats_overflow.inc();
      },
    }
  }

  pub fn record_dynamic_histogram(&self, name: &str, tags: BTreeMap<String, String>, value: f64) {
    match self.dynamic_scope.histogram_with_labels(name, tags) {
      Ok(histogram) => histogram.observe(value),
      Err(StatsError::ChangedType) => {
        handle_unexpected::<(), anyhow::Error>(
          Err(anyhow!("change in dynamic metric type")),
          "dynamic histogram type change",
        );
      },
      Err(StatsError::Overflow) => {
        log::debug!("dynamic metrics overflow");
        self.dynamic_stats_overflow.inc();
      },
    }
  }
}

//
// Stats
//

/// A wrapper around prometheus that implements dynamic stats and provides a number of convenience
/// functions for interacting with prometheus metric objects.
pub struct Stats {
  collector: Collector,
  dynamic_stats: Arc<DynamicStats>,
}

impl Stats {
  #[must_use]
  pub fn new(collector: Collector, dynamic_stats: Arc<DynamicStats>) -> Arc<Self> {
    Arc::new(Self {
      collector,
      dynamic_stats,
    })
  }

  #[must_use]
  pub fn scope(&self, name: &str) -> Scope {
    self.collector.scope(name)
  }

  /// Creates a flush handle that can be used to periodically flush the stats store.
  pub fn flush_handle(
    self: &Arc<Self>,
    runtime_loader: &Arc<ConfigLoader>,
    shutdown: ComponentShutdown,
    sdk_directory: &Path,
    data_flush_tx: tokio::sync::mpsc::Sender<DataUpload>,
  ) -> anyhow::Result<FlushHandles<SystemTimeProvider, RealSerializedFileSystem>> {
    self.flush_handle_helper(
      runtime_loader,
      shutdown,
      SystemTimeProvider {},
      data_flush_tx,
      Arc::new(RealSerializedFileSystem::new(sdk_directory.to_path_buf())),
    )
  }

  fn flush_handle_helper<T: TimeProvider, F: SerializedFileSystem>(
    self: &Arc<Self>,
    runtime_loader: &Arc<ConfigLoader>,
    shutdown: ComponentShutdown,
    time_provider: T,
    data_flush_tx: tokio::sync::mpsc::Sender<DataUpload>,
    fs: Arc<F>,
  ) -> anyhow::Result<FlushHandles<T, F>> {
    let flush_time_histogram = self.collector.scope("stats").histogram("flush_time");

    let (flush_trigger, flush_rx) = FlushTrigger::new();

    Ok(FlushHandles {
      flusher: Flusher::new(
        self.clone(),
        shutdown.clone(),
        runtime_loader.register_watch()?,
        flush_rx,
        time_provider,
        flush_time_histogram,
        fs.clone(),
      ),
      flush_trigger,
      uploader: Uploader::new(
        shutdown,
        runtime_loader.register_watch()?,
        data_flush_tx,
        fs,
      ),
    })
  }
}
