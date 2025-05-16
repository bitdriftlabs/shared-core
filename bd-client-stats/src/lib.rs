// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

mod file_manager;
pub mod stats;
pub mod test;

use crate::stats::Flusher;
use anyhow::anyhow;
use bd_api::DataUpload;
use bd_client_common::error::handle_unexpected;
use bd_client_common::filesystem::RealFileSystem;
use bd_client_stats_store::{Collector, Error as StatsError};
use bd_runtime::runtime::ConfigLoader;
use bd_shutdown::ComponentShutdown;
use bd_time::SystemTimeProvider;
use file_manager::FileManager;
use parking_lot::Mutex;
use stats::Ticker;
use std::collections::{BTreeMap, HashMap};
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

pub struct FlushHandles {
  pub flusher: Flusher,
  pub flush_trigger: FlushTrigger,
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

  pub fn blocking_flush_for_test(&self, completion_tx: FlushTriggerCompletionSender) {
    self.flush_tx.blocking_send(completion_tx).unwrap();
  }
}

//
// Stats
//

pub struct Stats {
  collector: Collector,
  overflows: Mutex<HashMap<String, u64>>,
}

impl Stats {
  #[must_use]
  pub fn new(collector: Collector) -> Arc<Self> {
    Arc::new(Self {
      collector,
      overflows: Mutex::default(),
    })
  }

  /// Creates a flush handle that can be used to periodically flush the stats store.
  pub fn flush_handle(
    self: &Arc<Self>,
    runtime_loader: &Arc<ConfigLoader>,
    shutdown: ComponentShutdown,
    sdk_directory: &Path,
    data_flush_tx: tokio::sync::mpsc::Sender<DataUpload>,
    flush_ticker: Box<dyn Ticker>,
    upload_ticker: Box<dyn Ticker>,
  ) -> anyhow::Result<FlushHandles> {
    Ok(self.flush_handle_helper(
      flush_ticker,
      upload_ticker,
      shutdown,
      data_flush_tx,
      Arc::new(FileManager::new(
        Box::new(RealFileSystem::new(sdk_directory.to_path_buf())),
        Arc::new(SystemTimeProvider),
        runtime_loader,
      )?),
    ))
  }

  fn flush_handle_helper(
    self: &Arc<Self>,
    flush_ticker: Box<dyn Ticker>,
    upload_ticker: Box<dyn Ticker>,
    shutdown: ComponentShutdown,
    data_flush_tx: tokio::sync::mpsc::Sender<DataUpload>,
    fs: Arc<FileManager>,
  ) -> FlushHandles {
    let flush_time_histogram = self.collector.scope("stats").histogram("flush_time");
    let (flush_trigger, flush_rx) = FlushTrigger::new();

    FlushHandles {
      flusher: Flusher::new(
        self.clone(),
        shutdown,
        flush_ticker,
        flush_rx,
        flush_time_histogram,
        upload_ticker,
        data_flush_tx,
        fs,
      ),
      flush_trigger,
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

  pub fn record_dynamic_counter(&self, tags: BTreeMap<String, String>, id: &str, value: u64) {
    match self.collector.dynamic_counter(tags, id) {
      Ok(counter) => counter.inc_by(value),
      Err(StatsError::ChangedType) => {
        handle_unexpected::<(), anyhow::Error>(
          Err(anyhow!("change in dynamic metric type")),
          "dynamic counter type change",
        );
      },
      Err(StatsError::Overflow) => {
        self.handle_overflow(id);
      },
    }
  }

  pub fn record_dynamic_histogram(&self, tags: BTreeMap<String, String>, id: &str, value: f64) {
    match self.collector.dynamic_histogram(tags, id) {
      Ok(histogram) => histogram.observe(value),
      Err(StatsError::ChangedType) => {
        handle_unexpected::<(), anyhow::Error>(
          Err(anyhow!("change in dynamic metric type")),
          "dynamic histogram type change",
        );
      },
      Err(StatsError::Overflow) => {
        self.handle_overflow(id);
      },
    }
  }

  pub fn limit(&self) -> Option<u32> {
    self.collector.limit()
  }
}
