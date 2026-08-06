// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./replay_test.rs"]
mod tests;

use crate::bootstrap::{Bootstrap, load_workflows_config};
use crate::corpus::{CorpusReader, SourceLog};
use bd_api::DataUpload;
use bd_client_stats::Stats;
use bd_client_stats_store::{Collector, Counter, Histogram};
use bd_log_primitives::Log;
use bd_log_primitives::tiny_set::TinySet;
use bd_state::InMemoryStateReader;
use bd_workflows::engine::{WorkflowsEngine, WorkflowsEngineConfig, WorkflowsEngineResult};
use bd_workflows::workflow::WorkflowEvent;
use std::borrow::Cow;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use tokio::task::JoinHandle;

const FIXTURE_CONFIG: &str = "fixtures/workflows.json";
const FIXTURE_LOGS: &str = "fixtures/logs.ndjson";

//
// BenchmarkCorpus
//

/// Parsed benchmark inputs shared by the report runner, Callgrind, and Criterion.
pub struct BenchmarkCorpus {
  bootstrap: Bootstrap,
  source_logs: Vec<SourceLog>,
}

impl BenchmarkCorpus {
  pub fn load(config_path: &Path, logs_path: &Path) -> anyhow::Result<Self> {
    Ok(Self {
      bootstrap: load_workflows_config(config_path)?,
      source_logs: load_logs(logs_path)?,
    })
  }

  #[must_use]
  pub const fn declared_workflow_count(&self) -> usize {
    self.bootstrap.declared_workflow_count
  }

  #[must_use]
  pub const fn loaded_workflow_count(&self) -> usize {
    self.bootstrap.loaded_workflow_count
  }

  #[must_use]
  pub fn log_count(&self) -> usize {
    self.source_logs.len()
  }

  #[must_use]
  pub(crate) fn logs(&self) -> &[SourceLog] {
    &self.source_logs
  }

  pub async fn new_replay(&self) -> WorkflowReplay {
    WorkflowReplay::new(self.bootstrap.config.clone()).await
  }

  /// Evaluate every log with one stateful replay engine.
  pub fn replay_all(&self, replay: &mut WorkflowReplay) {
    replay.replay_all(&self.source_logs);
  }
}

/// Paths to the checked-in, synthetic corpus used by default benchmark commands.
#[must_use]
pub fn fixture_paths() -> (PathBuf, PathBuf) {
  let root = Path::new(env!("CARGO_MANIFEST_DIR"));
  (root.join(FIXTURE_CONFIG), root.join(FIXTURE_LOGS))
}

//
// WorkflowReplay
//

/// A fresh workflow engine and its in-memory dependencies for one corpus replay.
pub struct WorkflowReplay {
  engine: WorkflowsEngine<Counter, Histogram>,
  state_reader: InMemoryStateReader,
  buffer_ids: TinySet<Cow<'static, str>>,
  data_upload_drain: JoinHandle<()>,
}

impl WorkflowReplay {
  async fn new(config: WorkflowsEngineConfig) -> Self {
    let collector = Collector::default();
    let scope = collector.scope("workflow_bench");
    let stats = Stats::new(collector);
    let (data_upload_tx, mut data_upload_rx) = tokio::sync::mpsc::channel::<DataUpload>(1024);
    let data_upload_drain =
      tokio::spawn(async move { while data_upload_rx.recv().await.is_some() {} });
    let (mut engine, _buffers_to_flush_rx) =
      WorkflowsEngine::new(&scope, None, None, data_upload_tx, stats, None);
    engine.start(config, false).await;

    Self {
      engine,
      state_reader: InMemoryStateReader::default(),
      buffer_ids: TinySet::default(),
      data_upload_drain,
    }
  }

  pub fn process_log(&mut self, log: &Log) -> WorkflowsEngineResult<'_> {
    self.engine.process_event(
      WorkflowEvent::Log(log),
      &self.buffer_ids,
      &self.state_reader,
      log.occurred_at,
    )
  }

  /// Evaluate the complete corpus while preserving state between its logs.
  pub(crate) fn replay_all(&mut self, source_logs: &[SourceLog]) {
    for source_log in source_logs {
      black_box(self.process_log(&source_log.log));
    }
  }
}

impl Drop for WorkflowReplay {
  fn drop(&mut self) {
    self.data_upload_drain.abort();
  }
}

fn load_logs(path: &Path) -> anyhow::Result<Vec<SourceLog>> {
  let mut reader = CorpusReader::open(path)?;
  let mut source_logs = Vec::new();
  while let Some(source_log) = reader.next_log()? {
    source_logs.push(source_log);
  }
  Ok(source_logs)
}
