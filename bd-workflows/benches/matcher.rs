// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_api::DataUpload;
use bd_client_stats::Stats;
use bd_client_stats_store::Collector;
use bd_log_primitives::{FieldsRef, LogFields, LogRef, LogType, log_level};
use bd_proto::protos::workflow::workflow::Workflow;
use bd_runtime::runtime::ConfigLoader;
use bd_test_helpers::workflow::macros::state;
use bd_test_helpers::{action, log_matches, rule, workflow_proto};
use bd_workflows::config::WorkflowsConfiguration;
use bd_workflows::engine::{WorkflowsEngine, WorkflowsEngineConfig};
use criterion::{Bencher, Criterion, criterion_group, criterion_main};
use std::collections::BTreeSet;
use std::future::Future;
use time::OffsetDateTime;

struct Setup {
  tmp_dir: tempfile::TempDir,
  data_upload_tx: tokio::sync::mpsc::Sender<DataUpload>,
  _data_upload_rx: tokio::sync::mpsc::Receiver<DataUpload>,
}

impl Setup {
  fn new() -> Self {
    let tmp_dir = tempfile::TempDir::with_prefix("bd_workflows").unwrap();
    let (data_upload_tx, data_upload_rx) = tokio::sync::mpsc::channel(1000);

    Self {
      tmp_dir,
      data_upload_tx,
      _data_upload_rx: data_upload_rx,
    }
  }

  async fn new_engine(&self, workflows: Vec<Workflow>) -> WorkflowsEngine {
    let config_loader = &ConfigLoader::new(self.tmp_dir.path());
    let collector = Collector::default();
    let scope = &collector.scope("test");
    let (mut engine, _) = WorkflowsEngine::new(
      scope,
      self.tmp_dir.path(),
      config_loader,
      self.data_upload_tx.clone(),
      Stats::new(collector),
    );
    assert!(!workflows.is_empty());

    engine
      .start(WorkflowsEngineConfig::new(
        WorkflowsConfiguration::new(workflows),
        BTreeSet::default(),
        BTreeSet::default(),
      ))
      .await;

    engine
  }

  async fn simple_workflow(&self) -> WorkflowsEngine {
    let b = state("B");
    let a = state("A").declare_transition_with_actions(
      &b,
      rule!(log_matches!(message == "foo")),
      &[action!(flush_buffers &["foo_buffer_id"]; id "foo")],
    );

    let config = workflow_proto!(a, b);
    self.new_engine(vec![config]).await
  }

  async fn many_simple_workflows(&self) -> WorkflowsEngine {
    let mut workflows = vec![];
    for i in 0 .. 30 {
      let b = state("B");
      let a = state("A").declare_transition_with_actions(
        &b,
        rule!(log_matches!(message == "foo")),
        &[action!(flush_buffers &["foo_buffer_id"]; id "foo")],
      );

      let mut config = workflow_proto!(a, b);
      config.id = format!("foo_{i}");

      workflows.push(config);
    }

    for i in 0 .. 30 {
      let b = state("B");
      let a = state("A").declare_transition_with_actions(
        &b,
        rule!(log_matches!(message == "baz")),
        &[action!(flush_buffers &["foo_buffer_id"]; id "foo")],
      );
      let mut config = workflow_proto!(a, b);
      config.id = format!("baz_{i}");
    }

    self.new_engine(workflows).await
  }
}

fn run_runtime_bench<T: Future<Output = WorkflowsEngine>>(
  bencher: &mut Bencher<'_>,
  engine: impl FnOnce() -> T,
) {
  tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()
    .unwrap()
    .block_on(async {
      let mut engine = engine().await;
      let now = OffsetDateTime::now_utc();
      bencher.iter(|| {
        engine.process_log(
          std::hint::black_box(&LogRef {
            log_type: LogType::Normal,
            log_level: log_level::DEBUG,
            message: &"foo".into(),
            fields: FieldsRef::new(&LogFields::new(), &LogFields::new()),
            session_id: "session_id",
            occurred_at: OffsetDateTime::now_utc(),
            capture_session: None,
          }),
          &BTreeSet::default(),
          now,
        );
      });
    });
}

fn criterion_benchmark(c: &mut Criterion) {
  let setup = Setup::new();

  bd_log::SwapLogger::initialize();

  c.bench_function("simple workflow", |b| {
    run_runtime_bench(b, || setup.simple_workflow());
  });
  c.bench_function("many simple workflows", |b| {
    run_runtime_bench(b, || setup.many_simple_workflows());
  });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
