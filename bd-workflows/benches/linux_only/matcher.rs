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
use bd_test_helpers::workflow::{WorkflowBuilder, state};
use bd_test_helpers::{action, log_matches, rule};
use bd_workflows::config::WorkflowsConfiguration;
use bd_workflows::engine::{WorkflowsEngine, WorkflowsEngineConfig};
use gungraun::{
  Callgrind,
  EntryPoint,
  LibraryBenchmarkConfig,
  library_benchmark,
  library_benchmark_group,
};
use std::collections::BTreeSet;
use std::future::Future;
use std::hint::black_box;
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

  async fn simple_workflow(self) -> WorkflowsEngine {
    let b = state("B");
    let a = state("A").declare_transition_with_actions(
      &b,
      rule!(log_matches!(message == "foo")),
      &[action!(flush_buffers &["foo_buffer_id"]; id "foo")],
    );

    let config = WorkflowBuilder::new("1", &[&a, &b]).build();
    self.new_engine(vec![config]).await
  }

  async fn many_simple_workflows(self) -> WorkflowsEngine {
    let mut workflows = vec![];
    for i in 0 .. 30 {
      let b = state("B");
      let a = state("A").declare_transition_with_actions(
        &b,
        rule!(log_matches!(message == "foo")),
        &[action!(flush_buffers &["foo_buffer_id"]; id "foo")],
      );

      let mut config = WorkflowBuilder::new("1", &[&a, &b]).build();
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
      let mut config = WorkflowBuilder::new("1", &[&a, &b]).build();
      config.id = format!("baz_{i}");

      workflows.push(config);
    }

    self.new_engine(workflows).await
  }
}

fn run_runtime_bench<T: Future<Output = WorkflowsEngine>>(engine: impl FnOnce() -> T) {
  tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()
    .unwrap()
    .block_on(async {
      let mut engine = engine().await;
      let now = OffsetDateTime::now_utc();
      // Warm up to initialize any one lock type things.
      let fields = LogFields::new();
      let log = LogRef {
        log_type: LogType::Normal,
        log_level: log_level::DEBUG,
        message: &"no match".into(),
        fields: FieldsRef::new(&fields, &fields),
        session_id: "session_id",
        occurred_at: OffsetDateTime::now_utc(),
        capture_session: None,
      };
      engine.process_log(&log, &BTreeSet::default(), now);

      gungraun::client_requests::callgrind::start_instrumentation();
      engine.process_log(&log, &BTreeSet::default(), now);
      gungraun::client_requests::callgrind::stop_instrumentation();
    });
}

#[library_benchmark(
  config = LibraryBenchmarkConfig::default()
        .tool(Callgrind::with_args(["--instr-atstart=no", "--dump-instr=yes"])
            .entry_point(EntryPoint::None)
        ))]
#[bench::simple(|setup: Setup| setup.simple_workflow())]
#[bench::many_simple(|setup: Setup| setup.many_simple_workflows())]
fn bench_workflow<T: Future<Output = WorkflowsEngine>>(engine: impl FnOnce(Setup) -> T) {
  let setup = Setup::new();

  // TODO(mattklein123): For reasons I have not figured out, using the normal logger with RUST_LOG
  // doesn't work, probably because the runner eats it. This logger does work if you want to
  // temporarily see what a benchmark is doing.
  //let _ = simplelog::SimpleLogger::init(simplelog::LevelFilter::Trace,
  // simplelog::Config::default());

  black_box(run_runtime_bench(|| engine(setup)));
}

library_benchmark_group!(
    name = benches;
    benchmarks = bench_workflow
);
