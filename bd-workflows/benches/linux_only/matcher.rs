// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_api::DataUpload;
use bd_client_stats::Stats;
use bd_client_stats_store::Collector;
use bd_log_primitives::tiny_set::TinySet;
use bd_log_primitives::{FieldsRef, Log, LogFields, LogRef, LogType, log_level};
use bd_proto::protos::workflow::workflow::Workflow;
use bd_runtime::runtime::ConfigLoader;
use bd_test_helpers::workflow::{WorkflowBuilder, make_flush_buffers_action, state};
use bd_test_helpers::{log_matches, rule};
use bd_workflows::config::WorkflowsConfiguration;
use bd_workflows::engine::{WorkflowsEngine, WorkflowsEngineConfig};
use gungraun::{
  Callgrind,
  EntryPoint,
  LibraryBenchmarkConfig,
  library_benchmark,
  library_benchmark_group,
};
use std::future::Future;
use std::hint::black_box;
use std::sync::Arc;
use time::OffsetDateTime;

// fixfix add streaming config

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
        WorkflowsConfiguration::new(workflows, vec![]),
        TinySet::from([Arc::new("default_buffer_id".into())]).into(),
        TinySet::from([Arc::new("continuous_buffer_id".into())]).into(),
      ))
      .await;

    engine
  }

  async fn simple_workflow(self) -> WorkflowsEngine {
    let b = state("B");
    let a = state("A").declare_transition_with_actions(
      &b,
      rule!(log_matches!(message == "foo")),
      &[make_flush_buffers_action(&[], None, "foo")],
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
        &[make_flush_buffers_action(&[], None, "foo")],
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
        &[make_flush_buffers_action(&[], None, "foo")],
      );
      let mut config = WorkflowBuilder::new("1", &[&a, &b]).build();
      config.id = format!("baz_{i}");

      workflows.push(config);
    }

    self.new_engine(workflows).await
  }
}

fn run_runtime_bench<T: Future<Output = WorkflowsEngine>>(engine: impl FnOnce() -> T, log: Log) {
  tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()
    .unwrap()
    .block_on(async {
      let mut engine = engine().await;
      let now = OffsetDateTime::now_utc();
      // Warm up to initialize any one lock type things.
      let log = LogRef {
        log_type: log.log_type,
        log_level: log.log_level,
        message: &log.message,
        fields: FieldsRef::new(&log.fields, &log.matching_fields),
        session_id: &log.session_id,
        occurred_at: log.occurred_at,
        capture_session: log.capture_session,
      };
      engine.process_log(&log, &TinySet::default(), now);

      gungraun::client_requests::callgrind::start_instrumentation();
      engine.process_log(&log, &TinySet::default(), now);
      gungraun::client_requests::callgrind::stop_instrumentation();
    });
}

fn not_matching_log() -> Log {
  Log {
    log_level: log_level::INFO,
    log_type: LogType::Normal,
    message: "not matching".into(),
    fields: LogFields::new(),
    matching_fields: LogFields::new(),
    session_id: "session".to_string(),
    occurred_at: OffsetDateTime::now_utc(),
    capture_session: None,
  }
}

fn matching_log() -> Log {
  Log {
    log_level: log_level::INFO,
    log_type: LogType::Normal,
    message: "foo".into(),
    fields: LogFields::new(),
    matching_fields: LogFields::new(),
    session_id: "session".to_string(),
    occurred_at: OffsetDateTime::now_utc(),
    capture_session: None,
  }
}

#[library_benchmark(
  config = LibraryBenchmarkConfig::default()
        .tool(Callgrind::with_args(["--instr-atstart=no", "--dump-instr=yes"])
            .entry_point(EntryPoint::None)
        ))]
#[bench::simple_no_match(|setup: Setup| setup.simple_workflow(), not_matching_log())]
#[bench::many_simple_no_match(|setup: Setup| setup.many_simple_workflows(), not_matching_log())]
#[bench::simple_match(|setup: Setup| setup.simple_workflow(), matching_log())]
#[bench::many_simple_match(|setup: Setup| setup.many_simple_workflows(), matching_log())]
fn bench_workflow<T: Future<Output = WorkflowsEngine>>(engine: impl FnOnce(Setup) -> T, log: Log) {
  let setup = Setup::new();

  // TODO(mattklein123): For reasons I have not figured out, using the normal logger with RUST_LOG
  // doesn't work, probably because the runner eats it. This logger does work if you want to
  // temporarily see what a benchmark is doing.
  //let _ = simplelog::SimpleLogger::init(simplelog::LevelFilter::Trace,
  // simplelog::Config::default());

  black_box(run_runtime_bench(|| engine(setup), log));
}

library_benchmark_group!(
    name = benches;
    benchmarks = bench_workflow
);
