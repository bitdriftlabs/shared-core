// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{StateStore, WorkflowsEngine};
use crate::actions_flush_buffers::BuffersToFlush;
use crate::config::{Action, FlushBufferId, WorkflowsConfiguration};
use crate::engine::{WorkflowsEngineConfig, WorkflowsEngineResult};
use crate::engine_assert_active_runs;
use crate::workflow::Workflow;
use assert_matches::assert_matches;
use bd_api::DataUpload;
use bd_api::upload::{IntentDecision, IntentResponse, UploadResponse};
use bd_client_stats_store::Collector;
use bd_client_stats_store::test::StatsHelper;
use bd_log_primitives::{FieldsRef, Log, LogFields, LogMessage, LogRef, log_level};
use bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::LogType;
use bd_proto::protos::client::api::log_upload_intent_request::Intent_type::WorkflowActionUpload;
use bd_proto::protos::client::api::sankey_path_upload_request::Node;
use bd_proto::protos::client::api::{
  SankeyIntentRequest,
  SankeyPathUploadRequest,
  log_upload_intent_request,
};
use bd_runtime::runtime::{ConfigLoader, FeatureFlag};
use bd_stats_common::labels;
use bd_test_helpers::runtime::{ValueKind, make_simple_update};
use bd_test_helpers::workflow::macros::{
  action,
  any,
  declare_transition,
  limit,
  log_matches,
  rule,
  state,
};
use bd_test_helpers::workflow::{
  TestFieldRef,
  TestFieldType,
  make_generate_log_action_proto,
  make_save_field_extraction,
  make_save_timestamp_extraction,
};
use bd_test_helpers::{metric_tag, metric_value, sankey_value};
use bd_time::TimeDurationExt;
use itertools::Itertools;
use pretty_assertions::assert_eq;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::vec;
use time::OffsetDateTime;
use time::ext::NumericalDuration;
use time::macros::datetime;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use tokio::time::timeout;

/// A macro that creates a workflow config using provided states.
/// See `workflow_proto` macro for more details.
/// A similar workflow! macro is available in `test_helpers::workflows`
/// but due to issues with referencing types defined in `workflows` crate
/// its usage from within `workflows` (current) crate results in a compilation error.
/// For this reason, we define a workflow! macro below.
macro_rules! workflow {
  ($($x:tt)*) => {
    $crate::config::Config::new(
      bd_test_helpers::workflow::macros::workflow_proto!($($x)*)
    ).unwrap()
  }
}

/// Asserts that the states of runs of specified workflow of a given workflow engine
/// are equal to expected list of states.
/// The macro assumes that each run has only one traversals.
/// For macros that don't meet this condition `engine_assert_active_run_traversals`
/// macro should be used instead.
#[macro_export]
macro_rules! engine_assert_active_runs {
  ($workflows_engine:expr; $workflow_index:expr; $($state_id:expr),+) => {
    let workflow = $workflows_engine.state.workflows[$workflow_index].clone();
    let config = $workflows_engine.configs[$workflow_index].clone();

    let expected_states = [$($state_id,)+];

    pretty_assertions::assert_eq!(
      expected_states.len(),
      workflow.runs().len(),
      "workflow runs' states list ({:?}) and expected states list ({:?}) have different lengths",
      workflow.runs_states(&config),
      expected_states
    );

    for (index, id) in expected_states.iter().enumerate() {
      let expected_state_index = config.states().iter()
        .position(|state| state.id() == *id);
      assert!(
        expected_state_index.is_some(),
        "failed to find state with \"{}\" ID", *id
      );
      pretty_assertions::assert_eq!(
        1,
        workflow.runs()[index].traversals().len(),
        "run has more than 1 traversal, use `assert_active_run_traversals` macro instead"
      );
      pretty_assertions::assert_eq!(
        expected_state_index.unwrap(),
        workflow.runs()[index].traversals()[0].state_index,
        "workflow runs' states list ({:?}) doesn't match expected states list ({:?}) length",
        workflow.runs_states(&config),
        expected_states
      );
    }

    for run in workflow.runs() {
      pretty_assertions::assert_eq!(1, run.traversals().len());
    }
  };
}

/// Asserts that the states of traversals of a specified workflow of a given workflow engine
/// are equal to expected list of states.
#[macro_export]
macro_rules! engine_assert_active_run_traversals {
  ($engine:expr; $workflow_index:expr => $run_index:expr; $($state_id:expr),+) => {
    let workflow = $engine.state.workflows[$workflow_index].clone();
    let config = $engine.configs[$workflow_index].clone();

    #[allow(unused_comparisons)]
    let run_exists = workflow.runs().len() >= $run_index;
    assert!(
      run_exists,
      "run with index ({}) doesn't exist, workflow has only {} runs: {:?}",
      $run_index,
      workflow.runs().len(),
      workflow.runs_states(&config)
    );

    let expected_states = [$($state_id,)+];
    pretty_assertions::assert_eq!(
      expected_states.len(),
      workflow.runs()[$run_index].traversals().len(),
      "workflow run traversals' states list ({:?}) \
      and expected states list ({:?}) have different lengths",
      workflow.traversals_states(&config, $run_index),
      expected_states
    );

    for (index, id) in expected_states.iter().enumerate() {
      let expected_state_index = config.states().iter()
        .position(|state| state.id() == *id);
      assert!(
        expected_state_index.is_some(),
        "failed to find state with \"{}\" ID", *id
      );
      pretty_assertions::assert_eq!(
        expected_state_index.unwrap(),
        workflow.runs()[$run_index].traversals()[index].state_index,
        "workflow runs traversals' states list ({:?}) doesn't match expected states list ({:?})",
        workflow.traversals_states(&config, $run_index),
        expected_states
      );
    }
  }
}

/// A macro that makes a given workflow engine process a log specified by
/// a message.
#[macro_export]
macro_rules! engine_process_log {
  ($workflows_engine:expr; $message:expr) => {{
    $workflows_engine.engine.process_log(
      &bd_log_primitives::LogRef {
        log_type:
          bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::LogType::Normal,
        log_level: log_level::DEBUG,
        message: &LogMessage::String($message.to_string()),
        session_id: &$workflows_engine.session_id,
        occurred_at: time::OffsetDateTime::now_utc(),
        fields: &FieldsRef::new(&LogFields::new(), &LogFields::new()),
        capture_session: None,
      },
      &$workflows_engine.log_destination_buffer_ids,
    )
  }};
  ($workflows_engine:expr; $message:expr; with $tags:expr) => {
    $workflows_engine.engine.process_log(
      &bd_log_primitives::LogRef {
        log_type:
          bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::LogType::Normal,
        log_level: log_level::DEBUG,
        message: &LogMessage::String($message.to_string()),
        fields: &FieldsRef::new(
          &bd_test_helpers::workflow::make_tags($tags),
          &LogFields::new(),
        ),
        session_id: &$workflows_engine.session_id,
        occurred_at: time::OffsetDateTime::now_utc(),
        capture_session: None,
      },
      &$workflows_engine.log_destination_buffer_ids,
    )
  };
  ($workflows_engine:expr; $message:expr; with $tags:expr; time $current_time:expr) => {
    $workflows_engine.engine.process_log(
      &bd_log_primitives::LogRef {
        log_type:
          bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::LogType::Normal,
        log_level: log_level::DEBUG,
        message: &LogMessage::String($message.to_string()),
        fields: &FieldsRef::new(
          &bd_test_helpers::workflow::make_tags($tags),
          &LogFields::new(),
        ),
        session_id: &$workflows_engine.session_id,
        occurred_at: $current_time,
        capture_session: None,
      },
      &$workflows_engine.log_destination_buffer_ids,
    )
  };
}

//
// AnnotatedWorkflowsEngine
//

#[derive(Default)]
struct Hooks {
  flushed_buffers: Vec<BuffersToFlush>,
  received_logs_upload_intents: Vec<log_upload_intent_request::WorkflowActionUpload>,
  awaiting_logs_upload_intent_decisions: Vec<IntentDecision>,

  sankey_uploads: Vec<SankeyPathUploadRequest>,
  received_sankey_upload_intents: Vec<SankeyIntentRequest>,
  awaiting_sankey_upload_intent_decisions: Vec<Option<IntentDecision>>,
}

struct AnnotatedWorkflowsEngine {
  engine: WorkflowsEngine,

  session_id: String,
  log_destination_buffer_ids: BTreeSet<Cow<'static, str>>,

  hooks: Arc<parking_lot::Mutex<Hooks>>,

  collector: Collector,

  task_handle: JoinHandle<()>,
}

impl AnnotatedWorkflowsEngine {
  fn new(
    engine: WorkflowsEngine,
    hooks: Arc<parking_lot::Mutex<Hooks>>,
    collector: Collector,
    task_handle: JoinHandle<()>,
  ) -> Self {
    Self {
      engine,

      session_id: "foo_session".to_string(),
      log_destination_buffer_ids: BTreeSet::new(),

      hooks,

      collector,

      task_handle,
    }
  }

  async fn run_once_for_test(&mut self, persist_periodically: bool) {
    self.engine.run_once(persist_periodically).await;
    // Give the task started inside of `run_for_test()` method chance to run before proceeding.
    1.milliseconds().sleep().await;
  }

  fn run_for_test(
    buffers_to_flush_rx: Receiver<BuffersToFlush>,
    data_upload_rx: Receiver<DataUpload>,
    hooks: Arc<parking_lot::Mutex<Hooks>>,
  ) -> JoinHandle<()> {
    let mut buffers_to_flush_rx = buffers_to_flush_rx;
    let mut data_upload_rx = data_upload_rx;

    tokio::spawn(async move {
      loop {
        tokio::select! {
          Some(buffers_to_flush) = buffers_to_flush_rx.recv() => {
              log::debug!("received new buffers to flush {buffers_to_flush:?}");
              hooks.lock().flushed_buffers.push(buffers_to_flush);
            },
            Some(data_upload) = data_upload_rx.recv() => {
              match data_upload {
                DataUpload::LogsUploadIntent(logs_upload_intent) => {

                if hooks.lock().awaiting_logs_upload_intent_decisions.is_empty() {
                  continue;
                }

                let Some(WorkflowActionUpload(upload)) =
                  logs_upload_intent.payload.intent_type.clone() else
                {
                  panic!("unexpected intent type");
                };

                let decision = hooks.lock().awaiting_logs_upload_intent_decisions.remove(0);
                log::debug!("responding \"{:?}\" to logs upload intent \"{}\" intent", decision, logs_upload_intent.uuid);

                hooks.lock().received_logs_upload_intents.push(upload.clone());

                if let Err(e) = logs_upload_intent
                  .response_tx
                  .send(IntentResponse {
                    uuid: logs_upload_intent.uuid.clone(),
                    decision,
                  })
                  {
                    panic!("failed to send response: {e:?}");
                  }
                },
                DataUpload::SankeyPathUploadIntent(sankey_upload_intent) => {
                  assert!(!hooks.lock().awaiting_sankey_upload_intent_decisions.is_empty(), "received sankey upload intent when there are no awaiting intents");

                  let sankey_upload_intent_payload = sankey_upload_intent.payload.clone();

                  let decision = hooks.lock().awaiting_sankey_upload_intent_decisions.remove(0);
                  let Some(decision) =  decision else {
                    log::debug!("no decision available for sankey upload intent, not responding");
                      continue;
                  };


                  log::debug!("responding \"{:?}\" to sankey upload intent \"{}\" intent", decision, sankey_upload_intent.uuid);

                  hooks.lock().received_sankey_upload_intents
                    .push(sankey_upload_intent_payload.clone());

                  if let Err(e) = sankey_upload_intent
                    .response_tx
                    .send(IntentResponse {
                      uuid: sankey_upload_intent.uuid.clone(),
                      decision,
                    })
                    {
                      panic!("failed to send response: {e:?}");
                    }
                },
                DataUpload::SankeyPathUpload(upload) => {
                  hooks.lock().sankey_uploads.push(upload.payload.clone());

                  upload.response_tx.send(UploadResponse {
                    success: true,
                    uuid: upload.uuid,
                  }).unwrap();
                },
                default => {
                  log::error!("received unhandled data upload: {default:?}");
                }
              }
            }
        };
      }
    })
  }

  fn flushed_buffers(&self) -> Vec<BTreeSet<Cow<'static, str>>> {
    self
      .hooks
      .lock()
      .flushed_buffers
      .iter()
      .map(|b| b.buffer_ids.clone())
      .collect()
  }

  fn complete_flushes(&self) {
    for b in &mut self.hooks.lock().flushed_buffers.drain(..) {
      b.response_tx.send(()).unwrap();
    }
  }

  fn received_logs_upload_intents(&self) -> Vec<log_upload_intent_request::WorkflowActionUpload> {
    self.hooks.lock().received_logs_upload_intents.clone()
  }

  fn set_awaiting_logs_upload_intent_decisions(&self, decisions: Vec<IntentDecision>) {
    for decision in decisions {
      self
        .hooks
        .lock()
        .awaiting_logs_upload_intent_decisions
        .push(decision);
    }
  }
}

impl std::ops::Deref for AnnotatedWorkflowsEngine {
  type Target = WorkflowsEngine;

  fn deref(&self) -> &Self::Target {
    &self.engine
  }
}

impl std::ops::DerefMut for AnnotatedWorkflowsEngine {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.engine
  }
}

impl std::ops::Drop for AnnotatedWorkflowsEngine {
  fn drop(&mut self) {
    self.task_handle.abort();
  }
}

//
// Setup
//

struct Setup {
  runtime: Arc<ConfigLoader>,
  collector: Collector,
  sdk_directory: Arc<tempfile::TempDir>,
}

impl Setup {
  fn new() -> Self {
    Self::new_with_sdk_directory(&Arc::new(tempfile::TempDir::with_prefix("root-").unwrap()))
  }

  fn new_with_sdk_directory(sdk_directory: &Arc<tempfile::TempDir>) -> Self {
    let runtime = ConfigLoader::new(sdk_directory.path());
    let collector = Collector::default();

    Self {
      runtime,
      collector,
      sdk_directory: sdk_directory.clone(),
    }
  }

  // Can be called at most once for each created `Setup`. Calling it more than once
  // results in a crash due to re-registration of some stats. Use `new_with_sdk_directory`
  // to re-initialize `Setup`.
  async fn make_workflows_engine(
    &self,
    workflows_engine_config: WorkflowsEngineConfig,
  ) -> AnnotatedWorkflowsEngine {
    let (data_upload_tx, data_upload_rx) = tokio::sync::mpsc::channel(1);

    let hooks = Arc::new(parking_lot::Mutex::new(Hooks::default()));

    let stats = bd_client_stats::Stats::new(self.collector.clone());

    let (mut workflows_engine, buffers_to_flush_rx) = WorkflowsEngine::new(
      &self.collector.scope(""),
      self.sdk_directory.path(),
      &self.runtime,
      data_upload_tx,
      stats,
    );

    let task_handle =
      AnnotatedWorkflowsEngine::run_for_test(buffers_to_flush_rx, data_upload_rx, hooks.clone());

    workflows_engine.start(workflows_engine_config).await;

    AnnotatedWorkflowsEngine::new(workflows_engine, hooks, self.collector.clone(), task_handle)
  }

  fn make_state_store(&self) -> StateStore {
    StateStore::new(
      self.sdk_directory.path(),
      &self.collector.scope("state_store"),
      &self.runtime,
    )
  }

  fn workflows_state_path(&self) -> PathBuf {
    self
      .sdk_directory
      .path()
      .join("workflows_state_snapshot.8.bin")
  }
}

#[tokio::test]
async fn engine_initialization_and_update() {
  let mut a = state("A");
  let b = state("B");

  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "foo"))
  );

  let workflows = vec![
    workflow!("1"; exclusive with a, b),
    workflow!("2"; exclusive with a, b),
  ];

  let setup = Setup::new();
  let mut workflows_engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      workflows,
    ))
    .await;

  assert_eq!(2, workflows_engine.state.workflows.len());
  setup.collector.assert_counter_eq(
    2,
    "workflows:workflows_total",
    labels! {"operation" => "start"},
  );

  let workflows = vec![
    workflow!("3"; exclusive with a, b),
    workflow!("4"; exclusive with a, b),
    workflow!("5"; exclusive with a, b),
  ];

  workflows_engine.update(WorkflowsEngineConfig::new_with_workflow_configurations(
    workflows,
  ));
  assert_eq!(3, workflows_engine.state.workflows.len());
  setup.collector.assert_counter_eq(
    5,
    "workflows:workflows_total",
    labels! {"operation" => "start"},
  );
  setup.collector.assert_counter_eq(
    2,
    "workflows:workflows_total",
    labels! {"operation" => "stop"},
  );
  setup
    .collector
    .assert_counter_eq(0, "workflows:runs_total", labels! {"operation" => "start"});
  setup.collector.assert_counter_eq(
    0,
    "workflows:runs_total",
    labels! {"operation" => "advance"},
  );
  setup.collector.assert_counter_eq(
    0,
    "workflows:runs_total",
    labels! {"operation" => "completion"},
  );
  setup
    .collector
    .assert_counter_eq(0, "workflows:runs_total", labels! {"operation" => "stop"});
  setup.collector.assert_counter_eq(
    0,
    "workflows:traversals_total",
    labels! {"operation" => "start"},
  );
  setup.collector.assert_counter_eq(
    0,
    "workflows:traversals_total",
    labels! {"operation" => "advance"},
  );
  setup.collector.assert_counter_eq(
    0,
    "workflows:traversals_total",
    labels! {"operation" => "completion"},
  );
  setup.collector.assert_counter_eq(
    0,
    "workflows:traversals_total",
    labels! {"operation" => "stop"},
  );
}

#[tokio::test]
async fn engine_update_after_sdk_update() {
  let mut a = state("A");
  let b = state("B");
  // The "flush buffers" action doesn't have streaming configuration. This simulates a scenario
  // where an old SDK version receives "flush buffers" action with a streaming configuration field
  // that it doesn't recognize.
  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "foo"));
    do action!(flush_buffers &["trigger_buffer_id"]; id "action_id")
  );

  let mut c = state("C");
  let d = state("D");
  declare_transition!(
    &mut c => &d;
    when rule!(log_matches!(message == "foo"))
  );

  let cached_config_update = WorkflowsEngineConfig::new(
    WorkflowsConfiguration::new_with_workflow_configurations_for_test(vec![
      workflow!("2"; exclusive with c, d),
      workflow!("1"; exclusive with a, b),
    ]),
    BTreeSet::from(["trigger_buffer_id".into()]),
    BTreeSet::from(["continuous_buffer_id".into()]),
  );

  let setup = Setup::new();
  let mut workflows_engine = setup
    .make_workflows_engine(cached_config_update.clone())
    .await;

  workflows_engine.maybe_persist(false).await;

  // The SDK has been updated and is relaunched.
  let setup = Setup::new_with_sdk_directory(&setup.sdk_directory);
  // The SDK loads cached configuration from the previous run.
  let mut workflows_engine = setup.make_workflows_engine(cached_config_update).await;

  let mut a = state("A");
  let b = state("B");
  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "foo"));
    do action!(
      flush_buffers &["trigger_buffer_id"]; continue_streaming_to vec!["continuous_buffer_id"]; logs_count 10; id "action_id"
    )
  );

  // The client receives the same config as last time, but this time it's capable of consuming the
  // streaming configuration portion of the 'flush buffers' action. The engine should replace the
  // old workflow config with its new updated version.
  workflows_engine.update(WorkflowsEngineConfig::new(
    WorkflowsConfiguration::new_with_workflow_configurations_for_test(vec![
      workflow!("1"; exclusive with a, b),
    ]),
    BTreeSet::from(["trigger_buffer_id".into()]),
    BTreeSet::from(["continuous_buffer_id".into()]),
  ));

  assert_eq!(workflows_engine.state.workflows.len(), 1);
  assert_matches!(
    &workflows_engine.configs[0].states()[0].transitions()[0].actions()[0],
    Action::FlushBuffers(flush_buffers)
      if flush_buffers.streaming.is_some()
  );

  setup.collector.assert_counter_eq(
    2,
    "workflows:workflows_total",
    labels! {"operation" => "stop"},
  );
  setup.collector.assert_counter_eq(
    3,
    "workflows:workflows_total",
    labels! {"operation" => "start"},
  );
}

#[tokio::test]
#[allow(clippy::many_single_char_names)]
async fn persistence_succeeds() {
  let mut a = state("A");
  let mut b = state("B");
  let c = state("C");
  let mut d = state("D");
  let e = state("E");

  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "foo"))
  );
  declare_transition!(
    &mut b => &c;
    when rule!(log_matches!(message == "bar"))
  );
  declare_transition!(
    &mut a => &d;
    when rule!(log_matches!(message == "foo"))
  );
  declare_transition!(
    &mut d => &e;
    when rule!(log_matches!(message == "zar"))
  );

  let workflows = vec![
    workflow!(exclusive with a, b, c, d, e),
    workflow!(exclusive with a, b, c, d, e),
  ];

  let setup = Setup::new();
  let mut workflows_engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      workflows.clone(),
    ))
    .await;

  setup.collector.assert_counter_eq(
    0,
    "workflows:state_loads_total",
    labels! {"result" => "success"},
  );
  setup.collector.assert_counter_eq(
    0,
    "workflows:state_loads_total",
    labels! {"result" => "failure"},
  );

  // Create a fork from state A to both B and D by matching "foo" to both transitions.
  // This run has 2 traversals.
  engine_process_log!(workflows_engine; "foo");

  // [(A -> B) AND (A -> D)] (x2)
  setup
    .collector
    .assert_counter_eq(4, "workflows:matched_logs_total", labels! {});
  assert!(workflows_engine.needs_state_persistence);
  workflows_engine.maybe_persist(false).await;

  assert!(!workflows_engine.needs_state_persistence);
  setup.collector.assert_counter_eq(
    1,
    "workflows:state_persistences_total",
    labels! {"result" => "success"},
  );

  let setup = Setup::new_with_sdk_directory(&setup.sdk_directory);
  let workflows_engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      workflows,
    ))
    .await;
  // The new workflow engine has an on-going run with two traversals
  engine_assert_active_run_traversals!(workflows_engine; 0 => 0; "B", "D");
  setup.collector.assert_counter_eq(
    1,
    "workflows:state_loads_total",
    labels! {"result" => "success"},
  );
}

#[tokio::test]
async fn persistence_skipped_if_no_workflow_progress_is_made() {
  let mut a = state("A");
  let mut b = state("B");
  let c = state("C");

  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "foo"))
  );
  declare_transition!(
    &mut b => &c;
    when rule!(log_matches!(message == "bar"))
  );

  let workflows = vec![workflow!(exclusive with a, b, c)];

  let setup = Setup::new();
  let mut workflows_engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      workflows,
    ))
    .await;

  // No matches, state is not dirty.
  engine_process_log!(workflows_engine; "bar");

  setup
    .collector
    .assert_counter_eq(0, "workflows:matched_logs_total", labels! {});
  assert!(!workflows_engine.needs_state_persistence);

  workflows_engine.maybe_persist(false).await;
  assert!(!workflows_engine.needs_state_persistence);

  // Assert no serialization persistence took place.
  assert!(
    !setup.workflows_state_path().exists(),
    "workflows state snapshot file should not exist"
  );
  setup.collector.assert_counter_eq(
    0,
    "workflows:state_persistences_total",
    labels! {"result" => "success"},
  );
  setup.collector.assert_counter_eq(
    0,
    "workflows:state_loads_total",
    labels! {"result" => "failure"},
  );
}

#[tokio::test]
async fn persistence_skipped_if_workflow_stays_in_an_initial_state() {
  let mut a = state("A");
  let b = state("B");

  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "foo"))
  );

  let workflows = vec![workflow!(exclusive with a, b)];

  let setup = Setup::new();
  let mut workflows_engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      workflows,
    ))
    .await;

  // Log is matched but the end state is equal to start state is equal to initial state
  // so no persistence is needed.
  engine_process_log!(workflows_engine; "foo");

  setup
    .collector
    .assert_counter_eq(1, "workflows:matched_logs_total", labels! {});
  setup.collector.assert_counter_eq(
    1,
    "workflows:runs_total",
    labels! { "operation" => "advance" },
  );
  setup.collector.assert_counter_eq(
    1,
    "workflows:runs_total",
    labels! { "operation" => "completion" },
  );
  assert!(!workflows_engine.needs_state_persistence);
}

#[tokio::test]
async fn persist_workflows_with_at_least_one_non_initial_state_run_only() {
  let mut a = state("A");
  let b = state("B");
  let mut c = state("C");
  let d = state("D");

  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "foo"); times 10)
  );
  declare_transition!(
    &mut c => &d;
    when rule!(log_matches!(message == "bar"))
  );

  let workflows = vec![
    workflow!("1"; exclusive with a, b),
    workflow!("2"; exclusive with c, d),
  ];

  let setup = Setup::new();
  let mut workflows_engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      workflows,
    ))
    .await;

  // Workflow "1" matches a log and its run is not initial state anymore.
  engine_process_log!(workflows_engine; "foo");

  setup
    .collector
    .assert_counter_eq(1, "workflows:matched_logs_total", labels! {});
  setup.collector.assert_counter_eq(
    0,
    "workflows:runs_total",
    labels! { "operation" => "advance" },
  );
  setup.collector.assert_counter_eq(
    0,
    "workflows:runs_total",
    labels! { "operation" => "completion" },
  );
  assert!(workflows_engine.needs_state_persistence);
  workflows_engine.maybe_persist(false).await;

  let store = setup.make_state_store();
  let workflows_state = store.load().await.unwrap();

  assert_eq!(1, workflows_state.workflows.len());
  assert_eq!(1, workflows_state.workflows[0].runs().len());
}

#[tokio::test]
async fn needs_persistence_if_workflow_moves_to_an_initial_state() {
  let mut a = state("A");
  let mut b = state("B");
  let c = state("C");

  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "foo"))
  );
  declare_transition!(
    &mut b => &c;
    when rule!(log_matches!(message == "bar"))
  );

  let workflows = vec![workflow!(exclusive with a, b, c)];

  let setup = Setup::new();
  let mut workflows_engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      workflows,
    ))
    .await;

  // Workflow's run moves to state 'B'.
  engine_process_log!(workflows_engine; "foo");

  setup
    .collector
    .assert_counter_eq(1, "workflows:matched_logs_total", labels! {});
  setup.collector.assert_counter_eq(
    1,
    "workflows:runs_total",
    labels! { "operation" => "advance" },
  );
  assert!(workflows_engine.needs_state_persistence);

  // Persist state
  workflows_engine.maybe_persist(false).await;
  assert!(!workflows_engine.needs_state_persistence);

  // Workflow's run moves to its final state 'C' and completes.
  engine_process_log!(workflows_engine; "bar");
  // Workflow needs persistence as its state changed.
  assert!(workflows_engine.needs_state_persistence);
  setup.collector.assert_counter_eq(
    1,
    "workflows:runs_total",
    labels! { "operation" => "completion" },
  );
}

#[tokio::test]
#[allow(clippy::many_single_char_names)]
async fn persistence_is_respected_through_consecutive_workflows() {
  // First workflow
  let mut a = state("A");
  let mut b = state("B");
  let c = state("C");

  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "foo"))
  );
  declare_transition!(
    &mut b => &c;
    when rule!(log_matches!(message == "bar"))
  );

  // Second workflow
  let mut x = state("X");
  let y = state("Y");

  declare_transition!(
    &mut x => &y;
    when rule!(log_matches!(message == "zoo"))
  );

  let workflows = vec![
    workflow!(exclusive with a, b, c),
    workflow!(exclusive with x, y),
  ];

  let setup = Setup::new();
  let mut workflows_engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      workflows,
    ))
    .await;

  // "foo" makes the first workflow advance from "A" to "B" making its state dirty
  // "foo" doesn't match anything in the second workflow so its state remains clean
  engine_process_log!(workflows_engine; "foo");

  setup
    .collector
    .assert_counter_eq(1, "workflows:matched_logs_total", labels! {});
  assert!(workflows_engine.needs_state_persistence);

  workflows_engine.maybe_persist(false).await;
  assert!(!workflows_engine.needs_state_persistence);
  setup.collector.assert_counter_eq(
    1,
    "workflows:state_persistences_total",
    labels! {"result" => "success"},
  );
}

#[tokio::test]
async fn persistence_performed_if_match_is_found_without_advancing() {
  let mut a = state("A");
  let mut b = state("B");
  let c = state("C");

  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "foo"); times 2)
  );
  declare_transition!(
    &mut b => &c;
    when rule!(log_matches!(message == "bar"))
  );

  let workflows = vec![workflow!(exclusive with a, b, c)];

  let setup = Setup::new();
  let mut workflows_engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      workflows,
    ))
    .await;

  // Matches, but it doesn't advance the state machine
  engine_process_log!(workflows_engine; "foo");

  engine_assert_active_runs!(workflows_engine; 0; "A");
  setup
    .collector
    .assert_counter_eq(1, "workflows:matched_logs_total", labels! {});
  assert!(workflows_engine.needs_state_persistence);

  workflows_engine.maybe_persist(false).await;
  assert!(!workflows_engine.needs_state_persistence);

  // Assert serialization persistence file exists
  assert!(
    setup.workflows_state_path().exists(),
    "Workflows State Snapshot file should have been created"
  );
}

#[tokio::test]
async fn traversals_count_limit_prevents_creation_of_new_workflows() {
  let mut a = state("A");
  let b = state("B");

  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "foo"); times 100)
  );

  let workflows = vec![
    workflow!("1"; exclusive with a, b),
    workflow!("2"; exclusive with a, b),
    workflow!("3"; exclusive with a, b),
    workflow!("4"; exclusive with a, b),
  ];

  let setup = Setup::new();
  setup
    .runtime
    .update_snapshot(&make_simple_update(vec![(
      bd_runtime::runtime::workflows::TraversalsCountLimitFlag::path(),
      ValueKind::Int(2),
    )]))
    .await;

  // We try to create 4 workflows (each with 1 run that has 1 traversal) but
  // the configured traversals count limit is 2. For this reason, we succeed
  // hit the traversals limit twice.
  let mut workflows_engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      workflows,
    ))
    .await;

  // All workflows are added to the engine but some of them have no runs
  // to keep the engine below the configured traversals count limit.
  assert_eq!(4, workflows_engine.state.workflows.len());
  assert!(workflows_engine.state.workflows[2].runs().is_empty());
  assert!(workflows_engine.state.workflows[3].runs().is_empty());

  // Process a log to force the engine to create initial runs for all workflows.
  engine_process_log!(workflows_engine; "foo");

  setup
    .collector
    .assert_counter_eq(2, "workflows:traversals_count_limit_hit_total", labels! {});
  setup.collector.assert_counter_eq(
    4,
    "workflows:workflows_total",
    labels! {"operation" => "start"},
  );

  let workflows = vec![
    workflow!("11"; exclusive with a, b),
    workflow!("22"; exclusive with a, b),
    workflow!("33"; exclusive with a, b),
  ];

  // We replace 2 of the existing workflows (each with 1 run that has 1 traversal)
  // with 3 new ones (each with 1 run that has 1 traversal -> we process a log to force
  // the engine to create initial state runs).
  // We start with 2 traversals, substract 2 and try to add 3. Addition of the third one
  // fails as we hit traversals count limit.
  workflows_engine.update(WorkflowsEngineConfig::new_with_workflow_configurations(
    workflows,
  ));
  engine_process_log!(workflows_engine; "foo");

  // All workflows are added to the engine but some of them have no runs
  // to keep the engine below the configured traversals count limit.
  assert_eq!(3, workflows_engine.state.workflows.len());
  assert!(workflows_engine.state.workflows[2].runs().is_empty());

  setup
    .collector
    .assert_counter_eq(3, "workflows:traversals_count_limit_hit_total", labels! {});
  setup.collector.assert_counter_eq(
    7,
    "workflows:workflows_total",
    labels! {"operation" => "start"},
  );
}

#[tokio::test]
async fn traversals_count_limit_prevents_creation_of_new_workflow_runs() {
  let mut a = state("A");
  let b = state("B");

  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "foo"); times 100)
  );

  let workflows = vec![workflow!(exclusive with a, b)];

  let setup = Setup::new();
  setup
    .runtime
    .update_snapshot(&make_simple_update(vec![(
      bd_runtime::runtime::workflows::TraversalsCountLimitFlag::path(),
      ValueKind::Int(2),
    )]))
    .await;

  let mut workflows_engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      workflows,
    ))
    .await;

  // In exclusive mode we will only ever have 1 run with a single traversal attempting to hit the
  // total count.
  engine_process_log!(workflows_engine; "foo");
  engine_process_log!(workflows_engine; "foo");
  engine_process_log!(workflows_engine; "foo");
  engine_process_log!(workflows_engine; "foo");
  engine_process_log!(workflows_engine; "foo");

  setup
    .collector
    .assert_counter_eq(0, "workflows:traversals_count_limit_hit_total", labels! {});
}

#[tokio::test]
#[allow(clippy::many_single_char_names)]
async fn traversals_count_limit_causes_run_removal_after_forking() {
  let mut a = state("A");
  let mut b = state("B");
  let mut c = state("C");
  let mut d = state("D");
  let e = state("E");
  let f = state("F");

  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "foo"))
  );
  declare_transition!(
    &mut b => &c;
    when rule!(log_matches!(message == "bar"))
  );
  declare_transition!(
    &mut b => &d;
    when rule!(log_matches!(message == "bar"))
  );
  declare_transition!(
    &mut c => &e;
    when rule!(log_matches!(message == "zar"))
  );
  declare_transition!(
    &mut d => &f;
    when rule!(log_matches!(message == "zar"))
  );

  let workflows = vec![workflow!(exclusive with a, b, c, d, e, f)];

  let setup = Setup::new();

  setup
    .runtime
    .update_snapshot(&make_simple_update(vec![(
      bd_runtime::runtime::workflows::TraversalsCountLimitFlag::path(),
      ValueKind::Int(2),
    )]))
    .await;

  let mut workflows_engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      workflows,
    ))
    .await;
  assert!(workflows_engine.state.workflows[0].runs().is_empty());

  // * A new run is created as workflows has no runs in an initial state.
  // * The existing run "A" matches "foo" and moves to B.
  // * Workflow has 2 traversals total (one run "B" trasversal and one run "A" traversal).
  engine_process_log!(workflows_engine; "foo");
  engine_assert_active_runs!(workflows_engine; 0; "B");

  // * A second run is created so that a workflow has a run in an initial state.
  // * Two outgoing transitions of the run "B" traversal match log "bar".
  // * We have 2 traversals and attempt to create 2 more which gives us 4 traversals total.
  // * We hit the configured limit of traversals (2).
  // * In order to stay below the limit we remove the run that caused us to hit the limit.
  // * We are left with run "A".
  engine_process_log!(workflows_engine; "bar");
  engine_assert_active_run_traversals!(workflows_engine; 0 => 0; "A");

  setup
    .collector
    .assert_counter_eq(1, "workflows:runs_total", labels! {"operation" => "stop"});
  setup.collector.assert_counter_eq(
    2,
    "workflows:traversals_total",
    labels! {"operation" => "stop"},
  );
  setup
    .collector
    .assert_counter_eq(1, "workflows:traversals_count_limit_hit_total", labels! {});
}

#[tokio::test(start_paused = true)]
#[allow(clippy::many_single_char_names)]
async fn persistence_to_disk_is_rate_limited() {
  let mut a = state("A");
  let mut b = state("B");
  let c = state("C");
  let mut d = state("D");
  let e = state("E");

  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "foo"))
  );
  declare_transition!(
    &mut b => &c;
    when rule!(log_matches!(message == "bar"))
  );
  declare_transition!(
    &mut a => &d;
    when rule!(log_matches!(message == "foo"))
  );
  declare_transition!(
    &mut d => &e;
    when rule!(log_matches!(message == "zar"))
  );

  let workflows = vec![workflow!(exclusive with a, b, c, d, e)];

  let setup = Setup::new();
  let mut workflows_engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      workflows.clone(),
    ))
    .await;

  // Create a fork from state A to both B and D by matching "foo" to both transitions.
  // This run has 2 traversals.
  engine_process_log!(workflows_engine; "foo");

  workflows_engine.maybe_persist(false).await;

  // We immediately advance the workflow to the next state.
  // * The first traversal of the first run matches this log
  engine_process_log!(workflows_engine; "bar");

  // This persistance should be skipped due to rate limiting.
  workflows_engine.maybe_persist(false).await;

  let setup = Setup::new_with_sdk_directory(&setup.sdk_directory);
  let other_workflows_engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      workflows.clone(),
    ))
    .await;
  // The other workflow engine has the old run with still two traversals
  engine_assert_active_run_traversals!(other_workflows_engine; 0 => 0; "B", "D");

  // Advance clock to allow rate limiting to kick in.
  let elapsed = *workflows_engine
    .state_store
    .persistence_write_interval_flag
    .read()
    + 50.milliseconds();
  elapsed.advance().await;

  workflows_engine.maybe_persist(false).await;

  // Create a copy from the persisted state.
  let setup = Setup::new_with_sdk_directory(&setup.sdk_directory);
  let other_workflows_engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      workflows,
    ))
    .await;
  // assert that the re-created workflow engine has an on-going run with only 1 traversals.
  engine_assert_active_runs!(other_workflows_engine; 0; "D");
}

#[tokio::test]
async fn runs_in_initial_state_are_not_persisted() {
  let mut a = state("A");
  let mut b = state("B");
  let c = state("C");

  declare_transition!(
    &mut a => &c;
    when rule!(log_matches!(message == "foo"); times 10)
  );
  declare_transition!(
    &mut b => &c;
    when rule!(log_matches!(message == "zar"))
  );

  let workflows = vec![
    workflow!("1"; exclusive with a, c),
    workflow!("2"; exclusive with b, c),
  ];

  let setup = Setup::new();
  let mut workflows_engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      workflows.clone(),
    ))
    .await;

  // * Workflow #1: The only existing run matches log but does not advance as the transition
  //   requires 10 matches.
  // * Workflow #2: a run in an initial state is created.
  engine_process_log!(workflows_engine; "foo");
  engine_assert_active_runs!(workflows_engine; 0; "A");
  engine_assert_active_runs!(workflows_engine; 1; "B");

  // * Workflow #1: An extra run with initial state is created as workflow uses parallel execution
  //   type.
  // * Workflow #2: Log is not matched. Nothing happens.
  engine_process_log!(workflows_engine; "bar");
  engine_assert_active_runs!(workflows_engine; 0; "A", "A");
  engine_assert_active_runs!(workflows_engine; 1; "B");

  // * Workflow #1: The state is persisted. We do not persist second run's state as it is an initial
  //   state.
  // * Workflow #2: The only run is not persisted as it's in an initial state.
  workflows_engine.maybe_persist(false).await;

  let setup = Setup::new_with_sdk_directory(&setup.sdk_directory);
  // We set up a new workflows engine that uses the same underlying workflows
  // state file.
  let mut workflows_engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      workflows,
    ))
    .await;

  // The persisted state was loaded.
  // * Workflow #1: The second run was not re-recreated as it was not stored on a disk.
  // * Workflow #2: No runs exists as no runs were stored on disk.
  engine_assert_active_runs!(workflows_engine; 0; "A");
  assert!(workflows_engine.state.workflows[1].runs().is_empty());
  setup.collector.assert_counter_eq(
    1,
    "workflows:runs_total",
    labels! { "operation" => "start" },
  );

  engine_process_log!(workflows_engine; "bar");
  // * Workflow #1: A new run in an initial state is created as workflow has a parallel execution
  //   type and no runs in initial state.
  // * Workflow #2: A new run in an initial state is created as workflow had not runs.
  engine_assert_active_runs!(workflows_engine; 0; "A", "A");
  engine_assert_active_runs!(workflows_engine; 1; "B");
  setup.collector.assert_counter_eq(
    3,
    "workflows:runs_total",
    labels! { "operation" => "start" },
  );
}

#[tokio::test]
#[allow(clippy::many_single_char_names)]
async fn ignore_persisted_state_if_corrupted() {
  let mut a = state("A");
  let mut b = state("B");
  let c = state("C");
  let mut d = state("D");
  let e = state("E");

  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "foo"))
  );
  declare_transition!(
    &mut b => &c;
    when rule!(log_matches!(message == "bar"))
  );
  declare_transition!(
    &mut a => &d;
    when rule!(log_matches!(message == "foo"))
  );
  declare_transition!(
    &mut d => &e;
    when rule!(log_matches!(message == "zar"))
  );

  let workflows = vec![workflow!(exclusive with a, b, c, d, e)];

  let setup = Setup::new();

  // Create a corrupted workflows snapshot file
  std::fs::write(setup.workflows_state_path(), vec![0, 1, 2, 3]).unwrap();

  // Engine creation should still succeed but with a default state
  let mut workflows_engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      workflows.clone(),
    ))
    .await;
  // The workflow has no runs.
  assert!(workflows_engine.state.workflows[0].runs().is_empty());

  // Assert corrupted file was deleted
  assert!(
    !setup.workflows_state_path().exists(),
    "Workflows State Snapshot file should not exist"
  );
  setup.collector.assert_counter_eq(
    1,
    "workflows:state_loads_total",
    labels! {"result" => "failure"},
  );

  // Change workflows state
  engine_process_log!(workflows_engine; "foo");

  // No errors should be reported since the file should be overwritten
  workflows_engine.maybe_persist(false).await;

  // Create new engine off the saved state
  let setup = Setup::new_with_sdk_directory(&setup.sdk_directory);
  let workflows_engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      workflows,
    ))
    .await;

  // The new workflow engine has an on-going run with two traversals
  engine_assert_active_run_traversals!(workflows_engine; 0 => 0; "B", "D");
  setup.collector.assert_counter_eq(
    1,
    "workflows:state_loads_total",
    labels! {"result" => "success"},
  );
}

struct TestReporter {}

impl bd_client_common::error::Reporter for TestReporter {
  fn report(
    &self,
    _message: &str,
    _detail: &Option<String>,
    _fields: &HashMap<Cow<'_, str>, Cow<'_, str>>,
  ) {
  }
}

#[tokio::test]
#[allow(clippy::many_single_char_names)]
async fn ignore_persisted_state_if_invalid_dir() {
  let mut a = state("A");
  let mut b = state("B");
  let c = state("C");
  let mut d = state("D");
  let e = state("E");

  // Default reporter panics in tests if unexpected error is found.
  // Register a custom one.
  let reporter = TestReporter {};
  bd_client_common::error::UnexpectedErrorHandler::set_reporter(std::sync::Arc::new(reporter));

  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "foo"))
  );
  declare_transition!(
    &mut b => &c;
    when rule!(log_matches!(message == "bar"))
  );
  declare_transition!(
    &mut a => &d;
    when rule!(log_matches!(message == "foo"))
  );
  declare_transition!(
    &mut d => &e;
    when rule!(log_matches!(message == "zar"))
  );

  let workflows = vec![workflow!(exclusive with a, b, c, d, e)];

  let collector = Collector::default();
  let sdk_directory = PathBuf::from("/invalid/path");

  let stats = bd_client_stats::Stats::new(collector.clone());

  // Engine creation should still succeed but with a default state
  let (tx, _) = tokio::sync::mpsc::channel(1);
  let (mut workflows_engine, _) = WorkflowsEngine::new(
    &collector.scope(""),
    sdk_directory.as_path(),
    &make_runtime(),
    tx,
    stats.clone(),
  );

  workflows_engine
    .start(WorkflowsEngineConfig::new_with_workflow_configurations(
      workflows.clone(),
    ))
    .await;

  // assert that the workflow has no runs.
  assert!(workflows_engine.state.workflows[0].runs().is_empty());
  collector.assert_counter_eq(
    1,
    "workflows:state_loads_total",
    labels! {"result" => "failure"},
  );

  // Change workflows state
  workflows_engine.process_log(
    &LogRef {
      log_type: LogType::Normal,
      log_level: log_level::DEBUG,
      message: &LogMessage::String("foo".to_string()),
      fields: &FieldsRef::new(
        &bd_test_helpers::workflow::make_tags(labels! {}),
        &LogFields::new(),
      ),
      session_id: "foo_session",
      occurred_at: time::OffsetDateTime::now_utc(),
      capture_session: None,
    },
    &BTreeSet::new(),
  );

  // Persistence is no-op if dir invalid
  workflows_engine.maybe_persist(false).await;
  assert!(!workflows_engine.needs_state_persistence);
  collector.assert_counter_eq(
    1,
    "workflows:state_persistences_total",
    labels! {"result" => "failure"},
  );

  // Create new engine using same invalid persistence path
  let collector = Collector::default();
  let (rx, _) = tokio::sync::mpsc::channel(1);
  let (mut workflows_engine, _) = WorkflowsEngine::new(
    &collector.scope(""),
    sdk_directory.as_path(),
    &make_runtime(),
    rx,
    stats,
  );

  workflows_engine
    .start(WorkflowsEngineConfig::new_with_workflow_configurations(
      workflows,
    ))
    .await;

  // assert that the workflow has a valid initial state - no runs.
  assert!(workflows_engine.state.workflows[0].runs().is_empty());
  collector.assert_counter_eq(
    1,
    "workflows:state_loads_total",
    labels! {"result" => "failure"},
  );
}

#[tokio::test]
async fn persists_state_on_periodic_basis() {
  let mut a = state("A");
  let mut b = state("B");
  let c = state("C");

  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "foo"); times 100)
  );
  declare_transition!(
    &mut b => &c;
    when rule!(log_matches!(message == "bar"))
  );

  let workflows = vec![workflow!(exclusive with a, b, c)];

  let setup = Setup::new();

  // Speed up periodic persistance so that the test can complete in a shorter
  // amount of time.
  setup
    .runtime
    .update_snapshot(&make_simple_update(vec![(
      bd_runtime::runtime::workflows::StatePeriodicWriteIntervalFlag::path(),
      ValueKind::Int(10),
    )]))
    .await;

  // Engine creation should still succeed but with a default state
  let mut workflows_engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      workflows,
    ))
    .await;

  engine_process_log!(workflows_engine; "foo");
  // Log made the state dirty.
  assert!(workflows_engine.needs_state_persistence);
  // One of run loop's responsibilities is periodic persistance of state.
  // Given enough time it should persist the state to disk and mark
  // state as being "clean".
  _ = timeout(Duration::from_millis(100), workflows_engine.run()).await;
  assert!(!workflows_engine.needs_state_persistence);

  setup.collector.assert_counter_eq(
    1,
    "workflows:state_persistences_total",
    labels! {"result" => "success"},
  );
}

#[tokio::test]
async fn engine_processing_log() {
  let mut a = state("A");
  let b = state("B");
  let mut c = state("C");
  let d = state("D");

  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "foo"));
    do action!(flush_buffers &["foo_buffer_id"]; id "foo_action_id")
  );
  declare_transition!(
    &mut c => &d;
    when rule!(log_matches!(message == "foo"));
    do action!(emit_counter "foo_metric"; value metric_value!(123))
  );

  let workflows = vec![
    workflow!("1"; exclusive with a, b),
    workflow!("2"; exclusive with c, d),
  ];

  let setup = Setup::new();
  let mut workflows_engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new(
      WorkflowsConfiguration::new_with_workflow_configurations_for_test(workflows),
      BTreeSet::from(["foo_buffer_id".into()]),
      BTreeSet::new(),
    ))
    .await;

  // * Two workflows are created in response to a passed workflows config.
  // * One run is created for each of the created workflows.
  // * Each workflow run advances from their initial to final state in response to "foo" log.
  workflows_engine.log_destination_buffer_ids = BTreeSet::from(["foo_buffer_id".into()]);
  let result = engine_process_log!(workflows_engine; "foo");
  assert_eq!(
    WorkflowsEngineResult {
      log_destination_buffer_ids: Cow::Owned(BTreeSet::from(["foo_buffer_id".into()])),
      triggered_flush_buffers_action_ids: BTreeSet::from([Cow::Owned(
        FlushBufferId::WorkflowActionId("foo_action_id".into())
      ),]),
      triggered_flushes_buffer_ids: BTreeSet::from(["foo_buffer_id".into()]),
      capture_screenshot: false,
      logs_to_inject: BTreeMap::new(),
    },
    result
  );

  workflows_engine
    .collector
    .assert_workflow_counter_eq(123, "foo_metric", labels! {});

  setup.collector.assert_counter_eq(
    2,
    "workflows:workflows_total",
    labels! {"operation" => "start"},
  );
  setup.collector.assert_counter_eq(
    0,
    "workflows:workflows_total",
    labels! {"operation" => "stop"},
  );
  setup
    .collector
    .assert_counter_eq(2, "workflows:runs_total", labels! {"operation" => "start"});
  setup.collector.assert_counter_eq(
    2,
    "workflows:runs_total",
    labels! {"operation" => "advance"},
  );
  setup.collector.assert_counter_eq(
    2,
    "workflows:runs_total",
    labels! {"operation" => "completion"},
  );
  setup
    .collector
    .assert_counter_eq(0, "workflows:runs_total", labels! {"operation" => "stop"});
  setup.collector.assert_counter_eq(
    2,
    "workflows:traversals_total",
    labels! {"operation" => "start"},
  );
  setup.collector.assert_counter_eq(
    2,
    "workflows:traversals_total",
    labels! {"operation" => "advance"},
  );
  setup.collector.assert_counter_eq(
    2,
    "workflows:traversals_total",
    labels! {"operation" => "completion"},
  );
  setup.collector.assert_counter_eq(
    0,
    "workflows:traversals_total",
    labels! {"operation" => "stop"},
  );
  setup
    .collector
    .assert_counter_eq(2, "workflows:matched_logs_total", labels! {});

  // Two new runs are created to ensure that each workflow has one run in an initial state.
  engine_process_log!(workflows_engine; "not matching");
  setup
    .collector
    .assert_counter_eq(4, "workflows:runs_total", labels! {"operation" => "start"});
  setup.collector.assert_counter_eq(
    4,
    "workflows:traversals_total",
    labels! {"operation" => "start"},
  );
}

#[tokio::test]
async fn exclusive_workflow_duration_limit() {
  let mut a = state("A");
  let mut b = state("B");
  let c = state("C");

  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "foo"))
  );
  declare_transition!(
    &mut b => &c;
    when rule!(log_matches!(message == "zar"))
  );

  let config = workflow!(
    exclusive with a, b, c;
    matches limit!(count 100);
    duration limit!(seconds 1)
  );

  let setup = Setup::new();
  let mut workflows_engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![config],
    ))
    .await;

  let now = time::OffsetDateTime::now_utc();

  // * A new run is created.
  // * The newly created run doesn't match a log.
  engine_process_log!(workflows_engine; "bar"; with labels!{}; time now);
  engine_assert_active_runs!(workflows_engine; 0; "A");
  setup.collector.assert_counter_eq(
    1,
    "workflows:runs_total",
    labels! { "operation" => "start" },
  );

  // * The run matches a log and advances. It leaves its initial state.
  engine_process_log!(workflows_engine; "foo"; with labels!{}; time now + Duration::from_secs(2));
  engine_assert_active_runs!(workflows_engine; 0; "B");
  setup.collector.assert_counter_eq(
    1,
    "workflows:runs_total",
    labels! { "operation" => "advance" },
  );

  // * A run in an initial state is created and added to the beginning of runs list.
  // * The run is not an initial state and has exceeded the maximum duration.
  // * The run is stopped.
  engine_process_log!(workflows_engine; "not matching"; with labels!{}; time now + Duration::from_secs(4));
  assert_eq!(workflows_engine.state.workflows[0].runs().len(), 1);
  engine_assert_active_runs!(workflows_engine; 0; "A");
  setup
    .collector
    .assert_counter_eq(1, "workflows:runs_total", labels! { "operation" => "stop" });

  // * A new run in an initial state is created.
  // * The new run matches a log and advances.
  engine_process_log!(workflows_engine; "foo"; with labels!{}; time now + Duration::from_secs(4));
  engine_assert_active_runs!(workflows_engine; 0; "B");
  setup.collector.assert_counter_eq(
    2,
    "workflows:runs_total",
    labels! { "operation" => "start" },
  );
  setup.collector.assert_counter_eq(
    2,
    "workflows:runs_total",
    labels! { "operation" => "advance" },
  );
}

#[tokio::test]
async fn log_without_destination() {
  let mut a = state("A");
  let b = state("B");

  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "foo"));
    do action!(
      flush_buffers &["trigger_buffer_id"];
      continue_streaming_to vec!["continuous_buffer_id"];
      logs_count 100_000;
      id "action"
    )
  );

  let workflows_engine_config = WorkflowsEngineConfig::new(
    WorkflowsConfiguration::new_with_workflow_configurations_for_test(vec![
      workflow!(exclusive with a, b),
    ]),
    BTreeSet::from(["trigger_buffer_id".into()]),
    BTreeSet::from(["continuous_buffer_id".into()]),
  );

  let setup = Setup::new();

  let mut workflows_engine = setup.make_workflows_engine(workflows_engine_config).await;
  workflows_engine.log_destination_buffer_ids = BTreeSet::new();

  let result = engine_process_log!(workflows_engine; "foo");

  assert_eq!(
    WorkflowsEngineResult {
      log_destination_buffer_ids: Cow::Owned(BTreeSet::new()),
      triggered_flush_buffers_action_ids: BTreeSet::from([Cow::Owned(
        FlushBufferId::WorkflowActionId("action".into())
      ),]),
      triggered_flushes_buffer_ids: BTreeSet::from(["trigger_buffer_id".into()]),
      capture_screenshot: false,
      logs_to_inject: BTreeMap::new(),
    },
    result
  );
}

#[tokio::test]
#[allow(clippy::many_single_char_names)]
async fn logs_streaming() {
  let mut a = state("A");
  let mut b = state("B");
  let mut c = state("C");
  let mut d = state("D");
  let mut e = state("E");
  let mut f = state("F");
  let mut g = state("G");
  let h = state("H");

  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "immediate_drop"));
    do action!(flush_buffers &["trigger_buffer_id"]; id "immediate_drop")
  );
  declare_transition!(
    &mut b => &c;
    when rule!(log_matches!(message == "immediate_upload_no_streaming"));
    do action!(flush_buffers &["trigger_buffer_id"]; id "immediate_upload_no_streaming")
  );
  declare_transition!(
    &mut c => &d;
    when rule!(log_matches!(message == "immediate_upload_streaming"));
    do action!(
      flush_buffers &["trigger_buffer_id"];
      continue_streaming_to vec!["continuous_buffer_id_2"];
      logs_count 10;
      id "immediate_upload_streaming"
    )
  );
  declare_transition!(
    &mut d => &e;
    when rule!(log_matches!(message == "relaunch_upload_no_streaming"));
    do action!(flush_buffers &["trigger_buffer_id"]; id "relaunch_upload_no_streaming")
  );
  declare_transition!(
    &mut e => &f;
    when rule!(log_matches!(message == "relaunch_upload_no_streaming"));
    do action!(flush_buffers &["trigger_buffer_id"]; id "relaunch_upload_no_streaming")
  );
  declare_transition!(
    &mut f => &g;
    when rule!(log_matches!(message == "relaunch_upload_streaming"));
    do action!(
      flush_buffers &["trigger_buffer_id"];
      continue_streaming_to vec![];
      logs_count 10;
      id "relaunch_upload_streaming"
    )
  );
  declare_transition!(
    &mut g => &h;
    when rule!(log_matches!(message == "relaunch_upload_streaming_2"));
    do action!(
      flush_buffers &["trigger_buffer_id"];
      continue_streaming_to vec![];
      logs_count 10;
      id "relaunch_upload_streaming_2"
    )
  );

  let workflows_engine_config = WorkflowsEngineConfig::new(
    WorkflowsConfiguration::new_with_workflow_configurations_for_test(vec![
      workflow!(exclusive with a, b, c, d, e, f, g, h),
    ]),
    BTreeSet::from(["trigger_buffer_id".into()]),
    BTreeSet::from([
      "continuous_buffer_id_1".into(),
      "continuous_buffer_id_2".into(),
    ]),
  );

  let setup = Setup::new();

  let mut workflows_engine = setup
    .make_workflows_engine(workflows_engine_config.clone())
    .await;
  workflows_engine.log_destination_buffer_ids = BTreeSet::from(["trigger_buffer_id".into()]);

  // Emit four logs that results in four flushes of the buffer(s).
  // The logs upload intents for the first two buffer flushes are processed soon immediately after
  // they are posted. The intents for the remaining two buffer flushes don't have a chance to be
  // proceeded until the SDK is shutdown and starts again.

  // Set up the mock logs upload intent server so that it accepts two incoming logs upload intents.
  workflows_engine.set_awaiting_logs_upload_intent_decisions(vec![
    IntentDecision::Drop,
    IntentDecision::UploadImmediately,
    IntentDecision::UploadImmediately,
  ]);

  // This should trigger a flush of a buffer.
  let result = engine_process_log!(workflows_engine; "immediate_drop"; with labels!{});
  assert_eq!(
    result.log_destination_buffer_ids,
    Cow::Owned(BTreeSet::from(["trigger_buffer_id".into()]))
  );

  // Allow the engine to perform logs upload intent and process the response to it (upload
  // immediately).
  workflows_engine.run_once_for_test(false).await;

  assert_eq!(
    vec![log_upload_intent_request::WorkflowActionUpload {
      workflow_action_ids: vec!["immediate_drop".to_string()],
      ..Default::default()
    }],
    workflows_engine.received_logs_upload_intents()
  );

  setup.collector.assert_counter_eq(
    1,
    "workflows:actions:logs_upload_intent_negotiation_initiations_total",
    labels! {},
  );
  setup.collector.assert_counter_eq(
    1,
    "workflows:actions:flush_buffers_action_initiations_total",
    labels! { "result" => "success" },
  );

  // This should trigger a flush of a buffer.
  let result =
    engine_process_log!(workflows_engine; "immediate_upload_no_streaming"; with labels!{});
  assert_eq!(
    result.log_destination_buffer_ids,
    Cow::Owned(BTreeSet::from(["trigger_buffer_id".into()]))
  );

  // Allow the engine to perform logs upload intent and process the response to it (upload
  // immediately).
  workflows_engine.run_once_for_test(false).await;

  assert_eq!(
    vec![
      log_upload_intent_request::WorkflowActionUpload {
        workflow_action_ids: vec!["immediate_drop".to_string()],
        ..Default::default()
      },
      log_upload_intent_request::WorkflowActionUpload {
        workflow_action_ids: vec!["immediate_upload_no_streaming".to_string()],
        ..Default::default()
      }
    ],
    workflows_engine.received_logs_upload_intents()
  );
  assert_eq!(
    workflows_engine.flushed_buffers(),
    vec![BTreeSet::from(["trigger_buffer_id".into()])],
  );

  setup.collector.assert_counter_eq(
    2,
    "workflows:actions:logs_upload_intent_negotiation_initiations_total",
    labels! {},
  );
  setup.collector.assert_counter_eq(
    2,
    "workflows:actions:flush_buffers_action_initiations_total",
    labels! { "result" => "success" },
  );

  // This should trigger a flush of a buffer that's followed by logs streaming to continuous log
  // buffer.
  let result = engine_process_log!(workflows_engine; "immediate_upload_streaming"; with labels!{});
  assert_eq!(
    result.log_destination_buffer_ids,
    Cow::Owned(BTreeSet::from(["trigger_buffer_id".into()]))
  );

  // Allow the engine to perform logs upload intent and process the response to it (upload
  // immediately).
  workflows_engine.run_once_for_test(false).await;

  assert_eq!(
    vec![
      log_upload_intent_request::WorkflowActionUpload {
        workflow_action_ids: vec!["immediate_drop".to_string()],
        ..Default::default()
      },
      log_upload_intent_request::WorkflowActionUpload {
        workflow_action_ids: vec!["immediate_upload_no_streaming".to_string()],
        ..Default::default()
      },
      log_upload_intent_request::WorkflowActionUpload {
        workflow_action_ids: vec!["immediate_upload_streaming".to_string()],
        ..Default::default()
      }
    ],
    workflows_engine.received_logs_upload_intents()
  );
  assert_eq!(
    workflows_engine.flushed_buffers(),
    vec![
      BTreeSet::from(["trigger_buffer_id".into()]),
      BTreeSet::from(["trigger_buffer_id".into()])
    ],
  );

  setup.collector.assert_counter_eq(
    3,
    "workflows:actions:logs_upload_intent_negotiation_initiations_total",
    labels! {},
  );
  setup.collector.assert_counter_eq(
    3,
    "workflows:actions:flush_buffers_action_initiations_total",
    labels! { "result" => "success" },
  );
  setup.collector.assert_counter_eq(
    1,
    "workflows:actions:streaming_buffers_action_initiations_total",
    labels! { "result" => "success" },
  );

  // This should trigger a flush of a buffer.
  let result =
    engine_process_log!(workflows_engine; "relaunch_upload_no_streaming"; with labels!{});
  assert_eq!(
    result.log_destination_buffer_ids,
    Cow::Owned(BTreeSet::from(["continuous_buffer_id_2".into()]))
  );

  // The resulting flush buffer action should be ignored as the same flush buffer action was
  // triggered above and related logs upload intent is still in-progress.
  let result =
    engine_process_log!(workflows_engine; "relaunch_upload_no_streaming"; with labels!{});
  assert_eq!(
    result.log_destination_buffer_ids,
    Cow::Owned(BTreeSet::from(["continuous_buffer_id_2".into()]))
  );

  // This should trigger a flush of a buffer that's followed by logs streaming to continuous log
  // buffer.
  // Processing of this log also confirms that log can be processed even as the engine has pending
  // logs upload intent(s).
  let result = engine_process_log!(workflows_engine; "relaunch_upload_streaming"; with labels!{});
  assert_eq!(
    result.log_destination_buffer_ids,
    Cow::Owned(BTreeSet::from(["continuous_buffer_id_2".into()]))
  );

  // Confirm that the state of the workflows engine is as expected prior to engine's shutdown.

  // Two of the triggered flush buffers actions are awaiting corresponding logs upload intents to be
  // processed.
  assert_eq!(workflows_engine.state.pending_flush_actions.len(), 2);

  // One logs streaming action is active.
  assert_eq!(workflows_engine.state.streaming_actions.len(), 1);

  // Make sure that workflows state was persisted to disk.
  assert!(workflows_engine.needs_state_persistence);
  workflows_engine.maybe_persist(false).await;
  assert!(!workflows_engine.needs_state_persistence);

  // Simulate relaunch of the app and a fresh configuration of the SDK.
  let setup = Setup::new_with_sdk_directory(&setup.sdk_directory);

  let mut workflows_engine = setup.make_workflows_engine(workflows_engine_config).await;
  workflows_engine.log_destination_buffer_ids = BTreeSet::from(["trigger_buffer_id".into()]);

  workflows_engine.set_awaiting_logs_upload_intent_decisions(vec![
    IntentDecision::UploadImmediately,
    IntentDecision::UploadImmediately,
  ]);

  let result = engine_process_log!(workflows_engine; "test log"; with labels!{});
  assert_eq!(
    result.log_destination_buffer_ids,
    Cow::Owned(BTreeSet::from(["continuous_buffer_id_2".into()]))
  );

  // Allow the engine to perform logs upload intent and process the response to it (upload
  // immediately).
  workflows_engine.run_once_for_test(false).await;

  assert_eq!(
    vec![
      log_upload_intent_request::WorkflowActionUpload {
        workflow_action_ids: vec!["relaunch_upload_no_streaming".to_string()],
        ..Default::default()
      },
      log_upload_intent_request::WorkflowActionUpload {
        workflow_action_ids: vec!["relaunch_upload_streaming".to_string()],
        ..Default::default()
      },
    ],
    workflows_engine.received_logs_upload_intents()
  );
  assert_eq!(
    workflows_engine.flushed_buffers(),
    vec![BTreeSet::from(["trigger_buffer_id".into()])],
  );

  setup.collector.assert_counter_eq(
    0,
    "workflows:actions:streaming_buffers_action_initiations_total",
    labels! { "result" => "success" },
  );

  let result = engine_process_log!(workflows_engine; "test log"; with labels!{});
  assert_eq!(
    result.log_destination_buffer_ids,
    Cow::Owned(BTreeSet::from(["continuous_buffer_id_2".into()]))
  );

  // Allow the engine to perform logs upload intent and process the response to it (upload
  // immediately).
  workflows_engine.run_once_for_test(false).await;

  assert_eq!(
    vec![
      log_upload_intent_request::WorkflowActionUpload {
        workflow_action_ids: vec!["relaunch_upload_no_streaming".to_string()],
        ..Default::default()
      },
      log_upload_intent_request::WorkflowActionUpload {
        workflow_action_ids: vec!["relaunch_upload_streaming".to_string()],
        ..Default::default()
      },
    ],
    workflows_engine.received_logs_upload_intents()
  );
  assert_eq!(
    workflows_engine.flushed_buffers(),
    vec![
      BTreeSet::from(["trigger_buffer_id".into()]),
      BTreeSet::from(["trigger_buffer_id".into()])
    ],
  );

  // This re-triggers `relaunch_upload_streaming` flush and stream logs action but is ignored by the
  // system as the previous action with the same ID is still streaming logs.
  let result = engine_process_log!(workflows_engine; "relaunch_upload_streaming"; with labels!{});
  assert_eq!(
    result.log_destination_buffer_ids,
    Cow::Owned(BTreeSet::from([
      "continuous_buffer_id_1".into(),
      "continuous_buffer_id_2".into()
    ]))
  );

  // No change in below assertions as compared to the previous assertions comparing received logs
  // upload intents and flushed buffers.
  assert_eq!(
    vec![
      log_upload_intent_request::WorkflowActionUpload {
        workflow_action_ids: vec!["relaunch_upload_no_streaming".to_string()],
        ..Default::default()
      },
      log_upload_intent_request::WorkflowActionUpload {
        workflow_action_ids: vec!["relaunch_upload_streaming".to_string()],
        ..Default::default()
      },
    ],
    workflows_engine.received_logs_upload_intents()
  );
  assert_eq!(
    workflows_engine.flushed_buffers(),
    vec![
      BTreeSet::from(["trigger_buffer_id".into()]),
      BTreeSet::from(["trigger_buffer_id".into()])
    ],
  );
  workflows_engine.complete_flushes();

  setup.collector.assert_counter_eq(
    1,
    "workflows:actions:streaming_buffers_action_initiations_total",
    labels! { "result" => "success" },
  );

  workflows_engine.session_id = "bar_session".to_string();

  // Streaming is disabled as a log with a new session ID was emitted.
  let result = engine_process_log!(workflows_engine; "test log"; with labels!{});
  assert_eq!(
    result.log_destination_buffer_ids,
    Cow::Owned(BTreeSet::from(["trigger_buffer_id".into()]))
  );

  assert!(workflows_engine.state.pending_flush_actions.is_empty());
  assert!(workflows_engine.state.streaming_actions.is_empty());

  // Make sure that workflows state was persisted to disk.
  assert!(workflows_engine.needs_state_persistence);
  workflows_engine.maybe_persist(false).await;
  assert!(!workflows_engine.needs_state_persistence);
}

#[tokio::test]
async fn engine_tracks_new_sessions() {
  let setup = Setup::new();

  let workflows_engine_config = WorkflowsEngineConfig::new(
    WorkflowsConfiguration::new_with_workflow_configurations_for_test(vec![]),
    BTreeSet::from(["trigger_buffer_id".into()]),
    BTreeSet::from(["continuous_buffer_id".into()]),
  );

  let mut workflows_engine = setup
    .make_workflows_engine(workflows_engine_config.clone())
    .await;

  engine_process_log!(workflows_engine; "foo"; with labels!{});
  engine_process_log!(workflows_engine; "foo"; with labels!{});
  assert_eq!(workflows_engine.stats.sessions_total.get(), 1);

  workflows_engine.session_id = "new session ID".to_string();

  engine_process_log!(workflows_engine; "foo"; with labels!{});
  engine_process_log!(workflows_engine; "foo"; with labels!{});
  assert_eq!(workflows_engine.stats.sessions_total.get(), 2);
}

#[tokio::test]
#[allow(clippy::many_single_char_names)]
async fn engine_does_not_purge_pending_actions_on_session_id_change() {
  let mut a = state("A");
  let mut b = state("B");
  let c = state("C");

  declare_transition!(
      &mut a => &b;
      when rule!(log_matches!(message == "foo"));
      do action!(
        flush_buffers &["trigger_buffer_id"];
        continue_streaming_to vec!["continuous_buffer_id"];
        logs_count 10;
        id "eventually_upload"
    )
  );
  declare_transition!(
    &mut b => &c;
    when rule!(log_matches!(message == "bar"))
  );

  let setup = Setup::new();

  let workflows_engine_config = WorkflowsEngineConfig::new(
    WorkflowsConfiguration::new_with_workflow_configurations_for_test(vec![
      workflow!(exclusive with a, b, c),
    ]),
    BTreeSet::from(["trigger_buffer_id".into()]),
    BTreeSet::from(["continuous_buffer_id".into()]),
  );

  let mut workflows_engine = setup
    .make_workflows_engine(workflows_engine_config.clone())
    .await;
  workflows_engine.log_destination_buffer_ids = BTreeSet::from(["trigger_buffer_id".into()]);

  // Set up no responses so that the actions continue to wait for the server's response.
  workflows_engine.set_awaiting_logs_upload_intent_decisions(vec![]);

  // The log below should trigger a buffer flush.
  let result = engine_process_log!(workflows_engine; "foo"; with labels!{});
  assert_eq!(
    result.log_destination_buffer_ids,
    Cow::Owned(BTreeSet::from(["trigger_buffer_id".into()]))
  );

  // The log below doesn't trigger a buffer flush, but it's emitted with a new session ID, which
  // should trigger a partial cleanup of the engine's state. It's worth noting that 'pending
  // actions' should not be cleared.
  workflows_engine.session_id = "new session ID".to_string();
  let result = engine_process_log!(workflows_engine; "not triggering"; with labels!{});
  assert_eq!(
    result.log_destination_buffer_ids,
    Cow::Owned(BTreeSet::from(["trigger_buffer_id".into()]))
  );

  // Confirm that the pending action was not cleaned up.
  assert_eq!(1, workflows_engine.state.pending_flush_actions.len());

  // Make sure that the engine's state is persisted to disk.
  assert!(workflows_engine.needs_state_persistence);
  workflows_engine.maybe_persist(false).await;
  assert!(!workflows_engine.needs_state_persistence);

  let setup = Setup::new_with_sdk_directory(&setup.sdk_directory);

  let mut workflows_engine = setup.make_workflows_engine(workflows_engine_config).await;
  workflows_engine.session_id = "new session ID".to_string();
  workflows_engine.log_destination_buffer_ids = BTreeSet::from(["trigger_buffer_id".into()]);

  workflows_engine
    .set_awaiting_logs_upload_intent_decisions(vec![IntentDecision::UploadImmediately]);

  workflows_engine.run_once_for_test(false).await;

  assert_eq!(
    vec![log_upload_intent_request::WorkflowActionUpload {
      workflow_action_ids: vec!["eventually_upload".to_string()],
      ..Default::default()
    },],
    workflows_engine.received_logs_upload_intents()
  );
  assert_eq!(
    workflows_engine.flushed_buffers(),
    vec![BTreeSet::from(["trigger_buffer_id".into()])],
  );
  workflows_engine.complete_flushes();

  let result = engine_process_log!(workflows_engine; "not triggering"; with labels!{});
  assert_eq!(
    result.log_destination_buffer_ids,
    Cow::Owned(BTreeSet::from(["trigger_buffer_id".into()]))
  );

  setup.collector.assert_counter_eq(
    1,
    "workflows:actions:streaming_buffers_action_initiations_total",
    labels! { "result" => "success" },
  );
  setup.collector.assert_counter_eq(
    1,
    "workflows:actions:streaming_buffers_action_completions_total",
    labels! { "type" => "session_changed" },
  );
}


#[tokio::test]
#[allow(clippy::many_single_char_names)]
async fn engine_continues_to_stream_upload_not_complete() {
  let mut a = state("A");
  let mut b = state("B");
  let c = state("C");

  declare_transition!(
      &mut a => &b;
      when rule!(log_matches!(message == "foo"));
      do action!(
        flush_buffers &["trigger_buffer_id"];
        continue_streaming_to vec!["continuous_buffer_id"];
        logs_count 10;
        id "eventually_upload"
    )
  );
  declare_transition!(
    &mut b => &c;
    when rule!(log_matches!(message == "bar"))
  );

  let setup = Setup::new();

  let workflows_engine_config = WorkflowsEngineConfig::new(
    WorkflowsConfiguration::new_with_workflow_configurations_for_test(vec![
      workflow!(exclusive with a, b, c),
    ]),
    BTreeSet::from(["trigger_buffer_id".into()]),
    BTreeSet::from(["continuous_buffer_id".into()]),
  );

  let mut workflows_engine = setup
    .make_workflows_engine(workflows_engine_config.clone())
    .await;
  workflows_engine.log_destination_buffer_ids = BTreeSet::from(["trigger_buffer_id".into()]);

  // Allow the intent to go through which should trigger an upload.
  workflows_engine
    .set_awaiting_logs_upload_intent_decisions(vec![IntentDecision::UploadImmediately]);

  // The log below should trigger a buffer flush.
  let result = engine_process_log!(workflows_engine; "foo"; with labels!{});
  assert_eq!(
    result.log_destination_buffer_ids,
    Cow::Owned(BTreeSet::from(["trigger_buffer_id".into()]))
  );

  log::info!("Running the engine for the first time.");
  workflows_engine.run_once_for_test(false).await;
  log::info!("after Running the engine for the first time.");

  assert_eq!(
    vec![log_upload_intent_request::WorkflowActionUpload {
      workflow_action_ids: vec!["eventually_upload".to_string()],
      ..Default::default()
    },],
    workflows_engine.received_logs_upload_intents()
  );
  assert_eq!(
    workflows_engine.flushed_buffers(),
    vec![BTreeSet::from(["trigger_buffer_id".into()])],
  );

  // Verify that we have transitioned to streaming.
  let result = engine_process_log!(workflows_engine; "streamed"; with labels!{});
  assert_eq!(
    result.log_destination_buffer_ids,
    Cow::Owned(BTreeSet::from(["continuous_buffer_id".into()]))
  );

  // Change the session. This would typically cause the engine to stop streaming, but we haven't
  // signaled that the upload is complete yet so the streaming action remains active.
  workflows_engine.session_id = "new session ID".to_string();
  let result = engine_process_log!(workflows_engine; "streamed"; with labels!{});
  assert_eq!(
    result.log_destination_buffer_ids,
    Cow::Owned(BTreeSet::from(["continuous_buffer_id".into()]))
  );

  workflows_engine.complete_flushes();

  // Now that the uploads have been completed, we'll be able to stop the streaming actions and
  // start routing logs back to the original buffer.
  let result = engine_process_log!(workflows_engine; "not streamed"; with labels!{});
  assert_eq!(
    result.log_destination_buffer_ids,
    Cow::Owned(BTreeSet::from(["trigger_buffer_id".into()]))
  );

  setup.collector.assert_counter_eq(
    1,
    "workflows:actions:streaming_buffers_action_initiations_total",
    labels! { "result" => "success" },
  );
  setup.collector.assert_counter_eq(
    1,
    "workflows:actions:streaming_buffers_action_completions_total",
    labels! { "type" => "session_changed" },
  );
}

#[tokio::test]
#[allow(clippy::cognitive_complexity)]
#[allow(clippy::many_single_char_names)]
async fn creating_new_runs_after_first_log_processing() {
  let mut a = state("A");
  let b = state("B");
  let mut c = state("C");
  let mut d = state("D");
  let e = state("E");

  declare_transition!(
    &mut a => &b;
    when rule!(
      any!(
        log_matches!(message == "foo"),
        log_matches!(tag("key") == "value"),
      ); times 100)
  );
  declare_transition!(
    &mut c => &d;
    when rule!(log_matches!(message == "bar"))
  );
  declare_transition!(
    &mut d => &e;
    when rule!(
      any!(
        log_matches!(message == "zar"),
        log_matches!(tag("key") == "value"),
      )
    )
  );

  let setup = Setup::new();
  setup
    .runtime
    .update_snapshot(&make_simple_update(vec![(
      bd_runtime::runtime::workflows::TraversalsCountLimitFlag::path(),
      ValueKind::Int(3),
    )]))
    .await;

  // This test assumes that internally `workflows_engine` iterates
  // over the list of its workflows in order.
  let mut workflows_engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![
        workflow!(exclusive with c, d, e),
        workflow!(exclusive with a, b),
      ],
    ))
    .await;
  assert!(workflows_engine.state.workflows[0].runs().is_empty());
  assert!(workflows_engine.state.workflows[1].runs().is_empty());

  engine_process_log!(workflows_engine; "bar");
  engine_assert_active_runs!(workflows_engine; 0; "D");
  engine_assert_active_runs!(workflows_engine; 1; "A");
  setup
    .collector
    .assert_counter_eq(2, "workflows:runs_total", labels! {"operation" => "start"});
  setup.collector.assert_counter_eq(
    1,
    "workflows:runs_total",
    labels! {"operation" => "advance"},
  );

  // * State "A" matches log but does not advance as its transition requires 100 matches.
  engine_process_log!(workflows_engine; "foo");
  engine_assert_active_runs!(workflows_engine; 0; "C", "D");
  engine_assert_active_runs!(workflows_engine; 1; "A");
  setup
    .collector
    .assert_counter_eq(0, "workflows:traversals_count_limit_hit_total", labels! {});

  // * States "D" (workflow #1) and "A" (workflow #2) match a log with (key => value) tag.
  // * State "D" advances to a final state "E" and the run is completed. The number of traversals
  //   goes from 3 to 2 and we are below traversals count limit.
  // * Before workflows engine starts processing workflow #2 (the one state "A") it checks whether
  //   the workflow needs a new run (in an initial state). It happens that a new run is needed so a
  //   run with initial state "A" is added to workflow #2.
  // * We process workflow #2. Both of its runs are in state "A" and match a log but do not advance
  //   as a transition requires 100 matches.
  engine_process_log!(workflows_engine; "not matching message"; with labels! { "key" => "value" });
  engine_assert_active_runs!(workflows_engine; 0; "C");
  engine_assert_active_runs!(workflows_engine; 1; "A", "A");
  setup
    .collector
    .assert_counter_eq(0, "workflows:traversals_count_limit_hit_total", labels! {});

  // In exclusive mode we will not make any new runs because both workflows have runs in the
  // initial state. With no matches and no traversals we stay under the limit.
  engine_process_log!(workflows_engine; "not matching message");
  engine_assert_active_runs!(workflows_engine; 0; "C");
  engine_assert_active_runs!(workflows_engine; 1; "A", "A");
  setup
    .collector
    .assert_counter_eq(0, "workflows:traversals_count_limit_hit_total", labels! {});
}

#[tokio::test]
async fn workflows_state_is_purged_when_session_id_changes() {
  let mut a = state("A");
  let mut b = state("B");
  let c = state("C");

  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "foo"); times 10)
  );
  declare_transition!(
    &mut b => &c;
    when rule!(log_matches!(message == "bar"))
  );

  let engine_config = WorkflowsEngineConfig::new_with_workflow_configurations(vec![
    workflow!(exclusive with a, b, c),
  ]);

  let setup = Setup::new();
  let mut workflows_engine = setup.make_workflows_engine(engine_config.clone()).await;

  // Session ID is empty on first engine initialization.
  assert!(workflows_engine.state.session_id.is_empty());
  // No traversals as no log has been processed yet.
  assert_eq!(0, workflows_engine.current_traversals_count);

  workflows_engine.process_log(
    &LogRef {
      log_type: LogType::Normal,
      log_level: log_level::DEBUG,
      message: &LogMessage::String("foo".to_string()),
      fields: &FieldsRef::new(
        &bd_test_helpers::workflow::make_tags(labels! {}),
        &LogFields::new(),
      ),
      session_id: "foo_session",
      occurred_at: time::OffsetDateTime::now_utc(),
      capture_session: None,
    },
    &BTreeSet::new(),
  );

  // Session ID captured from a process log.
  assert_eq!("foo_session", workflows_engine.state.session_id);
  assert!(workflows_engine.needs_state_persistence);

  workflows_engine.maybe_persist(false).await;
  assert!(!workflows_engine.needs_state_persistence);

  let setup = Setup::new_with_sdk_directory(&setup.sdk_directory);
  let mut workflows_engine = setup.make_workflows_engine(engine_config).await;

  // Read saved session ID from disk.
  assert_eq!("foo_session", workflows_engine.state.session_id);
  // Read saved workflow state from disk.
  engine_assert_active_runs!(workflows_engine; 0; "A");
  // One traversal trad from disk.
  assert_eq!(1, workflows_engine.current_traversals_count);

  // Process a log with a new session ID.
  workflows_engine.process_log(
    &LogRef {
      log_type: LogType::Normal,
      log_level: log_level::DEBUG,
      message: &LogMessage::String("bar".to_string()),
      fields: &FieldsRef::new(
        &bd_test_helpers::workflow::make_tags(labels! {}),
        &LogFields::new(),
      ),
      session_id: "bar_session",
      occurred_at: time::OffsetDateTime::now_utc(),
      capture_session: None,
    },
    &BTreeSet::new(),
  );

  // Session ID changed.
  assert_eq!("bar_session", workflows_engine.state.session_id);
  assert_eq!(1, workflows_engine.current_traversals_count);

  assert!(workflows_engine.needs_state_persistence);
  workflows_engine.maybe_persist(false).await;
  assert!(!workflows_engine.needs_state_persistence);

  // State was updated.
  assert_eq!(workflows_engine.state.session_id, "bar_session",);
  assert_eq!(1, workflows_engine.state.workflows.len());
  assert!(workflows_engine.state.pending_flush_actions.is_empty());
  assert!(workflows_engine.state.streaming_actions.is_empty());
  // No need to persist state as state file was removed already. The
  // only thing that needs storing is `session_ID` but having no session ID
  // stored on a disk is fine.
  assert!(!workflows_engine.needs_state_persistence);
  // In memory state was cleared.
  assert!(
    workflows_engine
      .state
      .workflows
      .iter()
      .all(Workflow::is_in_initial_state)
  );
}

#[tokio::test]
#[allow(clippy::cognitive_complexity)]
#[allow(clippy::many_single_char_names)]
async fn test_traversals_count_tracking() {
  let mut a = state("A");
  let mut b = state("B");
  let mut c = state("C");
  let d = state("D");
  let mut e = state("E");
  let f = state("F");

  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "foo"))
  );

  // First branch.
  declare_transition!(
    &mut b => &c;
    when rule!(log_matches!(message == "bar"))
  );
  declare_transition!(
    &mut c => &d;
    when rule!(log_matches!(message == "car"))
  );

  // Second branch.
  declare_transition!(
    &mut b => &e;
    when rule!(log_matches!(message == "dar"))
  );
  declare_transition!(
    &mut e => &f;
    when rule!(log_matches!(message == "far"))
  );

  let workflow = workflow!(exclusive with a, b, c, d, e, f);
  let setup = Setup::new();

  let engine_config = WorkflowsEngineConfig::new_with_workflow_configurations(vec![workflow]);
  let mut engine = setup.make_workflows_engine(engine_config.clone()).await;

  engine_process_log!(engine; "foo");
  assert_eq!(1, engine.state.workflows[0].runs().len());
  engine_assert_active_runs!(engine; 0; "B");
  assert_eq!(1, engine.current_traversals_count);

  // Log is matched and workflow moves to state "B".
  engine_process_log!(engine; "foo");
  assert_eq!(1, engine.state.workflows[0].runs().len());
  engine_assert_active_runs!(engine; 0; "B");
  assert_eq!(1, engine.current_traversals_count);

  // * A new initial state run is created and added to the beginning of runs list.
  // * Log is matched and workflow moves to state "C".
  engine_process_log!(engine; "bar");
  assert_eq!(2, engine.state.workflows[0].runs().len());
  engine_assert_active_runs!(engine; 0; "A", "C");
  assert_eq!(2, engine.current_traversals_count);

  // Log is not matched.
  engine_process_log!(engine; "dar");
  assert_eq!(2, engine.state.workflows[0].runs().len());
  engine_assert_active_runs!(engine; 0; "A", "C");
  assert_eq!(2, engine.current_traversals_count);

  // Log is matched and workflow is reset to its initial state.
  engine_process_log!(engine; "foo");
  assert_eq!(1, engine.state.workflows[0].runs().len());
  engine_assert_active_runs!(engine; 0; "B");
  assert_eq!(1, engine.current_traversals_count);

  // * A new initial state run is created and added to the beginning of runs list.
  // * Log is matched by the run that's not in an initial state and the run advances to state `C`.
  engine_process_log!(engine; "bar");
  assert_eq!(2, engine.state.workflows[0].runs().len());
  engine_assert_active_runs!(engine; 0; "A", "C");
  assert_eq!(2, engine.current_traversals_count);

  // Log is matched and the more advanced run moves to final state "D" and completes.
  engine_process_log!(engine; "car");
  engine_assert_active_runs!(engine; 0; "A");
  assert_eq!(1, engine.current_traversals_count);

  // Log is not matched.
  engine_process_log!(engine; "foo");
  assert_eq!(1, engine.state.workflows[0].runs().len());
  engine_assert_active_runs!(engine; 0; "B");
  assert_eq!(1, engine.current_traversals_count);

  // Log is matched and workflow moves to state "B".
  engine_process_log!(engine; "foo");
  assert_eq!(1, engine.state.workflows[0].runs().len());
  engine_assert_active_runs!(engine; 0; "B");
  assert_eq!(1, engine.current_traversals_count);

  // * A new initial state run is created and added to the beginning of runs list.
  // * Log is matched and workflow moves to "E" state.
  engine_process_log!(engine; "dar");
  assert_eq!(2, engine.state.workflows[0].runs().len());
  engine_assert_active_runs!(engine; 0; "A", "E");
  assert_eq!(2, engine.current_traversals_count);

  // Log is matched and workflow moves to final state "F" and completes.
  engine_process_log!(engine; "far");
  engine_assert_active_runs!(engine; 0; "A");
  assert_eq!(1, engine.current_traversals_count);

  // Log is not matched.
  engine_process_log!(engine; "no match");
  engine_assert_active_runs!(engine; 0; "A");
  assert_eq!(1, engine.current_traversals_count);

  // Checks that the traversal count does not change if we get update
  // with the same workflow.
  engine.update(engine_config.clone());
  assert_eq!(1, engine.current_traversals_count);

  // Check that traversals count goes to 0 if empty update happens.
  engine.update(WorkflowsEngineConfig::new_with_workflow_configurations(
    vec![],
  ));
  assert_eq!(0, engine.current_traversals_count);

  // Check that traversals stay at since no log has been processed yet.
  engine.update(engine_config);
  assert_eq!(0, engine.current_traversals_count);

  // A traversal is created to process an incoming log that's not matched.
  engine_process_log!(engine; "no match");
  engine_assert_active_runs!(engine; 0; "A");
  assert_eq!(1, engine.current_traversals_count);
}

#[tokio::test]
#[allow(clippy::cognitive_complexity)]
async fn test_exclusive_workflow_state_reset() {
  let mut a = state("A");
  let mut b = state("B");
  let mut c = state("C");
  let d = state("D");

  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "foo"))
  );
  declare_transition!(
    &mut b => &c;
    when rule!(log_matches!(message == "bar"))
  );
  declare_transition!(
    &mut c => &d;
    when rule!(log_matches!(message == "dar"))
  );

  let workflow = workflow!(exclusive with a, b, c, d);
  let setup = Setup::new();

  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![workflow],
    ))
    .await;

  // The log matches the transition coming out of the currently active node and workflow moves to
  // state `B`.
  engine_process_log!(engine; "foo");
  engine_assert_active_runs!(engine; 0; "B");
  setup
    .collector
    .assert_counter_eq(0, "workflows:workflow_resets_total", labels! {});
  setup.collector.assert_counter_eq(
    1,
    "workflows:runs_total",
    labels! { "operation" => "start" },
  );
  setup.collector.assert_counter_eq(
    1,
    "workflows:traversals_total",
    labels! { "operation" => "start" },
  );

  // * A new initial state run is created and added to the beginning of runs list so that the
  //   workflow has a run that's in an initial state.
  // * The log matches the transition coming out of the currently active node and workflow moves to
  //   state `C`.
  engine_process_log!(engine; "bar");
  engine_assert_active_runs!(engine; 0; "A", "C");
  setup
    .collector
    .assert_counter_eq(0, "workflows:workflow_resets_total", labels! {});
  setup.collector.assert_counter_eq(
    2,
    "workflows:runs_total",
    labels! { "operation" => "start" },
  );
  setup.collector.assert_counter_eq(
    2,
    "workflows:traversals_total",
    labels! { "operation" => "start" },
  );

  // The log is not matched by any of the runs.
  engine_process_log!(engine; "not matching");
  engine_assert_active_runs!(engine; 0;  "A", "C");
  setup
    .collector
    .assert_counter_eq(0, "workflows:workflow_resets_total", labels! {});
  setup.collector.assert_counter_eq(
    2,
    "workflows:runs_total",
    labels! { "operation" => "start" },
  );
  setup.collector.assert_counter_eq(
    2,
    "workflows:traversals_total",
    labels! { "operation" => "start" },
  );

  // * The log is not matched by the run that's not in an initial state.
  // * The log is matched by the run that's in an initial state. That causes the state advancement
  //   of the run and the removal of the other run.
  engine_process_log!(engine; "foo");
  engine_assert_active_runs!(engine; 0; "B");
  setup
    .collector
    .assert_counter_eq(1, "workflows:workflow_resets_total", labels! {});
  setup.collector.assert_counter_eq(
    2,
    "workflows:runs_total",
    labels! { "operation" => "start" },
  );
  setup
    .collector
    .assert_counter_eq(1, "workflows:runs_total", labels! { "operation" => "stop" });
  setup.collector.assert_counter_eq(
    2,
    "workflows:traversals_total",
    labels! { "operation" => "start" },
  );
  setup.collector.assert_counter_eq(
    1,
    "workflows:traversals_total",
    labels! { "operation" => "stop" },
  );
}

#[tokio::test]
#[allow(clippy::many_single_char_names)]
#[allow(clippy::cognitive_complexity)]
async fn test_exclusive_workflow_potential_fork() {
  let mut a = state("A");
  let mut b = state("B");
  let mut c = state("C");
  let mut d = state("D");
  let e = state("E");

  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "foo"))
  );
  declare_transition!(
    &mut b => &c;
    when rule!(log_matches!(message == "bar"))
  );
  declare_transition!(
    &mut c => &d;
    when rule!(log_matches!(message == "foo"))
  );
  declare_transition!(
    &mut d => &e;
    when rule!(log_matches!(message == "zar"))
  );

  let workflow = workflow!(exclusive with a, b, c, d, e);
  let setup = Setup::new();

  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![workflow],
    ))
    .await;

  // The log matches and workflow moves to state B.
  engine_process_log!(engine; "foo");
  engine_assert_active_runs!(engine; 0; "B");

  // * A new run is created and added to the beginning of runs list so that the workflow has a run
  //   that's in an initial state.
  // * The log matches and workflow moves to state C.
  engine_process_log!(engine; "bar");
  engine_assert_active_runs!(engine; 0; "A", "C");

  // The log matches transition from state `C` to `D` and at the same it matches the transition from
  // an initial state `A` to `B`. In this case, the workflow advances using `C` to `D` transition.
  engine_process_log!(engine; "foo");
  engine_assert_active_runs!(engine; 0; "A", "D");
}

fn sankey_workflow() -> crate::config::Config {
  let mut a = state("A");
  let mut b = state("B");
  let mut c = state("C");
  let d = state("D");

  let b_clone = b.clone();

  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "foo")),
    with { sankey_value!(fixed "sankey" => "first_extracted", counts_toward_limit false) }
  );
  declare_transition!(
    &mut b => &c;
    when rule!(log_matches!(message == "bar")),
    with { sankey_value!(extract_field "sankey" => "field_to_extract_key", counts_toward_limit false) }
  );
  declare_transition!(
    &mut b => &b_clone;
    when rule!(log_matches!(message == "bar_loop")),
    with { sankey_value!(fixed "sankey" => "loop", counts_toward_limit true) }
  );
  declare_transition!(
    &mut c => &d;
    when rule!(log_matches!(message == "dar"));
    do action!(
      emit_sankey "sankey";
      limit 2;
      tags {
        metric_tag!(extract "field_to_extract_from" => "extracted_field"),
        metric_tag!(fix "fixed_field" => "fixed_value")
      }
    )
  );

  workflow!(exclusive with a, b, c, d)
}

#[allow(clippy::many_single_char_names)]
#[tokio::test]
async fn generate_log_multiple() {
  let setup = Setup::new();

  let mut a = state("A");
  let mut b = state("B");
  let mut c = state("C");
  let mut d = state("D");
  let mut e = state("E");
  let f = state("F");
  let g = state("G");

  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "foo")),
    with {
      make_save_timestamp_extraction("timestamp1")
    }
  );

  declare_transition!(
    &mut b => &c;
    when rule!(log_matches!(message == "bar")),
    with {
      make_save_timestamp_extraction("timestamp2")
    }
  );

  declare_transition!(
    &mut c => &d;
    when rule!(log_matches!(message == "baz")),
    with {
      make_save_timestamp_extraction("timestamp3")
    };
    do action!(generate_log make_generate_log_action_proto("message1", &[
      ("duration",
       TestFieldType::Subtract(
        TestFieldRef::SavedTimestampId("timestamp2"),
        TestFieldRef::SavedTimestampId("timestamp1")
       ))
    ], "id1", LogType::Normal))
  );

  declare_transition!(
    &mut c => &e;
    when rule!(log_matches!(message == "baz")),
    with {
      make_save_timestamp_extraction("timestamp3")
    };
    do action!(generate_log make_generate_log_action_proto("message2", &[
      ("duration",
       TestFieldType::Subtract(
        TestFieldRef::SavedTimestampId("timestamp3"),
        TestFieldRef::SavedTimestampId("timestamp1")
       ))
    ], "id2", LogType::Normal))
  );

  declare_transition!(
    &mut d => &f;
    when rule!(log_matches!(tag("_generate_log_id") == "id1"));
    do action!(emit_counter "foo_metric"; value metric_value!(extract "duration"))
  );

  declare_transition!(
    &mut e => &g;
    when rule!(log_matches!(tag("_generate_log_id") == "id2"));
    do action!(emit_counter "bar_metric"; value metric_value!(extract "duration"))
  );

  let workflow = workflow!(exclusive with a, b, c, d, e, f, g);
  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![workflow],
    ))
    .await;
  let result = engine_process_log!(engine; "foo"; with labels!{};
                                   time datetime!(2023-01-01 00:00:00 UTC));
  assert!(result.logs_to_inject.is_empty());
  let result = engine_process_log!(engine; "bar"; with labels!{};
                                   time datetime!(2023-01-01 00:00:01 UTC));
  assert!(result.logs_to_inject.is_empty());
  let result = engine_process_log!(engine; "baz"; with labels!{};
                                   time datetime!(2023-01-01 00:00:02 UTC));
  assert_eq!(
    result.logs_to_inject.into_values().collect_vec(),
    vec![
      Log {
        log_level: log_level::DEBUG,
        log_type: LogType::Normal,
        message: "message1".into(),
        fields: [("duration".into(), "1000".into(),),].into(),
        matching_fields: [("_generate_log_id".into(), "id1".into(),)].into(),
        session_id: String::new(),
        occurred_at: OffsetDateTime::UNIX_EPOCH,
        capture_session: None,
      },
      Log {
        log_level: log_level::DEBUG,
        log_type: LogType::Normal,
        message: "message2".into(),
        fields: [("duration".into(), "2000".into(),),].into(),
        matching_fields: [("_generate_log_id".into(), "id2".into(),)].into(),
        session_id: String::new(),
        occurred_at: OffsetDateTime::UNIX_EPOCH,
        capture_session: None,
      }
    ]
  );

  // Simulate pushing both logs back through the engine.
  engine_process_log!(engine; "message1";
                      with labels!{"duration" => "1", "_generate_log_id" => "id1"};
                      time datetime!(2023-01-01 00:00:03 UTC));
  engine
    .collector
    .assert_workflow_counter_eq(1, "foo_metric", labels! {});
  engine_process_log!(engine; "message2";
                      with labels!{"duration" => "2", "_generate_log_id" => "id2"};
                      time datetime!(2023-01-01 00:00:03 UTC));
  engine
    .collector
    .assert_workflow_counter_eq(2, "bar_metric", labels! {});
}

#[tokio::test]
async fn generate_log_action() {
  let setup = Setup::new();

  let mut a = state("A");
  let mut b = state("B");
  let c = state("C");

  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "foo")),
    with {
      make_save_field_extraction("id1", "field1"),
      make_save_timestamp_extraction("timestamp1")
    }
  );

  declare_transition!(
    &mut b => &c;
    when rule!(log_matches!(message == "bar")),
    with { make_save_timestamp_extraction("timestamp2") };
    do action!(generate_log make_generate_log_action_proto("message", &[
      ("duration",
       TestFieldType::Subtract(
        TestFieldRef::SavedTimestampId("timestamp2"),
        TestFieldRef::SavedTimestampId("timestamp1")
       )),
       ("other", TestFieldType::Single(TestFieldRef::SavedFieldId("id1")))
    ], "id", LogType::Normal))
  );

  let workflow = workflow!(exclusive with a, b, c);
  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![workflow],
    ))
    .await;
  let result = engine_process_log!(engine; "foo"; with labels!{ "field1" => "value1" };
                                   time datetime!(2023-01-01 00:00:00 UTC));
  assert!(result.logs_to_inject.is_empty());
  let result = engine_process_log!(engine; "bar"; with labels!{};
                                   time datetime!(2023-01-01 00:00:00.003 UTC));
  assert_eq!(
    result.logs_to_inject.into_values().collect_vec(),
    vec![Log {
      log_level: log_level::DEBUG,
      log_type: LogType::Normal,
      message: "message".into(),
      fields: [
        ("duration".into(), "3".into(),),
        ("other".into(), "value1".into(),)
      ]
      .into(),
      matching_fields: [("_generate_log_id".into(), "id".into(),)].into(),
      session_id: String::new(),
      occurred_at: OffsetDateTime::UNIX_EPOCH,
      capture_session: None,
    }]
  );
}

#[tokio::test]
async fn sankey_action() {
  let setup = Setup::new();

  let workflow = sankey_workflow();
  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![workflow],
    ))
    .await;

  // Emit Sankey that's rejected for the upload by the server.

  engine
    .hooks
    .lock()
    .awaiting_sankey_upload_intent_decisions
    .push(Some(IntentDecision::Drop));

  engine_process_log!(engine; "foo");
  engine_process_log!(engine; "bar");
  engine_process_log!(engine; "dar");

  1.milliseconds().sleep().await;

  assert!(engine.hooks.lock().sankey_uploads.is_empty());
  assert_eq!(1, engine.hooks.lock().received_sankey_upload_intents.len());

  engine.collector.assert_workflow_counter_eq(
    1,
    "sankey",
    labels! {
      "_path_id" => "8b7712a10b290c2f0a386eef5a2d2b744305df42b0d4692e1c41911c98062afe",
      "fixed_field" => "fixed_value",
    },
  );

  // Emit Sankey that's accepted for the upload by the server.

  engine
    .hooks
    .lock()
    .awaiting_sankey_upload_intent_decisions
    .push(Some(IntentDecision::UploadImmediately));

  engine_process_log!(engine; "foo");
  engine_process_log!(engine; "bar_loop");
  engine_process_log!(engine; "bar_loop");
  engine_process_log!(engine; "bar_loop");
  engine_process_log!(engine; "bar"; with labels! { "field_to_extract_key" => "field_to_extract_value" });
  engine_process_log!(engine; "dar"; with labels! { "field_to_extract_from" => "extracted_value" });

  1.milliseconds().sleep().await;

  assert_eq!(1, engine.hooks.lock().sankey_uploads.len());
  assert_eq!(2, engine.hooks.lock().received_sankey_upload_intents.len());

  let mut first_upload = engine.hooks.lock().sankey_uploads[0].clone();

  // Confirm upload uuid is present and remove it from further comparisons.
  assert!(!first_upload.upload_uuid.is_empty());
  first_upload.upload_uuid = String::new();

  assert_eq!(
    SankeyPathUploadRequest {
      id: "sankey".to_string(),
      path_id: "8fdd001f37bfc8125d8f4704543fd6f3c089593d1b3c277f9eaa927c899f9aaa".to_string(),
      nodes: vec![
        Node {
          extracted_value: "first_extracted".to_string(),
          ..Default::default()
        },
        Node {
          extracted_value: "loop".to_string(),
          ..Default::default()
        },
        Node {
          extracted_value: "loop".to_string(),
          ..Default::default()
        },
        Node {
          extracted_value: "field_to_extract_value".to_string(),
          ..Default::default()
        },
      ],
      ..Default::default()
    },
    first_upload
  );

  engine.collector.assert_workflow_counter_eq(
    1,
    "sankey",
    labels! {
      "_path_id" => "8fdd001f37bfc8125d8f4704543fd6f3c089593d1b3c277f9eaa927c899f9aaa",
      "fixed_field" => "fixed_value",
      "extracted_field" => "extracted_value",
    },
  );

  // Emit exactly the same sankey path again. This time the Sankey path should not be uploaded as it
  // was uploaded already previously.

  engine_process_log!(engine; "foo");
  engine_process_log!(engine; "bar_loop");
  engine_process_log!(engine; "bar_loop");
  engine_process_log!(engine; "bar_loop");
  engine_process_log!(engine; "bar"; with labels! { "field_to_extract_key" => "field_to_extract_value" });
  engine_process_log!(engine; "dar"; with labels! { "field_to_extract_from" => "extracted_value" });

  1.milliseconds().sleep().await;

  assert_eq!(1, engine.hooks.lock().sankey_uploads.len());
  assert_eq!(2, engine.hooks.lock().received_sankey_upload_intents.len());

  engine.collector.assert_workflow_counter_eq(
    2,
    "sankey",
    labels! {
      "_path_id" => "8fdd001f37bfc8125d8f4704543fd6f3c089593d1b3c277f9eaa927c899f9aaa",
      "fixed_field" => "fixed_value",
      "extracted_field" => "extracted_value",
    },
  );
}

#[tokio::test]
async fn sankey_action_persistence() {
  let setup = Setup::new();

  let workflow = sankey_workflow();

  {
    let mut engine = setup
      .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
        vec![workflow.clone()],
      ))
      .await;

    // Emit a Sankey path but don't accept it.

    engine
      .hooks
      .lock()
      .awaiting_sankey_upload_intent_decisions
      .push(None);

    engine_process_log!(engine; "foo");
    engine_process_log!(engine; "bar");
    engine_process_log!(engine; "dar");

    1.milliseconds().sleep().await;

    engine.maybe_persist(false).await;
  }

  // After shutting down the engine, we only expect to see a response from the server if the Sankey
  // path upload was persisted to disk.

  let engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![workflow],
    ))
    .await;

  engine
    .hooks
    .lock()
    .awaiting_sankey_upload_intent_decisions
    .push(Some(IntentDecision::UploadImmediately));

  10.milliseconds().sleep().await;

  assert_eq!(1, engine.hooks.lock().sankey_uploads.len());
  assert_eq!(1, engine.hooks.lock().received_sankey_upload_intents.len());
}

#[tokio::test]
async fn sankey_action_persistence_limit() {
  let setup = Setup::new();

  let workflow = sankey_workflow();

  {
    let mut engine = setup
      .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
        vec![workflow.clone()],
      ))
      .await;

    // Emit 20 Sankey paths that we don't immediately accept.
    for i in 0 .. 20 {
      engine
        .hooks
        .lock()
        .awaiting_sankey_upload_intent_decisions
        .push(None);

      engine_process_log!(engine; "foo");
      engine_process_log!(engine; "bar"; with labels!{ "field_to_extract_key" => format!("value_{}", i) });
      engine_process_log!(engine; "dar");
    }

    1.milliseconds().sleep().await;

    engine.maybe_persist(false).await;
  }

  let engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![workflow],
    ))
    .await;

  // We only see 10 Sankey paths uploaded as we limit the number of enqueued Sankey paths to 10.
  for _ in 0 .. 10 {
    engine
      .hooks
      .lock()
      .awaiting_sankey_upload_intent_decisions
      .push(Some(IntentDecision::UploadImmediately));
  }

  10.milliseconds().sleep().await;

  assert_eq!(10, engine.hooks.lock().sankey_uploads.len());
  assert_eq!(10, engine.hooks.lock().received_sankey_upload_intents.len());
}

#[tokio::test]
async fn take_screenshot_action() {
  let mut a = state("A");
  let b = state("B");

  declare_transition!(
    &mut a => &b;
    when rule!(log_matches!(message == "foo"));
    do action!(screenshot "screenshot_action_id")
  );

  let workflow = workflow!(exclusive with a, b);
  let setup = Setup::new();

  let mut engine = setup
    .make_workflows_engine(WorkflowsEngineConfig::new_with_workflow_configurations(
      vec![workflow],
    ))
    .await;

  let result = engine_process_log!(engine; "foo");

  assert!(result.capture_screenshot);
}

fn make_runtime() -> std::sync::Arc<ConfigLoader> {
  let dir = tempfile::TempDir::with_prefix(".").unwrap();
  ConfigLoader::new(dir.path())
}
