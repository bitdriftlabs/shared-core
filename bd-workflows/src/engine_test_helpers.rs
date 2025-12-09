// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::{StateStore, WorkflowsEngine};
use crate::actions_flush_buffers::BuffersToFlush;
use crate::engine::{WORKFLOWS_STATE_FILE_NAME, WorkflowsEngineConfig, WorkflowsEngineResult};
use crate::test::TestLog;
use crate::workflow::{WorkflowDebugStateMap, WorkflowEvent, WorkflowTransitionDebugState};
use bd_api::DataUpload;
use bd_api::upload::{IntentDecision, IntentResponse, UploadResponse};
use bd_client_stats::Stats;
use bd_client_stats_store::Collector;
use bd_log_primitives::tiny_set::TinySet;
use bd_log_primitives::{LogFields, LogMessage, log_level};
use bd_proto::protos::client::api::log_upload_intent_request::Intent_type::WorkflowActionUpload;
use bd_proto::protos::client::api::{
  SankeyIntentRequest,
  SankeyPathUploadRequest,
  log_upload_intent_request,
};
use bd_proto::protos::logging::payload::LogType;
use bd_runtime::runtime::ConfigLoader;
use bd_stats_common::workflow::{WorkflowDebugStateKey, WorkflowDebugTransitionType};
use bd_time::TimeDurationExt;
use std::borrow::Cow;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};
use time::ext::NumericalDuration;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;

// Static empty TinySet for state changes, which don't write to log buffers
pub static EMPTY_BUFFER_IDS: LazyLock<TinySet<Cow<'static, str>>> =
  LazyLock::new(|| TinySet::from([]));

pub enum DebugStateType {
  StartOrReset,
  StateId(&'static str, WorkflowDebugTransitionType),
}

#[allow(clippy::type_complexity)]
pub fn assert_workflow_debug_state(
  result: &WorkflowsEngineResult<'_>,
  expected: &[(&str, &[(DebugStateType, WorkflowTransitionDebugState)])],
) {
  let expected: Vec<(String, WorkflowDebugStateMap)> = expected
    .iter()
    .map(|(workflow_id, states)| {
      (
        (*workflow_id).to_string(),
        WorkflowDebugStateMap(Box::new(
          states
            .iter()
            .map(|(state_type, state)| {
              (
                match state_type {
                  DebugStateType::StartOrReset => WorkflowDebugStateKey::StartOrReset,
                  DebugStateType::StateId(state_id, transition_type) => {
                    WorkflowDebugStateKey::new_state_transition(
                      (*state_id).to_string(),
                      *transition_type,
                    )
                  },
                },
                state.clone(),
              )
            })
            .collect(),
        )),
      )
    })
    .collect();

  assert_eq!(expected, result.workflow_debug_state);
}

//
// Hooks
//

#[derive(Default)]
pub struct Hooks {
  pub flushed_buffers: Vec<BuffersToFlush>,
  pub received_logs_upload_intents: Vec<log_upload_intent_request::WorkflowActionUpload>,
  pub awaiting_logs_upload_intent_decisions: Vec<IntentDecision>,

  pub sankey_uploads: Vec<SankeyPathUploadRequest>,
  pub received_sankey_upload_intents: Vec<SankeyIntentRequest>,
  pub awaiting_sankey_upload_intent_decisions: Vec<Option<IntentDecision>>,
}

//
// AnnotatedWorkflowsEngine
//

pub struct AnnotatedWorkflowsEngine {
  pub engine: WorkflowsEngine,

  pub session_id: String,
  pub log_destination_buffer_ids: TinySet<Cow<'static, str>>,

  pub hooks: Arc<parking_lot::Mutex<Hooks>>,

  pub client_stats: Arc<Stats>,

  pub task_handle: JoinHandle<()>,
}

impl AnnotatedWorkflowsEngine {
  pub fn new(
    engine: WorkflowsEngine,
    hooks: Arc<parking_lot::Mutex<Hooks>>,
    client_stats: Arc<Stats>,
    task_handle: JoinHandle<()>,
  ) -> Self {
    Self {
      engine,

      session_id: "foo_session".to_string(),
      log_destination_buffer_ids: TinySet::default(),

      hooks,

      client_stats,

      task_handle,
    }
  }

  pub fn process_log(&mut self, log: TestLog) -> WorkflowsEngineResult<'_> {
    self.engine.process_event(
      WorkflowEvent::Log(&bd_log_primitives::Log {
        log_type: LogType::NORMAL,
        log_level: log_level::DEBUG,
        message: LogMessage::String(log.message),
        session_id: log.session.unwrap_or_else(|| self.session_id.clone()),
        occurred_at: log.occurred_at,
        fields: bd_test_helpers::workflow::make_tags(log.tags),
        matching_fields: LogFields::new(),
        capture_session: None,
      }),
      &self.log_destination_buffer_ids,
      &bd_state::test::TestStateReader::default(),
      log.now,
    )
  }

  pub fn process_state_change(
    &mut self,
    state_change: &bd_state::StateChange,
  ) -> WorkflowsEngineResult<'_> {
    self.process_state_change_with_reader(state_change, &bd_state::test::TestStateReader::default())
  }

  pub fn process_state_change_with_reader(
    &mut self,
    state_change: &bd_state::StateChange,
    state_reader: &dyn bd_state::StateReader,
  ) -> WorkflowsEngineResult<'_> {
    // State changes don't write to log buffers, so use the static empty set
    // Most tests use empty fields for simplicity. Tests that need to verify global metadata
    // fields should call engine.process_event() directly with custom fields.
    let empty_fields = bd_log_primitives::LogFields::default();
    self.engine.process_event(
      WorkflowEvent::StateChange(
        state_change,
        bd_log_primitives::FieldsRef::new(&empty_fields, &empty_fields),
      ),
      &EMPTY_BUFFER_IDS,
      state_reader,
      state_change.timestamp,
    )
  }

  pub async fn run_once_for_test(&mut self) {
    self.engine.run_once().await;
    // Give the task started inside of `run_for_test()` method chance to run before proceeding.
    1.milliseconds().sleep().await;
  }

  pub fn run_for_test(
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
                  assert!(!hooks.lock().awaiting_sankey_upload_intent_decisions.is_empty(), "received sankey upload intent there are no awaiting intents");

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

  pub fn flushed_buffers(&self) -> Vec<TinySet<Cow<'static, str>>> {
    self
      .hooks
      .lock()
      .flushed_buffers
      .iter()
      .map(|b| b.buffer_ids.clone())
      .collect()
  }

  pub fn complete_flushes(&self) {
    for b in &mut self.hooks.lock().flushed_buffers.drain(..) {
      b.response_tx.send(()).unwrap();
    }
  }

  pub fn received_logs_upload_intents(
    &self,
  ) -> Vec<log_upload_intent_request::WorkflowActionUpload> {
    self.hooks.lock().received_logs_upload_intents.clone()
  }

  pub fn set_awaiting_logs_upload_intent_decisions(&self, decisions: Vec<IntentDecision>) {
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

pub struct Setup {
  pub runtime: Arc<ConfigLoader>,
  pub collector: Collector,
  pub sdk_directory: Arc<tempfile::TempDir>,
}

impl Setup {
  pub fn new() -> Self {
    Self::new_with_sdk_directory(&Arc::new(tempfile::TempDir::with_prefix("root-").unwrap()))
  }

  pub fn new_with_sdk_directory(sdk_directory: &Arc<tempfile::TempDir>) -> Self {
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
  pub async fn make_workflows_engine(
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
      stats.clone(),
    );

    let task_handle =
      AnnotatedWorkflowsEngine::run_for_test(buffers_to_flush_rx, data_upload_rx, hooks.clone());

    workflows_engine.start(workflows_engine_config, false).await;

    AnnotatedWorkflowsEngine::new(workflows_engine, hooks, stats, task_handle)
  }

  pub fn make_state_store(&self) -> StateStore {
    StateStore::new(
      self.sdk_directory.path(),
      &self.collector.scope("state_store"),
      &self.runtime,
    )
  }

  pub fn workflows_state_path(&self) -> PathBuf {
    self.sdk_directory.path().join(WORKFLOWS_STATE_FILE_NAME)
  }
}

pub fn make_runtime() -> std::sync::Arc<ConfigLoader> {
  let dir = tempfile::TempDir::with_prefix(".").unwrap();
  ConfigLoader::new(dir.path())
}

//
// State Change Test Helpers
//

use bd_proto::protos::state::matcher::{StateValueMatch, state_value_match};
use bd_proto::protos::state::scope::StateScope;
use bd_proto::protos::workflow::workflow::workflow::rule::Rule_type;
use bd_proto::protos::workflow::workflow::workflow::{Rule, RuleStateChangeMatch};

pub fn make_state_change_rule(
  scope: bd_state::Scope,
  key: &str,
  new_value_match: state_value_match::Value_match,
) -> Rule {
  Rule {
    rule_type: Some(Rule_type::RuleStateChangeMatch(RuleStateChangeMatch {
      scope: match scope {
        bd_state::Scope::FeatureFlagExposure => StateScope::FEATURE_FLAG.into(),
        bd_state::Scope::GlobalState => StateScope::GLOBAL_STATE.into(),
      },
      key: key.to_string(),
      previous_value: protobuf::MessageField::none(),
      new_value: protobuf::MessageField::some(StateValueMatch {
        value_match: Some(new_value_match),
        ..Default::default()
      }),
      ..Default::default()
    })),
    ..Default::default()
  }
}

pub fn make_string_match(value: &str) -> state_value_match::Value_match {
  use bd_proto::protos::value_matcher::value_matcher::Operator;
  use bd_proto::protos::value_matcher::value_matcher::string_value_match::String_value_match_type;

  state_value_match::Value_match::StringValueMatch(
    bd_proto::protos::value_matcher::value_matcher::StringValueMatch {
      operator: Operator::OPERATOR_EQUALS.into(),
      string_value_match_type: Some(String_value_match_type::MatchValue(value.to_string())),
      ..Default::default()
    },
  )
}

pub fn make_is_set_match() -> state_value_match::Value_match {
  state_value_match::Value_match::IsSetMatch(
    bd_proto::protos::value_matcher::value_matcher::IsSetMatch::default(),
  )
}

// Wrapper macros for engine tests that delegate to workflow-level assert macros
#[macro_export]
macro_rules! engine_assert_active_runs {
  ($engine:expr; $workflow_index:expr; $($state_id:expr),+) => {{
    // Create an AnnotatedWorkflow from the engine's workflow and config
    let annotated = $crate::workflow::workflow_test::AnnotatedWorkflow {
      config: $engine.engine.configs[$workflow_index].clone(),
      workflow: $engine.engine.state.workflows[$workflow_index].clone(),
    };
    $crate::assert_active_runs!(annotated; $($state_id),+);
  }};
}

#[macro_export]
macro_rules! engine_assert_active_run_traversals {
  ($engine:expr; $workflow_index:expr => $run_index:expr; $($state_id:expr),+) => {{
    // Create an AnnotatedWorkflow from the engine's workflow and config
    let annotated = $crate::workflow::workflow_test::AnnotatedWorkflow {
      config: $engine.engine.configs[$workflow_index].clone(),
      workflow: $engine.engine.state.workflows[$workflow_index].clone(),
    };
    $crate::assert_active_run_traversals!(annotated; $run_index; $($state_id),+);
  }};
}
