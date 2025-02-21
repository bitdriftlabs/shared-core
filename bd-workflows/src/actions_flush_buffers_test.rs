// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{Negotiator, NegotiatorOutput};
use crate::actions_flush_buffers::{
  FlushBuffersActionsProcessingResult,
  PendingFlushBuffersAction,
  Resolver,
  ResolverConfig,
  Streaming,
  StreamingBuffersAction,
  StreamingBuffersActionsProcessingResult,
};
use crate::config::ActionFlushBuffers;
use assert_matches::assert_matches;
use bd_api::DataUpload;
use bd_api::upload::{IntentDecision, IntentResponse};
use bd_client_stats_store::Collector;
use bd_client_stats_store::test::StatsHelper;
use bd_stats_common::labels;
use pretty_assertions::assert_eq;
use std::collections::BTreeSet;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;


// Easy-to-use wrapper that helps with "Negotiator" testing.
#[derive(Default)]
struct Setup {
  collector: Collector,
}

impl Setup {
  fn make_negotiator(&self, response_decision: IntentResponse) -> NegotiatorWrapper {
    let (input_tx, input_rx) = tokio::sync::mpsc::channel(1);
    let (data_upload_tx, data_upload_rx) = tokio::sync::mpsc::channel(1);

    let (negotiator, output_rx) =
      Negotiator::new(input_rx, data_upload_tx, &self.collector.scope("test"));

    NegotiatorWrapper {
      input_tx,
      output_rx,
      negotiator_task_handle: negotiator.run(),
      intent_server_task_handle: tokio::task::spawn(async move {
        let mut data_upload_rx = data_upload_rx;
        loop {
          if let Some(data_upload) = data_upload_rx.recv().await {
            if let DataUpload::LogsUploadIntentRequest(intent) = data_upload {
              intent.response_tx.send(response_decision.clone()).unwrap();
            } else {
              panic!("unknown request type");
            }
          }
        }
      }),
    }
  }
}

struct NegotiatorWrapper {
  input_tx: Sender<PendingFlushBuffersAction>,
  output_rx: Receiver<NegotiatorOutput>,

  negotiator_task_handle: JoinHandle<()>,
  intent_server_task_handle: JoinHandle<()>,
}

impl std::ops::Drop for NegotiatorWrapper {
  fn drop(&mut self) {
    self.negotiator_task_handle.abort();
    self.intent_server_task_handle.abort();
  }
}

#[tokio::test]
async fn pending_buffers_standardization_removes_references_to_non_existing_trigger_buffers() {
  let mut resolver = Resolver::new(&Collector::default().scope("test"));
  resolver.update(ResolverConfig::new(
    BTreeSet::from([
      "existing_trigger_buffer_id_1".into(),
      "existing_trigger_buffer_id_2".into(),
    ]),
    BTreeSet::new(),
  ));

  let result = resolver.standardize_pending_actions(BTreeSet::from([
    PendingFlushBuffersAction {
      id: "action_id_1".to_string(),
      session_id: "foo_session_id".to_string(),
      trigger_buffer_ids: BTreeSet::from([
        "existing_trigger_buffer_id_1".into(),
        "unknown_trigger_buffer_id".into(),
      ]),
      streaming: None,
    },
    PendingFlushBuffersAction {
      id: "action_id_2".to_string(),
      session_id: "foo_session_id".to_string(),
      trigger_buffer_ids: BTreeSet::from(["unknown_trigger_buffer_id".into()]),
      streaming: None,
    },
    PendingFlushBuffersAction {
      id: "action_id_3".to_string(),
      session_id: "bar_session_id".to_string(),
      trigger_buffer_ids: BTreeSet::from(["existing_trigger_buffer_id_2".into()]),
      streaming: Some(Streaming {
        destination_continuous_buffer_ids: BTreeSet::from(["unknown_continuous_buffer_id".into()]),
        max_logs_count: Some(10),
      }),
    },
  ]));

  assert_eq!(
    BTreeSet::from([
      PendingFlushBuffersAction {
        id: "action_id_1".to_string(),
        session_id: "foo_session_id".to_string(),
        trigger_buffer_ids: BTreeSet::from([
          // The unknown trigger buffer ID present in the original flush buffers action is no
          // longer present.
          "existing_trigger_buffer_id_1".into(),
        ]),
        streaming: None,
      },
      // "action_id_2" is not present anymore as it didn't define any valid (known) source
      // trigger buffer ID.
      PendingFlushBuffersAction {
        id: "action_id_3".to_string(),
        session_id: "bar_session_id".to_string(),
        trigger_buffer_ids: BTreeSet::from([
          // The unknown continuous buffer ID present in the original flush buffers action is
          // no longer present.
          "existing_trigger_buffer_id_2".into(),
        ]),
        streaming: Some(Streaming {
          destination_continuous_buffer_ids: BTreeSet::from(
            ["unknown_continuous_buffer_id".into()]
          ),
          max_logs_count: Some(10),
        }),
      },
    ]),
    result
  );
}

#[tokio::test]
async fn streaming_buffers_standardization_removes_references_to_non_existing_buffers() {
  let mut resolver = Resolver::new(&Collector::default().scope("test"));
  resolver.update(ResolverConfig::new(
    BTreeSet::from([
      "existing_trigger_buffer_id_1".into(),
      "existing_trigger_buffer_id_2".into(),
    ]),
    BTreeSet::from([
      "existing_continuous_buffer_id_1".into(),
      "existing_continuous_buffer_id_2".into(),
    ]),
  ));

  let result = resolver.standardize_streaming_buffers(vec![
    StreamingBuffersAction {
      id: "action_id_1".to_string(),
      session_id: "foo_session_id".to_string(),
      source_trigger_buffer_ids: BTreeSet::from(["existing_trigger_buffer_id_1".into()]),
      destination_continuous_buffer_ids: BTreeSet::from([
        "existing_continuous_buffer_id_1".into(),
        "unknown_continuous_buffer_id".into(),
      ]),
      max_logs_count: Some(10),
      logs_count: 0,
    },
    StreamingBuffersAction {
      id: "action_id_2".to_string(),
      session_id: "foo_session_id".to_string(),
      source_trigger_buffer_ids: BTreeSet::from(["unknown_trigger_buffer_id".into()]),
      destination_continuous_buffer_ids: BTreeSet::from([
        "existing_continuous_buffer_id_1".into(),
        "unknown_continuous_buffer_id".into(),
      ]),
      max_logs_count: Some(10),
      logs_count: 0,
    },
    StreamingBuffersAction {
      id: "action_id_3".to_string(),
      session_id: "foo_session_id".to_string(),
      source_trigger_buffer_ids: BTreeSet::from([
        "existing_trigger_buffer_id_1".into(),
        "unknown_trigger_buffer_id".into(),
      ]),
      destination_continuous_buffer_ids: BTreeSet::from([
        "existing_continuous_buffer_id_1".into(),
        "unknown_continuous_buffer_id".into(),
      ]),
      max_logs_count: Some(10),
      logs_count: 0,
    },
    StreamingBuffersAction {
      id: "action_id_4".to_string(),
      session_id: "foo_session_id".to_string(),
      source_trigger_buffer_ids: BTreeSet::from([
        "existing_trigger_buffer_id_1".into(),
        "unknown_trigger_buffer_id".into(),
      ]),
      destination_continuous_buffer_ids: BTreeSet::from(["unknown_continuous_buffer_id".into()]),
      max_logs_count: Some(10),
      logs_count: 0,
    },
    StreamingBuffersAction {
      id: "action_id_5".to_string(),
      session_id: "bar_session_id".to_string(),
      source_trigger_buffer_ids: BTreeSet::from([
        "existing_trigger_buffer_id_1".into(),
        "unknown_trigger_buffer_id".into(),
      ]),
      destination_continuous_buffer_ids: BTreeSet::from([
        "existing_continuous_buffer_id_1".into(),
        "unknown_continuous_buffer_id".into(),
      ]),
      max_logs_count: Some(10),
      logs_count: 0,
    },
  ]);

  assert_eq!(
    vec![
      StreamingBuffersAction {
        id: "action_id_1".to_string(),
        session_id: "foo_session_id".to_string(),
        source_trigger_buffer_ids: BTreeSet::from(["existing_trigger_buffer_id_1".into()]),
        destination_continuous_buffer_ids: BTreeSet::from([
          "existing_continuous_buffer_id_1".into(),
        ]),
        max_logs_count: Some(10),
        logs_count: 0,
      },
      StreamingBuffersAction {
        id: "action_id_3".to_string(),
        session_id: "foo_session_id".to_string(),
        source_trigger_buffer_ids: BTreeSet::from(["existing_trigger_buffer_id_1".into(),]),
        destination_continuous_buffer_ids: BTreeSet::from([
          "existing_continuous_buffer_id_1".into(),
        ]),
        max_logs_count: Some(10),
        logs_count: 0,
      },
      StreamingBuffersAction {
        id: "action_id_5".to_string(),
        session_id: "bar_session_id".to_string(),
        source_trigger_buffer_ids: BTreeSet::from(["existing_trigger_buffer_id_1".into(),]),
        destination_continuous_buffer_ids: BTreeSet::from([
          "existing_continuous_buffer_id_1".into(),
        ]),
        max_logs_count: Some(10),
        logs_count: 0,
      },
    ],
    result
  );
}

#[test]
fn process_flush_buffers_actions() {
  let collector = Collector::default();

  let mut resolver = Resolver::new(&collector.scope("test"));
  resolver.update(ResolverConfig::new(
    BTreeSet::from(["existing_trigger_buffer_id".into()]),
    BTreeSet::new(),
  ));

  let actions = BTreeSet::from([
    ActionFlushBuffers {
      id: "action_id_1".to_string(),
      buffer_ids: BTreeSet::from(["existing_trigger_buffer_id".to_string()]),
      streaming: None,
    },
    ActionFlushBuffers {
      id: "action_id_2".to_string(),
      buffer_ids: BTreeSet::from(["existing_trigger_buffer_id".to_string()]),
      streaming: None,
    },
    ActionFlushBuffers {
      id: "action_id_3".to_string(),
      buffer_ids: BTreeSet::from(["existing_trigger_buffer_id".to_string()]),
      streaming: None,
    },
    ActionFlushBuffers {
      id: "action_id_4".to_string(),
      buffer_ids: BTreeSet::from(["non_existing_trigger_buffer_id".to_string()]),
      streaming: None,
    },
  ]);

  let result = resolver.process_flush_buffer_actions(
    actions.iter().collect(),
    "foo_session_id",
    &BTreeSet::from([PendingFlushBuffersAction {
      id: "action_id_2".to_string(),
      session_id: "foo_session_id".to_string(),
      trigger_buffer_ids: BTreeSet::new(),
      streaming: None,
    }]),
    &[StreamingBuffersAction {
      id: "action_id_3".to_string(),
      session_id: "foo_session_id".to_string(),
      source_trigger_buffer_ids: BTreeSet::from(["existing_trigger_buffer_id".into()]),
      destination_continuous_buffer_ids: BTreeSet::new(),
      max_logs_count: Some(10),
      logs_count: 0,
    }],
  );

  assert_eq!(
    FlushBuffersActionsProcessingResult {
      new_pending_actions_to_add: BTreeSet::from([PendingFlushBuffersAction {
        id: "action_id_1".to_string(),
        session_id: "foo_session_id".to_string(),
        trigger_buffer_ids: BTreeSet::from(["existing_trigger_buffer_id".into(),]),
        streaming: None,
      }]),
      triggered_flush_buffers_action_ids: BTreeSet::from([
        "action_id_1",
        "action_id_2",
        "action_id_3",
        "action_id_4",
      ]),
      triggered_flushes_buffer_ids: BTreeSet::from(["existing_trigger_buffer_id".into(),])
    },
    result
  );

  collector.assert_counter_eq(
    1,
    "test:flush_buffers_action_initiations_total",
    labels! { "result" => "success" },
  );
  collector.assert_counter_eq(
    1,
    "test:flush_buffers_action_initiations_total",
    labels! { "result" => "dismiss_already_uploading" },
  );
  collector.assert_counter_eq(
    1,
    "test:flush_buffers_action_initiations_total",
    labels! { "result" => "dismiss_already_streaming" },
  );
  collector.assert_counter_eq(
    1,
    "test:flush_buffers_action_initiations_total",
    labels! { "result" => "dismiss_other" },
  );
}

#[test]
fn process_flush_buffer_action_with_no_buffers() {
  let collector = Collector::default();

  let mut resolver = Resolver::new(&collector.scope("test"));
  resolver.update(ResolverConfig::new(
    BTreeSet::from(["existing_trigger_buffer_id".into()]),
    BTreeSet::from(["existing_continuous_buffer_id".into()]),
  ));

  let actions = BTreeSet::from([ActionFlushBuffers {
    id: "action_id".to_string(),
    buffer_ids: BTreeSet::new(),
    streaming: Some(crate::config::Streaming {
      destination_continuous_buffer_ids: BTreeSet::new(),
      max_logs_count: Some(10),
    }),
  }]);

  let result = resolver.process_flush_buffer_actions(
    actions.iter().collect(),
    "foo_session_id",
    &BTreeSet::new(),
    &[],
  );

  assert_eq!(
    FlushBuffersActionsProcessingResult {
      new_pending_actions_to_add: BTreeSet::from([PendingFlushBuffersAction {
        id: "action_id".to_string(),
        session_id: "foo_session_id".to_string(),
        trigger_buffer_ids: BTreeSet::from(["existing_trigger_buffer_id".into(),]),
        streaming: Some(Streaming {
          destination_continuous_buffer_ids: BTreeSet::from([
            "existing_continuous_buffer_id".into()
          ]),
          max_logs_count: Some(10),
        }),
      }]),
      triggered_flush_buffers_action_ids: BTreeSet::from(["action_id",]),
      triggered_flushes_buffer_ids: BTreeSet::from(["existing_trigger_buffer_id".into(),])
    },
    result
  );
}

#[test]
fn process_streaming_buffers_actions() {
  let collector = Collector::default();

  let mut resolver = Resolver::new(&collector.scope("test"));
  resolver.update(ResolverConfig::new(
    BTreeSet::from(["existing_trigger_buffer_id".into()]),
    BTreeSet::from(["existing_continuous_buffer_id".into()]),
  ));

  let result = resolver.process_streaming_actions(
    vec![
      (
        StreamingBuffersAction {
          id: "action_id_1".to_string(),
          session_id: "foo_session_id".to_string(),
          source_trigger_buffer_ids: BTreeSet::from(["existing_trigger_buffer_id".into()]),
          destination_continuous_buffer_ids: BTreeSet::from(["continuous_buffer_id".into()]),
          max_logs_count: Some(10),
          logs_count: 0,
        },
        true,
      ),
      (
        StreamingBuffersAction {
          id: "action_id_2".to_string(),
          session_id: "foo_session_id".to_string(),
          source_trigger_buffer_ids: BTreeSet::from(["existing_trigger_buffer_id".into()]),
          destination_continuous_buffer_ids: BTreeSet::from(["continuous_buffer_id".into()]),
          max_logs_count: Some(10),
          logs_count: 10,
        },
        true,
      ),
    ],
    &BTreeSet::from(["existing_trigger_buffer_id".into()]),
    "foo_session_id",
  );

  assert_eq!(
    StreamingBuffersActionsProcessingResult {
      log_destination_buffer_ids: BTreeSet::from(["continuous_buffer_id".into()]),
      has_changed_streaming_actions: true,
      updated_streaming_actions: vec![StreamingBuffersAction {
        id: "action_id_1".to_string(),
        session_id: "foo_session_id".to_string(),
        source_trigger_buffer_ids: BTreeSet::from(["existing_trigger_buffer_id".into()]),
        destination_continuous_buffer_ids: BTreeSet::from(["continuous_buffer_id".into()]),
        max_logs_count: Some(10),
        logs_count: 1,
      },],
    },
    result
  );

  collector.assert_counter_eq(
    1,
    "test:streaming_buffers_action_applications_total",
    labels! {},
  );
  collector.assert_counter_eq(
    1,
    "test:streaming_buffers_action_completions_total",
    labels! { "type" => "termination_criterion_met" },
  );
}

#[tokio::test]
async fn negotiator_upload_flow() {
  let setup = Setup::default();
  let mut negotiator = setup.make_negotiator(IntentResponse {
    uuid: "action_id".to_string(),
    decision: IntentDecision::UploadImmediately,
  });

  let pending_action = PendingFlushBuffersAction {
    id: "action_id".to_string(),
    session_id: "session_id".to_string(),
    trigger_buffer_ids: BTreeSet::new(),
    streaming: None,
  };

  negotiator
    .input_tx
    .try_send(pending_action.clone())
    .unwrap();

  assert_matches!(
    negotiator.output_rx.recv().await.unwrap(),
    NegotiatorOutput::UploadApproved(action)
        if action == pending_action
  );

  setup.collector.assert_counter_eq(
    1,
    "test:logs_upload_intent_negotiation_initiations_total",
    labels! {},
  );
  setup.collector.assert_counter_eq(
    1,
    "test:logs_upload_intent_negotiation_completions_total",
    labels! { "result" => "upload" },
  );

  negotiator
    .input_tx
    .try_send(pending_action.clone())
    .unwrap();

  assert_matches!(
    negotiator.output_rx.recv().await.unwrap(),
    NegotiatorOutput::UploadApproved(action)
        if action == pending_action
  );

  setup.collector.assert_counter_eq(
    2,
    "test:logs_upload_intent_negotiation_initiations_total",
    labels! {},
  );
  setup.collector.assert_counter_eq(
    2,
    "test:logs_upload_intent_negotiation_completions_total",
    labels! { "result" => "upload" },
  );
}

#[tokio::test]
async fn negotiator_drop_flow() {
  let setup = Setup::default();
  let mut negotiator = setup.make_negotiator(IntentResponse {
    uuid: "action_id".to_string(),
    decision: IntentDecision::Drop,
  });

  let pending_action = PendingFlushBuffersAction {
    id: "action_id".to_string(),
    session_id: "session_id".to_string(),
    trigger_buffer_ids: BTreeSet::new(),
    streaming: None,
  };

  negotiator
    .input_tx
    .try_send(pending_action.clone())
    .unwrap();

  assert_matches!(
    negotiator.output_rx.recv().await.unwrap(),
    NegotiatorOutput::UploadRejected(action)
        if action == pending_action
  );

  setup.collector.assert_counter_eq(
    1,
    "test:logs_upload_intent_negotiation_initiations_total",
    labels! {},
  );
  setup.collector.assert_counter_eq(
    1,
    "test:logs_upload_intent_negotiation_completions_total",
    labels! { "result" => "drop" },
  );

  negotiator
    .input_tx
    .try_send(pending_action.clone())
    .unwrap();

  assert_matches!(
    negotiator.output_rx.recv().await.unwrap(),
    NegotiatorOutput::UploadRejected(action)
        if action == pending_action
  );

  setup.collector.assert_counter_eq(
    2,
    "test:logs_upload_intent_negotiation_initiations_total",
    labels! {},
  );
  setup.collector.assert_counter_eq(
    1,
    "test:logs_upload_intent_negotiation_completions_total",
    labels! { "result" => "drop_already_rejected" },
  );
  setup.collector.assert_counter_eq(
    1,
    "test:logs_upload_intent_negotiation_completions_total",
    labels! { "result" => "drop" },
  );
}
