// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./actions_flush_buffers_test.rs"]
mod actions_flush_buffers_test;

use crate::config::{ActionFlushBuffers, BufferId, FlushBufferId};
use anyhow::anyhow;
use bd_api::DataUpload;
use bd_api::upload::{Intent_type, IntentDecision, TrackedLogUploadIntent, WorkflowActionUpload};
use bd_client_stats_store::{Counter, Scope};
use bd_log_primitives::tiny_set::TinySet;
use bd_proto::protos::client::api::LogUploadIntentRequest;
use bd_proto::protos::client::api::log_upload_intent_request::ExplicitSessionCapture;
use bd_stats_common::labels;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc::{Receiver, Sender, channel};

//
// NegotiatorOutput
//

#[derive(Debug)]
pub(crate) enum NegotiatorOutput {
  UploadApproved(Rc<PendingFlushBuffersAction>),
  UploadRejected(Rc<PendingFlushBuffersAction>),
}

//
// NegotiatorStats
//

#[derive(Debug)]
#[allow(clippy::struct_field_names)]
struct NegotiatorStats {
  intent_initiations_total: Counter,

  intent_completion_uploads_total: Counter,
  intent_completion_already_rejected_drops_total: Counter,
  intent_completion_drops_total: Counter,

  intent_negotiator_channel_send_failures_total: Counter,
  intent_request_failures_total: Counter,
}

impl NegotiatorStats {
  fn new(scope: &Scope) -> Self {
    Self {
      intent_initiations_total: scope.counter("logs_upload_intent_negotiation_initiations_total"),
      intent_completion_uploads_total: scope.counter_with_labels(
        "logs_upload_intent_negotiation_completions_total",
        labels! {"result" => "upload"},
      ),
      intent_completion_drops_total: scope.counter_with_labels(
        "logs_upload_intent_negotiation_completions_total",
        labels! {"result" => "drop"},
      ),
      intent_completion_already_rejected_drops_total: scope.counter_with_labels(
        "logs_upload_intent_negotiation_completions_total",
        labels! {"result" => "drop_already_rejected"},
      ),
      intent_negotiator_channel_send_failures_total: scope
        .counter("logs_upload_intent_negotiation_completion_channel_send_failures_total"),
      intent_request_failures_total: scope
        .counter("logs_upload_intent_negotiation_request_failures_total"),
    }
  }
}

//
// Negotiator
//

pub(crate) struct Negotiator {
  input_rx: Receiver<Rc<PendingFlushBuffersAction>>,
  output_tx: Sender<NegotiatorOutput>,
  data_upload_tx: Sender<DataUpload>,

  /// The identifiers of actions for which the intent negotiation process returned a "drop"
  /// response. Used to avoid attempting to negotiate the intent for the same action multiple
  /// times, as the first rejection likely means that all subsequent attempts for the same action
  /// ID will also be rejected. Note: Although this optimization should have a net positive
  /// impact on customers, it's worth noting that in the case of long-lived sessions spanning
  /// multiple days (measured in UTC timezone), some upload actions may be rejected prematurely
  /// compared to if the requests to the server were made.
  rejected_intent_action_ids: BTreeSet<Rc<FlushBufferId>>,

  stats: NegotiatorStats,
}

impl Negotiator {
  pub(crate) fn new(
    input_rx: Receiver<Rc<PendingFlushBuffersAction>>,
    data_upload_tx: Sender<DataUpload>,
    scope: &Scope,
  ) -> (Self, Receiver<NegotiatorOutput>) {
    let (output_tx, output_rx) = channel(16);

    (
      Self {
        input_rx,
        output_tx,
        data_upload_tx,
        rejected_intent_action_ids: BTreeSet::new(),
        stats: NegotiatorStats::new(scope),
      },
      output_rx,
    )
  }

  pub(crate) fn run(mut self) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_local(async move {
      loop {
        if let Some(pending_action) = self.input_rx.recv().await {
          self.process_pending_action(pending_action).await;
        }
      }
    })
  }

  async fn process_pending_action(&mut self, pending_action: Rc<PendingFlushBuffersAction>) {
    log::debug!("processing pending action: {:?}", pending_action.id);

    self.stats.intent_initiations_total.inc();

    // If there exists an action with the same ID that has been rejected (upload intent was
    // rejected), we ignore the currently processed action since there is a high chance that it
    // would be rejected again.
    if self.rejected_intent_action_ids.contains(&pending_action.id) {
      log::debug!(
        "ignoring triggered pending processing flush buffers action: {:?}",
        pending_action.id
      );

      if let Err(e) = self
        .output_tx
        .send(NegotiatorOutput::UploadRejected(pending_action))
        .await
      {
        log::debug!("failed to send \"drop_already_rejected\": {e:?}");
        self
          .stats
          .intent_negotiator_channel_send_failures_total
          .inc();
      }

      self
        .stats
        .intent_completion_already_rejected_drops_total
        .inc();
      return;
    }

    let result = self
      .perform_action_intent_negotiation(&pending_action)
      .await;

    match result {
      Ok(true) => {
        log::debug!(
          "action intent negotiation returned \"upload immediately\", action ID {:?}",
          pending_action.id
        );

        self.stats.intent_completion_uploads_total.inc();

        if let Err(e) = self
          .output_tx
          .send(NegotiatorOutput::UploadApproved(pending_action))
          .await
        {
          log::debug!("failed to send \"upload approved\" signal: {e:?}");
          self
            .stats
            .intent_negotiator_channel_send_failures_total
            .inc();
        }
      },
      Ok(false) => {
        log::debug!(
          "action intent negotiation returned \"Drop\", action ID {:?}",
          pending_action.id
        );

        self.stats.intent_completion_drops_total.inc();

        self
          .rejected_intent_action_ids
          .insert(pending_action.id.clone());

        if let Err(e) = self
          .output_tx
          .send(NegotiatorOutput::UploadRejected(pending_action))
          .await
        {
          log::debug!("failed to send \"drop\" signal: {e:?}");
          self
            .stats
            .intent_negotiator_channel_send_failures_total
            .inc();
        }
      },
      Err(e) => {
        log::debug!(
          "action intent negotiation failed with error: {e}, action ID {:?}",
          pending_action.id
        );

        self.stats.intent_completion_drops_total.inc();

        if let Err(e) = self
          .output_tx
          .send(NegotiatorOutput::UploadRejected(pending_action))
          .await
        {
          log::debug!("failed to send \"drop\" signal: {e:?}");
          self
            .stats
            .intent_negotiator_channel_send_failures_total
            .inc();
        }
      },
    }
  }

  async fn perform_action_intent_negotiation(
    &self,
    action: &PendingFlushBuffersAction,
  ) -> anyhow::Result<bool> {
    let intent_uuid = TrackedLogUploadIntent::upload_uuid();

    let intent_request = LogUploadIntentRequest {
      log_count: 0,
      byte_count: 0,
      // The API expects one buffer ID in here even though an action may be responsible for
      // uploading multiple buffers at once. Take the first buffer ID from the list in here to
      // make the API happy.
      // TODO(Augustyniak): Change the API to address above comment.
      buffer_id: action
        .trigger_buffer_ids
        .first()
        .map_or("no_buffer", |id| id.as_ref())
        .to_string(),
      intent_uuid: intent_uuid.clone(),
      intent_type: Some(match action.id.as_ref() {
        FlushBufferId::WorkflowActionId(action_id) => {
          Intent_type::WorkflowActionUpload(WorkflowActionUpload {
            workflow_action_ids: vec![action_id.clone()],
            ..Default::default()
          })
        },
        FlushBufferId::ExplicitSessionCapture(id) => {
          Intent_type::ExplicitSessionCapture(ExplicitSessionCapture {
            id: id.clone(),
            ..Default::default()
          })
        },
      }),
      ..Default::default()
    };

    match self
      .perform_intent_negotiation(intent_request.clone())
      .await
    {
      Ok(true) => {
        log::debug!(
          "intent accepted ({:?}), proceeding with action {:?}",
          intent_request.intent_uuid,
          action.id
        );
        Ok(true)
      },
      Ok(false) => {
        log::debug!(
          "intent rejected ({:?}), dropping action {:?}",
          intent_request.intent_uuid,
          action.id
        );
        Ok(false)
      },
      Err(e) => {
        log::debug!(
          "failed to perform action's ({:?}) intent ({:?}) negotiation: {:?}",
          action.id,
          intent_request.intent_uuid,
          e
        );
        Err(e)
      },
    }
  }

  async fn perform_intent_negotiation(
    &self,
    intent_request: LogUploadIntentRequest,
  ) -> anyhow::Result<bool> {
    let intent_uuid = intent_request.intent_uuid.clone();

    log::debug!("issuing log intent upload ({intent_uuid:?}) for trigger");

    // TODO(snowp): Add max retries to intent negotiation.
    loop {
      // We continue to accept new logs into buffers while awaiting the results of intent
      // negotiation. Therefore, it's possible that the log triggering the flushing of the
      // buffer (and consequently the intent negotiation process) will not be in the buffer
      // once the intent negotiation process is complete.
      let (intent, response) =
        TrackedLogUploadIntent::new(intent_uuid.clone(), intent_request.clone());
      self
        .data_upload_tx
        .send(DataUpload::LogsUploadIntent(intent))
        .await
        .map_err(|_| anyhow!("tokio send error: intent upload"))?;

      log::debug!("intent sent, awaiting response");

      let Ok(intent_response) = response.await else {
        self.stats.intent_request_failures_total.inc();
        log::debug!("API stream closed while waiting for intent response, retrying");
        continue;
      };

      match intent_response.decision {
        IntentDecision::UploadImmediately => {
          log::debug!("uploading trigger buffer, intent accepted (\"{intent_uuid}\")");
          return Ok(true);
        },
        IntentDecision::Drop => {
          log::debug!("not uploading trigger, intent dropped (\"{intent_uuid}\")");
          return Ok(false);
        },
      }
    }
  }
}

//
// Resolver
//

#[derive(Debug)]
/// Responsible for orchestrating and managing flush buffer actions.
pub(crate) struct Resolver {
  trigger_buffer_ids: Rc<TinySet<Rc<BufferId>>>,
  continuous_buffer_ids: Rc<TinySet<Rc<BufferId>>>,

  stats: ResolverStats,
}

impl Resolver {
  pub(crate) fn new(stats_scope: &Scope) -> Self {
    Self {
      stats: ResolverStats::new(stats_scope),
      trigger_buffer_ids: Rc::default(),
      continuous_buffer_ids: Rc::default(),
    }
  }

  pub(crate) fn update(&mut self, config: ResolverConfig) {
    log::debug!("resolver config update: \"{config:?}\"");

    self.trigger_buffer_ids = config.trigger_buffer_ids;
    self.continuous_buffer_ids = config.continuous_buffer_ids;
  }

  /// Process flush buffer actions. Create pending buffer action instances for those flush
  /// buffer actions that require further processing.
  pub(crate) fn process_flush_buffer_actions<'a>(
    &self,
    actions: TinySet<&Rc<ActionFlushBuffers>>,
    session_id: &str,
    pending_actions: &TinySet<Rc<PendingFlushBuffersAction>>,
    streaming_actions: &[Rc<StreamingBuffersAction>],
  ) -> FlushBuffersActionsProcessingResult {
    let mut created_actions = TinySet::default();
    let mut triggered_flush_buffers_action_ids = TinySet::default();
    let mut triggered_flushes_buffer_ids = TinySet::default();

    for action in actions {
      triggered_flush_buffers_action_ids.insert(action.id.clone());

      let Some(action) = PendingFlushBuffersAction::new(
        &action,
        session_id.to_string(),
        &self.trigger_buffer_ids,
        &self.continuous_buffer_ids,
      ) else {
        log::debug!(
          "failed to initialize pending flush buffers action: \"{:?}\"",
          action.id
        );
        self.stats.action_initiations_other_drops.inc();
        continue;
      };

      log::debug!("initialized pending flush buffers action: \"{action:?}\"");

      triggered_flushes_buffer_ids.extend(action.trigger_buffer_ids.iter().cloned());

      // fixfix move these up?
      if pending_actions.iter().any(|a| a.id == action.id) {
        log::debug!("ignoring flush buffers action: \"{action:?}\", already uploading",);
        self.stats.action_initiations_already_uploading_drops.inc();
        continue;
      }

      if streaming_actions.iter().any(|a| a.id == action.id) {
        log::debug!("ignoring flush buffers action: \"{action:?}\", already streaming",);
        self.stats.action_initiations_already_streaming_drops.inc();
        continue;
      }

      log::debug!("added flush buffers action: \"{action:?}\"");

      self.stats.action_initiations_successes.inc();
      created_actions.insert(action.into());
    }

    FlushBuffersActionsProcessingResult {
      new_pending_actions_to_add: created_actions,

      triggered_flush_buffers_action_ids,
      triggered_flushes_buffer_ids,
    }
  }

  /// Process streaming actions. Update the streaming state of existing actions, remove those
  /// that should be terminated based on the number of logs they've already streamed, and compute
  /// the final destination buffer IDs for the currently processed log.
  pub(crate) fn process_streaming_actions<'a>(
    &self,
    mut streaming_actions: Vec<(Rc<StreamingBuffersAction>, bool)>,
    log_destination_buffer_ids: &TinySet<Rc<BufferId>>,
    session_id: &str,
  ) -> StreamingBuffersActionsProcessingResult {
    let mut has_changed_streaming_actions = false;

    // Remove streaming actions that should be terminated.
    streaming_actions.retain_mut(|(a, flush_completed)| {
      let meets_termination_criteria = a.meets_termination_criteria();
      let session_id_changed = a.session_id != session_id;

      log::trace!(
        "streaming buffers action {:?}, streamed logs: {:?} (limit {:?}), \
         meets_termination_criteria: {:?}, session_id_changed: {:?}, flush_completed: {:?}",
        a.id,
        a.logs_count,
        a.max_logs_count,
        meets_termination_criteria,
        session_id_changed,
        flush_completed
      );
      if (meets_termination_criteria || session_id_changed) && *flush_completed {
        log::debug!(
          "terminating streaming buffers action {:?}, streamed logs: {:?} (limit {:?})",
          a.id,
          a.logs_count,
          a.max_logs_count
        );

        if meets_termination_criteria {
          self
            .stats
            .streaming_action_completion_termination_criterion_met
            .inc();
        } else if session_id_changed {
          self.stats.streaming_action_completion_session_changes.inc();
        }

        has_changed_streaming_actions = true;
        return false;
      }

      true
    });

    // Process log streaming rules to determine the destination buffer(s) for a given log. As part
    // of this process, the state of all active streaming actions is updated, with a counter of logs
    // streamed for each active streaming rule being incremented accordingly.

    let mut final_log_destination_buffer_ids: TinySet<_> = TinySet::default();

    let mut not_rerouted_buffer_ids: TinySet<_> = log_destination_buffer_ids
      .clone()
      .into_iter()
      .filter(|id|
        // Allow for both trigger and continuous buffers as long as they both exist.
        self.trigger_buffer_ids.contains(id)
        || self.continuous_buffer_ids.contains(id))
      .collect();

    let mut has_been_rerouted = false;

    for (action, _) in &mut streaming_actions {
      let intersection: TinySet<_> = action
        .source_trigger_buffer_ids
        .intersection(log_destination_buffer_ids)
        .collect();

      if intersection.is_empty() {
        continue;
      }

      has_been_rerouted = true;

      // TODO(Augustyniak): Delay copying to when we move IDs to be a part of the `result`.
      not_rerouted_buffer_ids.retain(|id| !intersection.contains(id));
      action.logs_count.fetch_add(1, Ordering::SeqCst);

      final_log_destination_buffer_ids
        .extend(action.destination_continuous_buffer_ids.iter().cloned());
    }

    if has_been_rerouted {
      log::trace!(
        "streaming: log redirected from: \"{log_destination_buffer_ids:?}\" to \
         \"{final_log_destination_buffer_ids:?}\" buffer(s)"
      );
    }

    if has_been_rerouted {
      self.stats.streaming_action_applications.inc();
    }

    final_log_destination_buffer_ids.extend(not_rerouted_buffer_ids);

    has_changed_streaming_actions |= has_been_rerouted;

    StreamingBuffersActionsProcessingResult {
      log_destination_buffer_ids: final_log_destination_buffer_ids,
      has_changed_streaming_actions,
      updated_streaming_actions: streaming_actions.into_iter().map(|(a, _)| a).collect(),
    }
  }

  pub(crate) fn make_streaming_action(
    &self,
    pending_action: &PendingFlushBuffersAction,
  ) -> Option<Rc<StreamingBuffersAction>> {
    let streaming_action = StreamingBuffersAction::new(pending_action, &self.continuous_buffer_ids);
    if streaming_action.is_some() {
      self.stats.streaming_action_initiation_successes.inc();
    } else {
      self.stats.streaming_action_initiation_failures.inc();
    }

    streaming_action
  }

  /// Standardize the pending actions. Remove references to non-existing trigger buffer IDs. If,
  /// after this process, an action is left with no trigger buffer IDs, it is dropped.
  pub(crate) fn standardize_pending_actions(
    &self,
    pending_actions: TinySet<Rc<PendingFlushBuffersAction>>,
  ) -> TinySet<Rc<PendingFlushBuffersAction>> {
    pending_actions
      .into_iter()
      .filter_map(|action| {
        if action.trigger_buffer_ids == self.trigger_buffer_ids {
          return Some(action);
        }

        // Fixfix not currently used, slow path.
        /*let mut action = action;
        action.trigger_buffer_ids = action
          .trigger_buffer_ids
          .intersection(&self.trigger_buffer_ids)
          .cloned()
          .collect();
        Some(action)*/
        unimplemented!("fixfix")
      })
      .collect()
  }

  /// Standardize the streaming actions. Remove references to non-existing continuous buffer IDs.
  /// If, after this process, an action is left with no source trigger or destination continuos
  /// buffer IDs, it is dropped.
  pub(crate) fn standardize_streaming_buffers(
    &self,
    streaming_buffers: Vec<Rc<StreamingBuffersAction>>,
  ) -> Vec<Rc<StreamingBuffersAction>> {
    streaming_buffers
      .into_iter()
      .filter_map(|action| {
        #[allow(unused_assignments)] // fixfix
        let mut source_unchanged = false;
        let source_trigger_buffer_ids: Rc<TinySet<_>> =
          if action.source_trigger_buffer_ids == self.trigger_buffer_ids {
            source_unchanged = true;
            self.trigger_buffer_ids.clone()
          } else {
            /*action
            .source_trigger_buffer_ids
            .intersection(&self.trigger_buffer_ids)
            .cloned()
            .collect()*/
            unimplemented!("fixfix")
          };
        #[allow(unused_assignments)] // fixfix
        let mut destination_unchanged = false;
        let destination_continuous_buffer_ids: Rc<TinySet<_>> =
          if action.destination_continuous_buffer_ids == self.continuous_buffer_ids {
            destination_unchanged = true;
            self.continuous_buffer_ids.clone()
          } else {
            /*action
            .destination_continuous_buffer_ids
            .intersection(&self.continuous_buffer_ids)
            .cloned()
            .collect();*/
            unimplemented!("fixfix")
          };

        if source_trigger_buffer_ids.is_empty() || destination_continuous_buffer_ids.is_empty() {
          return None;
        }

        if source_unchanged && destination_unchanged {
          return Some(action);
        }

        /*let mut action = action;
        action.destination_continuous_buffer_ids = destination_continuous_buffer_ids;
        action.source_trigger_buffer_ids = source_trigger_buffer_ids;
        Some(action)*/
        unimplemented!("fixfix")
      })
      .collect()
  }
}

//
// ResolverStats
//

// Stats emitted by `Resolver`.
#[derive(Debug)]
struct ResolverStats {
  action_initiations_already_uploading_drops: Counter,
  action_initiations_already_streaming_drops: Counter,
  action_initiations_other_drops: Counter,
  action_initiations_successes: Counter,

  streaming_action_initiation_successes: Counter,
  streaming_action_initiation_failures: Counter,
  streaming_action_applications: Counter,
  streaming_action_completion_termination_criterion_met: Counter,
  streaming_action_completion_session_changes: Counter,
}

impl ResolverStats {
  fn new(scope: &Scope) -> Self {
    let action_initiations_already_uploading_drops = scope.counter_with_labels(
      "flush_buffers_action_initiations_total",
      labels!("result" => "dismiss_already_uploading"),
    );
    let action_initiations_already_streaming_drops = scope.counter_with_labels(
      "flush_buffers_action_initiations_total",
      labels!("result" => "dismiss_already_streaming"),
    );
    let action_initiations_other_drops = scope.counter_with_labels(
      "flush_buffers_action_initiations_total",
      labels!("result" => "dismiss_other"),
    );
    let action_initiations_successes = scope.counter_with_labels(
      "flush_buffers_action_initiations_total",
      labels!("result" => "success"),
    );

    let streaming_action_initiation_successes = scope.counter_with_labels(
      "streaming_buffers_action_initiations_total",
      labels!("result" => "success"),
    );
    let streaming_action_initiation_failures = scope.counter_with_labels(
      "streaming_buffers_action_initiations_total",
      labels!("result" => "failure"),
    );

    let streaming_action_completion_termination_criterion_met = scope.counter_with_labels(
      "streaming_buffers_action_completions_total",
      labels!("type" => "termination_criterion_met"),
    );
    let streaming_action_completion_session_changes = scope.counter_with_labels(
      "streaming_buffers_action_completions_total",
      labels!("type" => "session_changed"),
    );

    Self {
      action_initiations_already_uploading_drops,
      action_initiations_already_streaming_drops,
      action_initiations_other_drops,
      action_initiations_successes,

      streaming_action_initiation_successes,
      streaming_action_initiation_failures,
      streaming_action_applications: scope.counter("streaming_buffers_action_applications_total"),
      streaming_action_completion_termination_criterion_met,
      streaming_action_completion_session_changes,
    }
  }
}

//
// ResolverConfig
//

#[derive(Debug)]
pub(crate) struct ResolverConfig {
  trigger_buffer_ids: Rc<TinySet<Rc<BufferId>>>,
  continuous_buffer_ids: Rc<TinySet<Rc<BufferId>>>,
}

impl ResolverConfig {
  pub(crate) const fn new(
    trigger_buffer_ids: Rc<TinySet<Rc<BufferId>>>,
    continuous_buffer_ids: Rc<TinySet<Rc<BufferId>>>,
  ) -> Self {
    Self {
      trigger_buffer_ids,
      continuous_buffer_ids,
    }
  }
}

//
// FlushBuffersActionsProcessingResult
//

#[derive(Debug, PartialEq)]
pub(crate) struct FlushBuffersActionsProcessingResult {
  pub(crate) new_pending_actions_to_add: TinySet<Rc<PendingFlushBuffersAction>>,
  pub(crate) triggered_flush_buffers_action_ids: TinySet<Rc<FlushBufferId>>,
  pub(crate) triggered_flushes_buffer_ids: TinySet<Rc<BufferId>>,
}

//
// StreamingBuffersActionsProcessingResult
//

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) struct StreamingBuffersActionsProcessingResult {
  pub(crate) log_destination_buffer_ids: TinySet<Rc<BufferId>>,
  pub(crate) has_changed_streaming_actions: bool,
  pub(crate) updated_streaming_actions: Vec<Rc<StreamingBuffersAction>>,
}

//
// PendingFlushBuffersAction
//

// The action created by a flush buffer workflow action. This tracks the action while upload intent
// negotiation is performed. At that point, it either transitions into a `StreamingBuffersAction` if
// streaming was configured for the action.
#[derive(Serialize, Deserialize, PartialEq)]
pub(crate) struct PendingFlushBuffersAction {
  pub(crate) id: Rc<FlushBufferId>,
  session_id: String,

  trigger_buffer_ids: Rc<TinySet<Rc<BufferId>>>,

  streaming: Option<Streaming>,
}

impl std::fmt::Debug for PendingFlushBuffersAction {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("PendingFlushBuffersAction")
      .field("id", &self.id)
      .field("trigger_buffer_ids", &self.trigger_buffer_ids)
      .field(
        "streaming_destination_buffer_ids",
        self.streaming.as_ref().map_or(&"EMPTY", |streaming| {
          &streaming.destination_continuous_buffer_ids
        }),
      )
      .finish_non_exhaustive()
  }
}

impl PendingFlushBuffersAction {
  fn new(
    action: &ActionFlushBuffers,
    session_id: String,
    trigger_buffer_ids: &Rc<TinySet<Rc<BufferId>>>,
    continuous_buffer_ids: &Rc<TinySet<Rc<BufferId>>>,
  ) -> Option<Self> {
    log::debug!("evaluating flush buffers action: {action:?}");

    let streaming = action.streaming.as_ref().and_then(|streaming_proto| {
      if continuous_buffer_ids.is_empty() {
        log::debug!("buffers streaming not activated: no configured continuous buffer IDs");
        return None;
      }

      // If no destination continuous buffer IDs are specified by the streaming action, use the ID
      // of the first available continuous buffer.
      let destination_continuous_buffer_ids =
        if streaming_proto.destination_continuous_buffer_ids.is_empty() {
          continuous_buffer_ids.clone()
        } else if streaming_proto.destination_continuous_buffer_ids == **continuous_buffer_ids {
          continuous_buffer_ids.clone()
        } else {
          // Make sure that we dismiss invalid (not existing) continuous buffer IDs.
          /*continuous_buffer_ids
          .iter()
          .filter(|id| {
            streaming_proto
              .destination_continuous_buffer_ids
              .contains(id.as_ref())
          })
          .cloned()
          .collect()*/
          unimplemented!("fixfix")
        };

      if destination_continuous_buffer_ids.is_empty() {
        log::debug!("buffers streaming not activated: specified continuous buffer IDs don't exist");
        return None;
      }

      Some(Streaming {
        destination_continuous_buffer_ids,
        max_logs_count: streaming_proto.max_logs_count,
      })
    });

    let trigger_buffer_ids: Rc<TinySet<_>> = if action.buffer_ids.is_empty() {
      // Empty buffer IDs means that the action should be applied to all buffers.
      trigger_buffer_ids.clone()
    } else {
      // TODO(mattklein123): This path is not actually used today. We might need to optimize this
      // case if it becomes more common.
      /*Arc::new(
        action
          .buffer_ids
          .iter()
          .filter(|id| trigger_buffer_ids.contains(id.as_str()))
          .map(Into::into)
          .collect(),
      )*/
      unimplemented!("fixfix")
    };

    if trigger_buffer_ids.is_empty() {
      log::debug!("buffers flush action not activated: no eligible trigger buffers found",);
      return None;
    }

    Some(Self {
      id: action.id.clone(),
      session_id,
      trigger_buffer_ids,
      streaming,
    })
  }
}

//
// Streaming
//

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct Streaming {
  destination_continuous_buffer_ids: Rc<TinySet<Rc<BufferId>>>,
  max_logs_count: Option<u64>,
}

//
// StreamingBuffersAction
//

#[derive(Serialize, Deserialize)]
// The action created in response to flush buffer actions that were accepted for upload and had a
// streaming configuration attached to them.
pub(crate) struct StreamingBuffersAction {
  pub(crate) id: Rc<FlushBufferId>,
  session_id: String,

  source_trigger_buffer_ids: Rc<TinySet<Rc<BufferId>>>,
  destination_continuous_buffer_ids: Rc<TinySet<Rc<BufferId>>>,

  max_logs_count: Option<u64>,

  logs_count: AtomicU64, // fixfix?
}

#[cfg(test)]
impl PartialEq for StreamingBuffersAction {
  fn eq(&self, other: &Self) -> bool {
    self.id == other.id
      && self.session_id == other.session_id
      && self.source_trigger_buffer_ids == other.source_trigger_buffer_ids
      && self.destination_continuous_buffer_ids == other.destination_continuous_buffer_ids
      && self.max_logs_count == other.max_logs_count
      && self.logs_count.load(Ordering::SeqCst) == other.logs_count.load(Ordering::SeqCst)
  }
}

impl std::fmt::Debug for StreamingBuffersAction {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("StreamingBuffersAction")
      .field("id", &self.id)
      .field("source_trigger_buffer_ids", &self.source_trigger_buffer_ids)
      .field(
        "destination_continuous_buffer_ids",
        &self.destination_continuous_buffer_ids,
      )
      .field("logs_count", &self.logs_count)
      .field("max_logs_count", &self.max_logs_count)
      .finish_non_exhaustive()
  }
}

impl StreamingBuffersAction {
  pub(crate) fn new(
    action: &PendingFlushBuffersAction,
    continuous_buffer_ids: &Rc<TinySet<Rc<BufferId>>>,
  ) -> Option<Rc<Self>> {
    let Some(streaming) = &action.streaming else {
      log::trace!("buffers streaming not activated: no streaming configuration");
      return None;
    };

    if streaming
      .destination_continuous_buffer_ids
      .is_disjoint(continuous_buffer_ids)
    {
      log::debug!(
        "buffers streaming not activated: specified continuous buffer IDs are either empty or \
         don't exist"
      );
      return None;
    }

    Some(Rc::new(Self {
      id: action.id.clone(),
      session_id: action.session_id.clone(), // fixfix
      source_trigger_buffer_ids: action.trigger_buffer_ids.clone(),
      destination_continuous_buffer_ids: streaming.destination_continuous_buffer_ids.clone(),
      max_logs_count: streaming.max_logs_count,
      logs_count: AtomicU64::new(0),
    }))
  }

  fn meets_termination_criteria(&self) -> bool {
    if let Some(max_logs_count) = self.max_logs_count
      && self.logs_count.load(Ordering::SeqCst) >= max_logs_count
    {
      // The streaming action hit the number of logs it was supposed to stream.
      return true;
    }
    false
  }
}

//
// BuffersToFlush
//

#[derive(Debug)]
pub struct BuffersToFlush {
  // Unique IDs of buffers to flush.
  pub buffer_ids: Rc<TinySet<Rc<BufferId>>>,
  // Channel to notify the caller that the flush has been completed.
  pub response_tx: tokio::sync::oneshot::Sender<()>,
}

impl BuffersToFlush {
  pub(crate) fn new(
    action: &PendingFlushBuffersAction,
  ) -> (Self, tokio::sync::oneshot::Receiver<()>) {
    let (response_tx, response_rx) = tokio::sync::oneshot::channel();

    (
      Self {
        buffer_ids: action.trigger_buffer_ids.clone(),
        response_tx,
      },
      response_rx,
    )
  }
}
