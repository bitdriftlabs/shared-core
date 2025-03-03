// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./engine_test.rs"]
mod engine_test;

use crate::actions_flush_buffers::{
  BuffersToFlush,
  Negotiator,
  NegotiatorOutput,
  PendingFlushBuffersAction,
  Resolver,
  ResolverConfig,
  StreamingBuffersAction,
};
use crate::config::{
  ActionEmitMetric,
  ActionFlushBuffers,
  ActionTakeScreenshot,
  Config,
  WorkflowsConfiguration,
};
use crate::metrics::MetricsCollector;
use crate::sankey_diagram::{self, PendingSankeyPathUpload};
use crate::workflow::{SankeyPath, TriggeredAction, TriggeredActionEmitSankey, Workflow};
use anyhow::anyhow;
use bd_api::DataUpload;
use bd_client_stats::DynamicStats;
use bd_client_stats_store::{Counter, Histogram, Scope};
use bd_log_primitives::{Log, LogRef};
pub use bd_matcher::FieldProvider;
use bd_runtime::runtime::workflows::{
  PersistenceWriteIntervalFlag,
  StatePeriodicWriteIntervalFlag,
  TraversalsCountLimitFlag,
};
use bd_runtime::runtime::{ConfigLoader, DurationWatch};
use bd_stats_common::labels;
use bd_time::TimeDurationExt as _;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot::error::TryRecvError;
use tokio::task::JoinHandle;
use tokio::time::Instant;

//
// WorkflowsEngine
//

#[derive(Debug)]
/// Orchestrates the execution and management of workflows. It is also responsible for
/// persisting and restoring its state in disk when any workflow has changed.
pub struct WorkflowsEngine {
  // Number of elements in configs and
  // state.workflows should be always the same.
  // A config at index `i` corresponds to a workflow (state.workflows list)
  // at index `i`.
  configs: Vec<Config>,
  state: WorkflowsState,
  state_store: StateStore,
  pending_buffer_flushes: HashMap<String, tokio::sync::oneshot::Receiver<()>>,

  needs_state_persistence: bool,

  stats: WorkflowsEngineStats,

  current_traversals_count: u32,
  /// Used to keep track of the current number of traversals.
  /// The other way to do this would be to iterate over all workflows
  /// and their runs and their traversals every time we want to
  /// know how many traversals are there but this could turn out to
  /// be expensive for cases when we have a lot of workflows/traversals/runs.
  traversals_count_limit: u32,
  state_periodic_write_interval: time::Duration,

  flush_buffers_actions_resolver: Resolver,
  flush_buffers_negotiator_input_tx: Sender<PendingFlushBuffersAction>,
  flush_buffers_negotiator_output_rx: Receiver<NegotiatorOutput>,
  flush_buffers_negotiator_join_handle: JoinHandle<()>,

  sankey_processor_input_tx: Sender<SankeyPath>,
  sankey_processor_output_rx: Receiver<SankeyPath>,

  sankey_processor_join_handle: JoinHandle<()>,

  metrics_collector: MetricsCollector,

  buffers_to_flush_tx: Sender<BuffersToFlush>,
}

impl WorkflowsEngine {
  pub fn new(
    stats: &Scope,
    sdk_directory: &Path,
    runtime: &ConfigLoader,
    data_upload_tx: Sender<DataUpload>,
    dynamic_stats: Arc<DynamicStats>,
  ) -> (Self, Receiver<BuffersToFlush>) {
    let scope = stats.scope("workflows");

    let traversals_count_limit_flag = TraversalsCountLimitFlag::register(runtime).unwrap();
    let state_periodic_write_interval_flag =
      StatePeriodicWriteIntervalFlag::register(runtime).unwrap();

    // An arbitrary size for the channel that should be sufficient to handle incoming flush buffer
    // triggers. It should be acceptable to drop events if the size limit is exceeded.
    let (buffers_to_flush_tx, buffers_to_flush_rx) = tokio::sync::mpsc::channel(10);

    let actions_scope = scope.scope("actions");

    let (input_tx, input_rx) = tokio::sync::mpsc::channel(10);
    let (flush_buffers_actions_negotiator, output_rx) =
      Negotiator::new(input_rx, data_upload_tx.clone(), &actions_scope);
    let flush_buffers_negotiator_join_handle = flush_buffers_actions_negotiator.run();

    let flush_buffers_actions_resolver = Resolver::new(&actions_scope);

    let (sankey_input_tx, sankey_input_rx) = tokio::sync::mpsc::channel(10);
    let (sankey_output_tx, sankey_output_rx) = tokio::sync::mpsc::channel(10);
    let sankey_diagram_processor = sankey_diagram::Processor::new(
      sankey_input_rx,
      data_upload_tx,
      sankey_output_tx,
      &actions_scope,
    );
    let sankey_processor_join_handle = sankey_diagram_processor.run();

    let workflows_engine = Self {
      configs: vec![],
      state: WorkflowsState::default(),
      stats: WorkflowsEngineStats::new(&scope),
      state_store: StateStore::new(sdk_directory, &scope, runtime),
      needs_state_persistence: false,
      current_traversals_count: 0,
      traversals_count_limit: *traversals_count_limit_flag.read(),
      state_periodic_write_interval: *state_periodic_write_interval_flag.read(),
      flush_buffers_actions_resolver,
      flush_buffers_negotiator_join_handle,
      flush_buffers_negotiator_input_tx: input_tx,
      flush_buffers_negotiator_output_rx: output_rx,
      sankey_processor_input_tx: sankey_input_tx,
      sankey_processor_output_rx: sankey_output_rx,
      sankey_processor_join_handle,
      metrics_collector: MetricsCollector::new(dynamic_stats),
      buffers_to_flush_tx,
      pending_buffer_flushes: HashMap::new(),
    };

    (workflows_engine, buffers_to_flush_rx)
  }

  pub fn start(&mut self, config: WorkflowsEngineConfig) {
    self
      .flush_buffers_actions_resolver
      .update(ResolverConfig::new(
        config.trigger_buffer_ids,
        config.continuous_buffer_ids,
      ));

    let workflows_state = self.state_store.load();

    if let Some(state) = workflows_state {
      self.state.pending_flush_actions = self
        .flush_buffers_actions_resolver
        .standardize_pending_actions(state.pending_flush_actions);
      self.state.streaming_actions = self
        .flush_buffers_actions_resolver
        .standardize_streaming_buffers(state.streaming_actions);
      self.state.pending_sankey_actions = state.pending_sankey_actions;

      self.state.session_id.clone_from(&state.session_id);
      self.add_workflows(
        config.workflows_configuration.workflows,
        Some(state.workflows),
      );
    } else {
      self.add_workflows(config.workflows_configuration.workflows, None);
    }

    for action in &self.state.pending_flush_actions {
      if let Err(e) = self
        .flush_buffers_negotiator_input_tx
        .try_send(action.clone())
      {
        log::debug!("failed to send pending action for intent negotiation: {e}");
        self.stats.intent_negotiation_channel_send_failures.inc();
      }
    }

    for sankey_path in &self.state.pending_sankey_actions {
      if let Err(e) = self
        .sankey_processor_input_tx
        .try_send(sankey_path.sankey_path.clone())
      {
        log::debug!("failed to process sankey: {e}");
      }
    }

    log::debug!(
      "started workflows engine with {} workflow(s); {} pending processing action(s); {} pending \
       sankey path uploads; {} streaming action(s); session ID: \"{}\"; traversals count limit: {}",
      self.state.workflows.len(),
      self.state.pending_flush_actions.len(),
      self.state.pending_sankey_actions.len(),
      self.state.streaming_actions.len(),
      self.state.session_id,
      self.traversals_count_limit,
    );
  }

  /// Updates the configuration of workflows managed by the receiver with a provided
  /// workflows list proto config.
  /// As the result of each update:
  ///  * workflows that exists on the client and which are not a part of the provided workflow
  ///    config update (together with their runs and traversals) are removed.
  ///  * new workflows are created for workflows that are a part of the provided workflow config
  ///    update but do not exists in client's memory yet.
  pub fn update(&mut self, config: WorkflowsEngineConfig) {
    // To limit the memory usage remove the outdated workflows before
    // creating new ones.

    log::debug!(
      "received workflows config update containing {} workflow(s)",
      config.workflows_configuration.workflows.len()
    );

    // Introduce hash look ups to avoid N^2 complexity later in this method.
    let latest_workflow_ids_to_index: HashMap<&str, usize> = config
      .workflows_configuration
      .workflows
      .iter()
      .enumerate()
      .map(|(index, config)| (config.id(), index))
      .collect();

    // Process workflows in reversed order as we may end up removing workflows
    // while iterating.
    for index in (0 .. self.state.workflows.len()).rev() {
      let should_remove_existing_workflow =
        match latest_workflow_ids_to_index.get(self.state.workflows[index].id()) {
          Some(latest_workflows_index) => {
            // Handle an edge case where a client receives a config update containing a workflow
            // with a config field unknown to the client, which is later updated to
            // understand this field. In such cases, the server's config is converted to
            // the client's representation and cached for future use.
            // If the client is later updated to understand the previously unsupported field,
            // we check for a config mismatch between the cached client representation and
            // the fresh client representation from the server. If there's a mismatch, we discard
            // the cached version and replace it with the fresh one later on.
            config.workflows_configuration.workflows[*latest_workflows_index] != self.configs[index]
          },
          // The latest config update doesn't contain a workflow with an ID of the existing
          // workflow. Remove existing workflow.
          None => true,
        };

      if should_remove_existing_workflow {
        self.remove_workflow(index);
      }
    }

    // Introduce hash look ups to avoid N^2 complexity later in this method.
    let existing_workflow_ids_to_index: HashMap<String, usize> = self
      .state
      .workflows
      .iter()
      .enumerate()
      .map(|(index, workflow)| (workflow.id().to_string(), index))
      .collect();

    // Iterate over the list of configs from the latest update and
    // create workflows for the ones that do not have their representation
    // on the client yet.
    for workflow_config in config.workflows_configuration.workflows {
      if !existing_workflow_ids_to_index.contains_key(workflow_config.id()) {
        self.add_workflow(
          Workflow::new(workflow_config.id().to_string()),
          workflow_config,
        );
      }
    }

    self
      .flush_buffers_actions_resolver
      .update(ResolverConfig::new(
        config.trigger_buffer_ids,
        config.continuous_buffer_ids,
      ));

    self.state.pending_flush_actions = self
      .flush_buffers_actions_resolver
      .standardize_pending_actions(self.state.pending_flush_actions.clone());
    self.state.streaming_actions = self
      .flush_buffers_actions_resolver
      .standardize_streaming_buffers(self.state.streaming_actions.clone());

    log::debug!(
      "consumed received workflows config update; workflows engine contains {} workflow(s)",
      self.state.workflows.len()
    );
  }

  fn add_workflows(&mut self, workflows: Vec<Config>, existing_workflows: Option<Vec<Workflow>>) {
    // `workflows` is the source of truth for workflow configurations from the server
    // while `existing_workflows` contains state of workflow who have been running on a device.
    //  * Combine the two by iterating over the list of configs from the server and associating
    //    configs with relevant workflow state (if any).
    //  * Discard workflow states that don't have a corresponding server-side config.
    let mut existing_workflow_ids_to_workflows =
      existing_workflows.map_or_else(HashMap::new, |workflows| {
        workflows
          .into_iter()
          .map(|workflow| (workflow.id().to_string(), workflow))
          .collect()
      });

    let workflows_and_configs = workflows.into_iter().map(|config| {
      let workflow = existing_workflow_ids_to_workflows
        .remove(config.id())
        .map_or_else(
          // We have cache state for a given workflow ID.
          || Workflow::new(config.id().to_string()),
          //   No cached state for a given workflow ID, start from scratch.
          |workflow| workflow,
        );
      (workflow, config)
    });

    for (workflow, config) in workflows_and_configs {
      self.add_workflow(workflow, config);
    }
  }

  fn add_workflow(&mut self, workflow: Workflow, config: Config) {
    let mut workflow = workflow;

    let workflow_traversals_count = workflow.traversals_count();
    if workflow_traversals_count + self.current_traversals_count > self.traversals_count_limit {
      log::debug!(
        "failed to add workflow with {} traversal(s) due to traversals count limit being hit \
         ({}); current traversals count {}",
        workflow_traversals_count,
        self.traversals_count_limit,
        self.current_traversals_count
      );

      // Workflow being added has too many traversal for the engine to not exceed
      // the configured traversals count limit. Keep removing workflow's run until
      // its addition to the engine is possible.
      // Process traversals in reversed order as we may end up modifying the array
      // starting at `index` in any given iteration of the loop + we want to start
      // removing runs starting from the "latest" ones and they are at the end of
      // the enumerated array.
      let mut remaining_workflow_traversals_count = workflow_traversals_count;
      for index in (0 .. workflow.runs().len()).rev() {
        let run_traversals_count = workflow.runs()[index].traversals_count();

        log::debug!(
          "removing workflow run with {} traversal(s)",
          run_traversals_count
        );

        remaining_workflow_traversals_count -= run_traversals_count;
        self.stats.traversals_count_limit_hit_total.inc();
        workflow.remove_run(index);

        if remaining_workflow_traversals_count + self.current_traversals_count
          <= self.traversals_count_limit
        {
          break;
        }
      }
    }

    self.stats.workflow_starts_total.inc();
    self
      .stats
      .run_starts_total
      .inc_by(workflow.runs().len() as u64);
    self
      .stats
      .traversal_starts_total
      .inc_by(workflow_traversals_count.into());

    log::trace!(
      "workflow={}: workflow added, runs count {}, traversal count {}",
      workflow.id(),
      workflow.runs().len(),
      workflow_traversals_count,
    );

    self.current_traversals_count += workflow.traversals_count();

    self.configs.push(config);
    self.state.workflows.push(workflow);
  }

  fn remove_workflow(&mut self, workflow_index: usize) {
    self.configs.remove(workflow_index);
    let workflow = self.state.workflows.remove(workflow_index);

    let workflow_traversals_count = workflow.traversals_count();
    self.stats.workflow_stops_total.inc();
    self
      .stats
      .run_stops_total
      .inc_by(workflow.runs().len() as u64);
    self
      .stats
      .traversal_stops_total
      .inc_by(workflow_traversals_count.into());

    self.current_traversals_count = self
      .current_traversals_count
      .saturating_sub(workflow_traversals_count);

    log::debug!("workflow={}: workflow removed", workflow.id());

    // If there exists a run that is not in an initial state.
    if !workflow.is_in_initial_state() {
      log::debug!(
        "workflow={}: workflow removed, marking state as dirty",
        workflow.id()
      );
      // Mark the state as dirty so that the state associated with
      // the workflow that was just removed can be removed from disk.
      self.needs_state_persistence = true;
    }
  }

  /// Attempts to persist the client-side state of the workflows to disk while ignoring any errors.
  /// Does nothing if state has not changed since the last time it was saved.
  ///
  /// # Arguments
  ///
  /// * `force` - If `true`, the state will be stored regardless of the time since the last save.
  pub async fn maybe_persist(&mut self, force: bool) {
    // Only attempt to persist the state if there has been any changes
    if !self.needs_state_persistence {
      return;
    }

    if self.state_store.maybe_store(&self.state, force).await {
      self.needs_state_persistence = false;
    }
  }

  pub async fn run(&mut self) {
    loop {
      self.run_once(true).await;
    }
  }

  pub(crate) async fn run_once(&mut self, persist_periodically: bool) {
    // TODO(Augustyniak): Fix periodic state persistence as it's effectively a no-op now due to the
    // way `run` method is being called.
    let state_persistence_in = self.state_periodic_write_interval.sleep();

    tokio::select! {
      () = state_persistence_in, if persist_periodically => {
        self.maybe_persist(false).await;
      },
      Some(negotiator_output) = self.flush_buffers_negotiator_output_rx.recv() => {
        log::debug!("received flush buffers negotiator output: \"{:?}\"", negotiator_output);

        match negotiator_output {
          NegotiatorOutput::UploadApproved(action) => {
              self.on_log_upload_approved(action).await;
          },
          NegotiatorOutput::UploadRejected(action) => {
            self.state.pending_flush_actions.remove(&action);
            self.needs_state_persistence = true;
          }
        }
      },
      Some(processed_sankey_path) = self.sankey_processor_output_rx.recv() => {
        log::debug!("received processed sankey path: \"{:?}\"", processed_sankey_path);

        self.state.pending_sankey_actions.remove(&PendingSankeyPathUpload {
            sankey_path: processed_sankey_path
        });
        self.needs_state_persistence = true;
      },
    }
  }

  async fn on_log_upload_approved(&mut self, action: PendingFlushBuffersAction) {
    self.state.pending_flush_actions.remove(&action);

    // If there is already a pending buffer flush we don't want to signal another one, as
    // this would do nothing but mess up our tracking of the in-flight flush.
    let flush_buffers =
      if let Some(pending_buffer_flush) = self.pending_buffer_flushes.get_mut(&action.id) {
        match pending_buffer_flush.try_recv() {
          Ok(()) => {
            self.pending_buffer_flushes.remove(&action.id);
            log::debug!(
              "allowing upload due to pending buffer flush completed: \"{}\"",
              action.id
            );
            true
          },
          Err(TryRecvError::Empty) => {
            log::debug!(
              "not uploading due to pending buffer flush: \"{}\"",
              action.id
            );
            false
          },
          Err(TryRecvError::Closed) => {
            self.pending_buffer_flushes.remove(&action.id);

            log::debug!(
              "pending buffer flush receiver closed without response: \"{}\"",
              action.id
            );

            true
          },
        }
      } else {
        true
      };

    log::debug!(
      "uploading due to flush buffers action: \"{}\"; flush_buffers={}",
      action.id,
      flush_buffers
    );

    if flush_buffers {
      let (buffer_flush, rx) = BuffersToFlush::new(&action);

      match self.buffers_to_flush_tx.try_send(buffer_flush) {
        Err(e) => {
          self.stats.buffers_to_flush_channel_send_failures.inc();
          log::debug!("failed to send information about buffers to flush: {e}");
        },
        Ok(()) => {
          self.pending_buffer_flushes.insert(action.id.clone(), rx);
        },
      }
    }

    match self
      .flush_buffers_actions_resolver
      .make_streaming_action(action.clone())
    {
      Some(streaming_action) => {
        log::debug!("streaming started: \"{streaming_action:?}\"");
        self.state.streaming_actions.push(streaming_action);
      },
      None => {
        log::debug!("no streaming configuration defined for action: \"{action:?}\"");
      },
    }

    self.needs_state_persistence = true;
  }

  /// Processes a given log. Returns actions that should be performed
  /// as the result of processing a log.
  pub fn process_log<'a>(
    &'a mut self,
    log: &LogRef<'_>,
    log_destination_buffer_ids: &'a BTreeSet<Cow<'a, str>>,
  ) -> WorkflowsEngineResult<'a> {
    // Measure duration in here even if the list of workflows is empty.
    let _timer = self.stats.process_log_duration.start_timer();

    if self.state.session_id.is_empty() {
      log::debug!(
        "workflows engine: moving from no session to session \"{}\"",
        log.session_id
      );
      // There was no state on a disk when workflows engine was started
      // and engine just observed first session ID.
      // We do not have to rush to persist it to disk until any of the
      // workflows makes progress. That's because in the context of cleaning workflows
      // state on session change, having empty session ID
      // ("") stored on disk is equal to storing a session ID (i.e., "foo") with
      // all workflows in their initial states.
      self.state.session_id = log.session_id.to_string();
    } else if self.state.session_id != log.session_id {
      log::debug!(
        "workflows engine: moving from \"{}\" to new session \"{}\", cleaning workflows state",
        self.state.session_id,
        log.session_id
      );
      // We are lazy and don't say that state needs persistence.
      // That may result in new session ID not being stored to disk
      // (if the app is killed before the next time we store state)
      // which means that the next time SDK launches we start with empty
      // session ID (""). It should be OK as in the context of cleaning workflows
      // state on session change, having empty session ID
      // ("") stored on disk is equal to storing a session ID (i.e., "foo") with
      // all workflows in their initial states.
      self.clean_state();
      self.state.session_id = log.session_id.to_string();
    }

    // Return early if there are no workflows.
    if self.state.workflows.is_empty() {
      self
        .stats
        .active_traversals_total
        .observe(self.current_traversals_count.into());

      return WorkflowsEngineResult {
        log_destination_buffer_ids: Cow::Borrowed(log_destination_buffer_ids),
        triggered_flushes_buffer_ids: BTreeSet::new(),
        triggered_flush_buffers_action_ids: BTreeSet::new(),
        capture_screenshot: false,
        logs_to_inject: vec![],
      };
    }

    let mut actions: Vec<TriggeredAction<'_>> = vec![];
    let mut logs_to_inject: Vec<Log> = vec![];
    for (index, workflow) in &mut self.state.workflows.iter_mut().enumerate() {
      let was_in_initial_state = workflow.is_in_initial_state();
      let result = workflow.process_log(
        &self.configs[index],
        log,
        &mut self.current_traversals_count,
        self.traversals_count_limit,
      );

      macro_rules! inc_by {
        ($field:ident, $value:ident) => {
          self.stats.$field.inc_by(u64::from(result.stats().$value));
        };
      }

      inc_by!(
        exclusive_workflow_resets_total,
        reset_exclusive_workflows_count
      );
      inc_by!(
        exclusive_workflow_potential_forks_total,
        potential_fork_exclusive_workflows_count
      );
      inc_by!(run_starts_total, created_runs_count);
      inc_by!(run_advances_total, advanced_runs_count);
      inc_by!(run_stops_total, stopped_runs_count);
      inc_by!(run_completions_total, completed_runs_count);

      inc_by!(traversal_starts_total, created_traversals_count);
      inc_by!(traversal_advances_total, advanced_traversals_count);
      inc_by!(traversal_stops_total, stopped_traversals_count);
      inc_by!(traversal_completions_total, completed_traversals_count);

      inc_by!(
        traversals_count_limit_hit_total,
        traversals_count_limit_hit_count
      );

      inc_by!(matched_logs_total, matched_logs_count);

      // Not every case of a workflow making a progress needs a state persistence.
      // If the workflow was in an initial state prior to processing a log and is in
      // an initial state after processing the log then the state of workflow did not change
      // as the result of processing a log and does not have to be persisted. An example for when
      // a workflow makes progress but does not needs persistence is the following workflow
      // with 2 nodes/states - a start log matching node and a final emit metric node,
      // such workflow may:
      //   * be in an initial state
      //   * match a single log
      //   * execute emit metric action
      //   * be an initial state
      if result.stats().did_make_progress()
        && !(was_in_initial_state && workflow.is_in_initial_state())
      {
        self.needs_state_persistence = true;
      }

      let (triggered_actions, capture_screenshot_actions) = result.into_parts();
      actions.extend(triggered_actions);
      logs_to_inject.extend(capture_screenshot_actions);
    }

    self
      .stats
      .active_traversals_total
      .observe(self.current_traversals_count.into());

    debug_assert!(
      self
        .state
        .workflows
        .iter()
        .map(Workflow::traversals_count)
        .sum::<u32>()
        == self.current_traversals_count,
      "current_traversals_count is not equal to computed traversals count"
    );

    let (
      flush_buffers_actions,
      emit_metric_actions,
      emit_sankey_diagrams_actions,
      capture_screenshot_actions,
    ) = Self::prepare_actions(actions);

    let result = self
      .flush_buffers_actions_resolver
      .process_streaming_actions(
        self
          .state
          .streaming_actions
          .drain(..)
          .map(|action| {
            // If there is no flush completion for this ID, we assume that no flush ever happened
            // and we can happily terminate the streaming action.

            let completed = self
              .pending_buffer_flushes
              .get_mut(&action.id)
              .is_none_or(|rx| !matches!(rx.try_recv(), Err(TryRecvError::Empty)));

            (action, completed)
          })
          .collect(),
        log_destination_buffer_ids,
        &self.state.session_id,
      );

    self.state.streaming_actions = result.updated_streaming_actions;

    self.needs_state_persistence |= result.has_changed_streaming_actions;

    let flush_buffers_actions_processing_result = self
      .flush_buffers_actions_resolver
      .process_flush_buffer_actions(
        flush_buffers_actions,
        &self.state.session_id,
        &self.state.pending_flush_actions,
        &self.state.streaming_actions,
      );

    self
      .metrics_collector
      .emit_metrics(&emit_metric_actions, log);

    self
      .metrics_collector
      .emit_sankeys(&emit_sankey_diagrams_actions, log);

    for action in emit_sankey_diagrams_actions {
      // There is no real limit on the number of sankey paths we might want to upload, so ensure
      // that we don't hold to too many of them in memory.
      if self.state.pending_sankey_actions.len() >= sankey_diagram::MAX_PENDING_SANKEY_PATH_UPLOADS
      {
        log::debug!("pending sankey actions limit reached, skipping sankey diagram upload");
        break;
      }

      self
        .state
        .pending_sankey_actions
        .insert(PendingSankeyPathUpload {
          sankey_path: action.path.clone(),
        });

      if let Err(e) = self.sankey_processor_input_tx.try_send(action.path) {
        log::debug!("failed to process sankey: {e}");
      }

      self.needs_state_persistence = true;
    }

    for action in flush_buffers_actions_processing_result.new_pending_actions_to_add {
      self.state.pending_flush_actions.insert(action.clone());
      if let Err(e) = self.flush_buffers_negotiator_input_tx.try_send(action) {
        log::debug!("failed to send flush buffers action intent for intent negotiation: {e}");
        self.stats.intent_negotiation_channel_send_failures.inc();
      }

      self.needs_state_persistence = true;
    }

    WorkflowsEngineResult {
      log_destination_buffer_ids: Cow::Owned(result.log_destination_buffer_ids),
      triggered_flush_buffers_action_ids: flush_buffers_actions_processing_result
        .triggered_flush_buffers_action_ids,
      triggered_flushes_buffer_ids: flush_buffers_actions_processing_result
        .triggered_flushes_buffer_ids,
      capture_screenshot: !capture_screenshot_actions.is_empty(),
      logs_to_inject,
    }
  }

  fn clean_state(&mut self) {
    self.current_traversals_count = 0;
    // We clear the ongoing workflows state as opposed to the whole state because:
    // * pending actions (uploads) are not affected by the session change, and ongoing logs uploads
    //   should continue even as the session ID changes.
    // * streaming actions after the session ID change are cleared on the next call to the
    //   Resolver's resolve method.
    self.state.clear_ongoing_workflows_state();
    self.needs_state_persistence = true;
  }

  /// Handles deduping metrics based on their tags, ensuring that the same emit metric
  /// action triggered multiple times as part of separate workflows processing the same log results
  /// in only one metric emission.
  /// TODO (Augustyniak): Fix deduping logic in the case of `parallel` workflows (it works correctly
  /// for `exclusive` workflows). Currently, multiple metric emission actions triggered by runs of
  /// the same workflow result in only one metric emission, which is incorrect behavior.
  fn prepare_actions<'a>(
    actions: Vec<TriggeredAction<'a>>,
  ) -> (
    BTreeSet<&'a ActionFlushBuffers>,
    BTreeSet<&'a ActionEmitMetric>,
    BTreeSet<TriggeredActionEmitSankey<'a>>,
    BTreeSet<&'a ActionTakeScreenshot>,
  ) {
    if actions.is_empty() {
      return (
        BTreeSet::new(),
        BTreeSet::new(),
        BTreeSet::new(),
        BTreeSet::new(),
      );
    }

    let flush_buffers_actions: BTreeSet<&ActionFlushBuffers> = actions
      .iter()
      .filter_map(|action| {
        if let TriggeredAction::FlushBuffers(flush_buffers_action) = action {
          Some(*flush_buffers_action)
        } else {
          None
        }
      })
      .collect();

    let emit_metric_actions: BTreeSet<&ActionEmitMetric> = actions
      .iter()
      .filter_map(|action| {
        if let TriggeredAction::EmitMetric(emit_metric_action) = action {
          Some(*emit_metric_action)
        } else {
          None
        }
      })
      // TODO(Augustyniak): Should we make sure that elements are unique by their ID *only*?
      .collect();

    let capture_screenshot_actions = actions
      .iter()
      .filter_map(|action| {
        if let TriggeredAction::TakeScreenshot(action) = action {
          Some(*action)
        } else {
          None
        }
      })
      .collect();

    let sankey_diagrams_actions: BTreeSet<TriggeredActionEmitSankey<'a>> = actions
      .into_iter()
      .filter_map(|action| {
        if let TriggeredAction::SankeyDiagram(action) = action {
          Some(action)
        } else {
          None
        }
      })
      // TODO(Augustyniak): Should we make sure that elements are unique by their ID *only*?
      .collect();

    (
      flush_buffers_actions,
      emit_metric_actions,
      sankey_diagrams_actions,
      capture_screenshot_actions,
    )
  }
}

impl Drop for WorkflowsEngine {
  fn drop(&mut self) {
    self.flush_buffers_negotiator_join_handle.abort();
    self.sankey_processor_join_handle.abort();
  }
}

//
// WorkflowsEngineResult
//

#[derive(Debug, PartialEq, Eq)]
pub struct WorkflowsEngineResult<'a> {
  pub log_destination_buffer_ids: Cow<'a, BTreeSet<Cow<'a, str>>>,

  // The identifier of workflow actions that triggered buffers flush(es).
  pub triggered_flush_buffers_action_ids: BTreeSet<&'a str>,
  // The identifier of trigger buffers that should be flushed.
  pub triggered_flushes_buffer_ids: BTreeSet<Cow<'static, str>>,

  // Whether a screenshot should be taken in response to processing the log.
  pub capture_screenshot: bool,

  // Logs to be injected back into the workflow engine after field attachment and other processing.
  pub logs_to_inject: Vec<Log>,
}

//
// WorkflowsEngineConfig
//

#[cfg_attr(test, derive(Clone))]
#[derive(Debug)]
pub struct WorkflowsEngineConfig {
  pub(crate) workflows_configuration: WorkflowsConfiguration,

  pub(crate) trigger_buffer_ids: BTreeSet<Cow<'static, str>>,
  pub(crate) continuous_buffer_ids: BTreeSet<Cow<'static, str>>,
}

impl WorkflowsEngineConfig {
  #[must_use]
  pub const fn new(
    workflows_configuration: WorkflowsConfiguration,
    trigger_buffer_ids: BTreeSet<Cow<'static, str>>,
    continuous_buffer_ids: BTreeSet<Cow<'static, str>>,
  ) -> Self {
    Self {
      workflows_configuration,
      trigger_buffer_ids,
      continuous_buffer_ids,
    }
  }

  #[cfg(test)]
  #[must_use]
  pub const fn new_with_workflow_configurations(workflow_configs: Vec<Config>) -> Self {
    Self::new(
      WorkflowsConfiguration::new_with_workflow_configurations_for_test(workflow_configs),
      BTreeSet::new(),
      BTreeSet::new(),
    )
  }
}

//
// StateStore
//

#[derive(Debug)]
struct StateStore {
  state_path: PathBuf,
  last_persisted: Option<Instant>,
  stats: StateStoreStats,
  persistence_write_interval_flag: DurationWatch<PersistenceWriteIntervalFlag>,
}

impl StateStore {
  fn new(sdk_directory: &Path, scope: &Scope, runtime: &ConfigLoader) -> Self {
    let stats = StateStoreStats::new(scope);

    let description =
      format!("failed to deserialize workflows: invalid sdk dir: {sdk_directory:?}");
    bd_client_common::error::handle_unexpected::<(), anyhow::Error>(
      if sdk_directory.is_dir() {
        Ok(())
      } else {
        stats.state_load_failures_total.inc();
        Err(anyhow!(description.clone()))
      },
      description.as_str(),
    );

    Self {
      state_path: sdk_directory.join("workflows_state_snapshot.6.bin"),
      last_persisted: None,
      stats,
      persistence_write_interval_flag: runtime.register_watch().unwrap(),
    }
  }

  fn load(&self) -> Option<WorkflowsState> {
    // Try to deserialize any persisted workflows state into a map
    let workflows_state = if self.state_path.exists() {
      let _timer = self.stats.state_load_duration.start_timer();
      // State is cached
      self
        .load_state()
        .inspect(|_| {
          self.stats.state_load_successes_total.inc();
        })
        .map_err(|e| {
          log::debug!("failed to deserialize workflows: {e}");
          self.stats.state_load_failures_total.inc();
          // Clean-up the corrupted file
          self.purge();
        })
        .ok()
    } else {
      // Nothing has been cached yet
      None
    };

    if workflows_state.is_some() {
      log::debug!("read workflows state from disk: {workflows_state:?}");
    } else {
      log::debug!("no workflows state available");
    }

    workflows_state
  }

  pub(self) fn load_state(&self) -> anyhow::Result<WorkflowsState> {
    let file = File::open(self.state_path.as_path())?;
    // Wrap file in buffer to deserialize efficiently in place
    let buf_reader = BufReader::new(file);
    Ok(bincode::deserialize_from::<_, WorkflowsState>(buf_reader)?)
  }

  /// Stores states of the passed workflows if all pre-conditions are met.
  /// Returns `true` if an attempt to store state was made, false otherwise.
  ///
  /// # Arguments
  ///
  /// * `workflows_state` - The workflows state to store.
  /// * `force` - If `true`, the state will be stored regardless of the time since the last save. If
  ///   `false`, the state will be stored only if the time since the last save is greater than the
  ///   configured interval.
  async fn maybe_store(&mut self, workflows_state: &WorkflowsState, force: bool) -> bool {
    // Check if enough time has passed since the last save
    let now = Instant::now();
    if force {
      log::debug!("forcing persisting workflows state to disk");
    } else if let Some(last_save_time) = self.last_persisted {
      let persistence_write_interval_ms = *self.persistence_write_interval_flag.read();
      if now.duration_since(last_save_time) < persistence_write_interval_ms {
        return false;
      }
    };

    log::debug!("persisting workflows state to disk");

    let _timer = self.stats.state_persistence_duration.start_timer();

    let workflows_state = workflows_state.optimized();

    // Serialize state snapshot and write to disk
    let state_path = self.state_path.clone();
    match tokio::task::spawn_blocking(move || Self::store(&state_path, &workflows_state)).await {
      Ok(Ok(())) => {
        log::trace!("finished persisting workflows state to disk");
        self.last_persisted = Some(now);
        self.stats.state_persistence_successes_total.inc();
      },
      Ok(Err(e)) => {
        log::debug!("failed to serialize workflows: {e}");
        self.stats.state_persistence_failures_total.inc();
      },
      Err(e) => {
        log::debug!("failed to serialize workflows: {e}");
        self.stats.state_persistence_failures_total.inc();
      },
    }

    true
  }

  fn store(state_path: &Path, state: &WorkflowsState) -> anyhow::Result<()> {
    let file = File::create(state_path)?;
    // Wrap the file in a BufWriter for better performance
    let mut writer = BufWriter::new(file);
    // Use serialize_into to avoid allocating memory twice
    bincode::serialize_into(&mut writer, state)?;
    // Flush the writer to ensure data is written
    writer.flush()?;
    Ok(())
  }

  fn purge(&self) {
    if !self.state_path.exists() {
      return;
    }

    if let Err(e) = std::fs::remove_file(&self.state_path) {
      log::debug!("failed to remove workflows state file: {e}");
    }
  }
}

//
// WorkflowsState
//

/// Maintains state about the workflow engine that is persisted to disk.
#[derive(Debug, Serialize, Deserialize, Default)]
pub(crate) struct WorkflowsState {
  session_id: String,
  workflows: Vec<Workflow>,

  pending_flush_actions: BTreeSet<PendingFlushBuffersAction>,
  pending_sankey_actions: BTreeSet<PendingSankeyPathUpload>,
  streaming_actions: Vec<StreamingBuffersAction>,
}

impl WorkflowsState {
  /// An optimized version of the workflows state with removed
  /// initial state runs and workflows that do not have any
  /// non initial state runs.
  fn optimized(&self) -> Self {
    Self {
      session_id: self.session_id.to_string(),
      workflows: self
        .workflows
        .iter()
        .filter_map(|workflow| {
          // Don't store information about workflow runs that are in the initial state
          // as these can be always recreated when needed.
          let workflow = workflow.optimized();

          // Omit workflows that have no run in non initial state.
          if workflow.runs().is_empty() {
            None
          } else {
            Some(workflow)
          }
        })
        .collect(),
      pending_flush_actions: self.pending_flush_actions.clone(),
      pending_sankey_actions: self.pending_sankey_actions.clone(),
      streaming_actions: self.streaming_actions.clone(),
    }
  }

  // Clear ongoing workflows state without clearing the state of pending and streaming actions.
  fn clear_ongoing_workflows_state(&mut self) {
    for workflow in &mut self.workflows {
      workflow.remove_all_runs();
    }
    self.session_id = String::new();
  }
}

//
// WorkflowsEngineStats
//

/// A simple wrapper for various workflows-related stats.
#[derive(Debug)]
struct WorkflowsEngineStats {
  /// The number of started workflows. Workflows are started on SDK configuration
  /// and in response to workflow config updates from a server.
  workflow_starts_total: Counter,
  /// The number of workflows on the client that were stopped due to an update from a
  /// server that removed them from the list of workflows.
  workflow_stops_total: Counter,

  /// The number of times the state of an exclusive was reset and the workflow moved back to its
  /// initial state.
  exclusive_workflow_resets_total: Counter,
  /// The number of times the exclusive workflow processed a log that matched two of its nodes -
  /// the currently active node (that's not an initial node of the workflow) and the initial
  /// workflow node.
  exclusive_workflow_potential_forks_total: Counter,

  /// The number of started runs. Each workflow has at least one and can
  /// have significantly more runs.
  run_starts_total: Counter,
  /// The number of runs who made progress. A progress is defined as a move of one or more
  /// of run's traversals from one state to another.
  run_advances_total: Counter,
  /// A number of runs that completed. Completion means that all of their traversals reached
  /// the final state.
  run_completions_total: Counter,
  /// A number of runs stopped in response to config update from a server or exceeding
  /// one of the workflows-related limits controlled with either a workflows config or
  /// SDK runtime settings.
  run_stops_total: Counter,

  /// The number of started traversals. See `Traversal` for more details.
  traversal_starts_total: Counter,
  /// The number of workflow traversals that made progress. A progress is defined as move from one
  /// state to another.
  traversal_advances_total: Counter,
  /// The number of workflow traversals that reached one of workflow's final state.
  traversal_completions_total: Counter,
  /// A number of traversals stopped in response to config update from a server or exceeding
  /// one of the workflows-related limits controlled with either a workflows config or
  /// SDK runtime settings.
  traversal_stops_total: Counter,

  /// The number of active traversals.
  active_traversals_total: Histogram,
  /// The number of times the engine prevented a run and/or traversal from being
  /// created due to a configured traversals count limit.
  traversals_count_limit_hit_total: Counter,
  /// The number of matched logs. A single log can be matched multiple times
  matched_logs_total: Counter,

  /// The amount of time workflows engine spend on each `process_log` method call.
  process_log_duration: Histogram,

  /// The number of times the engine failed to send information about buffers to flush over a tokio
  /// channel.
  buffers_to_flush_channel_send_failures: Counter,
  // The number of times the engine failed to send a log upload intent request to the intent
  // negotiator.
  intent_negotiation_channel_send_failures: Counter,
}

impl WorkflowsEngineStats {
  fn new(scope: &Scope) -> Self {
    // TODO(Augustyniak): Consider adding "workflow advance" stat.
    let workflow_starts_total =
      scope.counter_with_labels("workflows_total", labels!("operation" => "start"));
    let workflow_stops_total =
      scope.counter_with_labels("workflows_total", labels!("operation" => "stop"));

    let exclusive_workflow_resets_total =
      scope.counter_with_labels("workflow_resets_total", labels!("type" => "exclusive"));

    let exclusive_workflow_potential_forks_total = scope.counter_with_labels(
      "workflow_potential_forks_total",
      labels!("type" => "exclusive"),
    );

    let run_starts_total = scope.counter_with_labels("runs_total", labels!("operation" => "start"));
    let run_advances_total =
      scope.counter_with_labels("runs_total", labels!("operation" => "advance"));
    let run_completions_total =
      scope.counter_with_labels("runs_total", labels!("operation" => "completion"));
    let run_stops_total = scope.counter_with_labels("runs_total", labels!("operation" => "stop"));

    let traversal_starts_total =
      scope.counter_with_labels("traversals_total", labels!("operation" => "start"));
    let traversal_advances_total =
      scope.counter_with_labels("traversals_total", labels!("operation" => "advance"));
    let traversal_completions_total =
      scope.counter_with_labels("traversals_total", labels!("operation" => "completion"));
    let traversal_stops_total =
      scope.counter_with_labels("traversals_total", labels!("operation" => "stop"));

    Self {
      workflow_starts_total,
      workflow_stops_total,

      exclusive_workflow_resets_total,
      exclusive_workflow_potential_forks_total,

      run_starts_total,
      run_advances_total,
      run_completions_total,
      run_stops_total,

      traversal_starts_total,
      traversal_advances_total,
      traversal_completions_total,
      traversal_stops_total,

      active_traversals_total: scope.histogram("traversal_active_total"),
      traversals_count_limit_hit_total: scope.counter("traversals_count_limit_hit_total"),
      matched_logs_total: scope.counter("matched_logs_total"),

      process_log_duration: scope.histogram("engine_process_log_duration_s"),

      buffers_to_flush_channel_send_failures: scope
        .counter("buffers_to_flush_channel_send_failures_total"),
      intent_negotiation_channel_send_failures: scope
        .counter("intent_negotiation_channel_send_failures_total"),
    }
  }
}

//
// StateStoreStats
//

#[derive(Debug)]
#[allow(clippy::struct_field_names)]
struct StateStoreStats {
  /// The number of workflows state persistence operations that have succeeded.
  state_persistence_successes_total: Counter,
  /// The number of workflows state persistence operations that have failed.
  state_persistence_failures_total: Counter,
  /// The amount of time workflows engine needs to persist state.
  state_persistence_duration: Histogram,
  /// The number of workflows state load operations that have succeeded.
  state_load_successes_total: Counter,
  /// The number of workflows state load operations that have failed.
  state_load_failures_total: Counter,
  /// The amount of time workflows engine needs to load workflows state.
  state_load_duration: Histogram,
}

impl StateStoreStats {
  fn new(scope: &Scope) -> Self {
    let state_persistence_successes_total =
      scope.counter_with_labels("state_persistences_total", labels!("result" => "success"));
    let state_persistence_failures_total =
      scope.counter_with_labels("state_persistences_total", labels!("result" => "failure"));

    let state_load_successes_total =
      scope.counter_with_labels("state_loads_total", labels!("result" => "success"));
    let state_load_failures_total =
      scope.counter_with_labels("state_loads_total", labels!("result" => "failure"));

    Self {
      state_persistence_successes_total,
      state_persistence_failures_total,
      state_persistence_duration: scope.histogram("state_persistence_duration_s"),
      state_load_successes_total,
      state_load_failures_total,
      state_load_duration: scope.histogram("state_load_duration_s"),
    }
  }
}
