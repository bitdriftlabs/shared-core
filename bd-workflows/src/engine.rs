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
use crate::config::{Action, ActionEmitMetric, ActionFlushBuffers, Config, WorkflowsConfiguration};
use crate::metrics::MetricsCollector;
use crate::workflow::Workflow;
use anyhow::anyhow;
use bd_api::DataUpload;
use bd_client_stats::DynamicStats;
use bd_client_stats_store::{Counter, Histogram, Scope};
use bd_log_primitives::LogRef;
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
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::Instant;

//
// WorkflowsEngine
//

#[derive(Debug)]
/// Orchestrates the execution and management of workflows. It is also responsible for
/// persisting and restoring its state in disk when any workflow has changed.
pub struct WorkflowsEngine {
  configs: BTreeMap<String, Config>,
  state: EngineState,
  remote_config_mediator: RemoteConfigMediator,
  state_store: StateStore,

  current_traversals_count: u32,
  needs_state_persistence: bool,

  stats: WorkflowsEngineStats,

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
      Negotiator::new(input_rx, data_upload_tx, &actions_scope);
    let flush_buffers_negotiator_join_handle = flush_buffers_actions_negotiator.run();

    let flush_buffers_actions_resolver = Resolver::new(&actions_scope);

    let (stats, remote_config_mediator_stats) = WorkflowsEngineStats::new(&scope);

    let workflows_engine = Self {
      configs: BTreeMap::new(),
      state: EngineState::default(),
      remote_config_mediator: RemoteConfigMediator::new(remote_config_mediator_stats),
      stats,
      state_store: StateStore::new(sdk_directory, &scope, runtime),
      current_traversals_count: 0,
      needs_state_persistence: false,
      traversals_count_limit: traversals_count_limit_flag.read(),
      state_periodic_write_interval: state_periodic_write_interval_flag.read(),
      flush_buffers_actions_resolver,
      flush_buffers_negotiator_join_handle,
      flush_buffers_negotiator_input_tx: input_tx,
      flush_buffers_negotiator_output_rx: output_rx,
      metrics_collector: MetricsCollector::new(dynamic_stats),
      buffers_to_flush_tx,
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

    if let Some(engine_state) = self.state_store.load() {
      for loaded_workflows_state in engine_state.into_iter() {
        let workflows_state = self
          .state
          .get_or_insert_session_state(&loaded_workflows_state.session_id);

        workflows_state.pending_actions = self
          .flush_buffers_actions_resolver
          .standardize_pending_actions(loaded_workflows_state.pending_actions);
        workflows_state.streaming_actions = self
          .flush_buffers_actions_resolver
          .standardize_streaming_buffers(loaded_workflows_state.streaming_actions);

        self.remote_config_mediator.add_workflows(
          &mut workflows_state.workflows,
          loaded_workflows_state.workflows,
          &config.workflows_configuration.workflows,
          &mut self.current_traversals_count,
          self.traversals_count_limit,
        );
      }
    } else {
      let workflows_state = self.state.get_or_insert_session_state("");

      self.remote_config_mediator.add_workflows(
        &mut workflows_state.workflows,
        vec![],
        &config.workflows_configuration.workflows,
        &mut self.current_traversals_count,
        self.traversals_count_limit,
      );
    }

    self.configs = config
      .workflows_configuration
      .workflows
      .into_iter()
      .map(|config| (config.id().to_string(), config))
      .collect();

    for workflows_state in self.state.iter() {
      for action in &workflows_state.pending_actions {
        if let Err(e) = self
          .flush_buffers_negotiator_input_tx
          .try_send(action.clone())
        {
          log::debug!("failed to send pending action for intent negotiation: {e}");
          self.stats.intent_negotiation_channel_send_failures.inc();
        }
      }
    }

    log::debug!(
      "started workflows engine with {} workflow(s); {} pending processing action(s); {} \
       streaming action(s); session ID: \"{}\"; traversals count limit: {}",
      self.state.current_session_state().workflows.len(),
      self.state.current_session_state().pending_actions.len(),
      self.state.current_session_state().streaming_actions.len(),
      self.state.current_session_id(),
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

    for state in self.state.states.iter_mut() {
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
      for index in (0 .. state.workflows.len()).rev() {
        let should_remove_existing_workflow =
          match latest_workflow_ids_to_index.get(state.workflows[index].id()) {
            Some(latest_workflows_index) => {
              let workflow_id = config.workflows_configuration.workflows[*latest_workflows_index].id();
              // Handle an edge case where a client receives a config update containing a workflow
              // with a config field unknown to the client, which is later updated to
              // understand this field. In such cases, the server's config is converted to
              // the client's representation and cached for future use.
              // If the client is later updated to understand the previously unsupported field,
              // we check for a config mismatch between the cached client representation and
              // the fresh client representation from the server. If there's a mismatch, we discard
              // the cached version and replace it with the fresh one later on.
              Some(&config.workflows_configuration.workflows[*latest_workflows_index]) != self.configs.get(workflow_id)
            },
            // The latest config update doesn't contain a workflow with an ID of the existing
            // workflow. Remove existing workflow.
            None => true,
          };

        if should_remove_existing_workflow {
          self.remote_config_mediator.remove_workflow(
            state,
            index,
            &mut self.configs,
            &mut self.traversals_count_limit,
          );
        }
      }
    }

    self.configs = config.workflows_configuration.workflows.into_iter().map(|config| (config.id().to_string(), config)).collect();

    self
      .flush_buffers_actions_resolver
      .update(ResolverConfig::new(
        config.trigger_buffer_ids,
        config.continuous_buffer_ids,
      ));

    for state in self.state.states.iter_mut() {
      // Introduce hash look ups to avoid N^2 complexity later in this method.
      let existing_workflow_ids_to_index: HashMap<String, usize> = state
        .workflows
        .iter()
        .enumerate()
        .map(|(index, workflow)| (workflow.id().to_string(), index))
        .collect();

      // Iterate over the list of configs from the latest update and
      // create workflows for the ones that do not have their representation
      // on the client yet.
      for (_, workflow_config) in &self.configs {
        if !existing_workflow_ids_to_index.contains_key(workflow_config.id()) {
          self.remote_config_mediator.add_workflow(
            Workflow::new(workflow_config.id().to_string()),
            &mut state.workflows,
            &mut self.current_traversals_count,
            self.traversals_count_limit,
          );
        }
      }

      state.pending_actions = self
        .flush_buffers_actions_resolver
        .standardize_pending_actions(state.pending_actions.clone());
      state.streaming_actions = self
        .flush_buffers_actions_resolver
        .standardize_streaming_buffers(state.streaming_actions.clone());
    }

    log::debug!(
      "consumed received workflows config update; workflows engine contains {} workflow(s)",
      self.state.current_session_state().workflows.len()
    );
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
            if let Some(state) = self.state.get_session_state(action.session_id.as_str()) {
              state.pending_actions.remove(&action);
            };

            if let Err(e) = self.buffers_to_flush_tx.try_send(BuffersToFlush::new(&action)) {
              self.stats.buffers_to_flush_channel_send_failures.inc();
              log::debug!("failed to send information about buffers to flush: {e}");
            }

            match self.flush_buffers_actions_resolver.make_streaming_action(action.clone()) {
              Some(streaming_action) => {
                log::debug!("streaming started: \"{streaming_action:?}\"");
                if let Some(state) = self.state.get_session_state(action.session_id.as_str()) {
                  state.streaming_actions.push(streaming_action);
                };
              },
              None => {
                log::debug!("no streaming configuration defined for action: \"{action:?}\"");
              }
            }

            self.needs_state_persistence = true;
          },
          NegotiatorOutput::UploadRejected(action) => {
            if let Some(state) = self.state.get_session_state(action.session_id.as_str()) {
              state.pending_actions.remove(&action);
            };
            self.needs_state_persistence = true;
          }
        }
      },
    }
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

    let state = self.state.get_or_insert_session_state(log.session_id);

    // Return early if there are no workflows.
    if state.workflows.is_empty() {
      self
        .stats
        .active_traversals_total
        .observe(self.current_traversals_count.into());

      return WorkflowsEngineResult {
        log_destination_buffer_ids: Cow::Borrowed(log_destination_buffer_ids),
        triggered_flushes_buffer_ids: BTreeSet::new(),
        triggered_flush_buffers_action_ids: BTreeSet::new(),
      };
    }

    let mut actions: Vec<&Action> = vec![];
    for workflow in &mut state.workflows.iter_mut() {
      let was_in_initial_state = workflow.is_in_initial_state();

      let Some(config) = self.configs.get(workflow.id()) else {
        log::debug!(
          "attempted to evaluate not existing workflow: {:?}",
          workflow.id()
        );
        continue;
      };

      let result = workflow.process_log(
        &config,
        log,
        &mut self.current_traversals_count,
        self.traversals_count_limit,
      );

      self
        .stats
        .exclusive_workflow_resets_total
        .inc_by(u64::from(result.stats().reset_exclusive_workflows_count));
      self
        .stats
        .exclusive_workflow_potential_forks_total
        .inc_by(u64::from(
          result.stats().potential_fork_exclusive_workflows_count,
        ));

      self
        .stats
        .run_starts_total
        .inc_by(u64::from(result.stats().created_runs_count));
      self
        .stats
        .run_advances_total
        .inc_by(u64::from(result.stats().advanced_runs_count));
      self
        .stats
        .run_stops_total
        .inc_by(u64::from(result.stats().stopped_runs_count));
      self
        .stats
        .run_completions_total
        .inc_by(u64::from(result.stats().completed_runs_count));

      self
        .stats
        .traversal_starts_total
        .inc_by(u64::from(result.stats().created_traversals_count));
      self
        .stats
        .traversal_advances_total
        .inc_by(u64::from(result.stats().advanced_traversals_count));
      self
        .stats
        .traversal_stops_total
        .inc_by(u64::from(result.stats().stopped_traversals_count));
      self
        .stats
        .traversal_completions_total
        .inc_by(u64::from(result.stats().completed_traversals_count));

      self
        .stats
        .traversals_count_limit_hit_total
        .inc_by(u64::from(result.stats().traversals_count_limit_hit_count));

      self
        .stats
        .matched_logs_total
        .inc_by(u64::from(result.stats().matched_logs_count));

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

      let t = result.actions();
      actions.extend(t);
    }

    debug_assert!(
      state
        .workflows
        .iter()
        .map(Workflow::traversals_count)
        .sum::<u32>()
        == self.current_traversals_count,
      "current_traversals_count is not equal to computed traversals count"
    );

    self
      .stats
      .active_traversals_total
      .observe(self.current_traversals_count.into());

    let (flush_buffers_actions, emit_metric_actions) = Self::prepare_actions(&actions);

    let result = self
      .flush_buffers_actions_resolver
      .process_streaming_actions(
        &mut state.streaming_actions,
        log_destination_buffer_ids,
        log.session_id,
      );

    self.needs_state_persistence |= result.has_changed_streaming_actions;

    let flush_buffers_actions_processing_result = self
      .flush_buffers_actions_resolver
      .process_flush_buffer_actions(
        flush_buffers_actions,
        log.session_id,
        &state.pending_actions,
        &state.streaming_actions,
      );

    self
      .metrics_collector
      .emit_metrics(&emit_metric_actions, log);

    for action in flush_buffers_actions_processing_result.new_pending_actions_to_add {
      state.pending_actions.insert(action.clone());
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
    }
  }

  /// Handles deduping metrics based on their tags, ensuring that the same emit metric
  /// action triggered multiple times as part of separate workflows processing the same log results
  /// in only one metric emission.
  /// TODO (Augustyniak): Fix deduping logic in the case of `parallel` workflows (it works correctly
  /// for `exclusive` workflows). Currently, multiple metric emission actions triggered by runs of
  /// the same workflow result in only one metric emission, which is incorrect behavior.
  fn prepare_actions<'a>(
    actions: &[&'a Action],
  ) -> (
    BTreeSet<&'a ActionFlushBuffers>,
    BTreeSet<&'a ActionEmitMetric>,
  ) {
    if actions.is_empty() {
      return (BTreeSet::new(), BTreeSet::new());
    }

    let flush_buffers_actions: BTreeSet<&ActionFlushBuffers> = actions
      .iter()
      .filter_map(|action| {
        if let Action::FlushBuffers(flush_buffers_action) = action {
          Some(flush_buffers_action)
        } else {
          None
        }
      })
      .collect();

    let emit_metric_actions: BTreeSet<&ActionEmitMetric> = actions
      .iter()
      .filter_map(|action| {
        if let Action::EmitMetric(emit_metric_action) = action {
          Some(emit_metric_action)
        } else {
          None
        }
      })
      // TODO(Augustyniak): Should we make sure that elements are unique by their ID *only*?
      .collect();

    (flush_buffers_actions, emit_metric_actions)
  }
}

impl Drop for WorkflowsEngine {
  fn drop(&mut self) {
    self.flush_buffers_negotiator_join_handle.abort();
  }
}

//
// RemoteConfigMediatorStats
//

#[allow(clippy::struct_field_names)]
#[derive(Debug)]
struct RemoteConfigMediatorStats {
  /// The number of started workflows. Workflows are started on SDK configuration
  /// and in response to workflow config updates from a server.
  workflow_starts_total: Counter,
  /// The number of workflows on the client that were stopped due to an update from a
  /// server that removed them from the list of workflows.
  workflow_stops_total: Counter,

  /// The number of started runs. Each workflow has at least one and can
  /// have significantly more runs.
  run_starts_total: Counter,
  /// A number of runs stopped in response to config update from a server or exceeding
  /// one of the workflows-related limits controlled with either a workflows config or
  /// SDK runtime settings.
  run_stops_total: Counter,

  /// The number of started traversals. See `Traversal` for more details.
  traversal_starts_total: Counter,
  /// A number of traversals stopped in response to config update from a server or exceeding
  /// one of the workflows-related limits controlled with either a workflows config or
  /// SDK runtime settings.
  traversal_stops_total: Counter,

  /// The number of times the engine prevented a run and/or traversal from being
  /// created due to a configured traversals count limit.
  traversals_count_limit_hit_total: Counter,
}

impl RemoteConfigMediatorStats {
  fn new(
    scope: &Scope,
    run_starts_total: Counter,
    run_stops_total: Counter,
    traversal_starts_total: Counter,
    traversal_stops_total: Counter,
    traversals_count_limit_hit_total: Counter,
  ) -> Self {
    Self {
      workflow_starts_total: scope
        .counter_with_labels("workflows_total", labels!("operation" => "start")),
      workflow_stops_total: scope
        .counter_with_labels("workflows_total", labels!("operation" => "stop")),

      run_starts_total,
      run_stops_total,

      traversal_starts_total,
      traversal_stops_total,

      traversals_count_limit_hit_total,
    }
  }
}

//
//
//

#[derive(Debug)]
struct RemoteConfigMediator {
  stats: RemoteConfigMediatorStats,
}

impl RemoteConfigMediator {
  fn new(stats: RemoteConfigMediatorStats) -> Self {
    Self { stats }
  }

  fn add_workflows(
    &self,
    workflows: &mut Vec<Workflow>,
    existing_workflows: Vec<Workflow>,
    configs: &Vec<Config>,
    current_traversals_count: &mut u32,
    traversals_count_limit: u32,
  ) {
    // `configs` is the source of truth for workflow configurations from the server
    // while `existing_workflows` contains state of workflow who have been running on a device.
    //  * Combine the two by iterating over the list of configs from the server and associating
    //    configs with relevant workflow state (if any).
    //  * Discard workflow states that don't have a corresponding server-side config.
    let mut existing_workflow_ids_to_workflows: HashMap<_, _> = existing_workflows
      .into_iter()
      .map(|workflow| (workflow.id().to_string(), workflow))
      .collect();

    let workflows_to_add = configs.into_iter().map(|config| {
      let workflow = existing_workflow_ids_to_workflows
        .remove(config.id())
        .map_or_else(
          // No cached state for a given workflow ID, start from scratch.
          || Workflow::new(config.id().to_string()),
          // We have cache state for a given workflow ID.
          |workflow| workflow,
        );
      workflow
    });

    for workflow in workflows_to_add {
      self.add_workflow(
        workflow,
        workflows,
        current_traversals_count,
        traversals_count_limit,
      );
    }
  }

  fn add_workflow(
    &self,
    workflow: Workflow,
    workflows: &mut Vec<Workflow>,
    current_traversals_count: &mut u32,
    traversals_count_limit: u32,
  ) {
    let mut workflow = workflow;

    let workflow_traversals_count = workflow.traversals_count();
    if workflow_traversals_count + *current_traversals_count > traversals_count_limit {
      log::debug!(
        "failed to add workflow with {} traversal(s) due to traversals count limit being hit \
         ({}); current traversals count {}",
        workflow_traversals_count,
        traversals_count_limit,
        current_traversals_count
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

        if remaining_workflow_traversals_count + *current_traversals_count <= traversals_count_limit
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

    log::debug!(
      "workflow={}: workflow added, runs count {}, traversal count {}",
      workflow.id(),
      workflow.runs().len(),
      workflow_traversals_count,
    );

    *current_traversals_count += workflow.traversals_count();

    workflows.push(workflow);
  }

  fn remove_workflow(
    &self,
    state: &mut WorkflowsState,
    workflow_index: usize,
    configs: &mut BTreeMap<String, Config>,
    current_traversals_count: &mut u32,
  ) {
    configs.remove_entry(state.workflows[workflow_index].id());
    let workflow = state.workflows.remove(workflow_index);

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

    *current_traversals_count = current_traversals_count.saturating_sub(workflow_traversals_count);

    log::debug!("workflow={}: workflow removed", workflow.id());

    // If there exists a run that is not in an initial state.
    if !workflow.is_in_initial_state() {
      log::debug!(
        "workflow={}: workflow removed, marking state as dirty",
        workflow.id()
      );
      // Mark the state as dirty so that the state associated with
      // the workflow that was just removed can be removed from disk.
      // TODO(Augustyniak): Implement this.
      // self.needs_state_persistence = true;
    }
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
      state_path: sdk_directory.join("workflows_state_snapshot.3.bin"),
      last_persisted: None,
      stats,
      persistence_write_interval_flag: runtime.register_watch().unwrap(),
    }
  }

  fn load(&self) -> Option<EngineState> {
    // Try to deserialize any persisted workflows state into a map
    let engine_state = if self.state_path.exists() {
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

    if engine_state.is_some() {
      log::debug!("read workflows engine state from disk: {engine_state:?}");
    } else {
      log::debug!("no workflows engine state available");
    }

    engine_state
  }

  pub(self) fn load_state(&self) -> anyhow::Result<EngineState> {
    let file = File::open(self.state_path.as_path())?;
    // Wrap file in buffer to deserialize efficiently in place
    let buf_reader = BufReader::new(file);
    Ok(bincode::deserialize_from::<_, EngineState>(buf_reader)?)
  }

  /// Stores states of the passed workflows if all pre-conditions are met.
  /// Returns `true` if an attempt to store state was made, false otherwise.
  ///
  /// # Arguments
  ///
  /// * `engine_state` - The engine state to store.
  /// * `force` - If `true`, the state will be stored regardless of the time since the last save. If
  ///   `false`, the state will be stored only if the time since the last save is greater than the
  ///   configured interval.
  async fn maybe_store(&mut self, engine_state: &EngineState, force: bool) -> bool {
    // Check if enough time has passed since the last save
    let now = Instant::now();
    if force {
      log::debug!("forcing persisting workflows engine state to disk");
    } else if let Some(last_save_time) = self.last_persisted {
      let persistence_write_interval_ms = self.persistence_write_interval_flag.read();
      if now.duration_since(last_save_time) < persistence_write_interval_ms {
        return false;
      }
    };

    log::debug!("persisting workflows state to disk");

    let _timer = self.stats.state_persistence_duration.start_timer();

    let engine_state = engine_state.optimized();

    // Serialize state snapshot and write to disk
    let state_path = self.state_path.clone();
    match tokio::task::spawn_blocking(move || Self::store(&state_path, &engine_state)).await {
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

  fn store(state_path: &Path, state: &EngineState) -> anyhow::Result<()> {
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
      log::debug!("failed to remove workflows engine state file: {e}");
    }
  }
}

//
// EngineState
//

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct EngineState {
  states: Vec<WorkflowsState>,
}

impl EngineState {
  fn optimized(&self) -> Self {
    Self {
      states: self.states.iter().map(WorkflowsState::optimized).collect(),
      // current_traversals_count: 0,    // TODO(Augustyniak): IMPLEMENT
      // needs_state_persistence: false, // TODO(Augustyniak): IMPLEMENT
    }
  }

  fn into_iter(self) -> impl Iterator<Item = WorkflowsState> {
    self.states.into_iter()
  }

  fn iter(&self) -> impl Iterator<Item = &WorkflowsState> {
    self.states.iter()
  }

  fn current_session_id(&self) -> String {
    self
      .states
      .first()
      .map_or_else(String::new, |state| state.session_id.clone())
  }

  fn current_session_state(&mut self) -> &mut WorkflowsState {
    if self.states.is_empty() {
      let state = WorkflowsState::default();
      self.states.push(state);
    }

    self.states.first_mut().unwrap()
  }

  fn get_session_state(&mut self, session_id: &str) -> Option<&mut WorkflowsState> {
    self
      .states
      .iter_mut()
      .find(|state| state.session_id == session_id)
  }

  fn get_or_insert_session_state(&mut self, session_id: &str) -> &mut WorkflowsState {
    if self
      .states
      .iter()
      .find(|state| state.session_id == session_id)
      .is_none()
    {
      if let Some(previous_session) = self.states.first_mut() {
        // We are lazy and don't say that state needs persistance.
        // That may result in new session ID not being stored to disk
        // (if the app is killed before the next time we store state)
        // which means that the next time SDK launches we start with empty
        // session ID (""). It should be OK as in the context of cleaning workflows
        // state on session change, having empty session ID
        // ("") stored on disk is equal to storing a session ID (i.e., "foo") with
        // all workflows in their initial states.
        log::debug!(
          "workflows engine: moving from \"{}\" to new session \"{}\", cleaning workflows state",
          previous_session.session_id,
          session_id
        );

        for workflow in &mut previous_session.workflows {
          workflow.remove_all_runs();
        }

        // TODO(Augustyniak): Implement in other place.
        // self.current_traversals_count = 0;
        // self.needs_state_persistence = true;
      } else {
        // There was no state on a disk when workflows engine was started
        // and engine just observed first session ID.
        // We do not have to rush to persist it to disk until any of the
        // workflows makes progress. That's because in the context of cleaning workflows
        // state on session change, having empty session ID
        // ("") stored on disk is equal to storing a session ID (i.e., "foo") with
        // all workflows in their initial states.
        log::debug!(
          "workflows engine: moving from no session to session \"{}\"",
          session_id
        );
      }

      let state = WorkflowsState::new(session_id);
      self.states.insert(0, state);

      if self.states.len() > 2 {
        self.states.remove(self.states.len() - 1);
      }
    }

    // # Safety
    // Unwrapping is safe since we are adding the state that we search for a few lines above.
    self
      .states
      .iter_mut()
      .find(|state| state.session_id == session_id)
      .unwrap()
  }
}

//
// WorkflowsState
//

#[derive(Debug, Default, Serialize, Deserialize)]
/// Holds a complete representation of all the active Workflows' state.
pub(crate) struct WorkflowsState {
  session_id: String,
  workflows: Vec<Workflow>,

  pending_actions: BTreeSet<PendingFlushBuffersAction>,
  streaming_actions: Vec<StreamingBuffersAction>,
}

impl WorkflowsState {
  fn new(session_id: &str) -> Self {
    Self {
      session_id: session_id.to_string(),
      workflows: vec![],
      pending_actions: BTreeSet::new(),
      streaming_actions: vec![],
    }
  }

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
      pending_actions: self.pending_actions.clone(),
      streaming_actions: self.streaming_actions.clone(),
    }
  }
}

//
// WorkflowsEngineStats
//

/// A simple wrapper for various workflows-related stats.
#[derive(Debug)]
struct WorkflowsEngineStats {
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
  fn new(scope: &Scope) -> (Self, RemoteConfigMediatorStats) {
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

    let traversals_count_limit_hit_total = scope.counter("traversals_count_limit_hit_total");

    let remote_config_mediator_stats = RemoteConfigMediatorStats::new(
      scope,
      run_starts_total.clone(),
      run_stops_total.clone(),
      traversal_starts_total.clone(),
      traversal_stops_total.clone(),
      traversals_count_limit_hit_total.clone(),
    );

    (
      Self {
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
      },
      remote_config_mediator_stats,
    )
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
