// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./workflow_test.rs"]
pub(crate) mod workflow_test;

use crate::config::{
  Action,
  ActionEmitMetric,
  ActionEmitSankey,
  ActionFlushBuffers,
  Config,
  Predicate,
  WorkflowDebugMode,
};
use crate::generate_log::generate_log_action;
use bd_log_primitives::tiny_set::TinyMap;
use bd_log_primitives::{FieldsRef, Log, log_level};
use bd_proto::protos::logging::payload::LogType;
use bd_proto_util::serialization::TimestampMicros;
use bd_stats_common::workflow::{WorkflowDebugStateKey, WorkflowDebugTransitionType};
use bd_time::OffsetDateTimeExt;
use itertools::Itertools;
use sha2::Digest;
use std::borrow::Cow;
use std::collections::HashMap;
use time::OffsetDateTime;

//
// WorkflowEvent
//

/// Represents an event that can trigger workflow transitions.
#[derive(Clone, Copy)]
pub enum WorkflowEvent<'a> {
  /// A log message was received
  Log(&'a Log),
  /// A state change occurred, with optional global metadata fields
  StateChange(&'a bd_state::StateChange, FieldsRef<'a>),
}

impl WorkflowEvent<'_> {
  pub(crate) fn session_id(&self) -> Option<&str> {
    match self {
      WorkflowEvent::Log(log) => Some(log.session_id.as_str()),
      WorkflowEvent::StateChange(..) => None,
    }
  }

  pub(crate) fn capture_session(&self) -> Option<&'static str> {
    match self {
      WorkflowEvent::Log(log) => log.capture_session,
      WorkflowEvent::StateChange(..) => None,
    }
  }

  pub(crate) fn occurred_at(&self) -> OffsetDateTime {
    match self {
      WorkflowEvent::Log(log) => log.occurred_at,
      WorkflowEvent::StateChange(state_change, _) => state_change.timestamp,
    }
  }
}

//
// Workflow
//

/// Encapsulates a logic responsible for running a workflow.
/// It consists of two main pieces:
///   * a config ID.
///   * a list of workflow runs that manage the state for each of the runs of a workflow. Runs are
///     lightweight 'pointers' pointing at specific parts of the workflow config + a minimal amount
///     of extra state that's necessary to execute a given workflow i.e., the number of times a
///     given log matcher rule has matched a log.
#[cfg_attr(test, derive(Clone))]
#[bd_macros::proto_serializable]
#[derive(Debug)]
pub struct Workflow {
  #[field(id = 1)]
  id: String,
  // The list of runs. Runs are organized in the following way: New runs are added to the beginning
  // of the list and the maximum number of runs is equal to two. If runs list is non-empty then the
  // first run in the list is guaranteed to be in an initial state. If there are two runs then the
  // second run is guaranteed not to be in an initial state.
  #[field(id = 2)]
  pub(crate) runs: Vec<Run>,
  // Persisted workflow debug state. This is only used for live debugging. Global debugging is
  // persisted as part of stats snapshots.
  #[field(id = 3)]
  workflow_debug_state: Option<WorkflowDebugStateMap>,
  // Whether we need to emit a "start" metric for this workflow. This is true when the workflow
  // is first delivered to the client. If the workflow is subsequently loaded from cache it
  // will not increment.
  #[field(id = 4)]
  needs_start_metric: bool,
}

impl Workflow {
  pub(crate) const fn new(config_id: String, needs_start_metric: bool) -> Self {
    Self::new_from_parts(config_id, vec![], None, needs_start_metric)
  }

  pub(crate) const fn new_from_parts(
    config_id: String,
    runs: Vec<Run>,
    workflow_debug_state: OptWorkflowDebugStateMap,
    needs_start_metric: bool,
  ) -> Self {
    Self {
      id: config_id,
      runs,
      workflow_debug_state,
      needs_start_metric,
    }
  }

  #[must_use]
  pub(crate) fn runs(&self) -> &[Run] {
    &self.runs
  }

  pub(crate) fn id(&self) -> &str {
    &self.id
  }

  pub(crate) fn workflow_debug_state(&self) -> &OptWorkflowDebugStateMap {
    &self.workflow_debug_state
  }

  pub(crate) fn clear_workflow_debug_state(&mut self) {
    self.workflow_debug_state = None;
  }

  pub(crate) fn process_event<'a>(
    &mut self,
    config: &'a Config,
    event: WorkflowEvent<'_>,
    state_reader: &dyn bd_state::StateReader,
    now: OffsetDateTime,
  ) -> WorkflowResult<'a> {
    let mut result = WorkflowResult::default();

    if self.needs_new_run() {
      log::trace!("workflow={}: creating a new run", self.id);
      // The timeout is only initialized here (if applicable) if this is the primary run. If this
      // is an initial state run it will not be initialized until the primary run is complete. If
      // the initial state run happens to progress beyond the timeout on its own that is fine.
      let run = Run::new(config, now, self.runs.is_empty());
      if run
        .traversals
        .first()
        .is_some_and(|t| t.timeout_unix_ms.is_some())
      {
        result.stats.processed_timeout = true;
      }

      self.runs.insert(0, run);
    }

    let mut did_make_progress = false;
    // Process runs in reversed order as we may end up modifying the array
    // starting at `index` in any given iteration of the loop.
    for index in (0 .. self.runs.len()).rev() {
      log::trace!("processing run {index} for workflow {}", self.id);
      let is_initial_run = index == 0;
      let Some(run) = self.runs.get_mut(index) else {
        continue;
      };
      let mut run_result = run.process_event(config, event, state_reader, now);

      result.incorporate_run_result(&mut run_result);

      let run_result_did_make_progress = run_result.did_make_progress();

      match run_result.state {
        RunState::Stopped => {
          self.runs.remove(index);
        },
        RunState::Completed => {
          debug_assert!(
            run.traversals.is_empty(),
            "completing a run with active traversals"
          );

          did_make_progress = true;
          log::trace!("completed run, workflow id={}", self.id);
          self.runs.remove(index);

          // In the case where we completed the active run *and* there is a pending initial state
          // run, we need to see if there is a timeout to initialize on the initial state run. In
          // this case by definition the initial state run should have a single traversal as it
          // has not progressed.
          if !is_initial_run {
            debug_assert!(self.runs.first().map(|r| r.traversals.len()) == Some(1));
            if let Some(f) = self.runs.first_mut().and_then(|r| r.traversals.get_mut(0)) {
              f.initialize_timeout(config, now);
            }
          }
        },
        RunState::Running => {
          if run_result_did_make_progress {
            did_make_progress = true;
          }
        },
      }

      // Processing exclusive workflow runs looks as follows:
      // There are either one or two active runs for the workflow, the first one in the list is
      // always the initial state run. We begin by attempting to process the log with the run
      // at the end of the list.
      //  1. If it matches the log then processing of the log by a *workflow* is considered to be
      //     done.
      //  2. If it doesn't match the log we attempt to process it with the previous run in the list
      //     (we iterate over the runs list in reversed order) if one exists. In this case the
      //     second run evaluated must be the initial state run. If this run matches the log, we'll
      //     "reset" the workflow by removing the previously evaluated run and replacing it with the
      //     continuation of the run matched in this second step. If this run does not match, we'll
      //     do nothing.
      debug_assert!(
        self.runs.len() <= 2,
        "exclusive workflow should never have more than 2 runs"
      );

      // An exclusive workflow can reset or potentially fork only if it has two runs, one that's
      // in an initial state and another one that is not in an initial state.
      let has_active_run = self.runs.len() > 1;
      let has_active_run_and_can_reset = has_active_run && did_make_progress;

      if !has_active_run_and_can_reset {
        continue;
      }

      // There exists more than two runs and the index of the current run is 0.
      if is_initial_run {
        // Handling of "resetting" logic that's unique to exclusive workflows:
        // * remove the workflow run processed in previous iteration of the loop (if any), the one
        //   that was active at the time when the processing of the log started.
        // * keep the run that advanced in the current iteration of the loop, it moved out of the
        //   the initial state as result of processing the log.

        // # Safety
        // This is safe as `has_active_run == true means that there are at least two runs and
        // `is_initial_run == true`` means that we are processing run with index == 0.
        log::trace!("resetting workflow due to initial state transition");
        self.runs.remove(index + 1);
      } else {
        // The active state run made progress and the next run to be processed (if there is any)
        // is an initial state run that we do not want to expose to the log.
        log::trace!("active state run made progress");
        break;
      }
    }

    if self.needs_start_metric {
      log::trace!("workflow {} delivered", self.id);
      result
        .incremental_workflow_debug_state
        .push(WorkflowDebugStateKey::StartOrReset);
      self.needs_start_metric = false;
    }

    result.finalize(config, &mut self.workflow_debug_state, now)
  }

  pub(crate) fn needs_new_run(&self) -> bool {
    if self.runs.is_empty() {
      // Create a new run if there is no runs left. This can happen if all runs were completed as
      // the result of processing a log.
      return true;
    }

    // If workflow doesn't have a run in an initial state.
    !self.runs.iter().any(Run::is_in_initial_state)
  }

  /// Returns workflow without these of its runs that are in initial state.
  pub(crate) fn optimized(&self) -> Self {
    // This should prevent us from storing a bunch of unnecessary data to disk i.e., imagine
    // that server sends 200 workflows to client and only one of these workflows
    // advances. If we stored workflow runs even if they are in their initial state
    // the client would need to store information about 200 runs to a disk,
    // by filtering out runs with initial state we reduce that number to 1.
    let runs = self
      .runs()
      .iter()
      .filter(|&run| !run.is_in_initial_state())
      .cloned()
      .collect_vec();

    Self::new_from_parts(
      self.id().to_string(),
      runs,
      self.workflow_debug_state.clone(),
      false,
    )
  }

  /// Whether a given workflow has no runs or all of its runs are in an initial state.
  /// While in theory the implementation of the method needs to iterate over the list of all
  /// workflow's run in practice it's able to tell whether a workflow is an initial state
  /// after checking at most two of its workflow runs.
  /// That's because:
  /// * if there are no runs workflow is an initial state.
  /// * if there is one run the implementation needs to call `Run::is_initial` to check whether
  ///   workflow is an initial state or not.
  /// * if there is more than one run the first run cannot be in an initial state hence the whole
  ///   workflow cannot be in an initial state. Each workflow can have at most one initial state run
  ///   and initial state workflows can exists only at the end of `workflows` list of workflows.
  pub fn is_in_initial_state(&self) -> bool {
    let is_in_initial_state = self.runs.iter().all(Run::is_in_initial_state);
    debug_assert!(
      if self.runs.len() > 1 {
        !is_in_initial_state
      } else {
        true
      },
      "workflow with more than 1 run cannot be in an initial state"
    );

    is_in_initial_state
  }

  /// Remove all workflow runs.
  pub(crate) fn remove_all_runs(&mut self) {
    self.runs.clear();
  }

  /// Returns the list of states of all run's traversals.
  #[cfg(test)]
  pub(crate) fn runs_states(&self, config: &Config) -> Vec<String> {
    (0 .. self.runs.len())
      .flat_map(|run_index| {
        self.runs[run_index].traversals.iter().map(|traversal| {
          config.inner().states()[traversal.state_index]
            .id()
            .to_string()
        })
      })
      .collect()
  }

  /// Returns the list of states of given run's traversals.
  #[cfg(test)]
  pub(crate) fn traversals_states(&self, config: &Config, run_index: usize) -> Vec<String> {
    self.runs[run_index]
      .traversals
      .iter()
      .map(|traversal| {
        config.inner().states()[traversal.state_index]
          .id()
          .to_string()
      })
      .collect()
  }
}

//
// WorkflowResult
//

/// A result of performing a workflow.
/// It consists of actions that should be performed as the result
/// of processing a log by a given workflow and stats-like measurements
/// that describe what internal operations workflow performed
/// when it processed a given log.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct WorkflowResult<'a> {
  triggered_actions: Vec<TriggeredAction<'a>>,
  logs_to_inject: TinyMap<&'a str, Log>,
  stats: WorkflowResultStats,
  // Persisted workflow debug state. This is only used for live debugging. Global debugging is
  // persisted as part of stats snapshots.
  cumulative_workflow_debug_state: OptWorkflowDebugStateMap,
  // Incremental debug data is always returned and is ultimately stored with stats snapshots.
  incremental_workflow_debug_state: Vec<WorkflowDebugStateKey>,
}

impl<'a> WorkflowResult<'a> {
  pub fn into_parts(
    self,
  ) -> (
    Vec<TriggeredAction<'a>>,
    TinyMap<&'a str, Log>,
    OptWorkflowDebugStateMap,
    Vec<WorkflowDebugStateKey>,
  ) {
    (
      self.triggered_actions,
      self.logs_to_inject,
      self.cumulative_workflow_debug_state,
      self.incremental_workflow_debug_state,
    )
  }

  pub const fn stats(&self) -> &WorkflowResultStats {
    &self.stats
  }

  fn incorporate_run_result(&mut self, run_result: &mut RunResult<'a>) {
    self
      .triggered_actions
      .append(&mut run_result.triggered_actions);
    self.logs_to_inject.append(&mut run_result.logs_to_inject);
    self.stats.matched_logs_count += run_result.matched_logs_count;
    self.stats.processed_timeout |= run_result.processed_timeout;

    self
      .incremental_workflow_debug_state
      .append(&mut run_result.workflow_debug_state);
  }

  fn finalize(
    mut self,
    config: &'a Config,
    workflow_debug_state: &mut OptWorkflowDebugStateMap,
    now: OffsetDateTime,
  ) -> Self {
    // First we move any results into the persisted map so it survives across restarts if we are
    // doing active live debugging.
    if config.mode() != WorkflowDebugMode::None && !self.incremental_workflow_debug_state.is_empty()
    {
      workflow_debug_state
        .get_or_insert_default()
        .merge(&self.incremental_workflow_debug_state, now);
    }

    // If this is debug only, clear the incremental state so it doesn't get persisted with stats.
    if config.mode() == WorkflowDebugMode::DebugOnly {
      self.incremental_workflow_debug_state.clear();
    }

    // If there is any debug state (whether from this run or previous runs) return it in the
    // result so that it can be bundled up and send to the server.
    if let Some(workflow_debug_state) = workflow_debug_state {
      self.cumulative_workflow_debug_state = Some(workflow_debug_state.clone());
    }

    self
  }
}

//
// WorkflowResultStats
//

/// Describes the internal operations performed by a workflow
/// while processing a log.
#[derive(Debug, Default, PartialEq, Eq)]
#[allow(clippy::struct_field_names)]
pub(crate) struct WorkflowResultStats {
  pub(crate) matched_logs_count: u32,
  processed_timeout: bool,
}

impl WorkflowResultStats {
  pub(crate) const fn did_make_progress(&self) -> bool {
    self.matched_logs_count > 0 || self.processed_timeout
  }
}

//
// SankeyPath
//

#[bd_macros::proto_serializable]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
pub(crate) struct SankeyPath {
  #[field(id = 1)]
  pub(crate) sankey_id: String,
  #[field(id = 2)]
  pub(crate) nodes: Vec<String>,
  #[field(id = 3)]
  pub(crate) path_id: String,
  #[field(id = 4)]
  pub(crate) is_trimmed: bool,
}

impl SankeyPath {
  fn new(sankey_id: &str, sankey_state: SankeyState) -> Self {
    let path_id = Self::calculate_path_id(&sankey_state);

    Self {
      sankey_id: sankey_id.to_string(),
      nodes: sankey_state.nodes.into_iter().map(|n| n.value).collect(),
      path_id,
      is_trimmed: sankey_state.is_trimmed,
    }
  }

  fn calculate_path_id(state: &SankeyState) -> String {
    let mut hasher = sha2::Sha256::new();
    for node in &state.nodes {
      hasher.update(node.value.as_bytes());
    }

    format!("{:x}", hasher.finalize())
  }
}

//
// SankeyState
//

#[bd_macros::proto_serializable]
#[derive(Debug, Clone, PartialEq, Eq)]
struct SankeyNodeState {
  #[field(id = 1)]
  value: String,
  #[field(id = 2)]
  counts_toward_limit: bool,
}

#[bd_macros::proto_serializable]
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct SankeyState {
  #[field(id = 1)]
  nodes: Vec<SankeyNodeState>,
  #[field(id = 2)]
  is_trimmed: bool,
}

impl SankeyState {
  pub(crate) fn push(&mut self, value: String, limit: usize, counts_toward_limit: bool) {
    self.nodes.push(SankeyNodeState {
      value,
      counts_toward_limit,
    });

    if !counts_toward_limit {
      return;
    }

    if self.nodes.iter().filter(|n| n.counts_toward_limit).count() > limit
      && let Some((index, _)) = self
        .nodes
        .iter()
        .find_position(|node| node.counts_toward_limit)
    {
      self.is_trimmed = true;
      self.nodes.remove(index);
    }
  }
}

//
// Run
//

#[bd_macros::proto_serializable]
#[derive(Debug, Clone)]
pub(crate) struct Run {
  /// A list of active traversals for a given workflow run.
  /// A given run can have multiple traversals. That can happen
  /// if multiple transitions' rules are fulfilled for the same
  /// log or other event, i.e., an active state with two
  /// outgoing transitions matching on:
  ///  * message "foo"
  ///  * tag "key" equal to "value"
  ///
  /// sees a log with message "foo" and tag "key" equal to "value".
  #[field(id = 1)]
  pub(crate) traversals: Vec<Traversal>,
  /// The number of logs matched by a given workflow run.
  #[field(id = 2)]
  matched_logs_count: u32,
  /// The time at which run left its initial state. Used to implement
  /// duration limit.
  #[field(skip)]
  first_progress_occurred_at: Option<OffsetDateTime>,
}

impl Run {
  pub(crate) fn new(config: &Config, now: OffsetDateTime, initialize_timeout: bool) -> Self {
    let traversals = Traversal::new(
      config,
      0,
      TraversalExtractions::default(),
      now,
      initialize_timeout,
    )
    .map_or_else(Vec::new, |traversal| vec![traversal]);

    Self {
      traversals,
      matched_logs_count: 0,
      first_progress_occurred_at: None,
    }
  }

  #[cfg(test)]
  pub(crate) fn traversals(&self) -> &[Traversal] {
    &self.traversals
  }

  fn process_event<'a>(
    &mut self,
    config: &'a Config,
    event: WorkflowEvent<'_>,
    state_reader: &dyn bd_state::StateReader,
    now: OffsetDateTime,
  ) -> RunResult<'a> {
    // Optimize for the case when no traversal is advanced as it's
    // the most common situation.

    let mut run_triggered_actions = Vec::<TriggeredAction<'_>>::new();
    let mut run_logs_to_inject = TinyMap::<&'a str, Log>::default();
    let mut run_matched_logs_count = 0;
    let mut run_processed_timeout = false;
    let mut workflow_debug_state = Vec::new();

    // Process traversals in reversed order as we may end up modifying the array
    // starting at `index` in any given iteration of the loop.
    log::trace!(
      "processing {} traversal(s) for workflow {}",
      self.traversals.len(),
      config.inner().id()
    );
    for index in (0 .. self.traversals.len()).rev() {
      let Some(traversal) = self.traversals.get_mut(index) else {
        continue;
      };
      let mut traversal_result = traversal.process_event(config, event, state_reader, now);

      run_triggered_actions.append(&mut traversal_result.triggered_actions);
      run_logs_to_inject.append(&mut traversal_result.log_to_inject);
      workflow_debug_state.append(&mut traversal_result.workflow_debug_state);

      // Increase the counter of logs matched by a given workflow run.
      self.matched_logs_count += traversal_result.matched_logs_count;
      // Independently increment the match count for this specific run
      run_matched_logs_count += traversal_result.matched_logs_count;
      run_processed_timeout |= traversal_result.processed_timeout;

      // Check if we are over the limit of the logs that the workflow run is allowed to match.
      if let Some(matched_logs_count_limit) = config.inner().matched_logs_count_limit() {
        // A given workflow run has already matched more logs than its log counts limit allows for.
        // Mark it as stopped which will effectively get it removed.
        if self.matched_logs_count > matched_logs_count_limit {
          return RunResult {
            state: RunState::Stopped,
            triggered_actions: vec![],
            matched_logs_count: run_matched_logs_count,
            processed_timeout: run_processed_timeout,
            workflow_debug_state,
            logs_to_inject: TinyMap::default(),
          };
        }
      }

      if let Some(duration_limit) = config.inner().duration_limit()
        && let Some(first_progress_occurred_at) = self.first_progress_occurred_at
      {
        let current_time = event.occurred_at();

        let duration_since_first_progress = current_time - first_progress_occurred_at;
        if duration_since_first_progress > duration_limit {
          log::debug!(
            "run stopped due to exceeding duration limit ({duration_limit:?}), duration since the \
             run first made progress progress: {duration_since_first_progress:?}"
          );
          return RunResult {
            state: RunState::Stopped,
            triggered_actions: vec![],
            matched_logs_count: run_matched_logs_count,
            processed_timeout: run_processed_timeout,
            workflow_debug_state,
            logs_to_inject: TinyMap::default(),
          };
        }
      }

      // Update the value of `first_progress_occurred_at` if this is the first progress a run
      // has made.
      if self.first_progress_occurred_at.is_none() && traversal_result.did_make_progress() {
        self.first_progress_occurred_at = Some(event.occurred_at());
      }

      // Check if the traversal should advance.
      if traversal_result.followed_transitions_count == 0 {
        // Go to processing the next traversal.
        continue;
      }

      // Replace advanced traversals with their successors.
      // Each advanced traversal may have 0 or more successors.
      // Notes:
      //  * In great majority of cases `output_traversals` has 0 or 1 element. See notes above for
      //    more details.
      //  * if there are 0 successors of a given advanced traversal then the traversal arrived at
      //    one of workflow's final states and can be removed.
      //  * if after advancing traversals there are 0 traversals left then a given workflow run is
      //    finished and can be removed.
      self
        .traversals
        .splice(index ..= index, traversal_result.output_traversals);
    }

    let state = if self.traversals.is_empty() {
      RunState::Completed
    } else {
      RunState::Running
    };

    RunResult {
      state,
      triggered_actions: run_triggered_actions,
      matched_logs_count: run_matched_logs_count,
      processed_timeout: run_processed_timeout,
      logs_to_inject: run_logs_to_inject,
      workflow_debug_state,
    }
  }

  // Whether a given run is in its initial state meaning a state
  // that's equal to its state at a time of creation. While in theory the
  // implementation has to iterate over the list of all traversals to learn whether
  // in practice it's able to tell whether a workflow is an initial state
  // after checking at most two of its workflow traversals.
  // That's because:
  // * if there are no traversals then the run is in an initial state.
  // * if there is one traversal the implementation needs to call `Traversal::is_in_initial_state`
  //   to learn whether the run is in an initial state or not.
  // * if there is more than one traversal the first traversal in the list cannot be in an initial
  //   state hence the whole run cannot be in an initial state. Each run can have at most one
  //   traversal in initial state an initial state traversals are added to the end of `traversals`
  //   list of traversals.
  fn is_in_initial_state(&self) -> bool {
    // TODO(Augustyniak): Consider optimizing by storing `is_initial` value.
    debug_assert!(
      self
        .traversals
        .iter()
        .filter(|&traversal| Traversal::is_in_initial_state(traversal))
        .count()
        <= 1
    );
    self.traversals.iter().all(Traversal::is_in_initial_state)
  }
}

//
// RunState
//

/// The state of the workflow run.
#[derive(Debug)]
pub(crate) enum RunState {
  /// The run is still active and can continue to match incoming logs.
  Running,
  /// The run has completed and can be removed.
  /// All of its traversals arrived at a final state.
  Completed,
  /// The run exceeded one of the workflow limits and was stopped.
  /// It can be removed.
  Stopped,
}

//
// RunResult
//

/// The result of workflow run processing a log.
#[derive(Debug)]
pub(crate) struct RunResult<'a> {
  /// The state of the workflow run.
  state: RunState,
  /// The list of triggered actions.
  triggered_actions: Vec<TriggeredAction<'a>>,
  /// The number of matched logs. A single log can be matched multiple times
  matched_logs_count: u32,
  /// Whether the run created a timeout or a timeout expired.
  processed_timeout: bool,
  /// Logs to be injected back into the workflow engine after field attachment and other
  /// processing.
  logs_to_inject: TinyMap<&'a str, Log>,
  /// Any debug state changes that occurred as a result of processing the traversal.
  workflow_debug_state: Vec<WorkflowDebugStateKey>,
}

impl RunResult<'_> {
  /// Whether run made any progress.
  const fn did_make_progress(&self) -> bool {
    self.matched_logs_count > 0
  }
}

//
// TraversalExtractions
//

#[bd_macros::proto_serializable]
#[derive(Debug, Clone, Default)]
pub(crate) struct TraversalExtractions {
  /// States of Sankey diagrams. It's a `None` when traversal is initialized and is set
  /// to `Some` after the first value for a Sankey and a given traversal is extracted.
  #[field(id = 1)]
  pub(crate) sankey_states: TinyMap<String, SankeyState>,
  /// Snapped timestamps, by extraction ID.
  #[field(id = 2)]
  pub(crate) timestamps: TinyMap<String, TimestampMicros>,
  /// Snapped field values, by extraction ID.
  #[field(id = 3)]
  pub(crate) fields: TinyMap<String, String>,
}

//
// Traversal
//

/// Helper function to process a transition and create the next traversal.
/// Used by both log matching and state change matching.
fn process_transition<'a>(
  result: &mut TraversalResult<'a>,
  mut extractions: TraversalExtractions,
  actions: &'a [Action],
  fields: FieldsRef<'_>,
  current_state_index: usize,
  next_state_index: usize,
  transition_type: WorkflowDebugTransitionType,
  config: &Config,
  now: OffsetDateTime,
) {
  result.followed_transitions_count += 1;

  // Collect triggered actions and injected logs.
  let (triggered_actions, logs_to_inject) =
    Traversal::triggered_actions(actions, &mut extractions, fields);

  result.triggered_actions.extend(triggered_actions);
  result.log_to_inject.extend(logs_to_inject);

  // Create next traversal. Subsequent traversals always have their timeout initialized if there
  // is one.
  if let Some(traversal) = Traversal::new(config, next_state_index, extractions, now, true) {
    if traversal.timeout_unix_ms.is_some() {
      result.processed_timeout = true;
    }
    result.output_traversals.push(traversal);
  }

  if let Some(state) = config.inner().states().get(current_state_index) {
    result
      .workflow_debug_state
      .push(WorkflowDebugStateKey::new_state_transition(
        state.id().to_string(),
        transition_type,
      ));
  }
}

/// A traversal points at a specific workflow state. Most workflow runs
/// have one traversal but it's possible for a workflow run to have multiple
/// traversals. That can happen when a given workflow traversal is forked when
/// multiple transitions outgoing from a given state match on the
/// same event (i.e., log).
#[bd_macros::proto_serializable]
#[derive(Debug, Clone)]
pub(crate) struct Traversal {
  /// The index of a state the traversal is currently at.
  #[field(id = 1, serialize_as = "u64")]
  pub(crate) state_index: usize,
  /// The number of logs matched by traversal's transitions.
  /// Each element in an array corresponds to one transition.
  #[field(id = 2)]
  pub(crate) matched_logs_counts: Vec<u32>,
  /// Extractions folded across all traversals in a path.
  #[field(id = 3)]
  pub(crate) extractions: TraversalExtractions,
  /// The unix timestamp in milliseconds of when the optional state timeout expires.
  #[field(id = 4)]
  timeout_unix_ms: Option<i64>,
}

impl Traversal {
  /// Creates a new traversal for a given state. The method returns `None`
  /// for cases when a given state is a final state and doesn't have
  /// any outgoing transitions.
  pub fn new(
    config: &Config,
    state_index: usize,
    extractions: TraversalExtractions,
    now: OffsetDateTime,
    initialize_timeout: bool,
  ) -> Option<Self> {
    let state = &config.inner().states().get(state_index)?;
    if state.transitions().is_empty() && state.timeout().is_none() {
      None
    } else {
      let mut traversal = Self {
        state_index,
        // The number of logs matched by a given traversal.
        // Start at 0 for a new traversal.
        matched_logs_counts: vec![0; state.transitions().len()],
        extractions,
        timeout_unix_ms: None,
      };

      if initialize_timeout {
        traversal.initialize_timeout(config, now);
      }

      Some(traversal)
    }
  }

  fn initialize_timeout(&mut self, config: &Config, now: OffsetDateTime) {
    let Some(state) = &config.inner().states().get(self.state_index) else {
      return;
    };
    self.timeout_unix_ms = state.timeout().map(|timeout| {
      log::trace!(
        "setting timeout for traversal at state {} to {}",
        state.id(),
        timeout.duration
      );
      now.unix_timestamp_ms()
        + i64::try_from(timeout.duration.whole_milliseconds()).unwrap_or_default()
    });
  }

  fn process_event<'a>(
    &mut self,
    config: &'a Config,
    event: WorkflowEvent<'_>,
    state_reader: &dyn bd_state::StateReader,
    now: OffsetDateTime,
  ) -> TraversalResult<'a> {
    let transitions = config
      .inner()
      .transitions_for_traversal(self)
      .unwrap_or_default();

    let mut result = TraversalResult::default();
    // In majority of cases each traversal has 0 or 1 successor. A case when
    // more than 1 successor is created is possible if a state corresponding to
    // currently processed traversal has multiple outgoing transitions and a
    // processed log ends up fulfills conditions for more than 1 of these transitions.
    log::trace!(
      "processing {} transition(s) for workflow {}/{}",
      transitions.len(),
      config.inner().id(),
      config
        .inner()
        .states()
        .get(self.state_index)
        .map(super::config::State::id)
        .unwrap_or_default()
    );

    // Try to match explicit transitions (logs or state changes).
    // Multiple transitions can match the same event, causing workflow forking.
    for (index, transition) in transitions.iter().enumerate() {
      match (&transition.rule(), event) {
        (
          Predicate::LogMatch {
            matcher,
            required_matches,
          },
          WorkflowEvent::Log(log),
        ) => {
          self.process_log_match(
            config,
            log,
            matcher,
            *required_matches,
            index,
            state_reader,
            now,
            &mut result,
          );
        },
        (
          Predicate::StateChangeMatch {
            state_change_match,
            extra_matcher,
          },
          WorkflowEvent::StateChange(state_change, fields),
        ) => {
          self.process_state_change_match(
            config,
            state_change,
            state_change_match,
            extra_matcher.as_ref(),
            fields,
            index,
            state_reader,
            now,
            &mut result,
          );
        },
        _ => { /* No match, continue to next transition */ },
      }
    }

    // Timeout handling: Check timeouts for both logs and state changes.
    // Timeout transitions use default extractions and don't extract from the triggering event,
    // so this works correctly for both event types.
    if result.output_traversals.is_empty()
      && let Some(timeout_unix_ms) = self.timeout_unix_ms
      && now.unix_timestamp_ms() >= timeout_unix_ms
      && let Some(actions) = config.inner().actions_for_timeout(self.state_index)
      && let Some(next_state_index) = config
        .inner()
        .next_state_index_for_timeout(self.state_index)
    {
      // Timeout transitions use default extractions and pass appropriate fields based on event
      // type. For logs, we pass the log fields. For state changes, we pass the fields that were
      // collected (typically global metadata).
      let fields = match event {
        WorkflowEvent::Log(log) => FieldsRef::new(&log.fields, &log.matching_fields),
        WorkflowEvent::StateChange(_, fields) => fields,
      };

      process_transition(
        &mut result,
        TraversalExtractions::default(),
        actions,
        fields,
        self.state_index,
        next_state_index,
        WorkflowDebugTransitionType::Timeout,
        config,
        now,
      );
      result.processed_timeout = true;

      log::trace!(
        "traversal timed out and is advancing, workflow id={:?}",
        config.inner().id(),
      );
    }

    result
  }

  /// Processes a log match for a single transition.
  /// Increments match counters and triggers the transition if the count threshold is reached.
  fn process_log_match<'a>(
    &mut self,
    config: &'a Config,
    log: &Log,
    log_match: &bd_log_matcher::matcher::Tree,
    count: u32,
    index: usize,
    state_reader: &dyn bd_state::StateReader,
    now: OffsetDateTime,
    result: &mut TraversalResult<'a>,
  ) {
    if !log_match.do_match(
      log.log_level,
      log.log_type,
      &log.message,
      FieldsRef::new(&log.fields, &log.matching_fields),
      state_reader,
      &self.extractions.fields,
    ) {
      return;
    }

    let Some(matched_logs_counts) = self.matched_logs_counts.get_mut(index) else {
      return;
    };
    *matched_logs_counts += 1;
    let matched_logs_counts = *matched_logs_counts;

    // We do mark the log being matched on the `Run` level even if
    // the transition doesn't end up happening for when
    // self.matched_logs_count < count.
    result.matched_logs_count += 1;

    if matched_logs_counts == count
      && let Some(actions) = config.inner().actions_for_traversal(self, index)
      && let Some(next_state_index) = config.inner().next_state_index_for_traversal(self, index)
    {
      process_transition(
        result,
        self.do_log_extractions(config, index, log, state_reader),
        actions,
        FieldsRef::new(&log.fields, &log.matching_fields),
        self.state_index,
        next_state_index,
        WorkflowDebugTransitionType::Normal(index as u64),
        config,
        now,
      );

      log::trace!(
        "traversal's transition {} matched log ({} matches in total) and is advancing, workflow \
         id={:?}",
        index,
        matched_logs_counts,
        config.inner().id(),
      );
    } else {
      log::trace!(
        "traversal's transition {} matched log ({} matches in total) but needs {} matches to \
         advance, workflow id={:?}",
        index,
        matched_logs_counts,
        count,
        config.inner().id(),
      );
    }
  }

  /// Processes a state change match for a single transition.
  /// Checks scope, key, and value matchers, then triggers the transition if all match.
  fn process_state_change_match<'a>(
    &self,
    config: &'a Config,
    state_change: &bd_state::StateChange,
    state_change_match: &crate::config::StateChangeMatch,
    extra_matcher: Option<&bd_log_matcher::matcher::Tree>,
    fields: FieldsRef<'_>,
    index: usize,
    state_reader: &dyn bd_state::StateReader,
    now: OffsetDateTime,
    result: &mut TraversalResult<'a>,
  ) {
    use bd_state::Value_type;

    if state_change.scope != state_change_match.scope || state_change.key != state_change_match.key
    {
      return;
    }

    // Extract previous and new values based on change type
    let (previous_value, new_value) = match &state_change.change_type {
      bd_state::StateChangeType::Inserted { value } => (
        None,
        match value.value_type {
          Some(Value_type::StringValue(ref s)) => Some(Cow::Borrowed(s.as_str())),
          Some(Value_type::IntValue(i)) => Some(Cow::Owned(i.to_string())),
          Some(Value_type::DoubleValue(d)) => Some(Cow::Owned(d.to_string())),
          Some(Value_type::BoolValue(true)) => Some(Cow::Borrowed("true")),
          Some(Value_type::BoolValue(false)) => Some(Cow::Borrowed("false")),
          None => None,
        },
      ),
      bd_state::StateChangeType::Updated {
        old_value,
        new_value,
      } => {
        let old = match old_value.value_type {
          Some(Value_type::StringValue(ref s)) => Some(Cow::Borrowed(s.as_str())),
          Some(Value_type::IntValue(i)) => Some(Cow::Owned(i.to_string())),
          Some(Value_type::DoubleValue(d)) => Some(Cow::Owned(d.to_string())),
          Some(Value_type::BoolValue(true)) => Some(Cow::Borrowed("true")),
          Some(Value_type::BoolValue(false)) => Some(Cow::Borrowed("false")),
          None => None,
        };
        let new = match new_value.value_type {
          Some(Value_type::StringValue(ref s)) => Some(Cow::Borrowed(s.as_str())),
          Some(Value_type::IntValue(i)) => Some(Cow::Owned(i.to_string())),
          Some(Value_type::DoubleValue(d)) => Some(Cow::Owned(d.to_string())),
          Some(Value_type::BoolValue(true)) => Some(Cow::Borrowed("true")),
          Some(Value_type::BoolValue(false)) => Some(Cow::Borrowed("false")),
          None => None,
        };
        (old, new)
      },
      bd_state::StateChangeType::Removed { old_value } => (
        match old_value.value_type {
          Some(Value_type::StringValue(ref s)) => Some(Cow::Borrowed(s.as_str())),
          Some(Value_type::IntValue(i)) => Some(Cow::Owned(i.to_string())),
          Some(Value_type::DoubleValue(d)) => Some(Cow::Owned(d.to_string())),
          Some(Value_type::BoolValue(true)) => Some(Cow::Borrowed("true")),
          Some(Value_type::BoolValue(false)) => Some(Cow::Borrowed("false")),
          None => None,
        },
        None,
      ),
      bd_state::StateChangeType::NoChange => return,
    };

    // Check if previous value matches (if matcher specified)
    let empty_fields = bd_log_primitives::tiny_set::TinyMap::default();
    let previous_matches = state_change_match
      .previous_value
      .as_ref()
      .is_none_or(|matcher| matcher.matches(previous_value.as_deref(), &empty_fields));

    if !previous_matches {
      return;
    }

    // Check if new value matches
    if !state_change_match
      .new_value
      .matches(new_value.as_deref(), &empty_fields)
    {
      return;
    }

    // Check extra matcher. Now we can match against global metadata fields that were collected
    // for this state change event.
    if let Some(extra_matcher) = extra_matcher.as_ref()
      && !extra_matcher.do_match(
        log_level::DEBUG,
        LogType::INTERNAL_SDK,
        &"".into(),
        fields,
        state_reader,
        &self.extractions.fields,
      )
    {
      return;
    }

    if let Some(actions) = config.inner().actions_for_traversal(self, index)
      && let Some(next_state_index) = config.inner().next_state_index_for_traversal(self, index)
    {
      // Use the collected fields (typically global metadata) for state changes
      process_transition(
        result,
        self.do_state_change_extractions(config, index, state_change, fields, state_reader),
        actions,
        fields,
        self.state_index,
        next_state_index,
        WorkflowDebugTransitionType::Normal(index as u64),
        config,
        now,
      );

      log::trace!(
        "traversal's transition {} matched state change and is advancing, workflow id={:?}",
        index,
        config.inner().id(),
      );
    }
  }

  /// Performs extractions for a log event.
  /// We're able to extract sankey values, timestamps, and field values from the log in addition to
  /// the shared state reader.
  fn do_log_extractions(
    &self,
    config: &Config,
    index: usize,
    log: &Log,
    state_reader: &dyn bd_state::StateReader,
  ) -> TraversalExtractions {
    // TODO(mattklein123): In the common case without forking we should be able to move this data
    // and not clone it. It will require some thinking on how to do this given the loops involved.
    // Maybe some CoW thing.
    let mut new_extractions = self.extractions.clone();

    let Some(extractions) = config.inner().extractions(self, index) else {
      return new_extractions;
    };
    for extraction in &extractions.sankey_extractions {
      let Some(extracted_value) = extraction.value.extract_value(
        FieldsRef::new(&log.fields, &log.matching_fields),
        &log.message,
        state_reader,
      ) else {
        continue;
      };

      new_extractions
        .sankey_states
        .get_mut_or_insert_default(extraction.sankey_id.clone())
        .push(
          extracted_value.into_owned(),
          extraction.limit,
          extraction.counts_toward_sankey_values_extraction_limit,
        );
    }

    if let Some(timestamp_extraction_id) = &extractions.timestamp_extraction_id {
      let timestamp = log.occurred_at;
      log::debug!("extracted timestamp {timestamp} for extraction ID {timestamp_extraction_id}");
      new_extractions
        .timestamps
        .insert(timestamp_extraction_id.clone(), TimestampMicros(timestamp));
    }

    for extraction in &extractions.field_extractions {
      if let Some(value) = log.field_value(&extraction.field_name) {
        log::debug!(
          "extracted field value {} for extraction ID {}",
          value,
          extraction.id
        );
        new_extractions
          .fields
          .insert(extraction.id.clone(), value.to_string());
      }
    }

    new_extractions
  }

  /// Performs extractions for a state change event.
  /// State changes can extract timestamps and values from the state reader.
  /// Uses empty fields since state changes don't have log fields or messages.
  fn do_state_change_extractions(
    &self,
    config: &Config,
    index: usize,
    state_change: &bd_state::StateChange,
    fields: FieldsRef<'_>,
    state_reader: &dyn bd_state::StateReader,
  ) -> TraversalExtractions {
    let mut new_extractions = self.extractions.clone();

    let Some(extractions) = config.inner().extractions(self, index) else {
      return new_extractions;
    };

    let empty_messsage = "".into();

    // Sankey extractions: Can extract from state reader (e.g., feature flags) and from fields
    for extraction in &extractions.sankey_extractions {
      let Some(extracted_value) =
        extraction
          .value
          .extract_value(fields, &empty_messsage, state_reader)
      else {
        continue;
      };

      new_extractions
        .sankey_states
        .get_mut_or_insert_default(extraction.sankey_id.clone())
        .push(
          extracted_value.into_owned(),
          extraction.limit,
          extraction.counts_toward_sankey_values_extraction_limit,
        );
    }

    // Timestamp extraction: Use state change timestamp
    if let Some(timestamp_extraction_id) = &extractions.timestamp_extraction_id {
      let timestamp = state_change.timestamp;
      log::debug!(
        "extracted timestamp {timestamp} from state change for extraction ID \
         {timestamp_extraction_id}"
      );
      new_extractions
        .timestamps
        .insert(timestamp_extraction_id.clone(), TimestampMicros(timestamp));
    }

    // Field extractions: Can extract from fields (typically global metadata like device_id,
    // app_version, etc.)
    for extraction in &extractions.field_extractions {
      if let Some(value) = fields.field_value(&extraction.field_name) {
        log::debug!(
          "extracted field value {} from state change for extraction ID {}",
          value,
          extraction.id
        );
        new_extractions
          .fields
          .insert(extraction.id.clone(), value.to_string());
      }
    }

    new_extractions
  }

  fn triggered_actions<'a>(
    actions: &'a [Action],
    extractions: &mut TraversalExtractions,
    current_log_fields: FieldsRef<'_>,
  ) -> (Vec<TriggeredAction<'a>>, TinyMap<&'a str, Log>) {
    let mut triggered_actions = vec![];
    let mut logs_to_inject = TinyMap::default();
    for action in actions {
      match action {
        Action::FlushBuffers(action) => {
          triggered_actions.push(TriggeredAction::FlushBuffers(action));
        },
        Action::EmitMetric(action) => {
          triggered_actions.push(TriggeredAction::EmitMetric(action));
        },
        Action::EmitSankey(action) => {
          let Some(sankey_state) = extractions.sankey_states.remove(action.id()) else {
            debug_assert!(
              false,
              "sankey_states for Sankey with {:?} ID should be present",
              action.id()
            );
            continue;
          };

          triggered_actions.push(TriggeredAction::SankeyDiagram(TriggeredActionEmitSankey {
            action,
            path: SankeyPath::new(action.id(), sankey_state),
          }));
        },
        Action::TakeScreenshot => {
          triggered_actions.push(TriggeredAction::TakeScreenshot);
        },
        Action::GenerateLog(action) => {
          if let Some(log) = generate_log_action(extractions, action, current_log_fields) {
            logs_to_inject.insert(action.id.as_str(), log);
          }
        },
      }
    }
    (triggered_actions, logs_to_inject)
  }

  // Whether a given traversal is an initial state.
  fn is_in_initial_state(&self) -> bool {
    self.state_index == 0
      && self.matched_logs_counts.iter().all(|&e| e == 0)
      && self.timeout_unix_ms.is_none()
  }
}

//
// TriggeredAction
//

#[derive(Clone, Debug, PartialEq)]
/// The action to perform.
pub(crate) enum TriggeredAction<'a> {
  FlushBuffers(&'a ActionFlushBuffers),
  EmitMetric(&'a ActionEmitMetric),
  SankeyDiagram(TriggeredActionEmitSankey<'a>),
  TakeScreenshot,
}

//
// TriggeredActionEmitSankey
//

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TriggeredActionEmitSankey<'a> {
  pub(crate) action: &'a ActionEmitSankey,
  pub(crate) path: SankeyPath,
}

//
// TraversalResult
//

/// The traversal result.
#[derive(Debug, Default)]
struct TraversalResult<'a> {
  /// The indices of transitions that should be advanced.
  output_traversals: Vec<Traversal>,
  /// The list of triggered actions.
  triggered_actions: Vec<TriggeredAction<'a>>,
  /// The number of matched logs. A single log can be matched multiple times
  /// i.e., when multiple transitions coming out of a given state all match the
  /// same log.
  matched_logs_count: u32,
  /// Whether a timeout was created or expired as a result of processing a log.
  processed_timeout: bool,
  /// The number of transitions that were followed in response to processing a log. This can
  /// include a timeout.
  followed_transitions_count: u32,
  /// Logs to be injected back into the workflow engine after field attachment and other
  /// processing.
  log_to_inject: TinyMap<&'a str, Log>,
  /// Any debug state changes that occurred as a result of processing the traversal.
  workflow_debug_state: Vec<WorkflowDebugStateKey>,
}

impl TraversalResult<'_> {
  /// Whether traversal made any progress.
  fn did_make_progress(&self) -> bool {
    self.followed_transitions_count > 0
  }
}

//
// WorkflowTransitionDebugState
//

#[bd_macros::proto_serializable]
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct WorkflowTransitionDebugState {
  #[field(id = 1)]
  pub count: u64,
  #[field(id = 2)]
  #[field(default = "TimestampMicros::from(OffsetDateTime::UNIX_EPOCH)")]
  pub last_transition_time: TimestampMicros,
}

impl Default for WorkflowTransitionDebugState {
  fn default() -> Self {
    Self {
      count: 0,
      last_transition_time: OffsetDateTime::UNIX_EPOCH.into(),
    }
  }
}

//
// WorkflowDebugStateMap
//

// Note: Using HashMap directly instead of Box<HashMap> to support protobuf serialization.
// The memory overhead of an empty HashMap is acceptable given debug mode is rarely enabled.
#[bd_macros::proto_serializable]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WorkflowDebugStateMap {
  #[field(id = 1)]
  pub(crate) inner: HashMap<WorkflowDebugStateKey, WorkflowTransitionDebugState>,
}
pub type OptWorkflowDebugStateMap = Option<WorkflowDebugStateMap>;

impl WorkflowDebugStateMap {
  #[must_use]
  pub fn into_inner(self) -> HashMap<WorkflowDebugStateKey, WorkflowTransitionDebugState> {
    self.inner
  }
}

impl WorkflowDebugStateMap {
  fn merge(&mut self, other: &[WorkflowDebugStateKey], now: OffsetDateTime) {
    for key in other {
      self
        .inner
        .entry(key.clone())
        .and_modify(|state| {
          state.count += 1;
          state.last_transition_time = now.into();
        })
        .or_insert_with(|| WorkflowTransitionDebugState {
          count: 1,
          last_transition_time: now.into(),
        });
    }
  }
}
