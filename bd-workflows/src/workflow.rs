// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./workflow_test.rs"]
mod workflow_test;

use crate::config::{
  Action,
  ActionEmitMetric,
  ActionEmitSankey,
  ActionFlushBuffers,
  ActionTakeScreenshot,
  Config,
  Predicate,
};
use crate::generate_log::generate_log_action;
use bd_log_primitives::{FieldsRef, Log, LogRef};
use bd_matcher::FieldProvider;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::collections::BTreeMap;
use std::time::SystemTime;
use time::OffsetDateTime;

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
#[derive(Debug, Serialize, Deserialize)]
pub struct Workflow {
  id: String,
  // The list of runs. Runs are organized in the following way:
  //  * For exclusive workflows, new runs are added to the beginning of the list and the maximum
  //    number of runs is equal to two. If runs list is non-empty then the first run in the list is
  //    guaranteed to be in an initial state. If there are two runs then the second run is
  //    guaranteed not to be in an initial state.
  runs: Vec<Run>,
}

impl Workflow {
  pub(crate) const fn new(config_id: String) -> Self {
    Self {
      id: config_id,
      runs: vec![],
    }
  }

  pub(crate) const fn new_with_runs(config_id: String, runs: Vec<Run>) -> Self {
    Self {
      id: config_id,
      runs,
    }
  }

  #[must_use]
  pub(crate) fn runs(&self) -> &[Run] {
    &self.runs
  }

  pub(crate) fn id(&self) -> &str {
    &self.id
  }

  pub(crate) fn process_log<'a>(
    &mut self,
    config: &'a Config,
    log: &LogRef<'_>,
    current_traversals_count: &mut u32,
    traversals_count_limit: u32,
  ) -> WorkflowResult<'a> {
    let mut result = WorkflowResult::default();

    if self.needs_new_run() {
      let run = Run::new(config);
      if self.maybe_add_run(
        run,
        current_traversals_count,
        traversals_count_limit,
        &mut result,
      ) {
        log::trace!("added a new run for workflow {}", self.id);
      }
    }

    let mut did_make_progress = false;
    // Process runs in reversed order as we may end up modifying the array
    // starting at `index` in any given iteration of the loop.
    for index in (0 .. self.runs.len()).rev() {
      let run = &mut self.runs[index];
      let mut run_result = run.process_log(config, log);

      result.incorporate_run_result(&mut run_result);

      *current_traversals_count += run_result.created_traversals_count;
      // TODO(Augustyniak): a 'standard' subtracting operation should be sufficient here for
      // production code but it's no enough for tests due to the way they track
      // `current_traversals_count` (in tests, the processing of any given always starts with
      // `current_traversals_count == 0` even if it was != 0 as the result of processing
      // previous logs). Rework tests to make it possible to replace `saturating_sub` with `-`
      // operator in here.
      *current_traversals_count =
        current_traversals_count.saturating_sub(run_result.completed_traversals_count);

      let is_over_traversals_count_limit = *current_traversals_count > traversals_count_limit;
      let run_result_did_make_progress = run_result.did_make_progress();

      match (run_result.state, is_over_traversals_count_limit) {
        // If after processing a given log by a workflow run we notice that we end up
        // over the limit of allowed traversals we remove the `run` which caused the
        // overflow.
        (RunState::Stopped, _) | (_, true) => {
          result.stats.stopped_runs_count += 1;

          if is_over_traversals_count_limit {
            result.stats.traversals_count_limit_hit_count += 1;
            log::debug!(
              "traversals count ({}) is over the limit ({}); stopping run",
              *current_traversals_count,
              traversals_count_limit
            );
          }

          let stopped_traversals_count = run.traversals_count();
          result.stats.stopped_traversals_count += stopped_traversals_count;
          *current_traversals_count =
            current_traversals_count.saturating_sub(stopped_traversals_count);

          self.runs.remove(index);
        },
        (RunState::Completed, _) => {
          debug_assert!(
            self.runs[index].traversals.is_empty(),
            "completing a run with active traversals"
          );

          did_make_progress = true;

          log::trace!("completed run, workflow id={:?}", self.id);

          result.stats.advanced_runs_count += 1;
          result.stats.completed_runs_count += 1;

          self.runs.remove(index);
        },
        (RunState::Running, _) => {
          if run_result_did_make_progress {
            did_make_progress = true;
          }

          // The run is still running.
          if run_result.advanced_traversals_count > 0 {
            // Since at least one traversal was advanced we
            // consider the enclosing run to be advanced too.
            result.stats.advanced_runs_count += 1;
          }
        },
      }

      // Processing exclusive workflows runs looks as follows:
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
      let is_initial_run = index == 0;
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
        let run = self.runs.remove(index + 1);

        let removed_traversals_count = run.traversals_count();
        result.stats.stopped_runs_count += 1;
        result.stats.stopped_traversals_count += removed_traversals_count;
        *current_traversals_count =
          current_traversals_count.saturating_sub(removed_traversals_count);

        result.stats.reset_exclusive_workflows_count += 1;
      } else {
        // The active state run made progress and the next run to be processed (if there is any)
        // is an initial state run that we do not want to expose to the log.
        log::trace!("active state run made progress");
        break;
      }
    }

    result
  }

  fn maybe_add_run(
    &mut self,
    run: Run,
    current_traversals_count: &mut u32,
    traversals_count_limit: u32,
    result: &mut WorkflowResult<'_>,
  ) -> bool {
    if run.traversals_count() + *current_traversals_count <= traversals_count_limit {
      log::trace!("workflow={}: creating a new run", self.id);

      result.stats.created_runs_count += 1;
      result.stats.created_traversals_count += run.traversals_count();

      *current_traversals_count += run.traversals_count();
      self.runs.insert(0, run);

      true
    } else {
      result.stats.traversals_count_limit_hit_count += 1;
      log::debug!(
        "workflow={}: traversals count ({}) is over the limit ({}); preventing a new run from \
         being added",
        self.id,
        *current_traversals_count,
        traversals_count_limit
      );

      false
    }
  }

  pub(crate) fn traversals_count(&self) -> u32 {
    self.runs.iter().map(Run::traversals_count).sum()
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
      .filter(|&run| (!run.is_in_initial_state()))
      .cloned()
      .collect_vec();

    Self::new_with_runs(self.id().to_string(), runs)
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

  pub(crate) fn remove_run(&mut self, index: usize) {
    self.runs.remove(index);
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
        self.runs[run_index]
          .traversals
          .iter()
          .map(|traversal| config.states()[traversal.state_index].id().to_string())
      })
      .collect()
  }

  /// Returns the list of states of given run's traversals.
  #[cfg(test)]
  pub(crate) fn traversals_states(&self, config: &Config, run_index: usize) -> Vec<String> {
    self.runs[run_index]
      .traversals
      .iter()
      .map(|traversal| config.states()[traversal.state_index].id().to_string())
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
  logs_to_inject: BTreeMap<&'a str, Log>,
  stats: WorkflowResultStats,
}

impl<'a> WorkflowResult<'a> {
  pub fn into_parts(self) -> (Vec<TriggeredAction<'a>>, BTreeMap<&'a str, Log>) {
    (self.triggered_actions, self.logs_to_inject)
  }

  pub const fn stats(&self) -> &WorkflowResultStats {
    &self.stats
  }

  fn incorporate_run_result(&mut self, run_result: &mut RunResult<'a>) {
    self
      .triggered_actions
      .append(&mut run_result.triggered_actions);
    self.logs_to_inject.append(&mut run_result.logs_to_inject);
    self.stats.created_traversals_count += run_result.created_traversals_count;
    self.stats.advanced_traversals_count += run_result.advanced_traversals_count;
    self.stats.completed_traversals_count += run_result.completed_traversals_count;
    self.stats.matched_logs_count += run_result.matched_logs_count;
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
  pub(crate) created_runs_count: u32,
  pub(crate) advanced_runs_count: u32,
  pub(crate) stopped_runs_count: u32,
  pub(crate) completed_runs_count: u32,

  pub(crate) created_traversals_count: u32,
  pub(crate) advanced_traversals_count: u32,
  pub(crate) stopped_traversals_count: u32,
  pub(crate) completed_traversals_count: u32,

  /// The number of times the engine prevented a run and/or traversal from being
  /// created due to a configured traversals count limit.
  pub(crate) traversals_count_limit_hit_count: u32,

  pub(crate) reset_exclusive_workflows_count: u32,

  pub(crate) matched_logs_count: u32,
}

impl WorkflowResultStats {
  pub(crate) const fn did_make_progress(&self) -> bool {
    self.created_runs_count > 0
      || self.advanced_runs_count > 0
      || self.stopped_runs_count > 0
      || self.completed_runs_count > 0
      || self.created_traversals_count > 0
      || self.advanced_traversals_count > 0
      || self.stopped_traversals_count > 0
      || self.completed_traversals_count > 0
      || self.matched_logs_count > 0
      || self.reset_exclusive_workflows_count > 0
  }
}

//
// SankeyPath
//

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SankeyPath {
  pub(crate) sankey_id: String,
  pub(crate) nodes: Vec<String>,
  pub(crate) path_id: String,
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

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
struct SankeyNodeState {
  value: String,
  counts_toward_limit: bool,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub(crate) struct SankeyState {
  nodes: Vec<SankeyNodeState>,
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

    if self.nodes.iter().filter(|n| n.counts_toward_limit).count() > limit {
      if let Some((index, _)) = self
        .nodes
        .iter()
        .find_position(|node| node.counts_toward_limit)
      {
        self.is_trimmed = true;
        self.nodes.remove(index);
      }
    }
  }
}

//
// Run
//

#[derive(Debug, Serialize, Deserialize, Clone)]
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
  traversals: Vec<Traversal>,
  /// The number of logs matched by a given workflow run.
  matched_logs_count: u32,
  /// The time at which run left its initial state. Used to implement
  /// duration limit.
  first_progress_occurred_at: Option<SystemTime>,
}

impl Run {
  pub(crate) fn new(config: &Config) -> Self {
    let traversals = Traversal::new(config, 0, TraversalExtractions::default())
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

  fn process_log<'a>(&mut self, config: &'a Config, log: &LogRef<'_>) -> RunResult<'a> {
    // Optimize for the case when no traversal is advanced as it's
    // the most common situation.

    let mut run_triggered_actions = Vec::<TriggeredAction<'_>>::new();
    let mut run_logs_to_inject = BTreeMap::<&'a str, Log>::new();

    let mut created_traversals_count = 0;
    let mut advanced_traversals_count = 0;
    let mut completed_traversals_count = 0;
    let mut run_matched_logs_count = 0;

    // Process traversals in reversed order as we may end up modifying the array
    // starting at `index` in any given iteration of the loop.
    log::trace!(
      "processing {} traversals for workflow {}",
      self.traversals.len(),
      config.id()
    );
    for index in (0 .. self.traversals.len()).rev() {
      let traversal = &mut self.traversals[index];
      let mut traversal_result = traversal.process_log(config, log);

      run_triggered_actions.append(&mut traversal_result.triggered_actions);
      run_logs_to_inject.append(&mut traversal_result.log_to_inject);

      // Increase the counter of logs matched by a given workflow run.
      self.matched_logs_count += traversal_result.matched_logs_count;
      // Independently increment the match count for this specific run
      run_matched_logs_count += traversal_result.matched_logs_count;

      // Check if we are over the limit of the logs that the workflow run is allowed to match.
      if let Some(matched_logs_count_limit) = config.matched_logs_count_limit() {
        // A given workflow run has already matched more logs than its log counts limit allows for.
        // Mark it as stopped which will effectively get it removed.
        if self.matched_logs_count > matched_logs_count_limit {
          return RunResult {
            state: RunState::Stopped,
            triggered_actions: vec![],
            created_traversals_count: 0,
            advanced_traversals_count: 0,
            completed_traversals_count: 0,
            matched_logs_count: run_matched_logs_count,
            logs_to_inject: BTreeMap::new(),
          };
        }
      }

      if let Some(duration_limit) = config.duration_limit() {
        if let Some(first_progress_occurred_at) = self.first_progress_occurred_at {
          let current_time: SystemTime = log.occurred_at.into();

          match current_time.duration_since(first_progress_occurred_at) {
            Ok(duration_since_first_progress) => {
              if duration_since_first_progress > duration_limit {
                log::debug!(
                  "run stopped due to exceeding duration limit ({duration_limit:?}), duration \
                   since the run first made progress progress: {duration_since_first_progress:?}"
                );
                return RunResult {
                  state: RunState::Stopped,
                  triggered_actions: vec![],
                  created_traversals_count: 0,
                  advanced_traversals_count: 0,
                  completed_traversals_count: 0,
                  matched_logs_count: run_matched_logs_count,
                  logs_to_inject: BTreeMap::new(),
                };
              }
            },
            // `duration_since` fails if `earlier` time (passed as an argument) is after
            // `current_time`. This can happen as time instances processed in here come from
            // `TimeProvider` registered by SDK customer and nothing prevents these
            // providers from returning decreasing times.
            Err(e) => log::debug!(
              "failed to calculate time difference between current time {current_time:?} and \
               first progress occurred at time {first_progress_occurred_at:?}: {e}",
            ),
          }
        }
      }

      // Update the value of `first_progress_occurred_at` if this is the first progress a run
      // has made.
      if self.first_progress_occurred_at.is_none() && traversal_result.did_make_progress() {
        self.first_progress_occurred_at = Some(log.occurred_at.into());
      }

      // Check if the traversal should advance.
      if traversal_result.followed_transitions_count == 0 {
        // Go to processing the next traversal.
        continue;
      }

      // The currently processed traversal is about to advance.
      advanced_traversals_count += 1;

      if traversal_result.output_traversals.is_empty() {
        // Traversal has no successors so it's been completed.
        completed_traversals_count += 1;
      } else {
        #[allow(clippy::cast_possible_truncation)]
        let output_traversals_count = traversal_result.output_traversals.len() as u32;
        // The number of created traversals is the number of output traversals
        // minus the input traversal.
        created_traversals_count += output_traversals_count - 1;
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
      created_traversals_count,
      advanced_traversals_count,
      completed_traversals_count,
      matched_logs_count: run_matched_logs_count,
      logs_to_inject: run_logs_to_inject,
    }
  }

  // Whether a given run is in its initial state meaning a state
  // that's equal to its state at a time of creation. While in theory the
  // implementation has to iterate over the list of all traversals to learn whether
  // in practice it's able to tell whether a workflow is an initial state
  // after checking at most two of its workflow traversals.
  // That's because:
  // * if there is no traversals then run is in an initial state.
  // * if there is one traversals the implementation needs to call `Traversal::is_in_initial_state`
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

  pub(crate) fn traversals_count(&self) -> u32 {
    #[allow(clippy::cast_possible_truncation)]
    let traversals_count = self.traversals.len() as u32;
    traversals_count
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
  /// The number of newly created traversals.
  created_traversals_count: u32,
  /// The number of advanced traversals. The traversal is considered
  /// to be advanced when it transitions to the next state.
  advanced_traversals_count: u32,
  /// The number of completed traversals.
  completed_traversals_count: u32,
  /// The number of matched logs. A single log can be matched multiple times
  matched_logs_count: u32,
  // Logs to be injected back into the workflow engine after field attachment and other processing.
  logs_to_inject: BTreeMap<&'a str, Log>,
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

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub(crate) struct TraversalExtractions {
  /// States of Sankey diagrams. It's a `None` when traversal is initialized and is set
  /// to `Some` after the first value for a Sankey and a given traversal is extracted.
  pub(crate) sankey_states: Option<BTreeMap<String, SankeyState>>,
  /// Snapped timestamps, by extraction ID.
  pub(crate) timestamps: Option<BTreeMap<String, OffsetDateTime>>,
  /// Snapped field values, by extraction ID.
  pub(crate) fields: Option<BTreeMap<String, String>>,
}

//
// Traversal
//

/// A traversal points at a specific workflow state. Most workflow runs
/// have one traversal but it's possible for a workflow run to have multiple
/// traversals. That can happen when a given workflow traversal is forked when
/// multiple transitions outgoing from a given state match on the
/// same event (i.e., log).
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct Traversal {
  /// The index of a state the traversal is currently at.
  pub(crate) state_index: usize,
  /// The number of logs matched by traversal's transitions.
  /// Each element in an array corresponds to one transition.
  pub(crate) matched_logs_counts: Vec<u32>,
  /// Extractions folded across all traversals in a path.
  pub(crate) extractions: TraversalExtractions,
}

impl Traversal {
  /// Creates a new traversal for a given state. The method returns `None`
  /// for cases when a given state is a final state and doesn't have
  /// any outgoing transitions.
  pub fn new(
    config: &Config,
    state_index: usize,
    extractions: TraversalExtractions,
  ) -> Option<Self> {
    if config.states()[state_index].transitions().is_empty() {
      None
    } else {
      Some(Self {
        state_index,
        // The number of logs matched by a given traversal.
        // Start at 0 for a new traversal.
        matched_logs_counts: vec![0; config.states()[state_index].transitions().len()],
        extractions,
      })
    }
  }

  fn process_log<'a>(&mut self, config: &'a Config, log: &LogRef<'_>) -> TraversalResult<'a> {
    let transitions = config.transitions_for_traversal(self);

    let mut result = TraversalResult::default();
    // In majority of cases each traversal has 0 or 1 successor. A case when
    // more than 1 successor is created is possible if a state corresponding to
    // currently processed traversal has multiple outgoing transitions and a
    // processed log ends up fulfills conditions for more than 1 of these transitions.
    log::trace!(
      "processing {} transitions for workflow {}",
      transitions.len(),
      config.id()
    );
    for (index, transition) in transitions.iter().enumerate() {
      match &transition.rule() {
        Predicate::LogMatch(log_match, count) => {
          if log_match.do_match(
            log.log_level,
            log.log_type,
            log.message,
            log.fields,
            self.extractions.fields.as_ref(),
          ) {
            self.matched_logs_counts[index] += 1;

            // We do mark the log being matched on the `Run` level even if
            // the transition doesn't end up happening for when
            // self.matched_logs_count < *count.
            result.matched_logs_count += 1;

            if self.matched_logs_counts[index] == *count {
              result.followed_transitions_count += 1;
              // Update extractions.
              let mut updated_extractions = self.do_extractions(config, index, log);

              // Collect triggered actions and injected logs.
              let actions = config.actions_for_traversal(self, index);
              let (triggered_actions, logs_to_inject) =
                Self::triggered_actions(actions, &mut updated_extractions, log.fields);

              result.triggered_actions.extend(triggered_actions);
              result.log_to_inject.extend(logs_to_inject);

              // Create next traversal.
              let next_state_index = config.next_state_index_for_traversal(self, index);
              if let Some(traversal) = Self::new(config, next_state_index, updated_extractions) {
                result.output_traversals.push(traversal);
              }

              log::trace!(
                "traversal's transition {} matched log ({} matches in total) and is advancing, \
                 workflow id={:?}",
                index,
                self.matched_logs_counts[index],
                config.id(),
              );
            } else {
              log::trace!(
                "traversal's transition {} matched log ({} matches in total) but needs {} matches \
                 to advance, workflow id={:?}",
                index,
                self.matched_logs_counts[index],
                *count,
                config.id(),
              );
            }
          }
        },
      }
    }

    result
  }

  fn do_extractions(
    &self,
    config: &Config,
    index: usize,
    log: &LogRef<'_>,
  ) -> TraversalExtractions {
    // TODO(mattklein123): In the common case without forking we should be able to move this data
    // and not clone it. It will require some thinking on how to do this given the loops involved.
    // Maybe some CoW thing.
    let mut new_extractions = self.extractions.clone();

    let extractions = config.extractions(self, index);
    for extraction in &extractions.sankey_extractions {
      let Some(extracted_value) = extraction.value.extract_value(log) else {
        continue;
      };

      new_extractions
        .sankey_states
        .get_or_insert_with(BTreeMap::new)
        .entry(extraction.sankey_id.clone())
        .or_default()
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
        .get_or_insert_with(BTreeMap::new)
        .insert(timestamp_extraction_id.clone(), timestamp);
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
          .get_or_insert_with(BTreeMap::new)
          .insert(extraction.id.clone(), value.to_string());
      }
    }

    new_extractions
  }

  fn triggered_actions<'a>(
    actions: &'a [Action],
    extractions: &mut TraversalExtractions,
    current_log_fields: &FieldsRef<'_>,
  ) -> (Vec<TriggeredAction<'a>>, BTreeMap<&'a str, Log>) {
    let mut triggered_actions = vec![];
    let mut logs_to_inject = BTreeMap::new();
    for action in actions {
      match action {
        Action::FlushBuffers(action) => {
          triggered_actions.push(TriggeredAction::FlushBuffers(action));
        },
        Action::EmitMetric(action) => {
          triggered_actions.push(TriggeredAction::EmitMetric(action));
        },
        Action::EmitSankey(action) => {
          let Some(sankey_states) = &mut extractions.sankey_states else {
            continue;
          };

          let Some(sankey_state) = sankey_states.remove(action.id()) else {
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
        Action::TakeScreenshot(action) => {
          triggered_actions.push(TriggeredAction::TakeScreenshot(action));
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
    self.state_index == 0 && self.matched_logs_counts.iter().all(|&e| e == 0)
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
  TakeScreenshot(&'a ActionTakeScreenshot),
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
  // The number of transitions that were followed in response to processing a log.
  followed_transitions_count: u32,
  // Logs to be injected back into the workflow engine after field attachment and other processing.
  log_to_inject: BTreeMap<&'a str, Log>,
}

impl TraversalResult<'_> {
  /// Whether traversal made any progress. As it is now, the check could be reduced
  /// to checking whether any log was matched but the check is abstracted away to improve
  /// readability.
  fn did_make_progress(&self) -> bool {
    debug_assert!(
      if self.matched_logs_count == 0 {
        self.followed_transitions_count == 0
      } else {
        true
      }
    );
    self.matched_logs_count > 0
  }
}
