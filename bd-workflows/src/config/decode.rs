#[cfg(test)]
#[path = "./inline_test.rs"]
mod tests;

#[path = "./decode/actions.rs"]
mod actions;
#[path = "./decode/predicates.rs"]
mod predicates;

use super::{
  Action,
  Config,
  DEFAULT_PARALLEL_MAX_ACTIVE_RUNS,
  Execution,
  InnerConfig,
  Predicate,
  State,
  StateTimeout,
  Transition,
  TransitionExtractions,
  WorkflowDebugMode,
  WorkflowsConfiguration,
};
use anyhow::{anyhow, bail};
use bd_proto_util::serialization::inline::ProtoDeserialize;
use bd_proto_util::serialization::inline::views::workflow::{
  self as wire,
  WorkflowActionActionType as ActionKind,
  WorkflowExecutionExecutionType as ExecutionKind,
};
use std::collections::HashMap;
use time::Duration;

impl WorkflowsConfiguration {
  pub fn from_proto_bytes(workflows: &[u8], debug_workflows: &[u8]) -> anyhow::Result<Self> {
    Self::from_views(
      &wire::WorkflowsConfiguration::from_proto_bytes(workflows)?,
      &wire::WorkflowsConfiguration::from_proto_bytes(debug_workflows)?,
    )
  }

  pub fn from_views(
    workflows: &wire::WorkflowsConfiguration<'_>,
    debug_workflows: &wire::WorkflowsConfiguration<'_>,
  ) -> anyhow::Result<Self> {
    let deployed = workflows.workflows()?;
    let debug = debug_workflows.workflows()?;
    let debug_ids = debug
      .iter()
      .map(wire::Workflow::id)
      .collect::<anyhow::Result<Vec<_>>>()?;
    let mut found = vec![false; debug.len()];
    let mut configs = Vec::new();
    for message in &deployed {
      let id = message.id()?;
      let mode = debug_ids
        .iter()
        .position(|debug_id| *debug_id == id)
        .map_or(WorkflowDebugMode::None, |index| {
          if let Some(found) = found.get_mut(index) {
            *found = true;
          }
          WorkflowDebugMode::DebugAndDeployed
        });
      match Config::from_view(message, mode) {
        Ok(config) => configs.push(config),
        Err(error) => log::debug!("discarding invalid workflow {id:?}: {error}"),
      }
    }
    for (message, found) in debug.iter().zip(found) {
      if !found {
        match Config::from_view(message, WorkflowDebugMode::DebugOnly) {
          Ok(config) => configs.push(config),
          Err(error) => log::debug!("discarding invalid debug workflow: {error}"),
        }
      }
    }
    Ok(Self { workflows: configs })
  }
}

impl Config {
  pub fn from_proto_bytes(bytes: &[u8], mode: WorkflowDebugMode) -> anyhow::Result<Self> {
    Self::from_view(&wire::Workflow::from_proto_bytes(bytes)?, mode)
  }

  fn from_view(message: &wire::Workflow<'_>, mode: WorkflowDebugMode) -> anyhow::Result<Self> {
    let states = message.states()?;
    if states.is_empty() {
      bail!("invalid workflow states configuration: states list is empty");
    }
    let state_indices = states
      .iter()
      .enumerate()
      .map(|(index, state)| Ok((state.id()?, index)))
      .collect::<anyhow::Result<HashMap<_, _>>>()?;
    // Resolve limits before transitions, without constructing unrelated action payloads twice.
    let mut sankey_limits = HashMap::new();
    for state in &states {
      for transition in state.transitions()? {
        for action in transition.actions()? {
          if ActionKind::is_action_emit_sankey_diagram(action.as_message())?
            && let Some(ActionKind::ActionEmitSankeyDiagram(sankey)) = action.action_type()?
          {
            sankey_limits.insert(sankey.id()?.to_owned(), sankey.limit()?);
          }
        }
      }
    }
    let states = states
      .iter()
      .map(|state| State::from_view(state, &state_indices, &sankey_limits))
      .collect::<anyhow::Result<Vec<_>>>()?;
    if states
      .first()
      .is_none_or(|state| state.transitions.is_empty())
    {
      bail!("invalid workflow configuration: initial state must have at least one transition");
    }
    let execution = match message
      .execution()?
      .map(|execution| execution.execution_type())
      .transpose()?
      .flatten()
    {
      Some(ExecutionKind::ExecutionParallel(parallel)) => {
        let limit = parallel.max_active_runs()?.unwrap_or_default();
        Execution::Parallel {
          max_active_runs: if limit == 0 {
            DEFAULT_PARALLEL_MAX_ACTIVE_RUNS
          } else {
            limit
          },
        }
      },
      _ => Execution::Exclusive,
    };
    let matched_logs_count_limit = message
      .limit_matched_logs_count()?
      .map(|limit| {
        let count = limit.count()?;
        if count == 0 {
          bail!("invalid logs count limit configuration: matched logs count limit is equal to 0");
        }
        Ok(count)
      })
      .transpose()?;
    let duration_limit = message
      .limit_duration()?
      .map(|limit| {
        let milliseconds = limit.duration_ms()?;
        if milliseconds == 0 {
          bail!("invalid duration limit configuration: duration_ms limit is equal to 0");
        }
        Ok(Duration::milliseconds(milliseconds.try_into()?))
      })
      .transpose()?;
    Ok(Self {
      inner: InnerConfig {
        id: message.id()?.to_owned(),
        states,
        execution,
        duration_limit,
        matched_logs_count_limit,
      },
      mode,
    })
  }
}

impl State {
  fn from_view(
    message: &wire::WorkflowState<'_>,
    indices: &HashMap<&str, usize>,
    limits: &HashMap<String, u32>,
  ) -> anyhow::Result<Self> {
    let id = message.id()?.to_owned();
    let transitions = message.transitions()?;
    let mut compiled_transitions = Vec::with_capacity(transitions.len());
    for transition in &transitions {
      compiled_transitions.push(Transition::from_view(transition, indices, limits)?);
    }
    Ok(Self {
      id,
      transitions: compiled_transitions,
      timeout: message
        .timeout()?
        .map(|timeout| {
          Ok::<_, anyhow::Error>(StateTimeout {
            target_state_index: target_index(timeout.target_state_id()?, indices)?,
            duration: Duration::milliseconds(timeout.timeout_ms()?.try_into()?),
            actions: timeout
              .actions()?
              .iter()
              .map(Action::from_view)
              .collect::<anyhow::Result<_>>()?,
          })
        })
        .transpose()?,
    })
  }
}

impl Transition {
  fn from_view(
    message: &wire::WorkflowTransition<'_>,
    indices: &HashMap<&str, usize>,
    limits: &HashMap<String, u32>,
  ) -> anyhow::Result<Self> {
    Ok(Self {
      target_state_index: target_index(message.target_state_id()?, indices)?,
      rule: Predicate::from_view(
        &message
          .rule()?
          .ok_or_else(|| anyhow!("missing protobuf message field 2"))?,
      )?,
      actions: message
        .actions()?
        .iter()
        .map(Action::from_view)
        .collect::<anyhow::Result<_>>()?,
      extractions: TransitionExtractions::from_view(message, limits)?,
    })
  }
}

fn target_index(id: &str, indices: &HashMap<&str, usize>) -> anyhow::Result<usize> {
  indices.get(id).copied().ok_or_else(|| {
    anyhow!("invalid workflow state configuration: reference to an unexisting state")
  })
}
