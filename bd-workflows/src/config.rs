// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::workflow::Traversal;
use anyhow::anyhow;
use bd_log_matcher::matcher::Tree;
use bd_matcher::FieldProvider;
use bd_proto::protos::workflow::workflow;
use bd_proto::protos::workflow::workflow::workflow::transition_extension::Extension_type;
use bd_proto::protos::workflow::workflow::workflow::{
  Execution as ExecutionProto,
  LimitDuration as LimitDurationProto,
  LimitMatchedLogsCount,
};
use bd_proto::protos::workflow::workflow::WorkflowsConfiguration as WorkflowsConfigurationProto;
use itertools::Itertools;
use protobuf::MessageField;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::time::Duration;
use workflow::workflow::action::action_emit_metric::Value_extractor_type;
use workflow::workflow::action::action_flush_buffers::streaming::termination_criterion;
use workflow::workflow::action::tag::Tag_type;
use workflow::workflow::action::{
  ActionEmitMetric as ActionEmitMetricProto,
  ActionEmitSankeyDiagram as ActionEmitSankeyDiagramProto,
  Action_type,
};
use workflow::workflow::execution::Execution_type;
use workflow::workflow::rule::Rule_type;
use workflow::workflow::transition_extension::sankey_diagram_value_extraction;
use workflow::workflow::{
  Action as ActionProto,
  State as StateProto,
  Transition as TransitionProto,
};
use workflow::Workflow as WorkflowConfigProto;

pub(crate) type StateID = String;

//
// WorkflowsConfiguration
//

#[cfg_attr(test, derive(Clone))]
#[derive(Debug, Default)]
pub struct WorkflowsConfiguration {
  pub(crate) workflows: Vec<Config>,
}

impl WorkflowsConfiguration {
  pub fn new(workflows_configuration: &WorkflowsConfigurationProto) -> Self {
    let workflows = workflows_configuration
      .workflows
      .iter()
      .filter_map(|config| Config::new(config).ok())
      .collect();

    Self { workflows }
  }

  // This method should be used in tests only but cannot be attributed with cfg(test) as there are
  // tests outside of the current crate that use it.
  #[must_use]
  pub const fn new_with_workflow_configurations_for_test(workflows: Vec<Config>) -> Self {
    Self { workflows }
  }
}

//
// SankeyInfo
//

struct SankeyInfo {
  state_index: usize,
  limit: u32,
  root_to_action_path: Vec<usize>,
}

//
// Config
//

#[cfg_attr(test, derive(Clone))]
#[derive(Debug, PartialEq, Eq)]
pub struct Config {
  id: String,
  states: Vec<State>,
  execution: Execution,
  duration_limit: Option<Duration>,
  matched_logs_count_limit: Option<u32>,

  graph_helper: GraphHelper,
}

impl Config {
  pub fn new(config: &WorkflowConfigProto) -> anyhow::Result<Self> {
    if config.states.is_empty() {
      return Err(anyhow!(
        "invalid workflow states configuration: states list is empty"
      ));
    }

    let graph_helper = GraphHelper::new(&config)?;

    let state_index_by_id = config
      .states
      .iter()
      .enumerate()
      .map(|(index, s)| (s.id.clone(), index))
      .collect();

    let states = config
      .states
      .iter()
      .map(|s| State::try_from_proto(s, &state_index_by_id, &graph_helper))
      .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(Self {
      id: config.id.clone(),
      states,
      execution: Execution::new(&config.execution)?,
      duration_limit: Self::try_duration_limit_from_proto(&config.limit_duration)?,
      matched_logs_count_limit: Self::try_matched_logs_count_limit_from_proto(
        &config.limit_matched_logs_count,
      )?,
      graph_helper: GraphHelper::new(config)?,
    })
  }

  pub(crate) fn id(&self) -> &str {
    &self.id
  }

  pub(crate) fn states(&self) -> &[State] {
    &self.states
  }

  pub(crate) const fn execution(&self) -> &Execution {
    &self.execution
  }

  pub(crate) const fn duration_limit(&self) -> Option<Duration> {
    self.duration_limit
  }

  pub(crate) const fn matched_logs_count_limit(&self) -> Option<u32> {
    self.matched_logs_count_limit
  }

  fn try_duration_limit_from_proto(
    value: &MessageField<LimitDurationProto>,
  ) -> anyhow::Result<Option<Duration>> {
    value.as_ref().map_or(Ok(None), |duration_proto| {
      let duration_ms = duration_proto.duration_ms;
      if duration_ms > 0 {
        Ok(Some(Duration::from_millis(duration_ms)))
      } else {
        Err(anyhow!(
          "invalid duration limit configuration: duration_ms limit is equal to 0"
        ))
      }
    })
  }

  fn try_matched_logs_count_limit_from_proto(
    value: &MessageField<LimitMatchedLogsCount>,
  ) -> anyhow::Result<Option<u32>> {
    value
      .as_ref()
      .map_or(Ok(None), |matched_logs_count_limit_proto| {
        let count = matched_logs_count_limit_proto.count;
        if count > 0 {
          Ok(Some(count))
        } else {
          Err(anyhow!(
            "invalid logs count limit configuration: matched logs count limit is equal to 0",
          ))
        }
      })
  }

  pub(crate) fn transitions_for_traversal(&self, traversal: &Traversal) -> &[Transition] {
    &self.states[traversal.state_index].transitions
  }

  pub(crate) fn actions_for_traversal(
    &self,
    traversal: &Traversal,
    transition_index: usize,
  ) -> &[Action] {
    &self.states[traversal.state_index].transitions[transition_index].actions
  }

  pub(crate) fn sankey_extractions(
    &self,
    traversal: &Traversal,
    transition_index: usize,
  ) -> &[SankeyExtraction] {
    &self.states[traversal.state_index].transitions[transition_index].sankey_extractions
  }

  pub(crate) fn sankey_limit(
    &self,
    sankey_id: &str,
  ) -> anyhow::Result<u32> {
    if let Some(info) = self.graph_helper.sankey_info_by_sankey_id.get(sankey_id) {
      Ok(info.limit)
    } else {
      Err(anyhow!("invalid workflow configuration: no limit for an unexisting sankey diagram"))
    }
  }

  pub(crate) fn next_state_index_for_traversal(
    &self,
    traversal: &Traversal,
    transition_index: usize,
  ) -> usize {
    self.states[traversal.state_index].transitions[transition_index].target_state_index
  }
}

//
// SankeyHelper
//

pub struct SankeyhHelper {
  state_index_by_id: HashMap<String, usize>,
  sankey_info_by_sankey_id: HashMap<String, SankeyInfo>,

  incoming_transitions_by_index: Vec<Vec<usize>>,
  outgoing_transitions_by_index: Vec<Vec<usize>>
}

impl GraphHelper {
  pub fn new(config: &WorkflowConfigProto) -> anyhow::Result<Self> {
    let state_index_by_id: HashMap<_, _> = config
      .states
      .iter()
      .enumerate()
      .map(|(index, state)| (state.id.clone(), index))
      .collect();

    let mut incoming_transitions_by_index: Vec<Vec<_>> = Vec::with_capacity(config.states.len());
    let mut outgoing_transitions_by_index: Vec<Vec<_>> = Vec::with_capacity(config.states.len());

    let mut sankey_info_by_sankey_id = HashMap::new();

    for state in &config.states {
      for transition in &state.transitions {
        let Some(current_state_index) = state_index_by_id.get(&state.id) else {
          anyhow::bail!("invalid workflow configuration: reference to an unexisting state");
        };

        let Some(target_state_index) = state_index_by_id.get(&transition.target_state_id) else {
          anyhow::bail!("invalid workflow configuration: reference to an unexisting target state");
        };

        for action in &transition.actions {
          if let Some(Action_type::ActionEmitSankeyDiagram(action)) = &action.action_type {
            sankey_info_by_sankey_id.insert(
              action.id.clone(),
              SankeyInfo {
                state_index: *target_state_index,
                limit: action.limit,
                root_to_action_path: Vec::new(),
              },
            );
          }
        }

        incoming_transitions_by_index[*target_state_index].push(*current_state_index);
        outgoing_transitions_by_index[*current_state_index].push(*target_state_index);
      }
    }

    for info in sankey_info_by_sankey_id.values_mut() {
      info.root_to_action_path =
        Self::path_to_state(info.state_index, &incoming_transitions_by_index)?;
    }

    Ok(Self {
      state_index_by_id,
      sankey_info_by_sankey_id,

      incoming_transitions_by_index,
      outgoing_transitions_by_index
    })
  }

  pub fn is_included_in_sankey_diagram_limit(
    &self,
    sankey_id: &str,
    target_state_index: usize,
  ) -> anyhow::Result<bool> {
    let path = &self
      .sankey_info_by_sankey_id
      .get(sankey_id)
      .ok_or_else(|| {
        anyhow!("invalid workflow configuration: reference to an unexisting sankey diagram")
      })?
      .root_to_action_path;

    for index in path {
      if self.incoming_transitions_by_index[*index].len() > 1 {
        return Ok(false);
      }

      if *index == target_state_index {
        break;
      }
    }

    for index in path.iter().rev() {
      if self.outgoing_transitions_by_index[*index].len() > 1 {
        return Ok(false);
      }

      if *index == target_state_index {
        break;
      }
    }

    Ok(true)
  }

  fn path_to_state(
    state_index: usize,
    incoming_transitions_by_index: &[Vec<usize>],
  ) -> anyhow::Result<Vec<usize>> {
    // An assumption is make that any given matcher node cannot have more than one incoming edge.
    let mut visited: HashSet<usize> = HashSet::new();
    let mut queue = vec![state_index];
    let mut path = Vec::new();

    while !queue.is_empty() {
      let state_index = queue.remove(0);

      // This protects us from infinite loops caused by cycles in the graph.
      if visited.contains(&state_index) {
        continue;
      }

      visited.insert(state_index);
      path.push(state_index);

      let states_previous_on_path = &incoming_transitions_by_index[state_index];
      queue.extend(states_previous_on_path);
    }

    debug_assert!(
      !path.is_empty() && path[0] == 0,
      "the first state on the path should be the root state which is always at index 0"
    );

    Ok(path.into_iter().rev().collect_vec())
  }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct State {
  id: StateID,
  transitions: Vec<Transition>,
}

impl State {
  fn try_from_proto(
    state: &StateProto,
    state_index_by_id: &HashMap<StateID, usize>,
    graph_helper: &GraphHelper,
  ) -> anyhow::Result<Self> {
    Ok(Self {
      id: state.id.clone(),
      transitions: state
        .transitions
        .iter()
        .map(|transition| {
          if !state_index_by_id.contains_key(&transition.target_state_id) {
            return Err(anyhow!(
              "invalid workflow state configuration: reference to an unexisting state"
            ));
          }

          Transition::new(
            transition,
            state_index_by_id[&transition.target_state_id],
            graph_helper,
          )
        })
        .collect::<anyhow::Result<Vec<_>>>()?,
    })
  }

  #[cfg(test)]
  pub(crate) fn id(&self) -> &str {
    &self.id
  }

  pub(crate) fn transitions(&self) -> &[Transition] {
    &self.transitions
  }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Execution {
  Exclusive,
  Parallel,
}

impl Execution {
  pub fn new(value: &protobuf::MessageField<ExecutionProto>) -> anyhow::Result<Self> {
    match value
      .execution_type
      .as_ref()
      .ok_or_else(|| anyhow!("invalid execution configuration: missing execution_type"))?
    {
      Execution_type::ExecutionExclusive(_) => Ok(Self::Exclusive),
      Execution_type::ExecutionParallel(_) => Ok(Self::Parallel),
    }
  }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SankeyExtraction {
  pub(crate) sankey_id: String,
  pub(crate) value: TagValue,
  pub(crate) is_included_in_sankey_limits: bool,
}

impl SankeyExtraction {
  fn new(
    proto: &workflow::workflow::transition_extension::SankeyDiagramValueExtraction,
    is_included_in_sankey_limits: bool,
  ) -> anyhow::Result<Self> {
    let Some(value) = &proto.value_type else {
      anyhow::bail!("invalid sankey diagram value extraction configuration: missing value type")
    };

    Ok(Self {
      sankey_id: proto.sankey_diagram_id.to_string(),
      value: match value {
        sankey_diagram_value_extraction::Value_type::Fixed(value) => TagValue::Fixed(value.clone()),
        sankey_diagram_value_extraction::Value_type::FieldExtracted(extracted) => {
          TagValue::Extract(extracted.field_name.clone())
        },
      },
      is_included_in_sankey_limits,
    })
  }
}

//
// Transition
//

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Transition {
  target_state_index: usize,
  rule: Predicate,
  actions: Vec<Action>,
  sankey_extractions: Vec<SankeyExtraction>,
}

impl Transition {
  fn new(
    transition: &TransitionProto,
    target_state_index: usize,
    graph_helper: &GraphHelper,
  ) -> anyhow::Result<Self> {
    let rule = transition
      .rule
      .as_ref()
      .ok_or_else(|| anyhow!("invalid transition configuration: missing rule"))?;

    let rule = match rule
      .rule_type
      .as_ref()
      .ok_or_else(|| anyhow!("invalid transition configuration: missing rule type"))?
    {
      Rule_type::RuleLogMatch(rule) => {
        Predicate::LogMatch(Tree::new(&rule.log_matcher)?, rule.count)
      },
      Rule_type::RuleTimeout(rule) => {
        Predicate::TimeoutMatch(Duration::from_millis(rule.duration_ms))
      },
    };

    let actions = transition
      .actions
      .iter()
      .map(Action::try_from_proto)
      .collect::<anyhow::Result<Vec<_>>>()?;

    let sankey_extractions = transition
      .extensions
      .iter()
      .map(|extension| {
        let Some(extension_type) = &extension.extension_type else {
          return Ok(None);
        };

        match extension_type {
          Extension_type::SankeyDiagramValueExtraction(extension) => {
            let is_included_in_sankey_limits = graph_helper.is_included_in_sankey_diagram_limit(
              &extension.sankey_diagram_id,
              target_state_index,
            )?;
            Ok(Some(SankeyExtraction::new(
              extension,
              is_included_in_sankey_limits,
            )))
          },
          #[allow(unreachable_patterns)]
          _ => anyhow::bail!("invalid transition configuration: unknown extension type"),
        }
      })
      .filter_map(|result| match result {
        Ok(Some(value)) => Some(value),
        Err(err) => Some(Err(err)),
        _ => None,
      })
      .collect::<anyhow::Result<_>>()?;

    Ok(Self {
      target_state_index,
      rule,
      actions,
      sankey_extractions,
    })
  }

  pub(crate) const fn rule(&self) -> &Predicate {
    &self.rule
  }

  #[cfg(test)]
  pub(crate) fn actions(&self) -> &[Action] {
    &self.actions
  }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Predicate {
  LogMatch(Tree, u32),
  TimeoutMatch(Duration), // TODO(murki): implement
}

//
// Action
//

#[derive(Clone, Debug, PartialEq, Eq)]
/// The action to perform.
pub enum Action {
  FlushBuffers(ActionFlushBuffers),
  EmitMetric(ActionEmitMetric),
  SankeyDiagram(ActionEmitSankeyDiagram),
}

impl Action {
  fn try_from_proto(proto: &ActionProto) -> anyhow::Result<Self> {
    match proto
      .action_type
      .as_ref()
      .ok_or_else(|| anyhow!("invalid action configuration: missing action type"))?
    {
      Action_type::ActionFlushBuffers(action) => {
        let streaming = match action.streaming.clone().into_option() {
          Some(streaming_proto) => Some(Streaming::new(streaming_proto)?),
          None => None,
        };

        Ok(Self::FlushBuffers(ActionFlushBuffers {
          id: action.id.clone(),
          buffer_ids: action
            .buffer_ids
            .clone()
            .into_iter()
            .collect::<BTreeSet<_>>(),
          streaming,
        }))
      },
      Action_type::ActionEmitMetric(metric) => Ok(Self::EmitMetric(ActionEmitMetric::new(metric)?)),
      Action_type::ActionEmitSankeyDiagram(diagram) => Ok(Self::SankeyDiagram(
        ActionEmitSankeyDiagram::try_from_proto(diagram)?,
      )),
    }
  }
}

//
// ActionFlushBuffers
//

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
/// The flush buffer action to perform.
pub struct ActionFlushBuffers {
  /// The identifier of an action. It should be attached as
  /// part of the logs upload payload.
  pub id: String,
  /// The list of buffer IDs to flush.
  pub buffer_ids: BTreeSet<String>,
  /// The streaming configuration.
  pub(crate) streaming: Option<Streaming>,
}

//
// Streaming
//

/// The buffer streaming configuration to apply when flush buffer(s) action is performed.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Streaming {
  /// The list of destination streaming buffer IDs.
  pub(crate) destination_continuous_buffer_ids: BTreeSet<String>,

  /// The maximum number of logs to stream. No maximum number of logs is configured if this field
  /// is not set.
  pub(crate) max_logs_count: Option<u64>,
}

impl Streaming {
  fn new(
    streaming_proto: workflow::workflow::action::action_flush_buffers::Streaming,
  ) -> anyhow::Result<Self> {
    let destination_continuous_buffer_ids = streaming_proto
      .destination_streaming_buffer_ids
      .into_iter()
      .collect::<BTreeSet<_>>();

    let termination_criteria_types = streaming_proto
      .termination_criteria
      .into_iter()
      .filter_map(|c| c.type_);

    let max_logs_count: Option<u64> = termination_criteria_types
      .map(|criterion_type| match criterion_type {
        termination_criterion::Type::LogsCount(termination_criterion::LogsCount {
          max_logs_count,
          ..
        }) => max_logs_count,
      })
      .min();

    if let Some(max_logs_count) = max_logs_count {
      if max_logs_count == 0 {
        return Err(anyhow!(
          "invalid streaming configuration: max_logs_count has to be greater than 0",
        ));
      }
    }

    Ok(Self {
      destination_continuous_buffer_ids,
      max_logs_count,
    })
  }
}

//
// ActionEmitMetric
//

/// Describes a single dynamic counter that can be increment via emit
/// metric action.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ActionEmitMetric {
  pub id: String,
  pub tags: BTreeMap<String, TagValue>,
  /// How much to increment the associated counter with when applying this metric.
  pub increment: ValueIncrement,
  pub metric_type: MetricType,
}

impl ActionEmitMetric {
  /// Attempts to create a `Metric` from the protobuf description, failing if the
  /// provided configuration contains unknown oneof values.
  fn new(proto: &ActionEmitMetricProto) -> anyhow::Result<Self> {
    let tags: BTreeMap<String, TagValue> = proto
      .tags
      .iter()
      .map(|t| {
        let value = match &t.tag_type {
          Some(Tag_type::FixedValue(value)) => TagValue::Fixed(value.clone()),
          Some(Tag_type::FieldExtracted(extracted)) => {
            TagValue::Extract(extracted.field_name.to_string())
          },
          _ => {
            anyhow::bail!("invalid action emit metric configuration: unknown tag_type")
          },
        };

        Ok((t.name.clone(), value))
      })
      .collect::<anyhow::Result<BTreeMap<String, TagValue>>>()?;

    let metric_type = match &proto.metric_type {
      Some(proto) => match proto {
        workflow::workflow::action::action_emit_metric::Metric_type::Counter(_) => {
          MetricType::Counter
        },
        workflow::workflow::action::action_emit_metric::Metric_type::Histogram(_) => {
          MetricType::Histogram
        },
      },
      None => {
        anyhow::bail!("invalid action emit metric configuration: missing metric_type")
      },
    };

    match &proto.value_extractor_type {
      Some(Value_extractor_type::Fixed(value)) => Ok(Self {
        id: proto.id.clone(),
        tags,
        increment: ValueIncrement::Fixed(u64::from(*value)),
        metric_type,
      }),
      Some(Value_extractor_type::FieldExtracted(extracted)) => Ok(Self {
        id: proto.id.clone(),
        tags,
        increment: ValueIncrement::Extract(extracted.field_name.to_string()),
        metric_type,
      }),
      _ => Err(anyhow!(
        "invalid action emit metric configuration: unknown value_extractor_type"
      )),
    }
  }
}

//
// ActionEmitSankeyDiagram
//

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ActionEmitSankeyDiagram {
  id: String,
  limit: u32,
}

impl ActionEmitSankeyDiagram {
  fn try_from_proto(proto: &ActionEmitSankeyDiagramProto) -> anyhow::Result<Self> {
    Ok(Self {
      id: proto.id.to_string(),
      limit: proto.limit,
    })
  }

  #[must_use]
  pub fn id(&self) -> &str {
    &self.id
  }

  #[must_use]
  pub const fn limit(&self) -> u32 {
    self.limit
  }
}

//
// MetricType
//

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum MetricType {
  Counter,
  Histogram,
}

pub type FieldKey = String;

//
// ValueIncrement
//

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ValueIncrement {
  // Add a fixed value to the metric.
  Fixed(u64),

  // Extract the value from the specified field. If the field does not exist or is not convertible
  // to an integer, the metric is created (set to zero) but not incremented.
  Extract(FieldKey),
}

//
// TagValue
//

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TagValue {
  // Use the value of the specified tag without further modification.
  Extract(String),
  // Use a fixed value.
  Fixed(FieldKey),
}

impl TagValue {
  pub(crate) fn extract_value<'a>(&self, fields: &'a impl FieldProvider) -> Option<Cow<'a, str>> {
    match self {
      Self::Extract(field_key) => fields.field_value(field_key),
      Self::Fixed(value) => Some(Cow::Owned(value.to_string())),
    }
  }
}
