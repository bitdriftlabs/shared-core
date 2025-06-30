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
use bd_proto::protos::workflow::workflow::workflow::action::ActionGenerateLog;
use bd_proto::protos::workflow::workflow::workflow::transition_extension::Extension_type;
use bd_proto::protos::workflow::workflow::workflow::{
  LimitDuration as LimitDurationProto,
  LimitMatchedLogsCount,
};
use bd_stats_common::MetricType;
use protobuf::MessageField;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use time::Duration;
use workflow::Workflow as WorkflowConfigProto;
use workflow::workflow::action::action_emit_metric::Value_extractor_type;
use workflow::workflow::action::action_flush_buffers::streaming::termination_criterion;
use workflow::workflow::action::tag::Tag_type;
use workflow::workflow::action::{
  Action_type,
  ActionEmitMetric as ActionEmitMetricProto,
  ActionEmitSankeyDiagram as ActionEmitSankeyDiagramProto,
  ActionTakeScreenshot as ActionTakeScreenshotProto,
};
use workflow::workflow::rule::Rule_type;
use workflow::workflow::transition_extension::sankey_diagram_value_extraction;
use workflow::workflow::{
  Action as ActionProto,
  State as StateProto,
  Transition as TransitionProto,
};

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
  #[must_use]
  pub fn new(workflows: Vec<WorkflowConfigProto>) -> Self {
    let workflows = workflows
      .into_iter()
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
// Config
//

#[cfg_attr(test, derive(Clone))]
#[derive(Debug, PartialEq)]
pub struct Config {
  id: String,
  states: Vec<State>,
  duration_limit: Option<Duration>,
  matched_logs_count_limit: Option<u32>,
}

impl Config {
  pub fn new(config: WorkflowConfigProto) -> anyhow::Result<Self> {
    if config.states.is_empty() {
      return Err(anyhow!(
        "invalid workflow states configuration: states list is empty"
      ));
    }

    let state_index_by_id = config
      .states
      .iter()
      .enumerate()
      .map(|(index, s)| (s.id.clone(), index))
      .collect();

    let sankey_values_extraction_limit_by_id: HashMap<_, _> = config
      .states
      .iter()
      .flat_map(|state| &state.transitions)
      .flat_map(|transition| &transition.actions)
      .filter_map(|action| {
        if let Some(Action_type::ActionEmitSankeyDiagram(sankey)) = &action.action_type {
          Some((sankey.id.clone(), sankey.limit))
        } else {
          None
        }
      })
      .collect();

    let states = config
      .states
      .into_iter()
      .map(|s| State::try_from_proto(s, &state_index_by_id, &sankey_values_extraction_limit_by_id))
      .collect::<anyhow::Result<Vec<_>>>()?;

    // State 0 must have transitions to be a valid workflow.
    if states[0].transitions.is_empty() {
      return Err(anyhow!(
        "invalid workflow configuration: initial state must have at least one transition"
      ));
    }

    Ok(Self {
      id: config.id.clone(),
      states,
      duration_limit: Self::try_duration_limit_from_proto(&config.limit_duration)?,
      matched_logs_count_limit: Self::try_matched_logs_count_limit_from_proto(
        &config.limit_matched_logs_count,
      )?,
    })
  }

  pub(crate) fn id(&self) -> &str {
    &self.id
  }

  pub(crate) fn states(&self) -> &[State] {
    &self.states
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
        Ok(Some(Duration::milliseconds(duration_ms.try_into()?)))
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

  pub(crate) fn actions_for_timeout(&self, state_index: usize) -> &[Action] {
    &self.states[state_index].timeout.as_ref().unwrap().actions
  }

  pub(crate) fn extractions(
    &self,
    traversal: &Traversal,
    transition_index: usize,
  ) -> &TransitionExtractions {
    &self.states[traversal.state_index].transitions[transition_index].extractions
  }

  pub(crate) fn next_state_index_for_traversal(
    &self,
    traversal: &Traversal,
    transition_index: usize,
  ) -> usize {
    self.states[traversal.state_index].transitions[transition_index].target_state_index
  }

  pub(crate) fn next_state_index_for_timeout(&self, state_index: usize) -> usize {
    self.states[state_index]
      .timeout
      .as_ref()
      .unwrap()
      .target_state_index
  }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StateTimeout {
  target_state_index: usize,
  pub(crate) duration: Duration,
  actions: Vec<Action>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct State {
  id: StateID,
  transitions: Vec<Transition>,
  timeout: Option<StateTimeout>,
}

impl State {
  fn try_from_proto(
    state: StateProto,
    state_index_by_id: &HashMap<StateID, usize>,
    sankey_values_extraction_limit_by_id: &HashMap<String, u32>,
  ) -> anyhow::Result<Self> {
    Ok(Self {
      id: state.id.clone(),
      transitions: state
        .transitions
        .into_iter()
        .map(|transition| {
          if !state_index_by_id.contains_key(&transition.target_state_id) {
            return Err(anyhow!(
              "invalid workflow state configuration: reference to an unexisting state"
            ));
          }

          let target_state_index = state_index_by_id[&transition.target_state_id];
          Transition::new(
            transition,
            target_state_index,
            sankey_values_extraction_limit_by_id,
          )
        })
        .collect::<anyhow::Result<Vec<_>>>()?,
      timeout: state
        .timeout
        .into_option()
        .map(|timeout| {
          if !state_index_by_id.contains_key(&timeout.target_state_id) {
            return Err(anyhow!(
              "invalid workflow state configuration: reference to an unexisting state"
            ));
          }

          Ok(StateTimeout {
            target_state_index: state_index_by_id[&timeout.target_state_id],
            duration: Duration::milliseconds(timeout.timeout_ms.try_into()?),
            actions: timeout
              .actions
              .into_iter()
              .map(Action::try_from_proto)
              .collect::<anyhow::Result<Vec<_>>>()?,
          })
        })
        .transpose()?,
    })
  }

  pub(crate) fn id(&self) -> &str {
    &self.id
  }

  pub(crate) fn transitions(&self) -> &[Transition] {
    &self.transitions
  }

  pub(crate) fn timeout(&self) -> Option<&StateTimeout> {
    self.timeout.as_ref()
  }
}

//
// SankeyExtraction
//

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SankeyExtraction {
  pub(crate) sankey_id: String,
  pub(crate) value: TagValue,
  pub(crate) counts_toward_sankey_values_extraction_limit: bool,
  pub(crate) limit: usize,
}

impl SankeyExtraction {
  fn new(
    proto: &workflow::workflow::transition_extension::SankeyDiagramValueExtraction,
    limit: usize,
  ) -> anyhow::Result<Self> {
    let Some(value) = &proto.value_type else {
      anyhow::bail!("invalid sankey value extraction configuration: missing value type")
    };

    Ok(Self {
      sankey_id: proto.sankey_diagram_id.to_string(),
      value: match value {
        sankey_diagram_value_extraction::Value_type::Fixed(value) => TagValue::Fixed(value.clone()),
        sankey_diagram_value_extraction::Value_type::FieldExtracted(extracted) => {
          TagValue::Extract(extracted.field_name.clone())
        },
      },
      counts_toward_sankey_values_extraction_limit: proto.counts_toward_sankey_extraction_limit,
      limit,
    })
  }
}

//
// FieldExtractionConfig
//

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FieldExtractionConfig {
  pub(crate) field_name: String,
  pub(crate) id: String,
}

//
// TransitionExtractions
//

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TransitionExtractions {
  pub(crate) sankey_extractions: Vec<SankeyExtraction>,
  pub(crate) timestamp_extraction_id: Option<String>,
  pub(crate) field_extractions: Vec<FieldExtractionConfig>,
}

//
// Transition
//

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Transition {
  target_state_index: usize,
  rule: Predicate,
  actions: Vec<Action>,
  extractions: TransitionExtractions,
}

impl Transition {
  fn new(
    transition: TransitionProto,
    target_state_index: usize,
    sankey_values_extraction_limit_by_id: &HashMap<String, u32>,
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
    };

    let actions = transition
      .actions
      .into_iter()
      .map(Action::try_from_proto)
      .collect::<anyhow::Result<Vec<_>>>()?;

    let mut sankey_extractions = Vec::new();
    let mut field_extractions = Vec::new();
    let mut timestamp_extraction_id = None;
    for extension in &transition.extensions {
      let Some(extension_type) = &extension.extension_type else {
        continue;
      };

      match extension_type {
        Extension_type::SankeyDiagramValueExtraction(extension) => {
          let Some(sankey_limit) =
            sankey_values_extraction_limit_by_id.get(&extension.sankey_diagram_id)
          else {
            anyhow::bail!(
              "invalid transition configuration: missing sankey limit for sankey id {:?}",
              extension.sankey_diagram_id
            );
          };

          let Ok(limit) = <u32 as TryInto<usize>>::try_into(*sankey_limit) else {
            anyhow::bail!("sankey limit: conversion to usize failed");
          };

          sankey_extractions.push(SankeyExtraction::new(extension, limit)?);
        },
        Extension_type::SaveField(save_field) => {
          field_extractions.push(FieldExtractionConfig {
            field_name: save_field.field_name.clone(),
            id: save_field.id.clone(),
          });
        },
        Extension_type::SaveTimestamp(save_timestamp) => {
          // There is no reason to have multiple timestamp extractions in a single transition.
          if timestamp_extraction_id.is_some() {
            anyhow::bail!("invalid transition configuration: multiple timestamp extractions");
          }

          timestamp_extraction_id = Some(save_timestamp.id.clone());
        },
      }
    }

    Ok(Self {
      target_state_index,
      rule,
      actions,
      extractions: TransitionExtractions {
        sankey_extractions,
        timestamp_extraction_id,
        field_extractions,
      },
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
}

//
// Action
//

#[derive(Clone, Debug, PartialEq)]
/// The action to perform.
pub enum Action {
  FlushBuffers(ActionFlushBuffers),
  EmitMetric(ActionEmitMetric),
  EmitSankey(ActionEmitSankey),
  TakeScreenshot(ActionTakeScreenshot),
  GenerateLog(ActionGenerateLog),
}

impl Action {
  fn try_from_proto(proto: ActionProto) -> anyhow::Result<Self> {
    match proto
      .action_type
      .ok_or_else(|| anyhow!("invalid action configuration: missing action type"))?
    {
      Action_type::ActionFlushBuffers(action) => {
        let streaming = match action.streaming.into_option() {
          Some(streaming_proto) => Some(Streaming::new(streaming_proto)?),
          None => None,
        };

        Ok(Self::FlushBuffers(ActionFlushBuffers {
          id: FlushBufferId::WorkflowActionId(action.id.clone()),
          buffer_ids: action.buffer_ids.into_iter().collect::<BTreeSet<_>>(),
          streaming,
        }))
      },
      Action_type::ActionEmitMetric(metric) => Ok(Self::EmitMetric(ActionEmitMetric::new(metric)?)),
      Action_type::ActionEmitSankeyDiagram(diagram) => {
        Ok(Self::EmitSankey(ActionEmitSankey::try_from_proto(diagram)?))
      },
      Action_type::ActionTakeScreenshot(action) => Ok(Self::TakeScreenshot(
        ActionTakeScreenshot::try_from_proto(action),
      )),
      Action_type::ActionGenerateLog(action) => Ok(Self::GenerateLog(action)),
    }
  }
}

#[derive(
  serde::Serialize, serde::Deserialize, Hash, Clone, Debug, PartialEq, Eq, PartialOrd, Ord,
)]
pub enum FlushBufferId {
  /// Flush the buffer due to a workflow action triggering.
  WorkflowActionId(String),
  /// Flush the buffer in response to an explicit session capture request. The ID is provided to
  /// identify the origin of the session capture request.
  ExplicitSessionCapture(String),
}

//
// ActionFlushBuffers
//

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
/// The flush buffer action to perform.
pub struct ActionFlushBuffers {
  /// The identifier of an action. It should be attached as
  /// part of the logs upload payload.
  pub id: FlushBufferId,
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
  fn new(proto: ActionEmitMetricProto) -> anyhow::Result<Self> {
    let tags: BTreeMap<String, TagValue> = proto
      .tags
      .into_iter()
      .map(|t| {
        let value = match t.tag_type {
          Some(Tag_type::FixedValue(value)) => TagValue::Fixed(value),
          Some(Tag_type::FieldExtracted(extracted)) => TagValue::Extract(extracted.field_name),
          _ => {
            anyhow::bail!("invalid action emit metric configuration: unknown tag_type")
          },
        };

        Ok((t.name, value))
      })
      .collect::<anyhow::Result<BTreeMap<String, TagValue>>>()?;

    let metric_type = match proto.metric_type {
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

    match proto.value_extractor_type {
      Some(Value_extractor_type::Fixed(value)) => Ok(Self {
        id: proto.id.clone(),
        tags,
        increment: ValueIncrement::Fixed(u64::from(value)),
        metric_type,
      }),
      Some(Value_extractor_type::FieldExtracted(extracted)) => Ok(Self {
        id: proto.id,
        tags,
        increment: ValueIncrement::Extract(extracted.field_name),
        metric_type,
      }),
      _ => Err(anyhow!(
        "invalid action emit metric configuration: unknown value_extractor_type"
      )),
    }
  }
}

//
// ActionEmitSankey
//

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ActionEmitSankey {
  id: String,
  limit: u32,
  tags: BTreeMap<String, TagValue>,
}

impl ActionEmitSankey {
  fn try_from_proto(proto: ActionEmitSankeyDiagramProto) -> anyhow::Result<Self> {
    Ok(Self {
      id: proto.id,
      limit: proto.limit,
      tags: proto
        .tags
        .into_iter()
        .map(|tag| {
          let value = match tag.tag_type {
            Some(Tag_type::FixedValue(value)) => TagValue::Fixed(value),
            Some(Tag_type::FieldExtracted(extracted)) => TagValue::Extract(extracted.field_name),
            None => {
              anyhow::bail!("invalid action emit sankey diagram configuration: unknown tag_type")
            },
          };

          Ok((tag.name, value))
        })
        .collect::<anyhow::Result<BTreeMap<_, _>>>()?,
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

  #[must_use]
  pub const fn tags(&self) -> &BTreeMap<String, TagValue> {
    &self.tags
  }
}

//
// ActionTakeScreenshot
//

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ActionTakeScreenshot {
  id: String,
}

impl ActionTakeScreenshot {
  fn try_from_proto(proto: ActionTakeScreenshotProto) -> Self {
    Self { id: proto.id }
  }
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
