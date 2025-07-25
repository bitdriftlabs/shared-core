// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use action_generate_log::generated_field::Generated_field_value_type;
use action_generate_log::value_reference::Value_reference_type;
use action_generate_log::{GeneratedField, ValueReference, ValueReferencePair};
use bd_log_primitives::{LogFields, LogType, StringOrBytes};
use bd_proto::protos;
use bd_proto::protos::log_matcher::log_matcher::log_matcher;
use bd_proto::protos::workflow::workflow::workflow::action::action_flush_buffers::Streaming;
use bd_proto::protos::workflow::workflow::workflow::action::action_flush_buffers::streaming::{
  TerminationCriterion,
  termination_criterion,
};
use bd_proto::protos::workflow::workflow::workflow::action::{
  ActionGenerateLog,
  Tag,
  action_generate_log,
};
use bd_proto::protos::workflow::workflow::workflow::field_extracted::{Exact, Extraction_type};
use bd_proto::protos::workflow::workflow::workflow::transition_extension::{
  Extension_type,
  SankeyDiagramValueExtraction,
  SaveField,
  SaveTimestamp,
  sankey_diagram_value_extraction,
};
use bd_proto::protos::workflow::workflow::workflow::{FieldExtracted, TransitionExtension};
use log_matcher::base_log_matcher::string_value_match::String_value_match_type;
use protobuf::MessageField;
use protos::log_matcher::log_matcher::LogMatcher;
use protos::log_matcher::log_matcher::log_matcher::base_log_matcher::Match_type::{
  MessageMatch,
  TagMatch,
};
use protos::log_matcher::log_matcher::log_matcher::base_log_matcher::tag_match::Value_match;
use protos::log_matcher::log_matcher::log_matcher::{
  BaseLogMatcher,
  Matcher,
  MatcherList,
  base_log_matcher,
};
use protos::workflow::workflow::workflow::action::action_emit_metric::Value_extractor_type;
use protos::workflow::workflow::workflow::action::{
  Action_type,
  ActionEmitMetric as ActionEmitMetricProto,
  ActionEmitSankeyDiagram as ActionEmitSankeyDiagramProto,
  ActionFlushBuffers as ActionFlushBuffersProto,
  ActionTakeScreenshot as ActionTakeScreenshotProto,
  action_emit_metric,
};
use protos::workflow::workflow::workflow::rule::Rule_type;
use protos::workflow::workflow::workflow::{
  Action as ActionProto,
  Execution as ExecutionProto,
  Rule,
  State,
};
use std::collections::BTreeMap;

#[allow(clippy::module_inception)]
pub mod macros {
  /// A macro that creates a workflow config proto using provided states.
  #[macro_export]
  #[allow(clippy::module_name_repetitions)]
  macro_rules! workflow_proto {
    ($($state:expr),+) => {
      $crate::workflow::make_workflow_config_proto(
        "workflow_id",
        bd_proto::protos::workflow::workflow::workflow::execution::Execution_type
        ::ExecutionExclusive(
          Default::default()
        ),
        Default::default(),
        Default::default(),
        vec![$($state.clone().into_inner()),+],
      )
    };
    (
      $($state:expr),+;
      matches $matches_limit:expr;
      duration $duration_limit:expr) => {
      $crate::workflow::make_workflow_config_proto(
        "workflow_id",
        bd_proto::protos::workflow::workflow::workflow::execution::Execution_type
        ::ExecutionExclusive(
          Default::default()
        ),
        $matches_limit,
        $duration_limit,
        vec![$($state.clone().into_inner()),+],
      )
    };
    ($id:expr; $($state:expr),+) => {
      $crate::workflow::make_workflow_config_proto(
        $id,
        bd_proto::protos::workflow::workflow::workflow::execution::Execution_type
        ::ExecutionExclusive(
          Default::default()
        ),
        Default::default(),
        Default::default(),
        vec![$($state.clone().into_inner()),+],
      )
    };
  }

  #[derive(Clone)]
  pub struct StateBuilder {
    state: bd_proto::protos::workflow::workflow::workflow::State,
  }

  impl StateBuilder {
    #[must_use]
    pub fn declare_transition(
      mut self,
      to: &Self,
      rule: bd_proto::protos::workflow::workflow::workflow::Rule,
    ) -> Self {
      crate::workflow::add_transition(&mut self.state, &to.state, rule, &[], vec![]);

      self
    }

    #[must_use]
    pub fn declare_transition_with_actions(
      mut self,
      to: &Self,
      rule: bd_proto::protos::workflow::workflow::workflow::Rule,
      actions: &[bd_proto::protos::workflow::workflow::workflow::action::Action_type],
    ) -> Self {
      crate::workflow::add_transition(&mut self.state, &to.state, rule, actions, vec![]);

      self
    }

    #[must_use]
    pub fn declare_transition_with_extractions(
      mut self,
      to: &Self,
      rule: bd_proto::protos::workflow::workflow::workflow::Rule,
      extractions: &[bd_proto::protos::workflow::workflow::workflow::TransitionExtension],
    ) -> Self {
      crate::workflow::add_transition(&mut self.state, &to.state, rule, &[], extractions.to_vec());

      self
    }

    #[must_use]
    pub fn declare_transition_with_all(
      mut self,
      to: &Self,
      rule: bd_proto::protos::workflow::workflow::workflow::Rule,
      actions: &[bd_proto::protos::workflow::workflow::workflow::action::Action_type],
      extractions: &[bd_proto::protos::workflow::workflow::workflow::TransitionExtension],
    ) -> Self {
      crate::workflow::add_transition(
        &mut self.state,
        &to.state,
        rule,
        actions,
        extractions.to_vec(),
      );

      self
    }

    pub fn into_inner(self) -> bd_proto::protos::workflow::workflow::workflow::State {
      self.state
    }

    #[must_use]
    pub fn with_timeout(
      mut self,
      to: &Self,
      duration: Duration,
      actions: &[bd_proto::protos::workflow::workflow::workflow::action::Action_type],
    ) -> Self {
      self.state.timeout = Some(TransitionTimeout {
        target_state_id: to.state.id.clone(),
        timeout_ms: duration.whole_milliseconds().try_into().unwrap(),
        actions: actions
          .iter()
          .map(|a| ActionProto {
            action_type: Some(a.clone()),
            ..Default::default()
          })
          .collect(),
        ..Default::default()
      })
      .into();
      self
    }
  }

  #[must_use]
  pub fn state(id: &str) -> StateBuilder {
    StateBuilder {
      state: bd_proto::protos::workflow::workflow::workflow::State {
        id: id.to_string(),
        ..Default::default()
      },
    }
  }

  /// A macro that creates a matched logs count or duration limit.
  #[macro_export]
  macro_rules! limit {
    (count $matched_logs_count_limit:expr) => {
      Some(
        bd_proto::protos::workflow::workflow::workflow::LimitMatchedLogsCount {
          count: $matched_logs_count_limit,
          ..Default::default()
        },
      )
      .into()
    };
    (seconds $seconds:expr) => {
      Some(
        bd_proto::protos::workflow::workflow::workflow::LimitDuration {
          duration_ms: $seconds * 1_000,
          ..Default::default()
        },
      )
      .into()
    };
  }

  /// A macro that takes a matcher and creates a rule to use when
  /// to create a transition for moving to another state.
  #[macro_export]
  macro_rules! rule {
    ($matcher:expr) => {
      $crate::workflow::make_log_match_rule($matcher, 1)
    };
    ($matcher:expr; times $count:expr) => {
      $crate::workflow::make_log_match_rule($matcher, $count)
    };
  }

  /// A macro that takes a list of matchers and returns a matcher
  /// that matches if all of the matchers match.
  #[macro_export]
  macro_rules! all {
    ($($e:expr,)+) => {
      $crate::workflow::make_and_matcher(vec![$($e,)+])
    }
  }

  #[macro_export]
  /// A macro that takes a list of matchers and returns a matcher
  /// that matches if any of the matchers match.
  macro_rules! any {
    ($($e:expr,)+) => {
      $crate::workflow::make_or_matcher(vec![$($e,)+])
    }
  }

  #[macro_export]
  /// Return the invert of a given predicate.
  macro_rules! not {
    ($e:expr) => {
      $crate::workflow::make_not_matcher($e)
    };
  }

  /// Creates a matcher that pattern matches on the lhs and the
  /// operator in order to produce a more human readable way
  /// to express a matcher.
  #[macro_export]
  macro_rules! log_matches {
    (message == $message:expr) => {{
      use bd_proto::protos::log_matcher::log_matcher;
      $crate::workflow::make_log_message_matcher(
        $message,
        log_matcher::log_matcher::base_log_matcher::Operator::OPERATOR_EQUALS,
      )
    }};
    (message ~ = $message:expr) => {{
      use bd_proto::protos::log_matcher::log_matcher;
      $crate::workflow::make_log_message_matcher(
        $message,
        log_matcher::log_matcher::base_log_matcher::Operator::OPERATOR_REGEX,
      )
    }};
    (tag($name:expr) == $value:expr) => {
      $crate::workflow::make_log_tag_matcher($name, $value)
    };
  }

  /// Creates an action to be used with state
  /// transitions.
  #[macro_export]
  macro_rules! action {
    (flush_buffers $e:expr; id $id:expr) => {
      $crate::workflow::make_flush_buffers_action($e, None, $id)
    };
    (flush_buffers $trigger_buffer_ids:expr;
      continue_streaming_to $continuous_buffer_ids:expr;
      logs_count $max_logs_count:expr; id $id:expr) => {
      $crate::workflow::make_flush_buffers_action(
        $trigger_buffer_ids,
        Some(($continuous_buffer_ids, $max_logs_count)),
        $id
      )
    };
    (emit_counter $id:expr; value $value:expr) => {
      $crate::workflow::make_emit_metric_action(
        $id,
        bd_proto::protos::workflow::workflow::workflow::action
          ::action_emit_metric::Metric_type::Counter(
            bd_proto::protos::workflow::workflow::workflow::action::action_emit_metric
              ::Counter::default()
          ),
        $value,
        vec![]
      )
    };
    (emit_counter $id:expr; value $value:expr; tags { $($tag:expr),+ }) => {
      $crate::workflow::make_emit_metric_action(
        $id,
        bd_proto::protos::workflow::workflow::workflow::action
          ::action_emit_metric::Metric_type::Counter(
            bd_proto::protos::workflow::workflow::workflow::action::action_emit_metric
              ::Counter::default()
          ),
        $value,
        vec![$($tag,)+]
      )
    };
    (emit_histogram $id:expr; value $value:expr; tags { $($tag:expr),+ }) => {
      $crate::workflow::make_emit_metric_action(
        $id,
        bd_proto::protos::workflow::workflow::workflow::action
          ::action_emit_metric::Metric_type::Histogram(
            bd_proto::protos::workflow::workflow::workflow::action::action_emit_metric
              ::Histogram::default()
          ),
        $value,
        vec![$($tag,)+]
      )
    };
    (emit_sankey $id:expr; limit $limit:expr; tags { $($tag:expr),+ }) => {
      $crate::workflow::make_emit_sankey_action(
        $id,
        $limit,
        vec![$($tag,)+]
      )
    };
    (screenshot $id:expr) => {
      $crate::workflow::make_take_screenshot_action($id)
    };
    (generate_log $action:expr) => {
      $action
    };
  }

  /// Creates metric value.
  #[macro_export]
  macro_rules! metric_value {
    ($value:expr) => {
      bd_proto::protos::workflow::workflow::workflow::action::action_emit_metric
                                                        ::Value_extractor_type::Fixed(
                                                          $value
                                                      )
    };
    (extract $from:expr) => {
      bd_proto::protos::workflow::workflow::workflow::action::action_emit_metric
                                                          ::Value_extractor_type::FieldExtracted(
                                                            bd_proto::protos::workflow::workflow
                                                              ::workflow::FieldExtracted {
                                                                field_name: $from.to_string(),
                                                                ..Default::default()
                                                              }
                                                        )
    };
  }

  /// Creates metric tag.
  #[macro_export]
  macro_rules! metric_tag {
    (extract $from:expr => $to:expr) => {
      bd_proto::protos::workflow::workflow::workflow::action::Tag {
                          name: $to.into(),
                          tag_type: Some(bd_proto::protos::workflow::workflow::workflow
                            ::action::tag::Tag_type::FieldExtracted(
                              bd_proto::protos::workflow::workflow
                              ::workflow::FieldExtracted {
                                field_name: $from.into(),
                                extraction_type: Some(bd_proto::protos::workflow::workflow
                                  ::workflow::field_extracted::Extraction_type::Exact(
                                    bd_proto::protos::workflow::workflow::workflow::field_extracted
                                    ::Exact::default(),
                                  )
                                ),
                                ..Default::default()
                              },
                            )
                          ),
                          ..Default::default()
                        }
    };
    (fix $key:expr => $value:expr) => {
      bd_proto::protos::workflow::workflow::workflow::action::Tag {
                          name: $key.into(),
                          tag_type: Some(bd_proto::protos::workflow::workflow::workflow
                            ::action::tag::Tag_type::FixedValue($value.into())),
                          ..Default::default()
                        }
    };
  }

  /// Creates a Sankey value extraction extension.
  #[macro_export]
  macro_rules! sankey_value {
    (fixed $sankey_id:expr => $value:expr, counts_toward_limit $counts_toward_limit:expr)
      => {
      $crate::workflow::make_sankey_extraction(
                          $sankey_id,
                          $counts_toward_limit,
                          bd_proto::protos::workflow::workflow::workflow::transition_extension
                          ::sankey_diagram_value_extraction::Value_type::Fixed($value.to_string())
                        )
    };
    (extract_field $sankey_id:expr => $field_name:expr,
      counts_toward_limit $counts_toward_limit:expr
    )
      => {
      $crate::workflow::make_sankey_extraction(
        $sankey_id,
        $counts_toward_limit,
        $crate::workflow::make_sankey_value_field_extracted($field_name),
      )
    };
  }

  use bd_proto::protos::workflow::workflow::workflow::{Action as ActionProto, TransitionTimeout};
  use time::Duration;
  #[allow(clippy::module_name_repetitions)]
  pub use {
    action,
    all,
    any,
    limit,
    log_matches,
    metric_tag,
    metric_value,
    not,
    rule,
    sankey_value,
    workflow_proto,
  };
}

pub fn make_workflow_config_proto(
  id: &str,
  execution: bd_proto::protos::workflow::workflow::workflow::execution::Execution_type,
  matched_logs_count_limit: protobuf::MessageField<
    protos::workflow::workflow::workflow::LimitMatchedLogsCount,
  >,
  duration_limit: protobuf::MessageField<protos::workflow::workflow::workflow::LimitDuration>,
  states: Vec<State>,
) -> protos::workflow::workflow::Workflow {
  protos::workflow::workflow::Workflow {
    id: id.to_string(),
    states,
    execution: protobuf::MessageField::from_option(Some(ExecutionProto {
      execution_type: Some(execution),
      ..Default::default()
    })),
    limit_matched_logs_count: matched_logs_count_limit,
    limit_duration: duration_limit,
    ..Default::default()
  }
}

pub fn add_transition(
  from_state: &mut State,
  to_state: &State,
  rule: Rule,
  actions: &[Action_type],
  extensions: Vec<TransitionExtension>,
) {
  let mut transitions = from_state.transitions.clone();
  transitions.push(protos::workflow::workflow::workflow::Transition {
    target_state_id: to_state.id.clone(),
    rule: protobuf::MessageField::from_option(Some(rule)),
    actions: actions
      .iter()
      .map(|a| ActionProto {
        action_type: Some(a.clone()),
        ..Default::default()
      })
      .collect(),
    extensions,
    ..Default::default()
  });

  from_state.transitions = transitions;
}

#[must_use]
pub fn make_flush_buffers_action(
  buffer_ids: &[&str],
  streaming_config: Option<(Vec<&str>, u64)>,
  id: &str,
) -> Action_type {
  Action_type::ActionFlushBuffers(ActionFlushBuffersProto {
    id: id.to_string(),
    buffer_ids: buffer_ids.iter().map(|&s| s.into()).collect(),
    streaming: streaming_config.map_or(MessageField::none(), |(ids, max_logs_count)| {
      MessageField::some({
        Streaming {
          destination_streaming_buffer_ids: ids.iter().map(ToString::to_string).collect(),
          termination_criteria: vec![TerminationCriterion {
            type_: Some(termination_criterion::Type::LogsCount(
              termination_criterion::LogsCount {
                max_logs_count,
                ..Default::default()
              },
            )),
            ..Default::default()
          }],
          ..Default::default()
        }
      })
    }),
    ..Default::default()
  })
}

#[must_use]
pub fn make_emit_sankey_action(id: &str, limit: u32, tags: Vec<Tag>) -> Action_type {
  Action_type::ActionEmitSankeyDiagram(ActionEmitSankeyDiagramProto {
    id: id.to_string(),
    limit,
    tags,
    ..Default::default()
  })
}

#[must_use]
pub fn make_take_screenshot_action(id: &str) -> Action_type {
  Action_type::ActionTakeScreenshot(ActionTakeScreenshotProto {
    id: id.to_string(),
    ..Default::default()
  })
}

#[must_use]
pub fn make_save_field_extraction(id: &str, field_name: &str) -> TransitionExtension {
  TransitionExtension {
    extension_type: Some(Extension_type::SaveField(SaveField {
      id: id.to_string(),
      field_name: field_name.to_string(),
      ..Default::default()
    })),
    ..Default::default()
  }
}

#[must_use]
pub fn make_save_timestamp_extraction(id: &str) -> TransitionExtension {
  TransitionExtension {
    extension_type: Some(Extension_type::SaveTimestamp(SaveTimestamp {
      id: id.to_string(),
      ..Default::default()
    })),
    ..Default::default()
  }
}

#[must_use]
pub fn make_sankey_extraction(
  id: &str,
  counts_toward_sankey_extraction_limit: bool,
  value: sankey_diagram_value_extraction::Value_type,
) -> TransitionExtension {
  TransitionExtension {
    extension_type: Some(Extension_type::SankeyDiagramValueExtraction(
      SankeyDiagramValueExtraction {
        sankey_diagram_id: id.to_string(),
        counts_toward_sankey_extraction_limit,
        value_type: Some(value),
        ..Default::default()
      },
    )),
    ..Default::default()
  }
}

#[must_use]
pub fn make_sankey_value_field_extracted(
  field_name: &str,
) -> sankey_diagram_value_extraction::Value_type {
  sankey_diagram_value_extraction::Value_type::FieldExtracted(FieldExtracted {
    field_name: field_name.to_string(),
    extraction_type: Some(Extraction_type::Exact(Exact::default())),
    ..Default::default()
  })
}

#[must_use]
pub fn make_emit_metric_action(
  id: &str,
  metric_type: action_emit_metric::Metric_type,
  value: Value_extractor_type,
  tags: Vec<Tag>,
) -> Action_type {
  Action_type::ActionEmitMetric(ActionEmitMetricProto {
    id: id.to_string(),
    tags,
    metric_type: Some(metric_type),
    value_extractor_type: Some(value),
    ..Default::default()
  })
}

#[must_use]
pub fn make_and_matcher(matchers: Vec<LogMatcher>) -> LogMatcher {
  LogMatcher {
    matcher: Some(Matcher::AndMatcher(MatcherList {
      log_matchers: matchers,
      ..Default::default()
    })),
    ..Default::default()
  }
}

#[must_use]
pub fn make_or_matcher(matchers: Vec<LogMatcher>) -> LogMatcher {
  LogMatcher {
    matcher: Some(Matcher::OrMatcher(MatcherList {
      log_matchers: matchers,
      ..Default::default()
    })),
    ..Default::default()
  }
}

#[must_use]
pub fn make_not_matcher(matcher: LogMatcher) -> LogMatcher {
  LogMatcher {
    matcher: Some(Matcher::NotMatcher(Box::new(matcher))),
    ..Default::default()
  }
}

pub fn make_log_match_rule(matcher: LogMatcher, count: u32) -> Rule {
  Rule {
    rule_type: Some(Rule_type::RuleLogMatch(
      protos::workflow::workflow::workflow::RuleLogMatch {
        log_matcher: protobuf::MessageField::from_option(Some(matcher)),
        count,
        ..Default::default()
      },
    )),
    ..Default::default()
  }
}

#[must_use]
pub fn make_log_message_matcher(
  value: &str,
  operator: log_matcher::base_log_matcher::Operator,
) -> LogMatcher {
  LogMatcher {
    matcher: Some(Matcher::BaseMatcher(BaseLogMatcher {
      match_type: Some(MessageMatch(base_log_matcher::MessageMatch {
        string_value_match: protobuf::MessageField::from_option(Some(
          base_log_matcher::StringValueMatch {
            operator: operator.into(),
            string_value_match_type: Some(String_value_match_type::MatchValue(value.to_string())),
            ..Default::default()
          },
        )),
        ..Default::default()
      })),
      ..Default::default()
    })),
    ..Default::default()
  }
}

#[must_use]
pub fn make_log_tag_matcher(name: &str, value: &str) -> LogMatcher {
  LogMatcher {
    matcher: Some(Matcher::BaseMatcher(BaseLogMatcher {
      match_type: Some(TagMatch(base_log_matcher::TagMatch {
        tag_key: name.to_string(),
        value_match: Some(Value_match::StringValueMatch(
          base_log_matcher::StringValueMatch {
            operator: log_matcher::base_log_matcher::Operator::OPERATOR_EQUALS.into(),
            string_value_match_type: Some(String_value_match_type::MatchValue(value.to_string())),
            ..Default::default()
          },
        )),
        ..Default::default()
      })),
      ..Default::default()
    })),
    ..Default::default()
  }
}

#[allow(clippy::needless_pass_by_value)]
#[must_use]
pub fn make_tags(labels: BTreeMap<String, String>) -> LogFields {
  labels
    .into_iter()
    .map(|(key, value)| (key.into(), StringOrBytes::String(value)))
    .collect()
}

pub enum TestFieldRef {
  Fixed(&'static str),
  FieldFromCurrentLog(&'static str),
  SavedFieldId(&'static str),
  SavedTimestampId(&'static str),
  Uuid,
}

pub enum TestFieldType {
  Single(TestFieldRef),
  Subtract(TestFieldRef, TestFieldRef),
  Add(TestFieldRef, TestFieldRef),
  Multiply(TestFieldRef, TestFieldRef),
  Divide(TestFieldRef, TestFieldRef),
}

#[must_use]
pub fn make_generate_log_action(
  message: &'static str,
  fields: &'static [(&'static str, TestFieldType)],
  id: &str,
  log_type: LogType,
) -> ActionGenerateLog {
  fn make_field_ref(test_field_ref: &TestFieldRef) -> ValueReference {
    ValueReference {
      value_reference_type: Some(match test_field_ref {
        TestFieldRef::Fixed(value) => Value_reference_type::Fixed((*value).to_string()),
        TestFieldRef::FieldFromCurrentLog(field_name) => {
          Value_reference_type::FieldFromCurrentLog((*field_name).to_string())
        },
        TestFieldRef::SavedFieldId(saved_field_id) => {
          Value_reference_type::SavedFieldId((*saved_field_id).to_string())
        },
        TestFieldRef::SavedTimestampId(saved_timestamp_id) => {
          Value_reference_type::SavedTimestampId((*saved_timestamp_id).to_string())
        },
        TestFieldRef::Uuid => Value_reference_type::Uuid(true),
      }),
      ..Default::default()
    }
  }

  ActionGenerateLog {
    id: id.to_string(),
    message: message.to_string(),
    fields: fields
      .iter()
      .map(|(name, field_type)| {
        let generated_field_value_type = match field_type {
          TestFieldType::Single(field_ref) => {
            Generated_field_value_type::Single(make_field_ref(field_ref))
          },
          TestFieldType::Subtract(lhs, rhs) => {
            Generated_field_value_type::Subtract(ValueReferencePair {
              lhs: Some(make_field_ref(lhs)).into(),
              rhs: Some(make_field_ref(rhs)).into(),
              ..Default::default()
            })
          },
          TestFieldType::Add(lhs, rhs) => Generated_field_value_type::Add(ValueReferencePair {
            lhs: Some(make_field_ref(lhs)).into(),
            rhs: Some(make_field_ref(rhs)).into(),
            ..Default::default()
          }),
          TestFieldType::Multiply(lhs, rhs) => {
            Generated_field_value_type::Multiply(ValueReferencePair {
              lhs: Some(make_field_ref(lhs)).into(),
              rhs: Some(make_field_ref(rhs)).into(),
              ..Default::default()
            })
          },
          TestFieldType::Divide(lhs, rhs) => {
            Generated_field_value_type::Divide(ValueReferencePair {
              lhs: Some(make_field_ref(lhs)).into(),
              rhs: Some(make_field_ref(rhs)).into(),
              ..Default::default()
            })
          },
        };
        GeneratedField {
          name: (*name).to_string(),
          generated_field_value_type: Some(generated_field_value_type),
          ..Default::default()
        }
      })
      .collect(),
    log_type: log_type.0,
    ..Default::default()
  }
}

#[must_use]
pub fn make_generate_log_action_proto(
  message: &'static str,
  fields: &'static [(&'static str, TestFieldType)],
  id: &str,
  log_type: LogType,
) -> Action_type {
  Action_type::ActionGenerateLog(make_generate_log_action(message, fields, id, log_type))
}
