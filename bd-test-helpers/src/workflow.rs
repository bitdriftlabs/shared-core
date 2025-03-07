// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_log_primitives::{LogField, LogFields, StringOrBytes};
use bd_proto::protos;
use bd_proto::protos::log_matcher::log_matcher::log_matcher;
use bd_proto::protos::workflow::workflow::workflow::action::action_flush_buffers::streaming::{
  termination_criterion,
  TerminationCriterion,
};
use bd_proto::protos::workflow::workflow::workflow::action::action_flush_buffers::Streaming;
use bd_proto::protos::workflow::workflow::workflow::action::Tag;
use bd_proto::protos::workflow::workflow::workflow::field_extracted::{Exact, Extraction_type};
use bd_proto::protos::workflow::workflow::workflow::transition_extension::{
  sankey_diagram_value_extraction,
  Extension_type,
  SankeyDiagramValueExtraction,
};
use bd_proto::protos::workflow::workflow::workflow::{FieldExtracted, TransitionExtension};
use protobuf::MessageField;
use protos::log_matcher::log_matcher::log_matcher::base_log_matcher::tag_match::Value_match;
use protos::log_matcher::log_matcher::log_matcher::base_log_matcher::Match_type::{
  MessageMatch,
  TagMatch,
};
use protos::log_matcher::log_matcher::log_matcher::{
  base_log_matcher,
  BaseLogMatcher,
  Matcher,
  MatcherList,
};
use protos::log_matcher::log_matcher::LogMatcher;
use protos::workflow::workflow::workflow::action::action_emit_metric::Value_extractor_type;
use protos::workflow::workflow::workflow::action::{
  action_emit_metric,
  ActionEmitMetric as ActionEmitMetricProto,
  ActionEmitSankeyDiagram as ActionEmitSankeyDiagramProto,
  ActionFlushBuffers as ActionFlushBuffersProto,
  ActionTakeScreenshot as ActionTakeScreenshotProto,
  Action_type,
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
    (exclusive with $($state:expr),+) => {
      $crate::workflow::make_workflow_config_proto(
        "workflow_id",
        bd_proto::protos::workflow::workflow::workflow::execution::Execution_type
        ::ExecutionExclusive(
          Default::default()
        ),
        Default::default(),
        Default::default(),
        vec![$($state.clone()),+],
      )
    };
    (
      exclusive with $($state:expr),+;
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
        vec![$($state.clone()),+],
      )
    };
    (parallel with $($state:expr),+) => {
      $crate::workflow::make_workflow_config_proto(
        "workflow_id",
        bd_proto::protos::workflow::workflow::workflow::execution::Execution_type
        ::ExecutionParallel(
          Default::default()
        ),
        Default::default(),
        Default::default(),
        vec![$($state.clone()),+],
      )
    };
    (
      parallel with $($state:expr),+;
      matches $matches_limit:expr;
      duration $duration_limit:expr) => {
      $crate::workflow::make_workflow_config_proto(
        "workflow_id",
        bd_proto::protos::workflow::workflow::workflow::execution::Execution_type
        ::ExecutionParallel(
          Default::default()
        ),
        $matches_limit,
        $duration_limit,
        vec![$($state.clone()),+],
      )
    };
    ($id:expr; exclusive with $($state:expr),+) => {
      $crate::workflow::make_workflow_config_proto(
        $id,
        bd_proto::protos::workflow::workflow::workflow::execution::Execution_type
        ::ExecutionExclusive(
          Default::default()
        ),
        Default::default(),
        Default::default(),
        vec![$($state.clone()),+],
      )
    };
    ($id:expr; parallel with $($state:expr),+) => {
      $crate::workflow::make_workflow_config_proto(
        $id,
        bd_proto::protos::workflow::workflow::workflow::execution::Execution_type
        ::ExecutionParallel(
          Default::default()
        ),
        Default::default(),
        Default::default(),
        vec![$($state.clone()),+],
      )
    };
  }
  /// A macro for creating workflow state with a given identifier.
  #[macro_export]
  macro_rules! state {
    ($id:expr) => {
      bd_proto::protos::workflow::workflow::workflow::State {
        id: $id.to_string(),
        ..Default::default()
      }
    };
  }

  /// A macro that creates a transition between two states.
  /// It allows to specify rules that need to be met for the transition
  /// to occur and side-effects to perform as the result of the transition.
  #[macro_export]
  macro_rules! declare_transition {
    ($from:expr => $to:expr; when $rule:expr) => {
      $crate::workflow::add_transition($from, $to, $rule, &[], vec![])
    };
    ($from:expr => $to:expr; when $rule:expr; do $($action:expr),+) => {
      $crate::workflow::add_transition($from, $to, $rule, &[$($action),+], vec![])
    };
    ($from:expr => $to:expr; when $rule:expr, with { $($extraction:expr),+ }) => {
      $crate::workflow::add_transition($from, $to, $rule, &[], vec![$($extraction),+])
    };
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
    }
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

  #[allow(clippy::module_name_repetitions)]
  pub use {
    action,
    all,
    any,
    declare_transition,
    limit,
    log_matches,
    metric_tag,
    metric_value,
    not,
    rule,
    sankey_value,
    state,
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
            match_value: value.to_string(),
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
            match_value: value.to_string(),
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
    .iter()
    .map(|elem| LogField {
      key: (*elem.0).to_string(),
      value: StringOrBytes::String((*elem.1).to_string()),
    })
    .collect()
}
