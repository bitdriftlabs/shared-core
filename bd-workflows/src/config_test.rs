// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{ActionEmitMetric, JsonFieldExtraction, SankeyExtraction, TagValue, ValueIncrement};
use bd_log_matcher::matcher::JsonPathToken;
use bd_proto::protos::value_matcher::value_matcher::json_path_value_match::{
  KeyOrIndex,
  key_or_index,
};
use bd_proto::protos::workflow::workflow::workflow::FieldExtracted;
use bd_proto::protos::workflow::workflow::workflow::action::action_emit_metric::{
  Counter,
  Metric_type,
  Value_extractor_type,
};
use bd_proto::protos::workflow::workflow::workflow::action::tag::Tag_type;
use bd_proto::protos::workflow::workflow::workflow::action::{
  ActionEmitMetric as ProtoMetric,
  Tag,
};
use bd_proto::protos::workflow::workflow::workflow::field_extracted::{Extraction_type, JsonPath};
use bd_proto::protos::workflow::workflow::workflow::transition_extension::{
  SankeyDiagramValueExtraction,
  sankey_diagram_value_extraction::Value_type,
};
use key_or_index::Key_or_index;

#[test]
fn sankey_log_body_selector_uses_metric_extractor() {
  for marker in [true, false] {
    let extraction = SankeyExtraction::new(
      &SankeyDiagramValueExtraction {
        sankey_diagram_id: "sankey".into(),
        value_type: Some(Value_type::LogBodyExtracted(marker)),
        counts_toward_sankey_extraction_limit: true,
        ..Default::default()
      },
      10,
    )
    .unwrap();
    assert_eq!(extraction.value, TagValue::LogBodyExtract);
    assert_eq!(extraction.limit, 10);
    assert!(extraction.counts_toward_sankey_values_extraction_limit);
  }
}

#[test]
fn compiles_json_field_extraction_for_tags_and_values() {
  let extracted = FieldExtracted {
    field_name: "payload".into(),
    extraction_type: Some(Extraction_type::JsonPath(JsonPath {
      key_or_index: vec![KeyOrIndex {
        key_or_index: Some(Key_or_index::Key("value".into())),
        ..Default::default()
      }],
      ..Default::default()
    })),
    ..Default::default()
  };
  let metric = ActionEmitMetric::new(ProtoMetric {
    id: "metric".into(),
    metric_type: Some(Metric_type::Counter(Counter::default())),
    value_extractor_type: Some(Value_extractor_type::FieldExtracted(extracted.clone())),
    tags: vec![Tag {
      name: "value".into(),
      tag_type: Some(Tag_type::FieldExtracted(extracted)),
      ..Default::default()
    }],
    ..Default::default()
  })
  .unwrap();
  let expected = JsonFieldExtraction {
    field_name: "payload".into(),
    path: vec![JsonPathToken::Key("value".into())],
  };
  assert_eq!(
    metric.increment,
    ValueIncrement::JsonExtract(expected.clone())
  );
  assert_eq!(metric.tags["value"], TagValue::JsonExtract(expected));
}

#[test]
fn rejects_json_extraction_with_unset_path_segment() {
  let extracted = FieldExtracted {
    field_name: "payload".into(),
    extraction_type: Some(Extraction_type::JsonPath(JsonPath {
      key_or_index: vec![KeyOrIndex::default()],
      ..Default::default()
    })),
    ..Default::default()
  };
  assert!(TagValue::from_field_extracted(&extracted).is_err());
}

#[test]
fn state_change_match_accepts_only_feature_flag_scope() {
  use super::{StateChangeMatch, parse_state_scope};
  use bd_proto::protos::state::matcher::StateValueMatch;
  use bd_proto::protos::state::matcher::state_value_match::Value_match;
  use bd_proto::protos::state::scope::StateScope;
  use bd_proto::protos::value_matcher::value_matcher::IsSetMatch;
  use bd_proto::protos::workflow::workflow::workflow::RuleStateChangeMatch;

  let rule = |scope: i32| RuleStateChangeMatch {
    scope: protobuf::EnumOrUnknown::from_i32(scope),
    key: "key".into(),
    new_value: Some(StateValueMatch {
      value_match: Some(Value_match::IsSetMatch(IsSetMatch::default())),
      ..Default::default()
    })
    .into(),
    ..Default::default()
  };

  assert!(StateChangeMatch::try_from_proto(&rule(StateScope::FEATURE_FLAG as i32)).is_ok());
  assert!(parse_state_scope(StateScope::FEATURE_FLAG).is_ok());
  assert!(parse_state_scope(StateScope::UNSPECIFIED).is_err());
  assert!(parse_state_scope(StateScope::SYSTEM).is_err());
  // 2, 4 and 5 are the reserved global state, custom field and SDK-owned field scopes.
  for scope in [0, 2, 3, 4, 5] {
    assert!(StateChangeMatch::try_from_proto(&rule(scope)).is_err());
  }
}
