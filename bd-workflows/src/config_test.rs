// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used, clippy::indexing_slicing)]

use super::{ActionEmitMetric, JsonFieldExtraction, TagValue, ValueIncrement};
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
use key_or_index::Key_or_index;

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
