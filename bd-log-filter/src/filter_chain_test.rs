// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::FilterChain;
use bd_log_matcher::builder::{message_equals, message_regex_matches};
use bd_log_primitives::{DataValue, Log, LogFields, log_level};
use bd_proto::protos::filter::filter::{Filter, FiltersConfiguration};
use bd_proto::protos::logging::payload::LogType;
use bd_test_helpers::filter::macros::{
  regex_match_and_substitute_field,
  regex_match_and_substitute_global,
  regex_match_and_substitute_message,
};
use bd_test_helpers::{capture_field, field_value, remove_field, set_field};
use pretty_assertions::assert_eq;
use time::macros::datetime;

#[test]
fn filters_are_not_applied_to_non_matching_logs_only() {
  let filter_chain = make_filter_chain(
    message_equals("matching"),
    vec![capture_field!(single "foo")],
  );

  let fields = LogFields::default();
  let matching_fields: LogFields = [("foo".into(), "bar".into())].into();

  // Filter's transform are not applied to logs that don't match filter's matcher.
  let mut log = make_log("not matching", fields.clone(), matching_fields.clone());
  filter_chain.process(&mut log, &state_reader());
  assert_eq!(log, make_log("not matching", fields, matching_fields));
}

#[test]
fn filter_transforms_are_applied_in_order() {
  let filter_chain = make_filter_chain(
    message_equals("matching"),
    vec![
      set_field!(matching("foo") = field_value!("bar")),
      capture_field!(single "foo"),
    ],
  );

  let mut log = make_log("matching", [].into(), [].into());
  filter_chain.process(&mut log, &state_reader());
  assert_eq!(
    log,
    make_log(
      "matching",
      [("foo".into(), "bar".into(),)].into(),
      [].into()
    )
  );
}

#[test]
fn filters_are_applied_in_order() {
  let (filter_chain, _) = FilterChain::new(FiltersConfiguration {
    filters: vec![
      Filter {
        matcher: Some(message_equals("matching")).into(),
        transforms: vec![set_field!(matching("foo") = field_value!("bar"))],
        ..Default::default()
      },
      Filter {
        matcher: Some(message_equals("matching")).into(),
        transforms: vec![capture_field!(single "foo")],
        ..Default::default()
      },
    ],
    ..Default::default()
  });

  let mut log = make_log("matching", [].into(), [].into());
  filter_chain.process(&mut log, &state_reader());
  assert_eq!(
    log,
    make_log(
      "matching",
      LogFields::from([("foo".into(), "bar".into(),)]),
      [].into()
    )
  );
}

#[test]
fn capture_field_transform() {
  let filter_chain = make_filter_chain(
    message_equals("matching"),
    vec![capture_field!(single "foo")],
  );

  let fields = [].into();
  let matching_fields: LogFields = [("foo".into(), "bar".into())].into();

  // Filter's transform captures an existing matching field.
  let mut log = make_log("matching", [].into(), [].into());
  filter_chain.process(&mut log, &state_reader());
  assert!(log.fields.is_empty());
  assert!(log.matching_fields.is_empty());

  // Filter's transform does nothing when asked to capture a non-existing matching field.
  let mut log = make_log("matching", fields, matching_fields.clone());
  filter_chain.process(&mut log, &state_reader());
  assert_eq!(log.fields, matching_fields);
  assert!(log.matching_fields.is_empty());
}

#[test]
fn set_captured_field_transform_overrides_existing_field() {
  let filter_chain = make_filter_chain(
    message_equals("matching"),
    vec![set_field!(captured("foo") = field_value!("bar"))],
  );

  let mut log = make_log(
    "matching",
    LogFields::from([("foo".into(), "baz".into())]),
    [].into(),
  );
  filter_chain.process(&mut log, &state_reader());
  assert_eq!(LogFields::from([("foo".into(), "bar".into(),)]), log.fields);
}

#[test]
fn set_captured_field_transform_does_not_override_existing_field() {
  let filter_chain = make_filter_chain(
    message_equals("matching"),
    vec![set_field!(captured("foo") = field_value!("bar"), false)],
  );

  let mut log = make_log("matching", [("foo".into(), "baz".into())].into(), [].into());
  filter_chain.process(&mut log, &state_reader());
  assert_eq!(LogFields::from([("foo".into(), "baz".into(),)]), log.fields);
}

#[test]
fn set_captured_field_transform_adds_new_field() {
  let filter_chain = make_filter_chain(
    message_equals("matching"),
    vec![set_field!(captured("new_foo") = field_value!("bar"))],
  );

  let mut log = make_log(
    "matching",
    LogFields::from([("foo".into(), "bar".into())]),
    [].into(),
  );
  filter_chain.process(&mut log, &state_reader());
  assert_eq!(
    LogFields::from([
      ("foo".into(), "bar".into(),),
      ("new_foo".into(), "bar".into(),)
    ]),
    log.fields
  );
}

#[test]
fn set_captured_field_transform_copies_existing_field_value() {
  let filter_chain = make_filter_chain(
    message_equals("matching"),
    vec![set_field!(captured("new_foo") = field_value!(field "foo"))],
  );

  let mut log = make_log("matching", [("foo".into(), "bar".into())].into(), [].into());
  filter_chain.process(&mut log, &state_reader());
  assert_eq!(
    LogFields::from([
      ("foo".into(), "bar".into(),),
      ("new_foo".into(), "bar".into(),)
    ]),
    log.fields
  );
}

#[test]
fn set_matching_field_transform_overrides_existing_field() {
  let filter_chain = make_filter_chain(
    message_equals("matching"),
    vec![set_field!(matching("foo") = field_value!("bar"))],
  );

  let mut log = make_log("matching", [].into(), [("foo".into(), "baz".into())].into());
  filter_chain.process(&mut log, &state_reader());
  assert_eq!(
    LogFields::from([("foo".into(), "bar".into(),)]),
    log.matching_fields
  );
}

#[test]
fn set_matching_field_transform_adds_new_field() {
  let filter_chain = make_filter_chain(
    message_equals("matching"),
    vec![set_field!(matching("new_foo") = field_value!("bar"))],
  );

  let mut log = make_log("matching", [].into(), [("foo".into(), "bar".into())].into());
  filter_chain.process(&mut log, &state_reader());
  assert_eq!(
    LogFields::from([
      ("foo".into(), "bar".into(),),
      ("new_foo".into(), "bar".into(),)
    ]),
    log.matching_fields
  );
}

#[test]
fn remove_field_transform_removes_existing_fields() {
  let filter_chain =
    make_filter_chain(message_equals("matching"), vec![remove_field!("remove_me")]);

  let mut log = make_log(
    "matching",
    [
      ("remove_me".into(), "bar".into()),
      ("foo".into(), "bar".into()),
    ]
    .into(),
    [
      ("remove_me".into(), "bar".into()),
      ("foo".into(), "bar".into()),
    ]
    .into(),
  );

  filter_chain.process(&mut log, &state_reader());

  assert_eq!(LogFields::from([("foo".into(), "bar".into(),)]), log.fields);
  assert_eq!(
    LogFields::from([("foo".into(), "bar".into(),)]),
    log.matching_fields
  );
}

#[test]
fn regex_match_and_substitute() {
  let filter_chain: FilterChain = make_filter_chain(
    message_equals("matching"),
    vec![
      regex_match_and_substitute_field!(
        "foo",
        "[0-9a-f]{8}(?:-|_)?[0-9a-f]{4}(?:-|_)?[0-9a-f]{4}(?:-|_)?[0-9a-f]{4}(?:-|_)?[0-9a-f]{12}",
        "<id>"
      ),
      regex_match_and_substitute_field!(
        "bar",
        "[0-9a-f]{8}(?:-|_)?[0-9a-f]{4}(?:-|_)?[0-9a-f]{4}(?:-|_)?[0-9a-f]{4}(?:-|_)?[0-9a-f]{12}",
        "${1}<id>${3}"
      ),
    ],
  );

  let mut log = make_log(
    "matching",
    [
      (
        "foo".into(),
        "/foo/885fa9b2-97f1-435b-8fe3-a461d3235924/test/885fa9b2-97f1-435b-8fe3-a461d3235924"
          .into(),
      ),
      ("bar".into(), "/885fa9b2-97f1-435b-8fe3-a461d3235924".into()),
    ]
    .into(),
    [].into(),
  );

  filter_chain.process(&mut log, &state_reader());

  assert_eq!(
    LogFields::from([
      ("foo".into(), "/foo/<id>/test/<id>".into(),),
      ("bar".into(), "/<id>".into(),)
    ]),
    log.fields
  );
}

#[test]
fn regex_match_and_substitute_updates_named_field_in_all_field_sets() {
  let filter_chain: FilterChain = make_filter_chain(
    message_equals("matching"),
    vec![regex_match_and_substitute_field!(
      "foo",
      "[0-9a-f]{8}(?:-|_)?[0-9a-f]{4}(?:-|_)?[0-9a-f]{4}(?:-|_)?[0-9a-f]{4}(?:-|_)?[0-9a-f]{12}",
      "<id>"
    )],
  );

  let mut log = make_log(
    "matching",
    [(
      "foo".into(),
      "/captured/885fa9b2-97f1-435b-8fe3-a461d3235924".into(),
    )]
    .into(),
    [(
      "foo".into(),
      "/matching/885fa9b2-97f1-435b-8fe3-a461d3235924".into(),
    )]
    .into(),
  );

  filter_chain.process(&mut log, &state_reader());

  assert_eq!(
    LogFields::from([("foo".into(), "/captured/<id>".into(),)]),
    log.fields
  );
  assert_eq!(
    LogFields::from([("foo".into(), "/matching/<id>".into(),)]),
    log.matching_fields
  );
}

#[test]
fn regex_match_and_substitute_named_field_reuses_storage_when_no_match() {
  let filter_chain: FilterChain = make_filter_chain(
    message_equals("matching"),
    vec![regex_match_and_substitute_field!(
      "foo",
      "uuid-[0-9]+",
      "<id>"
    )],
  );

  let original = String::from("plain-text");
  let original_ptr = original.as_ptr();
  let mut log = make_log(
    "matching",
    [("foo".into(), DataValue::String(original))].into(),
    [].into(),
  );

  filter_chain.process(&mut log, &state_reader());

  let value = log.fields.get("foo").and_then(|value| match value {
    DataValue::String(value) => Some(value),
    _ => None,
  });
  assert_eq!(value.map(String::as_str), Some("plain-text"));
  assert_eq!(
    value.map(|value| value.as_str().as_ptr()),
    Some(original_ptr)
  );
}

#[test]
fn regex_match_and_substitute_with_empty_field_name_applies_to_all_fields() {
  let filter_chain: FilterChain = make_filter_chain(
    message_equals("matching"),
    vec![regex_match_and_substitute_global!(
      "[0-9a-f]{8}(?:-|_)?[0-9a-f]{4}(?:-|_)?[0-9a-f]{4}(?:-|_)?[0-9a-f]{4}(?:-|_)?[0-9a-f]{12}",
      "<id>"
    )],
  );

  let mut log = make_log(
    "matching",
    [
      (
        "foo".into(),
        "/foo/885fa9b2-97f1-435b-8fe3-a461d3235924/test".into(),
      ),
      ("untouched".into(), "plain-text".into()),
    ]
    .into(),
    [(
      "bar".into(),
      "885fa9b2-97f1-435b-8fe3-a461d3235924/suffix".into(),
    )]
    .into(),
  );

  filter_chain.process(&mut log, &state_reader());

  assert_eq!(
    LogFields::from([
      ("foo".into(), "/foo/<id>/test".into(),),
      ("untouched".into(), "plain-text".into(),)
    ]),
    log.fields
  );
  assert_eq!(
    LogFields::from([("bar".into(), "<id>/suffix".into(),)]),
    log.matching_fields
  );
}

#[test]
fn regex_match_and_substitute_global_respects_ignored_fields_and_message_body() {
  let filter_chain: FilterChain = make_filter_chain(
    message_equals("message 885fa9b2-97f1-435b-8fe3-a461d3235924"),
    vec![regex_match_and_substitute_global!(
      "[0-9a-f]{8}(?:-|_)?[0-9a-f]{4}(?:-|_)?[0-9a-f]{4}(?:-|_)?[0-9a-f]{4}(?:-|_)?[0-9a-f]{12}",
      "<id>",
      ignore = ["untouched"]
    )],
  );

  let mut log = make_log(
    "message 885fa9b2-97f1-435b-8fe3-a461d3235924",
    [
      (
        "foo".into(),
        "captured-885fa9b2-97f1-435b-8fe3-a461d3235924".into(),
      ),
      (
        "untouched".into(),
        "keep-885fa9b2-97f1-435b-8fe3-a461d3235924".into(),
      ),
    ]
    .into(),
    [(
      "bar".into(),
      "matching-885fa9b2-97f1-435b-8fe3-a461d3235924".into(),
    )]
    .into(),
  );

  filter_chain.process(&mut log, &state_reader());

  assert_eq!(log.message, "message <id>".into());
  assert_eq!(
    log.fields,
    [
      ("foo".into(), "captured-<id>".into()),
      (
        "untouched".into(),
        "keep-885fa9b2-97f1-435b-8fe3-a461d3235924".into(),
      ),
    ]
    .into()
  );
  assert_eq!(
    log.matching_fields,
    [("bar".into(), "matching-<id>".into())].into()
  );
}

#[test]
fn regex_match_and_substitute_message_body_only_updates_message() {
  let filter_chain: FilterChain = make_filter_chain(
    message_equals("message 885fa9b2-97f1-435b-8fe3-a461d3235924"),
    vec![regex_match_and_substitute_message!(
      "[0-9a-f]{8}(?:-|_)?[0-9a-f]{4}(?:-|_)?[0-9a-f]{4}(?:-|_)?[0-9a-f]{4}(?:-|_)?[0-9a-f]{12}",
      "<id>"
    )],
  );

  let mut log = make_log(
    "message 885fa9b2-97f1-435b-8fe3-a461d3235924",
    [(
      "foo".into(),
      "field-885fa9b2-97f1-435b-8fe3-a461d3235924".into(),
    )]
    .into(),
    [(
      "bar".into(),
      "field-885fa9b2-97f1-435b-8fe3-a461d3235924".into(),
    )]
    .into(),
  );

  filter_chain.process(&mut log, &state_reader());

  assert_eq!(log.message, "message <id>".into());
  assert_eq!(
    log.fields,
    [(
      "foo".into(),
      "field-885fa9b2-97f1-435b-8fe3-a461d3235924".into()
    )]
    .into()
  );
  assert_eq!(
    log.matching_fields,
    [(
      "bar".into(),
      "field-885fa9b2-97f1-435b-8fe3-a461d3235924".into()
    )]
    .into()
  );
}

#[test]
fn regex_match_and_invalid_substitute() {
  let filter_chain = make_filter_chain(
    message_equals("matching"),
    vec![regex_match_and_substitute_field!(
      "foo",
      "^(.*)([0-9a-f]{8}(?:-|_)?[0-9a-f]{4}(?:-|_)?[0-9a-f]{4}(?:-|_)?[0-9a-f]{4}(?:-|_)?\
       [0-9a-f]{12})(.*)$",
      "${1}<id>${2}${4}"
    )],
  );

  let mut log = make_log(
    "matching",
    [(
      "foo".into(),
      "/foo/885fa9b2-97f1-435b-8fe3-a461d3235924/test/885fa9b2-97f1-435b-8fe3-a461d3235924".into(),
    )]
    .into(),
    [].into(),
  );

  filter_chain.process(&mut log, &state_reader());

  assert_eq!(
    LogFields::from([(
      "foo".into(),
      "/foo/885fa9b2-97f1-435b-8fe3-a461d3235924/test/<id>885fa9b2-97f1-435b-8fe3-a461d3235924"
        .into(),
    )]),
    log.fields
  );
}

#[test]
fn invalid_regex_match() {
  let filter_chain = make_filter_chain(
    message_equals("matching"),
    vec![regex_match_and_substitute_field!(
      "foo",
      "([])([])([])", // invalid regex
      "${1}<id>${2}${4}"
    )],
  );

  assert!(filter_chain.filters.is_empty());
}

#[test]
fn extracts_message_portion_and_creates_field_with_it() {
  let filter_chain = make_filter_chain(
    message_regex_matches("^I like"),
    vec![
      set_field!(captured("fruit") = field_value!(field "_message")),
      regex_match_and_substitute_field!("fruit", "I like ()", "${1}"),
    ],
  );

  let mut log = make_log("I like apple", [].into(), [].into());

  filter_chain.process(&mut log, &state_reader());

  assert_eq!(log.fields, [("fruit".into(), "apple".into(),)].into());
}

#[test]
fn copies_log_level_and_log_type() {
  let filter_chain = make_filter_chain(
    message_regex_matches("foo"),
    vec![
      set_field!(captured("new_log_level") = field_value!(field "log_level")),
      set_field!(captured("new_log_type") = field_value!(field "log_type")),
    ],
  );

  let mut log = make_log("foo", [].into(), [].into());

  filter_chain.process(&mut log, &state_reader());

  assert_eq!(
    log.fields,
    [
      ("new_log_level".into(), "1".into(),),
      ("new_log_type".into(), "0".into(),)
    ]
    .into()
  );
}

fn make_filter_chain(
  matcher: bd_proto::protos::log_matcher::log_matcher::LogMatcher,
  transforms: std::vec::Vec<bd_proto::protos::filter::filter::filter::Transform>,
) -> FilterChain {
  FilterChain::new(FiltersConfiguration {
    filters: vec![Filter {
      matcher: Some(matcher).into(),
      transforms,
      ..Default::default()
    }],
    ..Default::default()
  })
  .0
}

fn make_log(message: &str, fields: LogFields, matching_fields: LogFields) -> Log {
  Log {
    log_level: log_level::DEBUG,
    log_type: LogType::NORMAL,
    message: message.into(),
    fields,
    matching_fields,
    session_id: "session_id".into(),
    occurred_at: datetime!(2020-01-01 0:00 UTC),
    capture_session: None,
  }
}

fn state_reader() -> bd_state::InMemoryStateReader {
  bd_state::InMemoryStateReader::default()
}
