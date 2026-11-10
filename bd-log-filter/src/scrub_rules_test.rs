// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{FilterChain, ScrubRules};
use bd_log_matcher::builder::message_equals;
use bd_log_primitives::{DataValue, Log, LogFields, log_level};
use bd_proto::protos::filter::filter::{Filter, FiltersConfiguration};
use bd_proto::protos::filter::scrub::{ScrubConfiguration, ScrubRule};
use bd_proto::protos::logging::payload::LogType;
use bd_test_helpers::filter::macros::{
  regex_match_and_substitute_field,
  regex_match_and_substitute_global,
  regex_match_and_substitute_message,
};
use bd_test_helpers::filter::scrub_rule;
use bd_test_helpers::{field_value, remove_field, set_field};
use pretty_assertions::assert_eq;
use time::macros::datetime;

#[test]
fn remove_field_removes_captured_and_matching_fields() {
  let rules = make_scrub_rules(vec![scrub_rule(remove_field!("secret"))]);

  let mut log = make_log(
    "message",
    [("secret".into(), "a".into()), ("keep".into(), "b".into())].into(),
    [("secret".into(), "c".into())].into(),
  );
  rules.process(&mut log);

  assert_eq!(
    log,
    make_log("message", [("keep".into(), "b".into())].into(), [].into())
  );
}

#[test]
fn regex_rules_scrub_fields_and_message_body() {
  let rules = make_scrub_rules(vec![
    scrub_rule(regex_match_and_substitute_field!(
      "email",
      "@.*",
      "@<redacted>"
    )),
    scrub_rule(regex_match_and_substitute_message!("\\d{4}", "<pin>")),
  ]);

  let mut log = make_log(
    "pin 1234",
    [("email".into(), "a@example.com".into())].into(),
    [("email".into(), "b@example.com".into())].into(),
  );
  rules.process(&mut log);

  assert_eq!(
    log,
    make_log(
      "pin <pin>",
      [("email".into(), "a@<redacted>".into())].into(),
      [("email".into(), "b@<redacted>".into())].into(),
    )
  );
}

#[test]
fn global_scrub_skips_ignored_fields() {
  let rules = make_scrub_rules(vec![scrub_rule(regex_match_and_substitute_global!(
    "token=\\w+",
    "token=<redacted>",
    ignore = ["safe"]
  ))]);

  let mut log = make_log(
    "token=abc",
    [
      ("url".into(), "x?token=def".into()),
      ("safe".into(), "token=ghi".into()),
    ]
    .into(),
    [].into(),
  );
  rules.process(&mut log);

  assert_eq!(
    log,
    make_log(
      "token=<redacted>",
      [
        ("url".into(), "x?token=<redacted>".into()),
        ("safe".into(), "token=ghi".into()),
      ]
      .into(),
      [].into(),
    )
  );
}

#[test]
fn rules_apply_in_order() {
  // The substitution produces a value the second rule matches, so the result depends on order.
  let rules = make_scrub_rules(vec![
    scrub_rule(regex_match_and_substitute_field!("f", "a", "b")),
    scrub_rule(regex_match_and_substitute_field!("f", "b", "c")),
  ]);

  let mut log = make_log("message", [("f".into(), "a".into())].into(), [].into());
  rules.process(&mut log);

  assert_eq!(
    log,
    make_log("message", [("f".into(), "c".into())].into(), [].into())
  );
}

#[test]
fn invalid_rules_are_counted_and_skipped() {
  let (rules, failures) = ScrubRules::new(ScrubConfiguration {
    rules: vec![
      // No rule set.
      ScrubRule::default(),
      // Invalid regex.
      scrub_rule(regex_match_and_substitute_field!("f", "(", "x")),
      scrub_rule(remove_field!("secret")),
    ],
    ..Default::default()
  });
  assert_eq!(failures, 2);
  assert_eq!(rules.rules.len(), 1);

  let mut log = make_log("message", [("secret".into(), "a".into())].into(), [].into());
  rules.process(&mut log);
  assert_eq!(log, make_log("message", [].into(), [].into()));
}

#[test]
fn filter_copying_a_scrubbed_field_copies_the_scrubbed_value() {
  // Mirrors how the logger runs scrub rules before the filter chain: a filter that copies a
  // field, such as the server-synthesized `_path_template` filter, sees the redacted value.
  let rules = make_scrub_rules(vec![scrub_rule(regex_match_and_substitute_field!(
    "_path",
    "/users/\\d+",
    "/users/<id>"
  ))]);
  let (filter_chain, failures) = FilterChain::new(FiltersConfiguration {
    filters: vec![Filter {
      matcher: Some(message_equals("request")).into(),
      transforms: vec![set_field!(
        captured("_path_template") = field_value!(field "_path")
      )],
      ..Default::default()
    }],
    ..Default::default()
  });
  assert_eq!(failures, 0);

  let mut log = make_log(
    "request",
    [("_path".into(), "/users/42".into())].into(),
    [].into(),
  );
  rules.process(&mut log);
  filter_chain.process(&mut log, &bd_state::InMemoryStateReader::default());

  assert_eq!(
    log,
    make_log(
      "request",
      [
        ("_path".into(), "/users/<id>".into()),
        ("_path_template".into(), "/users/<id>".into()),
      ]
      .into(),
      [].into(),
    )
  );
}

#[test]
fn scrub_field_matches_scrubbing_a_log() {
  let rules = make_scrub_rules(vec![
    scrub_rule(remove_field!("removed")),
    scrub_rule(regex_match_and_substitute_field!(
      "email",
      "@.*",
      "@<redacted>"
    )),
    scrub_rule(regex_match_and_substitute_global!(
      "token=\\w+",
      "token=<redacted>",
      ignore = ["safe"]
    )),
    // Message body rules do not apply to fields.
    scrub_rule(regex_match_and_substitute_message!(".*", "<message>")),
  ]);

  let fields: LogFields = [
    ("removed".into(), "a".into()),
    ("email".into(), "a@example.com token=x".into()),
    ("safe".into(), "token=y".into()),
    ("other".into(), "token=z".into()),
    ("number".into(), DataValue::U64(7)),
  ]
  .into();

  let mut log = make_log("message", fields.clone(), [].into());
  rules.process(&mut log);

  assert_eq!(rules.scrub_fields(&fields), log.fields);
  let mut scrubbed = fields;
  scrubbed.retain(|key, value| rules.scrub_field(key, value));

  assert_eq!(scrubbed, log.fields);
}

fn make_scrub_rules(rules: Vec<ScrubRule>) -> ScrubRules {
  let (rules, failures) = ScrubRules::new(ScrubConfiguration {
    rules,
    ..Default::default()
  });
  assert_eq!(failures, 0);
  rules
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
    command_attachment: None,
    capture_session: None,
  }
}
