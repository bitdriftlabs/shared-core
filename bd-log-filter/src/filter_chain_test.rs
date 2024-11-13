// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::FilterChain;
use bd_log_primitives::{log_level, Log, LogField, LogFields, LogType};
use bd_proto::protos::filter::filter::{Filter, FiltersConfiguration};
use bd_test_helpers::filter::macros::regex_match_and_substitute_field;
use bd_test_helpers::{capture_field, field_value, log_matches, remove_field, set_field};
use pretty_assertions::assert_eq;
use time::macros::datetime;

#[test]
fn filters_are_not_applied_to_non_matching_logs_only() {
  let filter_chain = make_filter_chain(
    log_matches!(message == "matching"),
    vec![capture_field!(single "foo")],
  );

  let fields = vec![];
  let matching_fields = vec![LogField {
    key: "foo".to_string(),
    value: "bar".into(),
  }];

  // Filter's transform are not applied to logs that don't match filter's matcher.
  let mut log = make_log("not matching", fields.clone(), matching_fields.clone());
  filter_chain.process(&mut log);
  assert_eq!(log, make_log("not matching", fields, matching_fields));
}

#[test]
fn filter_transforms_are_applied_in_order() {
  let filter_chain = make_filter_chain(
    log_matches!(message == "matching"),
    vec![
      set_field!(matching("foo") = field_value!("bar")),
      capture_field!(single "foo"),
    ],
  );

  let mut log = make_log("matching", vec![], vec![]);
  filter_chain.process(&mut log);
  assert_eq!(
    log,
    make_log(
      "matching",
      vec![LogField {
        key: "foo".to_string(),
        value: "bar".into(),
      }],
      vec![]
    )
  );
}

#[test]
fn filters_are_applied_in_order() {
  let (filter_chain, _) = FilterChain::new(FiltersConfiguration {
    filters: vec![
      Filter {
        matcher: Some(log_matches!(message == "matching")).into(),
        transforms: vec![set_field!(matching("foo") = field_value!("bar"))],
        ..Default::default()
      },
      Filter {
        matcher: Some(log_matches!(message == "matching")).into(),
        transforms: vec![capture_field!(single "foo")],
        ..Default::default()
      },
    ],
    ..Default::default()
  });

  let mut log = make_log("matching", vec![], vec![]);
  filter_chain.process(&mut log);
  assert_eq!(
    log,
    make_log(
      "matching",
      vec![LogField {
        key: "foo".to_string(),
        value: "bar".into(),
      }],
      vec![]
    )
  );
}

#[test]
fn capture_field_transform() {
  let filter_chain = make_filter_chain(
    log_matches!(message == "matching"),
    vec![capture_field!(single "foo")],
  );

  let fields = vec![];
  let matching_fields = vec![LogField {
    key: "foo".to_string(),
    value: "bar".into(),
  }];

  // Filter's transform captures an existing matching field.
  let mut log = make_log("matching", vec![], vec![]);
  filter_chain.process(&mut log);
  assert!(log.fields.is_empty());
  assert!(log.matching_fields.is_empty());

  // Filter's transform does nothing when asked to capture a non-existing matching field.
  let mut log = make_log("matching", fields, matching_fields.clone());
  filter_chain.process(&mut log);
  assert_eq!(log.fields, matching_fields);
  assert!(log.matching_fields.is_empty());
}

#[test]
fn set_captured_field_transform_overrides_existing_field() {
  let filter_chain = make_filter_chain(
    log_matches!(message == "matching"),
    vec![set_field!(captured("foo") = field_value!("bar"))],
  );

  let mut log = make_log(
    "matching",
    vec![LogField {
      key: "foo".to_string(),
      value: "baz".into(),
    }],
    vec![],
  );
  filter_chain.process(&mut log);
  assert_eq!(
    vec![LogField {
      key: "foo".to_string(),
      value: "bar".into(),
    }],
    log.fields
  );
}

#[test]
fn set_captured_field_transform_does_not_override_existing_field() {
  let filter_chain = make_filter_chain(
    log_matches!(message == "matching"),
    vec![set_field!(captured("foo") = field_value!("bar"), false)],
  );

  let mut log = make_log(
    "matching",
    vec![LogField {
      key: "foo".to_string(),
      value: "baz".into(),
    }],
    vec![],
  );
  filter_chain.process(&mut log);
  assert_eq!(
    vec![LogField {
      key: "foo".to_string(),
      value: "baz".into(),
    }],
    log.fields
  );
}

#[test]
fn set_captured_field_transform_adds_new_field() {
  let filter_chain = make_filter_chain(
    log_matches!(message == "matching"),
    vec![set_field!(captured("new_foo") = field_value!("bar"))],
  );

  let mut log = make_log(
    "matching",
    vec![LogField {
      key: "foo".to_string(),
      value: "bar".into(),
    }],
    vec![],
  );
  filter_chain.process(&mut log);
  assert_eq!(
    vec![
      LogField {
        key: "foo".to_string(),
        value: "bar".into(),
      },
      LogField {
        key: "new_foo".to_string(),
        value: "bar".into(),
      }
    ],
    log.fields
  );
}

#[test]
fn set_captured_field_transform_copies_existing_field_value() {
  let filter_chain = make_filter_chain(
    log_matches!(message == "matching"),
    vec![set_field!(captured("new_foo") = field_value!(field "foo"))],
  );

  let mut log = make_log(
    "matching",
    vec![LogField {
      key: "foo".to_string(),
      value: "bar".into(),
    }],
    vec![],
  );
  filter_chain.process(&mut log);
  assert_eq!(
    vec![
      LogField {
        key: "foo".to_string(),
        value: "bar".into(),
      },
      LogField {
        key: "new_foo".to_string(),
        value: "bar".into(),
      }
    ],
    log.fields
  );
}

#[test]
fn set_matching_field_transform_overrides_existing_field() {
  let filter_chain = make_filter_chain(
    log_matches!(message == "matching"),
    vec![set_field!(matching("foo") = field_value!("bar"))],
  );

  let mut log = make_log(
    "matching",
    vec![],
    vec![LogField {
      key: "foo".to_string(),
      value: "baz".into(),
    }],
  );
  filter_chain.process(&mut log);
  assert_eq!(
    vec![LogField {
      key: "foo".to_string(),
      value: "bar".into(),
    }],
    log.matching_fields
  );
}

#[test]
fn set_matching_field_transform_adds_new_field() {
  let filter_chain = make_filter_chain(
    log_matches!(message == "matching"),
    vec![set_field!(matching("new_foo") = field_value!("bar"))],
  );

  let mut log = make_log(
    "matching",
    vec![],
    vec![LogField {
      key: "foo".to_string(),
      value: "bar".into(),
    }],
  );
  filter_chain.process(&mut log);
  assert_eq!(
    vec![
      LogField {
        key: "foo".to_string(),
        value: "bar".into(),
      },
      LogField {
        key: "new_foo".to_string(),
        value: "bar".into(),
      }
    ],
    log.matching_fields
  );
}

#[test]
fn remove_field_transform_removes_existing_fields() {
  let filter_chain = make_filter_chain(
    log_matches!(message == "matching"),
    vec![remove_field!("remove_me")],
  );

  let mut log = make_log(
    "matching",
    vec![
      LogField {
        key: "remove_me".to_string(),
        value: "bar".into(),
      },
      LogField {
        key: "foo".to_string(),
        value: "bar".into(),
      },
    ],
    vec![
      LogField {
        key: "remove_me".to_string(),
        value: "bar".into(),
      },
      LogField {
        key: "foo".to_string(),
        value: "bar".into(),
      },
    ],
  );

  filter_chain.process(&mut log);

  assert_eq!(
    vec![LogField {
      key: "foo".to_string(),
      value: "bar".into(),
    }],
    log.fields
  );
  assert_eq!(
    vec![LogField {
      key: "foo".to_string(),
      value: "bar".into(),
    }],
    log.matching_fields
  );
}

#[test]
fn regex_match_and_substitute() {
  let filter_chain: FilterChain = make_filter_chain(
    log_matches!(message == "matching"),
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
    vec![
      LogField {
        key: "foo".to_string(),
        value: "/foo/885fa9b2-97f1-435b-8fe3-a461d3235924/test/\
                885fa9b2-97f1-435b-8fe3-a461d3235924"
          .into(),
      },
      LogField {
        key: "bar".to_string(),
        value: "/885fa9b2-97f1-435b-8fe3-a461d3235924".into(),
      },
    ],
    vec![],
  );

  filter_chain.process(&mut log);

  assert_eq!(
    vec![
      LogField {
        key: "foo".to_string(),
        value: "/foo/<id>/test/<id>".into(),
      },
      LogField {
        key: "bar".to_string(),
        value: "/<id>".into(),
      }
    ],
    log.fields
  );
}

#[test]
fn regex_match_and_invalid_substitute() {
  let filter_chain = make_filter_chain(
    log_matches!(message == "matching"),
    vec![regex_match_and_substitute_field!(
      "foo",
      "^(.*)([0-9a-f]{8}(?:-|_)?[0-9a-f]{4}(?:-|_)?[0-9a-f]{4}(?:-|_)?[0-9a-f]{4}(?:-|_)?\
       [0-9a-f]{12})(.*)$",
      "${1}<id>${2}${4}"
    )],
  );

  let mut log = make_log(
    "matching",
    vec![LogField {
      key: "foo".to_string(),
      value: "/foo/885fa9b2-97f1-435b-8fe3-a461d3235924/test/885fa9b2-97f1-435b-8fe3-a461d3235924"
        .into(),
    }],
    vec![],
  );

  filter_chain.process(&mut log);

  assert_eq!(
    vec![LogField {
      key: "foo".to_string(),
      value: "/foo/885fa9b2-97f1-435b-8fe3-a461d3235924/test/\
              <id>885fa9b2-97f1-435b-8fe3-a461d3235924"
        .into(),
    }],
    log.fields
  );
}

#[test]
fn invalid_regex_match() {
  let filter_chain = make_filter_chain(
    log_matches!(message == "matching"),
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
    log_matches!(message ~= "^I like"),
    vec![
      set_field!(captured("fruit") = field_value!(field "_message")),
      regex_match_and_substitute_field!("fruit", "I like ()", "${1}"),
    ],
  );

  let mut log = make_log("I like apple", vec![], vec![]);

  filter_chain.process(&mut log);

  assert_eq!(
    log.fields,
    vec![LogField {
      key: "fruit".to_string(),
      value: "apple".into(),
    }]
  );
}

#[test]
fn copies_log_level_and_log_type() {
  let filter_chain = make_filter_chain(
    log_matches!(message ~= "foo"),
    vec![
      set_field!(captured("new_log_level") = field_value!(field "log_level")),
      set_field!(captured("new_log_type") = field_value!(field "log_type")),
    ],
  );

  let mut log = make_log("foo", vec![], vec![]);

  filter_chain.process(&mut log);

  assert_eq!(
    log.fields,
    vec![
      LogField {
        key: "new_log_level".to_string(),
        value: "1".into(),
      },
      LogField {
        key: "new_log_type".to_string(),
        value: "0".into(),
      }
    ]
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
    log_type: LogType::Normal,
    message: message.into(),
    fields,
    matching_fields,
    session_id: "session_id".to_string(),
    occurred_at: datetime!(2020-01-01 0:00 UTC),
  }
}
