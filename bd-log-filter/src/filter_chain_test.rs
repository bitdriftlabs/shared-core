// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::FilterChain;
use bd_log_primitives::{log_level, Log, LogField, LogFields, LogType};
use bd_proto::protos::filter::filter::{Filter, FiltersConfiguration};
use bd_test_helpers::{capture_field, field_value, log_matches, set_field};
use time::macros::datetime;

#[test]
fn filters_are_not_applied_to_non_matching_logs_only() {
  let (chain, _) = FilterChain::new(FiltersConfiguration {
    filters: vec![Filter {
      matcher: Some(log_matches!(message == "matching")).into(),
      transforms: vec![capture_field!(single "foo")],
      ..Default::default()
    }],
    ..Default::default()
  });

  let fields = vec![];
  let matching_fields = vec![LogField {
    key: "foo".to_string(),
    value: "bar".into(),
  }];

  // Filter's transform are not applied to logs that don't match filter's matcher.
  let mut log = make_log("not matching", fields.clone(), matching_fields.clone());
  chain.process(&mut log);
  assert_eq!(log, make_log("not matching", fields, matching_fields));
}

#[test]
fn filter_transforms_are_applied_in_order() {
  let (chain, _) = FilterChain::new(FiltersConfiguration {
    filters: vec![Filter {
      matcher: Some(log_matches!(message == "matching")).into(),
      transforms: vec![
        set_field!(matching("foo") = field_value!("bar")),
        capture_field!(single "foo"),
      ],
      ..Default::default()
    }],
    ..Default::default()
  });

  let mut log = make_log("matching", vec![], vec![]);
  chain.process(&mut log);
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
  let (chain, _) = FilterChain::new(FiltersConfiguration {
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
  chain.process(&mut log);
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
  let (chain, _) = FilterChain::new(FiltersConfiguration {
    filters: vec![Filter {
      matcher: Some(log_matches!(message == "matching")).into(),
      transforms: vec![capture_field!(single "foo")],
      ..Default::default()
    }],
    ..Default::default()
  });

  let fields = vec![];
  let matching_fields = vec![LogField {
    key: "foo".to_string(),
    value: "bar".into(),
  }];

  // Filter's transform captures an existing matching field.
  let mut log = make_log("matching", vec![], vec![]);
  chain.process(&mut log);
  assert!(log.fields.is_empty());
  assert!(log.matching_fields.is_empty());

  // Filter's transform does nothing when asked to capture a non-existing matching field.
  let mut log = make_log("matching", fields, matching_fields.clone());
  chain.process(&mut log);
  assert_eq!(log.fields, matching_fields);
  assert!(log.matching_fields.is_empty());
}

#[test]
fn set_captured_field_transform_overrides_existing_field() {
  let (chain, _) = FilterChain::new(FiltersConfiguration {
    filters: vec![Filter {
      matcher: Some(log_matches!(message == "matching")).into(),
      transforms: vec![set_field!(captured("foo") = field_value!("bar"))],
      ..Default::default()
    }],
    ..Default::default()
  });

  let mut log = make_log(
    "matching",
    vec![LogField {
      key: "foo".to_string(),
      value: "baz".into(),
    }],
    vec![],
  );
  chain.process(&mut log);
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
  let (chain, _) = FilterChain::new(FiltersConfiguration {
    filters: vec![Filter {
      matcher: Some(log_matches!(message == "matching")).into(),
      transforms: vec![set_field!(captured("foo") = field_value!("bar"), false)],
      ..Default::default()
    }],
    ..Default::default()
  });

  let mut log = make_log(
    "matching",
    vec![LogField {
      key: "foo".to_string(),
      value: "baz".into(),
    }],
    vec![],
  );
  chain.process(&mut log);
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
  let (chain, _) = FilterChain::new(FiltersConfiguration {
    filters: vec![Filter {
      matcher: Some(log_matches!(message == "matching")).into(),
      transforms: vec![set_field!(captured("new_foo") = field_value!("bar"))],
      ..Default::default()
    }],
    ..Default::default()
  });

  let mut log = make_log(
    "matching",
    vec![LogField {
      key: "foo".to_string(),
      value: "bar".into(),
    }],
    vec![],
  );
  chain.process(&mut log);
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
  let (chain, _) = FilterChain::new(FiltersConfiguration {
    filters: vec![Filter {
      matcher: Some(log_matches!(message == "matching")).into(),
      transforms: vec![set_field!(captured("new_foo") = field_value!(field "foo"))],
      ..Default::default()
    }],
    ..Default::default()
  });

  let mut log = make_log(
    "matching",
    vec![LogField {
      key: "foo".to_string(),
      value: "bar".into(),
    }],
    vec![],
  );
  chain.process(&mut log);
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
  let (chain, _) = FilterChain::new(FiltersConfiguration {
    filters: vec![Filter {
      matcher: Some(log_matches!(message == "matching")).into(),
      transforms: vec![set_field!(matching("foo") = field_value!("bar"))],
      ..Default::default()
    }],
    ..Default::default()
  });

  let mut log = make_log(
    "matching",
    vec![],
    vec![LogField {
      key: "foo".to_string(),
      value: "baz".into(),
    }],
  );
  chain.process(&mut log);
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
  let (chain, _) = FilterChain::new(FiltersConfiguration {
    filters: vec![Filter {
      matcher: Some(log_matches!(message == "matching")).into(),
      transforms: vec![set_field!(matching("new_foo") = field_value!("bar"))],
      ..Default::default()
    }],
    ..Default::default()
  });

  let mut log = make_log(
    "matching",
    vec![],
    vec![LogField {
      key: "foo".to_string(),
      value: "bar".into(),
    }],
  );
  chain.process(&mut log);
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
  let (chain, _) = FilterChain::new(FiltersConfiguration {
    filters: vec![Filter {
      matcher: Some(log_matches!(message == "matching")).into(),
      transforms: vec![set_field!(matching("new_foo") = field_value!("bar"))],
      ..Default::default()
    }],
    ..Default::default()
  });

  let mut log = make_log(
    "matching",
    vec![LogField {
      key: "foo".to_string(),
      value: "bar".into(),
    }],
    vec![LogField {
      key: "foo".to_string(),
      value: "bar".into(),
    }],
  );

  chain.process(&mut log);
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
