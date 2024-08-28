use crate::FilterChain;
use bd_log_primitives::{log_level, Log, LogField, LogFields, LogType};
use bd_proto::protos::filter::filter::{Filter, FiltersConfiguration};
use bd_test_helpers::{capture_fields, log_matches};

#[test]
fn test_capture_fields_transform() {
  let config = FiltersConfiguration {
    filters: vec![Filter {
      matcher: Some(log_matches!(message == "matching")).into(),
      transforms: vec![capture_fields!(single "foo")],
      ..Default::default()
    }],
    ..Default::default()
  };

  let chain = FilterChain::new(config);

  let fields = vec![];
  let matching_fields = vec![LogField {
    key: "foo".to_string(),
    value: "bar".into(),
  }];

  let mut log = make_log("not matching", fields.clone(), matching_fields.clone());
  chain.process(&mut log);
  assert_eq!(
    log,
    make_log("not matching", fields.clone(), matching_fields.clone())
  );

  let mut log = make_log("matching", vec![], vec![]);
  chain.process(&mut log);
  assert!(log.fields.is_empty());
  assert!(log.matching_fields.is_empty());

  let mut log = make_log("matching", fields, matching_fields.clone());
  chain.process(&mut log);
  assert_eq!(log.fields, matching_fields);
  assert!(log.matching_fields.is_empty());
}

fn make_log(message: &str, fields: LogFields, matching_fields: LogFields) -> Log {
  Log {
    log_level: log_level::DEBUG,
    log_type: LogType::Normal,
    message: message.into(),
    fields,
    matching_fields,
    session_id: "session_id".to_string(),
    occurred_at: time::OffsetDateTime::from_unix_timestamp(123).unwrap(),
  }
}
