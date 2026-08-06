// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.

#![allow(clippy::indexing_slicing, clippy::unwrap_used)]

use crate::corpus::CorpusReader;
use bd_log_primitives::{DataValue, log_level};
use bd_proto::protos::logging::payload::LogType;
use std::fs;

#[test]
fn parses_exported_log_with_default_type() {
  let directory = tempfile::tempdir().unwrap();
  let path = directory.path().join("logs.json");
  fs::write(
    &path,
    r#"{"id":"1","timestamp":"2026-08-06T00:45:13.122000000Z","session_id":"s","log_level":"INFO","message":"hello","fields":{"fields":{"count":{"int_data":"42"},"tag":{"string_data":"value"}}}}"#,
  )
  .unwrap();

  let mut reader = CorpusReader::open(&path).unwrap();
  let source_log = reader.next_log().unwrap().unwrap();

  assert_eq!(1, source_log.source_line);
  assert_eq!(LogType::NORMAL, source_log.log.log_type);
  assert_eq!(log_level::INFO, source_log.log.log_level);
  assert_eq!(Some("value"), source_log.log.fields["tag"].as_str());
  assert_eq!(DataValue::U64(42), source_log.log.fields["count"]);
}

#[test]
fn reports_source_line_for_invalid_record() {
  let directory = tempfile::tempdir().unwrap();
  let path = directory.path().join("logs.json");
  fs::write(&path, "\nnot-json\n").unwrap();

  let mut reader = CorpusReader::open(&path).unwrap();
  let error = reader.next_log().unwrap_err();

  assert!(error.to_string().contains("line 2"));
}

#[test]
fn parses_all_exported_data_variants() {
  let directory = tempfile::tempdir().unwrap();
  let path = directory.path().join("logs.json");
  fs::write(
    &path,
    r#"{"id":"1","timestamp":"2026-08-06T00:45:13Z","session_id":"s","log_level":"INFO","message":"hello","fields":{"fields":{"binary":{"binary_data":{"payload":"AQI="}},"signed":{"sint_data":"-1"},"double":{"double_data":1.5},"boolean":{"bool_data":true},"map":{"map_data":{"entries":{"child":{"string_data":"value"}}}},"array":{"array_data":{"items":[{"int_data":2}]}}}}}"#,
  )
  .unwrap();

  let mut reader = CorpusReader::open(&path).unwrap();
  let source_log = reader.next_log().unwrap().unwrap();

  assert!(matches!(
    source_log.log.fields.get("binary"),
    Some(DataValue::Bytes(_))
  ));
  assert!(matches!(
    source_log.log.fields.get("signed"),
    Some(DataValue::I64(-1))
  ));
  assert!(matches!(
    source_log.log.fields.get("double"),
    Some(DataValue::Double(_))
  ));
  assert!(matches!(
    source_log.log.fields.get("boolean"),
    Some(DataValue::Boolean(true))
  ));
  assert!(matches!(
    source_log.log.fields.get("map"),
    Some(DataValue::Map(_))
  ));
  assert!(matches!(
    source_log.log.fields.get("array"),
    Some(DataValue::Array(_))
  ));
}
