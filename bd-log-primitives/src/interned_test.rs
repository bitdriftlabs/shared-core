// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{encoded_field_set_version, same_value};
use crate::{DataValue, EncodableLog, InternedFields, Log, LogFields};
use bd_proto::protos::logging::payload::log::CompressedContents;
use bd_proto::protos::logging::payload::{Log as ProtoLog, LogType};
use flate2::read::ZlibDecoder;
use protobuf::Message;
use std::collections::BTreeMap;
use std::io::Read;
use std::sync::Arc;
use time::OffsetDateTime;

fn fields(entries: &[(&'static str, &str)]) -> LogFields {
  entries
    .iter()
    .map(|(key, value)| ((*key).into(), DataValue::String((*value).to_string())))
    .collect()
}

fn log(fields: LogFields) -> Log {
  Log {
    log_level: 1,
    log_type: LogType::NORMAL,
    message: DataValue::String("message".to_string()),
    fields,
    matching_fields: LogFields::new(),
    session_id: "session".into(),
    occurred_at: OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
    command_attachment: None,
    capture_session: None,
  }
}

fn encode(log: Log, interned: Option<InternedFields>, compression_min_size: u64) -> Vec<u8> {
  let mut encodable = EncodableLog::new(log, compression_min_size);
  encodable.set_interned(interned);
  let size = encodable.compute_size(&[], &[]).unwrap();
  let mut bytes = vec![0; usize::try_from(size).unwrap()];
  encodable.serialize_to_bytes(&[], &[], &mut bytes).unwrap();
  bytes
}

// Inline fields by key, with `cleared` fields mapped to `None`.
fn decoded_fields(
  fields: &[bd_proto::protos::logging::payload::log::Field],
) -> BTreeMap<String, Option<String>> {
  fields
    .iter()
    .map(|field| {
      let value = if field.cleared() {
        None
      } else {
        Some(field.value().string_data().to_string())
      };
      (field.key.clone(), value)
    })
    .collect()
}

fn interned(log: &Log, version: u64, set: &[(&'static str, &str)]) -> Option<InternedFields> {
  InternedFields::new(&log.fields, version, Arc::new(fields(set)))
}

#[test]
fn interned_log_leaves_out_unchanged_fields() {
  let log = log(fields(&[
    ("user_id", "abc"),
    ("theme", "dark"),
    ("screen", "home"),
  ]));
  let interned = interned(
    &log,
    42,
    &[("user_id", "abc"), ("theme", "light"), ("locale", "en")],
  );
  assert_eq!(
    interned.as_ref().map(|interned| interned.cleared.clone()),
    Some(vec!["locale".into()])
  );
  let bytes = encode(log, interned, 0);

  let decoded = ProtoLog::parse_from_bytes(&bytes).unwrap();
  assert_eq!(decoded.field_set_version, 42);
  assert_eq!(
    decoded_fields(&decoded.fields),
    BTreeMap::from([
      ("locale".to_string(), None),
      ("screen".to_string(), Some("home".to_string())),
      ("theme".to_string(), Some("dark".to_string())),
    ])
  );
  assert_eq!(encoded_field_set_version(&bytes), 42);
}

#[test]
fn log_without_interning_is_unchanged() {
  let full = log(fields(&[("user_id", "abc")]));
  let bytes = encode(full, None, 0);
  let decoded = ProtoLog::parse_from_bytes(&bytes).unwrap();
  assert_eq!(decoded.field_set_version, 0);
  assert_eq!(
    decoded_fields(&decoded.fields),
    BTreeMap::from([("user_id".to_string(), Some("abc".to_string()))])
  );
  assert_eq!(encoded_field_set_version(&bytes), 0);
}

#[test]
fn compressed_interned_log_keeps_the_version_outside_the_contents() {
  let log = log(fields(&[("user_id", "abc"), ("screen", "home")]));
  let interned = interned(&log, 7, &[("user_id", "abc"), ("locale", "en")]);
  let bytes = encode(log, interned, 1);

  let decoded = ProtoLog::parse_from_bytes(&bytes).unwrap();
  assert_eq!(decoded.field_set_version, 7);
  assert_eq!(decoded.fields.len(), 0);
  assert_eq!(encoded_field_set_version(&bytes), 7);

  let mut contents = Vec::new();
  ZlibDecoder::new(decoded.compressed_contents.as_slice())
    .read_to_end(&mut contents)
    .unwrap();
  let contents = CompressedContents::parse_from_bytes(&contents).unwrap();
  assert_eq!(
    decoded_fields(&contents.fields),
    BTreeMap::from([
      ("locale".to_string(), None),
      ("screen".to_string(), Some("home".to_string())),
    ])
  );
}

#[test]
fn computed_size_matches_the_encoding() {
  let log = log(fields(&[("a", "1"), ("b", "2")]));
  let interned = interned(&log, 300, &[("a", "1"), ("gone", "x"), ("also_gone", "y")]);
  assert_eq!(
    interned.as_ref().map(|interned| interned.cleared.len()),
    Some(2)
  );
  let mut encodable = EncodableLog::new(log, 0);
  encodable.set_interned(interned);
  let size = encodable.compute_size(&["action"], &["stream"]).unwrap();
  let mut bytes = vec![0; usize::try_from(size).unwrap()];
  encodable
    .serialize_to_bytes(&["action"], &["stream"], &mut bytes)
    .unwrap();
  let decoded = ProtoLog::parse_from_bytes(&bytes).unwrap();
  assert_eq!(decoded.compute_size(), size);
  assert_eq!(decoded.field_set_version, 300);
}

#[test]
fn version_reader_rejects_malformed_input() {
  // Field 11 as varint 5, then a truncated length-delimited field.
  assert_eq!(encoded_field_set_version(&[0x58, 0x05]), 5);
  assert_eq!(
    encoded_field_set_version(&[0x58, 0x05, 0x1a, 0x05, 0x01]),
    0
  );
  assert_eq!(encoded_field_set_version(&[]), 0);
}

#[test]
fn values_compare_as_they_encode() {
  let double = |value: f64| DataValue::Double(ordered_float::NotNan::new(value).unwrap());
  assert!(same_value(&double(0.0), &double(0.0)));
  assert!(!same_value(&double(0.0), &double(-0.0)));
  assert!(same_value(
    &DataValue::String("a".to_string()),
    &DataValue::StaticString("a")
  ));
  assert!(!same_value(
    &DataValue::String("1".to_string()),
    &DataValue::U64(1)
  ));
  let array = |value: f64| DataValue::Array(crate::LogArrayData::new(vec![double(value)]));
  assert!(same_value(&array(1.5), &array(1.5)));
  assert!(!same_value(&array(0.0), &array(-0.0)));
}

#[test]
fn nothing_to_leave_out_is_not_interned() {
  let log = log(fields(&[("user_id", "abc"), ("screen", "home")]));
  assert_eq!(interned(&log, 1, &[("user_id", "xyz")]), None);
  assert_eq!(interned(&log, 1, &[("locale", "en")]), None);
  assert_eq!(interned(&log, 1, &[]), None);
}
