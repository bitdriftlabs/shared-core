// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_log_primitives::DataValue;
use bd_proto::protos::client::api::{FieldSet, LogUploadRequest};
use bd_proto::protos::logging::payload::data::Data_type;
use bd_proto::protos::logging::payload::log::field::Value_type;
use bd_proto::protos::logging::payload::log::{CompressedContents, Field};
use bd_proto::protos::logging::payload::{CommandAttachment, Data, Log, LogType};
use protobuf::Message;
use std::collections::{HashMap, HashSet};
use std::io::Read as _;

#[derive(Debug)]
pub struct LogUpload(pub(super) LogUploadRequest);

impl LogUpload {
  /// The uploaded logs as the server passes them on: interned logs have their persistent fields
  /// restored from the upload's field sets.
  pub fn logs(&self) -> Vec<WrappedLog> {
    hydrate_logs(&self.0).into_iter().map(WrappedLog).collect()
  }

  /// The logs exactly as uploaded.
  pub fn raw_logs(&self) -> Vec<Log> {
    self
      .0
      .proto_logs
      .iter()
      .map(|log| Log::parse_from_bytes(log).unwrap())
      .collect()
  }

  pub fn field_sets(&self) -> &[FieldSet] {
    &self.0.field_sets
  }

  pub fn upload_uuid(&self) -> &str {
    &self.0.upload_uuid
  }

  pub fn buffer_id(&self) -> &str {
    &self.0.buffer_uuid
  }

  pub fn trigger_uuids(&self) -> &[String] {
    &self.0.trigger_uuids
  }

  pub fn trigger_uuid(&self) -> Option<&str> {
    self.0.trigger_uuids.first().map(String::as_str)
  }

  #[must_use]
  pub fn command_id(&self) -> Option<&str> {
    self.0.command_id.as_deref()
  }

  #[must_use]
  pub fn proto_log_payload_bytes(&self) -> u64 {
    self
      .0
      .proto_logs
      .iter()
      .map(|log| u64::try_from(log.len()).unwrap_or(u64::MAX))
      .sum()
  }
}

#[derive(Debug)]
pub struct WrappedLog(Log);

impl WrappedLog {
  #[must_use]
  pub fn command_attachment(&self) -> Option<&CommandAttachment> {
    self.0.command_attachment.as_ref()
  }

  #[must_use]
  pub fn compressed_contents(&self) -> &[u8] {
    &self.0.compressed_contents
  }

  #[must_use]
  pub fn session_id(&self) -> &str {
    &self.0.session_id
  }

  #[must_use]
  pub fn has_message(&self) -> bool {
    self.0.message.is_some()
  }

  #[must_use]
  pub fn message(&self) -> &str {
    self.0.message.string_data()
  }

  #[must_use]
  pub fn log_level(&self) -> u32 {
    self.0.log_level
  }

  #[must_use]
  pub fn binary_message(&self) -> &[u8] {
    &self.0.message.binary_data().payload
  }

  #[must_use]
  pub fn typed_message(&self) -> DataValue {
    self
      .0
      .message
      .clone()
      .into_option()
      .and_then(DataValue::from_proto)
      .expect("missing message value")
  }

  #[must_use]
  pub fn typed_fields(&self) -> Vec<(String, DataValue)> {
    self
      .0
      .fields
      .iter()
      .map(|field| {
        let value = field
          .has_value()
          .then(|| field.value().clone())
          .and_then(DataValue::from_proto)
          .expect("missing field value");
        (field.key.clone(), value)
      })
      .collect()
  }

  #[must_use]
  pub fn field(&self, key: &str) -> &str {
    self
      .0
      .fields
      .iter()
      .find_map(|field| (field.key == key).then(|| field.value().string_data()))
      .unwrap_or_else(|| {
        panic!(
          "field {key:?} should exist, keys: {:?}",
          self.0.fields.iter().map(|f| &f.key).collect::<Vec<_>>()
        )
      })
  }

  #[must_use]
  pub fn has_field(&self, key: &str) -> bool {
    self.0.fields.iter().any(|field| field.key == key)
  }

  #[must_use]
  pub fn binary_field(&self, key: &str) -> &[u8] {
    self
      .0
      .fields
      .iter()
      .find_map(|field| (field.key == key).then(|| &field.value().binary_data().payload))
      .expect("field should exist")
  }

  #[must_use]
  pub fn timestamp(&self) -> time::OffsetDateTime {
    time::OffsetDateTime::from_unix_timestamp_nanos((self.0.timestamp_unix_micro * 1000).into())
      .unwrap()
  }

  #[must_use]
  pub fn workflow_action_ids(&self) -> &Vec<String> {
    &self.0.action_ids
  }

  #[must_use]
  pub fn stream_ids(&self) -> &Vec<String> {
    &self.0.stream_ids
  }

  #[must_use]
  pub fn log_type(&self) -> LogType {
    self.0.log_type.enum_value_or_default()
  }
}

/// The field the server adds to a log whose field set is missing from its upload.
pub const PERSISTENT_FIELDS_UNAVAILABLE: &str = "_persistent_fields_unavailable";

/// Restores the persistent fields of interned logs the way the server does at ingest: a log with a
/// `field_set_version` gains each field of that set it neither carries nor marks `cleared`, every
/// `cleared` field is dropped, and a log whose set is missing is marked. Logs without a version or
/// `cleared` field are returned unchanged.
#[must_use]
pub fn hydrate_logs(request: &LogUploadRequest) -> Vec<Log> {
  let mut field_sets: HashMap<u64, Vec<Field>> = HashMap::new();
  for set in &request.field_sets {
    if set.version == 0 || field_sets.contains_key(&set.version) {
      continue;
    }
    let mut keys = HashSet::new();
    let fields = set
      .fields
      .iter()
      .filter(|field| !field.key.is_empty() && field.has_value() && keys.insert(&field.key))
      .cloned()
      .collect();
    field_sets.insert(set.version, fields);
  }

  request
    .proto_logs
    .iter()
    .map(|bytes| {
      let mut log = Log::parse_from_bytes(bytes).unwrap();
      hydrate_log(&mut log, &field_sets);
      log
    })
    .collect()
}

fn hydrate_log(log: &mut Log, field_sets: &HashMap<u64, Vec<Field>>) {
  let interned = log.field_set_version != 0;
  if interned && !log.compressed_contents.is_empty() {
    let mut decompressed = Vec::new();
    flate2::read::ZlibDecoder::new(log.compressed_contents.as_slice())
      .read_to_end(&mut decompressed)
      .unwrap();
    let contents = CompressedContents::parse_from_bytes(&decompressed).unwrap();
    log.message = contents.message;
    log.fields = contents.fields;
    log.compressed_contents.clear();
  }
  if !interned && !log.fields.iter().any(Field::cleared) {
    return;
  }

  let cleared: HashSet<String> = log
    .fields
    .iter()
    .filter(|field| field.cleared())
    .map(|field| field.key.clone())
    .collect();
  log.fields.retain(|field| !field.cleared());
  if interned {
    match field_sets.get(&log.field_set_version) {
      Some(fields) => {
        let inline: HashSet<String> = log.fields.iter().map(|field| field.key.clone()).collect();
        log.fields.extend(
          fields
            .iter()
            .filter(|field| !inline.contains(&field.key) && !cleared.contains(&field.key))
            .cloned(),
        );
      },
      None => {
        if !log
          .fields
          .iter()
          .any(|field| field.key == PERSISTENT_FIELDS_UNAVAILABLE)
        {
          log.fields.push(Field {
            key: PERSISTENT_FIELDS_UNAVAILABLE.to_string(),
            value_type: Some(Value_type::Value(Data {
              data_type: Some(Data_type::BoolData(true)),
              ..Default::default()
            })),
            ..Default::default()
          });
        }
      },
    }
  }
  log.field_set_version = 0;
}
