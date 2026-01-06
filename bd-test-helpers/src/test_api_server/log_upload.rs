// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_log_primitives::StringOrBytes;
use bd_proto::protos::client::api::LogUploadRequest;
use bd_proto::protos::logging::payload::data::Data_type;
use bd_proto::protos::logging::payload::{Log, LogType};
use protobuf::Message;

#[derive(Debug)]
pub struct LogUpload(pub(super) LogUploadRequest);

impl LogUpload {
  pub fn logs(&self) -> Vec<WrappedLog> {
    self
      .0
      .proto_logs
      .iter()
      .map(|log| WrappedLog(Log::parse_from_bytes(log).unwrap()))
      .collect()
  }

  pub fn upload_uuid(&self) -> &str {
    &self.0.upload_uuid
  }

  pub fn buffer_id(&self) -> &str {
    &self.0.buffer_uuid
  }
}

#[derive(Debug)]
pub struct WrappedLog(Log);

impl WrappedLog {
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
  pub fn typed_message(&self) -> StringOrBytes {
    match self.0.message.data_type.as_ref().unwrap() {
      Data_type::StringData(string_data) => StringOrBytes::String(string_data.clone()),
      Data_type::BinaryData(binary_data) => {
        StringOrBytes::Bytes(binary_data.payload.clone().into())
      },
      Data_type::BoolData(bool_data) => StringOrBytes::Boolean(*bool_data),
      Data_type::IntData(int_data) => StringOrBytes::U64(*int_data),
      Data_type::SintData(sint_data) => StringOrBytes::I64(*sint_data),
      Data_type::DoubleData(double_data) => {
        StringOrBytes::Double(ordered_float::NotNan::new(*double_data).unwrap())
      },
    }
  }

  #[must_use]
  pub fn typed_fields(&self) -> Vec<(String, StringOrBytes)> {
    self
      .0
      .fields
      .iter()
      .map(|field| {
        if field.value.has_string_data() {
          (
            field.key.clone(),
            StringOrBytes::String(field.value.string_data().to_string()),
          )
        } else {
          (
            field.key.clone(),
            StringOrBytes::Bytes(field.value.binary_data().payload.clone().into()),
          )
        }
      })
      .collect()
  }

  #[must_use]
  pub fn field(&self, key: &str) -> &str {
    self
      .0
      .fields
      .iter()
      .find_map(|field| (field.key == key).then(|| field.value.string_data()))
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
      .find_map(|field| (field.key == key).then(|| &field.value.binary_data().payload))
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
