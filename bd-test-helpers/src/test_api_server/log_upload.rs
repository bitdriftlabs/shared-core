// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_log_primitives::{LogType, StringOrBytes};
use bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::root_as_log;
use bd_proto::protos::client::api::LogUploadRequest;

#[derive(Debug)]
pub struct LogUpload(pub(super) LogUploadRequest);

impl LogUpload {
  pub fn logs(&self) -> Vec<Log<'_>> {
    self
      .0
      .logs
      .iter()
      .map(|log| Log(root_as_log(log).unwrap()))
      .collect()
  }

  pub fn upload_uuid(&self) -> &str {
    &self.0.upload_uuid
  }

  pub fn buffer_id(&self) -> &str {
    &self.0.buffer_uuid
  }
}

pub struct Log<'a>(bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::Log<'a>);

impl Log<'_> {
  pub fn session_id(&self) -> &str {
    self.0.session_id().unwrap()
  }

  pub fn message(&self) -> &str {
    self.0.message_as_string_data().unwrap().data()
  }

  pub fn binary_message(&self) -> &[u8] {
    self.0.message_as_binary_data().unwrap().data().bytes()
  }

  pub fn typed_message(&self) -> StringOrBytes<String, Vec<u8>> {
    if self.0.message_as_string_data().is_some() {
      StringOrBytes::String(self.message().to_string())
    } else {
      StringOrBytes::Bytes(self.binary_message().to_vec())
    }
  }

  pub fn typed_fields(&self) -> Vec<(String, StringOrBytes<String, Vec<u8>>)> {
    self
      .0
      .fields()
      .unwrap_or_default()
      .iter()
      .map(|field| {
        let key = field.key();
        if field.value_as_string_data().is_some() {
          (
            key.to_string(),
            StringOrBytes::String(field.value_as_string_data().unwrap().data().to_string()),
          )
        } else {
          (
            key.to_string(),
            StringOrBytes::Bytes(
              field
                .value_as_binary_data()
                .unwrap()
                .data()
                .bytes()
                .to_vec(),
            ),
          )
        }
      })
      .collect()
  }

  pub fn field(&self, key: &str) -> &str {
    self
      .0
      .fields()
      .unwrap_or_default()
      .iter()
      .find_map(|field| {
        (field.key() == key).then_some(
          field
            .value_as_string_data()
            .expect("field should be string value")
            .data(),
        )
      })
      .expect("field should exist")
  }

  pub fn has_field(&self, key: &str) -> bool {
    self
      .0
      .fields()
      .unwrap_or_default()
      .iter()
      .any(|field| field.key() == key)
  }

  pub fn binary_field(&self, key: &str) -> &[u8] {
    self
      .0
      .fields()
      .unwrap_or_default()
      .iter()
      .find_map(|field| {
        (field.key() == key).then(|| {
          field
            .value_as_binary_data()
            .expect("field should be binary value")
            .data()
            .bytes()
        })
      })
      .expect("field should exist")
  }

  pub fn timestamp(&self) -> time::OffsetDateTime {
    let timestamp = self.0.timestamp().unwrap();

    #[allow(clippy::cast_lossless)]
    time::OffsetDateTime::from_unix_timestamp_nanos(
      (timestamp.seconds() as i128) * 1_000_000_000 + (timestamp.nanos() as i128),
    )
    .unwrap()
  }

  pub fn workflow_action_ids(&self) -> Vec<&str> {
    self
      .0
      .workflow_action_ids()
      .unwrap_or_default()
      .iter()
      .collect()
  }

  pub fn stream_ids(&self) -> Vec<&str> {
    self.0.stream_ids().unwrap_or_default().iter().collect()
  }

  pub fn log_type(&self) -> LogType {
    self.0.log_type()
  }
}
