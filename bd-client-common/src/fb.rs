// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use anyhow::anyhow;
use bd_log_primitives::{LogField, LogFields, LogLevel, LogMessage, StringOrBytes};
use bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::{
  root_as_log as fb_root_as_log,
  BinaryData,
  BinaryDataArgs,
  Data,
  Field,
  FieldArgs,
  Log,
  LogArgs,
  LogType,
  StringData,
  StringDataArgs,
  Timestamp as FbTimestamp,
  TimestampArgs,
};
use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};

pub fn root_as_log(bytes: &[u8]) -> anyhow::Result<Log<'_>> {
  fb_root_as_log(bytes).map_err(|e| anyhow!("An unexpected flatbuffer error ocurred: {e}"))
}

#[derive(Copy, Clone)]
pub struct Timestamp {
  pub secs: i64,
  pub nanos: i32,
}

fn create_string_vector<'a: 'b, 'b>(
  builder: &mut FlatBufferBuilder<'a>,
  strings: impl Iterator<Item = &'b str>,
) -> Option<WIPOffset<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<&'a str>>>> {
  let mut offsets = Vec::new();
  for s in strings {
    offsets.push(builder.create_string(s));
  }

  if offsets.is_empty() {
    return None;
  }
  Some(builder.create_vector(&offsets))
}

pub fn make_log<'a: 'b, 'b>(
  builder: &mut FlatBufferBuilder<'a>,
  log_level: LogLevel,
  log_type: LogType,
  message: &LogMessage,
  fields: &LogFields,
  session_id: &str,
  timestamp: time::OffsetDateTime,
  workflow_flush_buffer_action_ids: impl Iterator<Item = &'b str>,
  stream_ids: impl Iterator<Item = &'b str>,
  log_output: impl FnOnce(&[u8]) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
  let (message, message_type) = string_or_bytes_union(message, builder);

  let mut all_fields = Vec::new();
  for LogField { key, value } in fields {
    let key = builder.create_string(key.as_ref());
    let (value, value_type) = string_or_bytes_union(value, builder);
    all_fields.push(Field::create(
      builder,
      &FieldArgs {
        key: Some(key),
        value_type,
        value: Some(value),
      },
    ));
  }

  let stream_ids = create_string_vector(builder, stream_ids);

  let all_fields = builder.create_vector_from_iter(all_fields.into_iter());

  let session_id = builder.create_string(session_id);
  let timestamp = FbTimestamp::create(
    builder,
    &TimestampArgs {
      seconds: timestamp.unix_timestamp(),
      // This should never overflow because the nanos field is always less than 1_000_000_000 aka 1
      // second.
      #[allow(clippy::cast_possible_truncation)]
      nanos: (timestamp.unix_timestamp_nanos()
        - (i128::from(timestamp.unix_timestamp()) * 1_000_000_000)) as i32,
    },
  );

  let workflow_flush_buffer_action_ids =
    create_string_vector(builder, workflow_flush_buffer_action_ids);

  let log = Log::create(
    builder,
    &LogArgs {
      log_level,
      log_type,
      message_type,
      message: Some(message),
      fields: Some(all_fields),
      session_id: Some(session_id),
      timestamp: Some(timestamp),
      workflow_action_ids: workflow_flush_buffer_action_ids,
      stream_ids,
    },
  );
  builder.finish_minimal(log);
  let result = log_output(builder.finished_data());
  builder.reset();
  result
}

// Converts a StringOrBytes into the corresponding flatbuffer union value/type, i.e. either
// StringData or BinaryData.
fn string_or_bytes_union<T: AsRef<str>, B: AsRef<[u8]>>(
  string_or_bytes: &StringOrBytes<T, B>,
  builder: &mut FlatBufferBuilder<'_>,
) -> (WIPOffset<UnionWIPOffset>, Data) {
  match string_or_bytes {
    StringOrBytes::String(s) => {
      let value = builder.create_string(s.as_ref());
      (
        StringData::create(builder, &StringDataArgs { data: Some(value) }).as_union_value(),
        Data::string_data,
      )
    },
    StringOrBytes::SharedString(s) => {
      let value = builder.create_string(s.as_ref());
      (
        StringData::create(builder, &StringDataArgs { data: Some(value) }).as_union_value(),
        Data::string_data,
      )
    },
    StringOrBytes::Bytes(b) => {
      let value = builder.create_vector(b.as_ref());
      (
        BinaryData::create(
          builder,
          &BinaryDataArgs {
            data_type: None,
            data: Some(value),
          },
        )
        .as_union_value(),
        Data::binary_data,
      )
    },
  }
}
