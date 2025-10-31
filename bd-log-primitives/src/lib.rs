// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![deny(
  clippy::expect_used,
  clippy::panic,
  clippy::todo,
  clippy::unimplemented,
  clippy::unreachable,
  clippy::unwrap_used
)]

pub mod size;
pub mod tiny_set;

use crate::size::MemorySized;
use ahash::AHashMap;
use bd_proto::protos::logging::payload::LogType;
use protobuf::rt::WireType;
use protobuf::{CodedInputStream, CodedOutputStream};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::sync::{Arc, LazyLock};
use time::OffsetDateTime;

pub const LOG_FIELD_NAME_TYPE: &str = "log_type";
pub const LOG_FIELD_NAME_LEVEL: &str = "log_level";
pub const LOG_FIELD_NAME_MESSAGE: &str = "_message";

// Helpers for doing raw casts where we are sure the value fits and don't want to pay for
// checks and avoid clippy lints.
pub trait LossyIntToU32 {
  fn to_u32(self) -> u32;
}

#[allow(clippy::cast_possible_truncation)]
impl LossyIntToU32 for usize {
  fn to_u32(self) -> u32 {
    debug_assert!(u32::try_from(self).is_ok());
    self as u32
  }
}

#[allow(clippy::cast_possible_truncation)]
impl LossyIntToU32 for u64 {
  fn to_u32(self) -> u32 {
    debug_assert!(u32::try_from(self).is_ok());
    self as u32
  }
}

pub trait LossyIntToU64 {
  fn to_u64(self) -> u64;
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
impl LossyIntToU64 for i128 {
  fn to_u64(self) -> u64 {
    debug_assert!(u64::try_from(self).is_ok());
    self as u64
  }
}

/// A union type that allows representing either a UTF-8 string or an opaque series of bytes. This
/// is generic over the underlying String type to support different ownership models.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum StringOrBytes<S: AsRef<str>, B: AsRef<[u8]>> {
  String(S),
  SharedString(Arc<String>),
  Bytes(B),
}

impl<T: AsRef<str>, B: AsRef<[u8]>> StringOrBytes<T, B> {
  /// Extracts the underlying str if the enum represents a String, None otherwise.
  pub fn as_str(&self) -> Option<&str> {
    match self {
      Self::String(s) => Some(s.as_ref()),
      Self::SharedString(s) => Some(s.as_str()),
      Self::Bytes(_) => None,
    }
  }

  /// Extracts the underlying bytes if the enum represents a Bytes, None otherwise.
  pub fn as_bytes(&self) -> Option<&[u8]> {
    match self {
      Self::String(_) | Self::SharedString(_) => None,
      Self::Bytes(b) => Some(b.as_ref()),
    }
  }
}

impl From<String> for StringOrBytes<String, Vec<u8>> {
  fn from(s: String) -> Self {
    Self::String(s)
  }
}

impl<S: AsRef<str>> From<Arc<String>> for StringOrBytes<S, Vec<u8>> {
  fn from(s: Arc<String>) -> Self {
    Self::SharedString(s)
  }
}

impl From<Vec<u8>> for StringOrBytes<String, Vec<u8>> {
  fn from(s: Vec<u8>) -> Self {
    Self::Bytes(s)
  }
}

// Support converting a &str into a StringOrBytes<S>::String if S : From<&str>.
impl<'a, T: AsRef<str> + From<&'a str>, B: AsRef<[u8]>> From<&'a str> for StringOrBytes<T, B> {
  fn from(s: &'a str) -> Self {
    Self::String(s.into())
  }
}

// A &[u8] can be converted to a StringOrBytes<S>::Bytes.
impl<'a, T: AsRef<str>, B: AsRef<[u8]> + From<&'a [u8]>> From<&'a [u8]> for StringOrBytes<T, B> {
  fn from(slice: &'a [u8]) -> Self {
    Self::Bytes(slice.into())
  }
}

/// A log message is a borrowed string or binary value.
pub type LogMessage = StringOrBytes<String, Vec<u8>>;

impl std::fmt::Display for LogMessage {
  // This trait requires `fmt` with this exact signature.
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::String(s) => write!(f, "{s}"),
      Self::SharedString(s) => write!(f, "{s}"),
      Self::Bytes(b) => {
        write!(f, "bytes:{b:?}")
      },
    }
  }
}

pub type LogLevel = u32;

/// Well known log levels used by the library.
pub mod log_level {
  use crate::LogLevel;

  pub const ERROR: LogLevel = 4;
  pub const WARNING: LogLevel = 3;
  pub const INFO: LogLevel = 2;
  pub const DEBUG: LogLevel = 1;
  pub const TRACE: LogLevel = 0;
}

/// A convenience enum that can be used to represent log levels in a more type-safe manner.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypedLogLevel {
  Error,
  Warning,
  Info,
  Debug,
  Trace,
}

impl TypedLogLevel {
  #[must_use]
  pub fn as_u32(&self) -> LogLevel {
    match self {
      Self::Error => log_level::ERROR,
      Self::Warning => log_level::WARNING,
      Self::Info => log_level::INFO,
      Self::Debug => log_level::DEBUG,
      Self::Trace => log_level::TRACE,
    }
  }
}

pub type LogFieldKey = Cow<'static, str>;

//
// LogFieldValue
//

pub type LogFieldValue = StringOrBytes<String, Vec<u8>>;

//
// LogMessageValue
//

pub type LogMessageValue = StringOrBytes<String, Vec<u8>>;

//
// AnnotatedLogFields
//

/// The list of log fields annotated with extra information.
pub type AnnotatedLogFields = AHashMap<LogFieldKey, AnnotatedLogField>;

//
// LogFields
//

/// The list of owned log fields.
pub type LogFields = AHashMap<LogFieldKey, LogFieldValue>;

/// An empty `LogFields`, useful to referencing an empty set of fields without dealing with
/// lifetime issues.
pub static EMPTY_FIELDS: LazyLock<LogFields> = LazyLock::new(AHashMap::new);

//
// AnnotatedLogField
//

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnotatedLogField {
  pub value: LogFieldValue,
  pub kind: LogFieldKind,
}

impl AnnotatedLogField {
  #[must_use]
  pub fn new_ootb(value: impl Into<LogFieldValue>) -> Self {
    Self {
      value: value.into(),
      kind: LogFieldKind::Ootb,
    }
  }

  #[must_use]
  pub fn new_custom(value: impl Into<LogFieldValue>) -> Self {
    Self {
      value: value.into(),
      kind: LogFieldKind::Custom,
    }
  }
}

//
// LogFieldKind
//

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LogFieldKind {
  Ootb,
  Custom,
}

//
// Log
//

// This is a wrapper around an owned log entry that is being processed by the workflow engine.
// Most of this code is a custom protobuf serialization implementation to avoid the overhead of
// the normal protobuf generated code which requires extensive copying. Note that the proto
// implementation calculates size every time it's invoked vs. caching it. In general we expect
// the size calculation to be fast and in the common case it is only computed once making
// caching not useful.
#[derive(Debug, PartialEq, Eq)]
pub struct Log {
  // Remember to update the implementation of the `MemorySized` trait every time the struct is
  // modified!!!
  pub log_level: LogLevel,
  pub log_type: LogType,
  pub message: StringOrBytes<String, Vec<u8>>,
  pub fields: LogFields,
  pub matching_fields: LogFields,
  pub session_id: String,
  pub occurred_at: time::OffsetDateTime,
  pub capture_session: Option<&'static str>,
}

impl Log {
  #[must_use]
  pub fn field_value<'a>(&'a self, field_key: &str) -> Option<Cow<'a, str>> {
    FieldsRef::new(&self.fields, &self.matching_fields).field_value(field_key)
  }

  fn size_proto_data(value: &LogFieldValue) -> u64 {
    let mut my_size = 0;
    match value {
      StringOrBytes::String(s) => {
        my_size += ::protobuf::rt::string_size(1, s);
      },
      StringOrBytes::SharedString(s) => {
        my_size += ::protobuf::rt::string_size(1, s.as_ref());
      },
      StringOrBytes::Bytes(b) => {
        // This encodes the Binary proto message.
        let inner_len = ::protobuf::rt::bytes_size(2, b);
        my_size += 1 + ::protobuf::rt::compute_raw_varint64_size(inner_len) + inner_len;
      },
    }

    my_size
  }

  fn serialize_proto_data(
    field_number: u32,
    value: &LogFieldValue,
    os: &mut CodedOutputStream<'_>,
  ) -> anyhow::Result<()> {
    os.write_tag(field_number, WireType::LengthDelimited)?;
    os.write_raw_varint32(Self::size_proto_data(value).to_u32())?;
    match value {
      StringOrBytes::String(s) => {
        os.write_string(1, s)?;
      },
      StringOrBytes::SharedString(s) => {
        os.write_string(1, s.as_ref())?;
      },
      StringOrBytes::Bytes(b) => {
        // This encodes the Binary proto message.
        let inner_len = ::protobuf::rt::bytes_size(2, b);
        os.write_tag(2, WireType::LengthDelimited)?;
        os.write_raw_varint32(inner_len.to_u32())?;
        os.write_bytes(2, b)?;
      },
    }

    Ok(())
  }

  fn size_proto_field(key: &LogFieldKey, value: &LogFieldValue) -> u64 {
    let mut my_size = 0;
    my_size += ::protobuf::rt::string_size(1, key.as_ref());
    let value_len = Self::size_proto_data(value);
    my_size += 1 + ::protobuf::rt::compute_raw_varint64_size(value_len) + value_len;
    my_size
  }

  fn serialize_proto_field(
    field_number: u32,
    key: &LogFieldKey,
    value: &LogFieldValue,
    os: &mut CodedOutputStream<'_>,
  ) -> anyhow::Result<()> {
    os.write_tag(field_number, WireType::LengthDelimited)?;
    os.write_raw_varint32(Self::size_proto_field(key, value).to_u32())?;
    os.write_string(1, key.as_ref())?;
    Self::serialize_proto_data(2, value, os)?;
    Ok(())
  }

  #[must_use]
  pub fn serialize_proto_size_inner(
    log_level: u32,
    message: &LogFieldValue,
    fields: &LogFields,
    session_id: &str,
    occurred_at: OffsetDateTime,
    log_type: LogType,
    action_ids: &[&str],
    stream_ids: &[&str],
  ) -> u64 {
    let mut my_size = 0;

    my_size += ::protobuf::rt::uint64_size(1, occurred_at.unix_timestamp_nanos().to_u64() / 1000);

    if log_level != 0 {
      my_size += ::protobuf::rt::uint32_size(2, log_level);
    }

    let message_len = Self::size_proto_data(message);
    my_size += 1 + ::protobuf::rt::compute_raw_varint64_size(message_len) + message_len;

    for value in fields {
      let len = Self::size_proto_field(value.0, value.1);
      my_size += 1 + ::protobuf::rt::compute_raw_varint64_size(len) + len;
    }

    my_size += ::protobuf::rt::string_size(5, session_id);

    for value in action_ids {
      my_size += ::protobuf::rt::string_size(6, value);
    }

    if log_type != LogType::NORMAL {
      my_size += ::protobuf::rt::int32_size(7, log_type as i32);
    }

    for value in stream_ids {
      my_size += ::protobuf::rt::string_size(8, value);
    }

    my_size
  }

  #[must_use]
  pub fn serialized_proto_size(&self, action_ids: &[&str], stream_ids: &[&str]) -> u64 {
    Self::serialize_proto_size_inner(
      self.log_level,
      &self.message,
      &self.fields,
      &self.session_id,
      self.occurred_at,
      self.log_type,
      action_ids,
      stream_ids,
    )
  }

  pub fn serialize_proto_to_stream_inner(
    log_level: u32,
    message: &LogFieldValue,
    fields: &LogFields,
    session_id: &str,
    occurred_at: OffsetDateTime,
    log_type: LogType,
    action_ids: &[&str],
    stream_ids: &[&str],
    os: &mut CodedOutputStream<'_>,
  ) -> anyhow::Result<()> {
    os.write_uint64(1, occurred_at.unix_timestamp_nanos().to_u64() / 1000)?;

    if log_level != 0 {
      os.write_uint32(2, log_level)?;
    }

    Self::serialize_proto_data(3, message, os)?;

    for v in fields {
      Self::serialize_proto_field(4, v.0, v.1, os)?;
    }

    os.write_string(5, session_id)?;

    for v in action_ids {
      os.write_string(6, v)?;
    }

    if log_type != LogType::NORMAL {
      os.write_enum(7, log_type as i32)?;
    }

    for v in stream_ids {
      os.write_string(8, v)?;
    }

    Ok(())
  }

  pub fn serialized_proto_to_stream(
    &self,
    action_ids: &[&str],
    stream_ids: &[&str],
    os: &mut CodedOutputStream<'_>,
  ) -> anyhow::Result<()> {
    Self::serialize_proto_to_stream_inner(
      self.log_level,
      &self.message,
      &self.fields,
      &self.session_id,
      self.occurred_at,
      self.log_type,
      action_ids,
      stream_ids,
      os,
    )
  }

  pub fn serialized_proto_to_bytes(
    &self,
    action_ids: &[&str],
    stream_ids: &[&str],
    buffer: &mut [u8],
  ) -> anyhow::Result<()> {
    let mut os = CodedOutputStream::bytes(buffer);
    self.serialized_proto_to_stream(action_ids, stream_ids, &mut os)
  }

  // Currently this assumes timestamp is encoded first and will fail otherwise.
  #[must_use]
  pub fn extract_timestamp(bytes: &[u8]) -> Option<OffsetDateTime> {
    let mut cis = CodedInputStream::from_bytes(bytes);
    let raw_tag = cis.read_raw_tag_or_eof().ok()??;
    // Field number 1, WireType Varint
    if raw_tag == 8
      && let Some(ts_micros) = cis.read_uint64().ok()
    {
      return OffsetDateTime::from_unix_timestamp_nanos((ts_micros * 1000).into()).ok();
    }

    None
  }
}

//
// FieldsRef
//

/// A wrapper around log fields that are divided into two categories: captured fields and matching
/// fields. Captured fields are those that might be stored and uploaded to a remote server, while
/// matching fields are used solely for matching purposes and are never stored or uploaded.
#[derive(Clone, Copy, Debug)]
pub struct FieldsRef<'a> {
  pub captured_fields: &'a LogFields,
  /// Matching fields are fields that are used for matching but are not stored or leave the device.
  /// They should not be exposed publicly to prevent unintentional data leakage.
  matching_fields: &'a LogFields,
}

impl<'a> FieldsRef<'a> {
  #[must_use]
  pub const fn new(captured_fields: &'a LogFields, matching_fields: &'a LogFields) -> Self {
    Self {
      captured_fields,
      matching_fields,
    }
  }

  #[must_use]
  pub fn matching_field_value(&self, key: &str) -> Option<&'a str> {
    self.matching_fields.get(key)?.as_str()
  }

  /// Looks up the field value corresponding to the provided key. If the field doesn't exist or
  /// contains a binary value, None is returned.
  #[must_use]
  pub fn field_value(&self, field_key: &str) -> Option<Cow<'a, str>> {
    // In cases where there are conflicts between the keys of captured and matching fields, captured
    // fields take precedence, as they are potentially stored and uploaded to the remote server.
    if let Some(value) = self
      .captured_fields
      .get(field_key)
      .and_then(|value| value.as_str())
    {
      return Some(Cow::Borrowed(value));
    }

    self.matching_field_value(field_key).map(Cow::Borrowed)
  }
}

//
// LogInterceptor
//

pub trait LogInterceptor: Send + Sync {
  fn process(
    &self,
    log_level: LogLevel,
    log_type: LogType,
    msg: &LogMessage,
    fields: &mut AnnotatedLogFields,
    matching_fields: &mut AnnotatedLogFields,
  );
}

impl MemorySized for AnnotatedLogField {
  fn size(&self) -> usize {
    size_of_val(self) + self.value.size() + size_of_val(&self.kind)
  }
}

impl MemorySized for LogFieldValue {
  fn size(&self) -> usize {
    size_of_val(self)
      + match self {
        Self::String(s) => s.len(),
        // TODO(snowp): Can we avoid counting the size of the string if we know that it's "shared"?
        Self::SharedString(s) => s.len(),
        Self::Bytes(b) => b.capacity(),
      }
  }
}

impl MemorySized for Log {
  fn size(&self) -> usize {
    // The size cannot be computed by just calling a `size_of_val(self)` in here
    // as that does not account for various heap allocations.
    size_of_val(self)
      + self.message.size()
      + self.fields.size()
      + self.matching_fields.size()
      + self.session_id.len()
  }
}
