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

#[cfg(test)]
#[path = "./lib_test.rs"]
mod lib_test;

pub mod size;
pub mod tiny_set;
pub mod zlib;

use crate::size::MemorySized;
use crate::zlib::DEFAULT_MOBILE_ZLIB_COMPRESSION_LEVEL;
use ahash::AHashMap;
use bd_proto::protos::logging::payload::LogType;
use flate2::Compression;
use flate2::write::ZlibEncoder;
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
  fn to_u32_lossy(self) -> u32;
}

#[allow(clippy::cast_possible_truncation)]
impl LossyIntToU32 for usize {
  fn to_u32_lossy(self) -> u32 {
    debug_assert!(u32::try_from(self).is_ok());
    self as u32
  }
}

#[allow(clippy::cast_possible_truncation)]
impl LossyIntToU32 for u64 {
  fn to_u32_lossy(self) -> u32 {
    debug_assert!(u32::try_from(self).is_ok());
    self as u32
  }
}

pub trait LossyIntToU64 {
  fn to_u64_lossy(self) -> u64;
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
impl LossyIntToU64 for i128 {
  fn to_u64_lossy(self) -> u64 {
    debug_assert!(u64::try_from(self).is_ok());
    self as u64
  }
}

pub trait LossyIntToUsize {
  fn to_usize_lossy(self) -> usize;
}

#[allow(clippy::cast_possible_truncation)]
impl LossyIntToUsize for u64 {
  fn to_usize_lossy(self) -> usize {
    debug_assert!(usize::try_from(self).is_ok());
    self as usize
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
// the normal protobuf generated code which requires extensive copying.
//
// TODO(mattklein123): Very likely if we wrote a custom proc macro we could generate this code
// automatically (though with compression logic it may be tricky).
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
}

pub struct LogEncodingHelper {
  pub log: Log,
  compression_min_size: u64,
  cached_encoding_data: Option<CachedEncodingData>,
}

pub struct CachedEncodingData {
  // The size of everything except action IDs and stream IDs, since these are sent/encoded on
  // demand depending on the context.
  core_size: u64,
  // If we have decided to compress the log, this contains the compressed contents.
  compressed_contents: Option<Vec<u8>>,
}

impl LogEncodingHelper {
  #[must_use]
  pub fn new(log: Log, compression_min_size: u64) -> Self {
    Self {
      log,
      compression_min_size,
      cached_encoding_data: None,
    }
  }

  // bitdrift_public.protobuf.logging.v1.Data
  fn size_proto_data(value: &LogFieldValue) -> u64 {
    let mut my_size = 0;
    match value {
      StringOrBytes::String(s) => {
        // string string_data = 1;
        my_size += ::protobuf::rt::string_size(1, s);
      },
      StringOrBytes::SharedString(s) => {
        // string string_data = 1;
        my_size += ::protobuf::rt::string_size(1, s.as_ref());
      },
      StringOrBytes::Bytes(b) => {
        // This encodes the Binary proto message.
        // bytes payload = 2;
        let inner_len = ::protobuf::rt::bytes_size(2, b);
        // BinaryData binary_data = 2;
        my_size += 1 + ::protobuf::rt::compute_raw_varint64_size(inner_len) + inner_len;
      },
    }

    my_size
  }

  // bitdrift_public.protobuf.logging.v1.Data
  fn serialize_proto_data(
    field_number: u32,
    value: &LogFieldValue,
    os: &mut CodedOutputStream<'_>,
  ) -> anyhow::Result<()> {
    os.write_tag(field_number, WireType::LengthDelimited)?;
    os.write_raw_varint32(Self::size_proto_data(value).to_u32_lossy())?;
    match value {
      StringOrBytes::String(s) => {
        // string string_data = 1;
        os.write_string(1, s)?;
      },
      StringOrBytes::SharedString(s) => {
        // string string_data = 1;
        os.write_string(1, s.as_ref())?;
      },
      StringOrBytes::Bytes(b) => {
        // This encodes the Binary proto message.
        // bytes payload = 2;
        let inner_len = ::protobuf::rt::bytes_size(2, b);
        os.write_tag(2, WireType::LengthDelimited)?;
        os.write_raw_varint32(inner_len.to_u32_lossy())?;
        // BinaryData binary_data = 2;
        os.write_bytes(2, b)?;
      },
    }

    Ok(())
  }

  // bitdrift_public.protobuf.logging.v1.Log.Field
  fn size_proto_field(key: &LogFieldKey, value: &LogFieldValue) -> u64 {
    let mut my_size = 0;
    // string key = 1;
    my_size += ::protobuf::rt::string_size(1, key.as_ref());
    // Data value = 2;
    let value_len = Self::size_proto_data(value);
    my_size += 1 + ::protobuf::rt::compute_raw_varint64_size(value_len) + value_len;
    my_size
  }

  // bitdrift_public.protobuf.logging.v1.Log.Field
  fn serialize_proto_field(
    field_number: u32,
    key: &LogFieldKey,
    value: &LogFieldValue,
    os: &mut CodedOutputStream<'_>,
  ) -> anyhow::Result<()> {
    os.write_tag(field_number, WireType::LengthDelimited)?;
    os.write_raw_varint32(Self::size_proto_field(key, value).to_u32_lossy())?;
    // string key = 1;
    os.write_string(1, key.as_ref())?;
    // Data value = 2;
    Self::serialize_proto_data(2, value, os)?;
    Ok(())
  }

  // bitdrift_public.protobuf.logging.v1.Log
  pub fn serialize_proto_size_inner(
    log_level: u32,
    message: &LogFieldValue,
    fields: &LogFields,
    session_id: &str,
    occurred_at: OffsetDateTime,
    log_type: LogType,
    action_ids: &[&str],
    stream_ids: &[&str],
    cached_encoding_data: &mut Option<CachedEncodingData>,
    compression_min_size: u64,
  ) -> anyhow::Result<u64> {
    let mut my_size = match cached_encoding_data {
      None => {
        // Start by figuring out the size of message and fields as this is what we may want to
        // compress.
        let mut message_and_fields_size = 0;

        // Data message = 3;
        // No empty check as this should always be set.
        let message_len = Self::size_proto_data(message);
        message_and_fields_size +=
          1 + ::protobuf::rt::compute_raw_varint64_size(message_len) + message_len;

        // repeated Field fields = 4;
        for value in fields {
          let len = Self::size_proto_field(value.0, value.1);
          message_and_fields_size += 1 + ::protobuf::rt::compute_raw_varint64_size(len) + len;
        }

        let (message_and_fields_size, compressed_contents) =
          if compression_min_size <= message_and_fields_size {
            // If we are compressing, we need to actually do the compression here and we cache the
            // compression result. This allows us to compute the final size including with the
            // compression result.
            let compressed_output = Vec::with_capacity(message_and_fields_size.to_usize_lossy());
            let mut encoder = ZlibEncoder::new(
              compressed_output,
              Compression::new(DEFAULT_MOBILE_ZLIB_COMPRESSION_LEVEL),
            );
            let mut cos = CodedOutputStream::new(&mut encoder);

            // The following encodes bitdrift_public.protobuf.logging.v1.Log.CompressedContents
            // Data message = 1;
            Self::serialize_proto_data(1, message, &mut cos)?;

            // repeated Field fields = 2;
            for v in fields {
              Self::serialize_proto_field(2, v.0, v.1, &mut cos)?;
            }

            drop(cos);
            let compressed_output = encoder.finish()?;

            // bytes compressed_contents = 9;
            (
              ::protobuf::rt::bytes_size(9, &compressed_output),
              Some(compressed_output),
            )
          } else {
            (message_and_fields_size, None)
          };

        let mut my_size = message_and_fields_size;

        // uint64 timestamp_unix_micro = 1;
        // No zero check is this should always be set.
        my_size +=
          ::protobuf::rt::uint64_size(1, occurred_at.unix_timestamp_nanos().to_u64_lossy() / 1000);

        // uint32 log_level = 2;
        if log_level != 0 {
          my_size += ::protobuf::rt::uint32_size(2, log_level);
        }

        // string session_id = 5;
        // No zero check as this should always be set.
        my_size += ::protobuf::rt::string_size(5, session_id);

        // LogType log_type = 7;
        if log_type != LogType::NORMAL {
          my_size += ::protobuf::rt::int32_size(7, log_type as i32);
        }

        *cached_encoding_data = Some(CachedEncodingData {
          core_size: my_size,
          compressed_contents,
        });

        my_size
      },
      Some(cached_data) => cached_data.core_size,
    };

    // repeated string action_ids = 6;
    for value in action_ids {
      my_size += ::protobuf::rt::string_size(6, value);
    }

    // repeated string stream_ids = 8;
    for value in stream_ids {
      my_size += ::protobuf::rt::string_size(8, value);
    }

    Ok(my_size)
  }

  // bitdrift_public.protobuf.logging.v1.Log
  pub fn serialized_proto_size(
    &mut self,
    action_ids: &[&str],
    stream_ids: &[&str],
  ) -> anyhow::Result<u64> {
    Self::serialize_proto_size_inner(
      self.log.log_level,
      &self.log.message,
      &self.log.fields,
      &self.log.session_id,
      self.log.occurred_at,
      self.log.log_type,
      action_ids,
      stream_ids,
      &mut self.cached_encoding_data,
      self.compression_min_size,
    )
  }

  // bitdrift_public.protobuf.logging.v1.Log
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
    cached_encoding_data: Option<&CachedEncodingData>,
  ) -> anyhow::Result<()> {
    // uint64 timestamp_unix_micro = 1;
    os.write_uint64(1, occurred_at.unix_timestamp_nanos().to_u64_lossy() / 1000)?;

    // uint32 log_level = 2;
    if log_level != 0 {
      os.write_uint32(2, log_level)?;
    }

    if cached_encoding_data
      .is_none_or(|cached_encoding_data| cached_encoding_data.compressed_contents.is_none())
    {
      // Data message = 3;
      Self::serialize_proto_data(3, message, os)?;

      // repeated Field fields = 4;
      for v in fields {
        Self::serialize_proto_field(4, v.0, v.1, os)?;
      }
    }

    // string session_id = 5;
    os.write_string(5, session_id)?;

    // repeated string action_ids = 6;
    for v in action_ids {
      os.write_string(6, v)?;
    }

    // LogType log_type = 7;
    if log_type != LogType::NORMAL {
      os.write_enum(7, log_type as i32)?;
    }

    // repeated string stream_ids = 8;
    for v in stream_ids {
      os.write_string(8, v)?;
    }

    // bytes compressed_contents = 9;
    if let Some(cached_encoding_data) = cached_encoding_data
      && let Some(compressed_contents) = &cached_encoding_data.compressed_contents
    {
      os.write_bytes(9, compressed_contents)?;
    }

    Ok(())
  }

  // bitdrift_public.protobuf.logging.v1.Log
  pub fn serialized_proto_to_stream(
    &self,
    action_ids: &[&str],
    stream_ids: &[&str],
    os: &mut CodedOutputStream<'_>,
  ) -> anyhow::Result<()> {
    Self::serialize_proto_to_stream_inner(
      self.log.log_level,
      &self.log.message,
      &self.log.fields,
      &self.log.session_id,
      self.log.occurred_at,
      self.log.log_type,
      action_ids,
      stream_ids,
      os,
      self.cached_encoding_data.as_ref(),
    )
  }

  // bitdrift_public.protobuf.logging.v1.Log
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
    // uint64 timestamp_unix_micro = 1;
    if raw_tag == 8 // field number 1, wire type 0
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
