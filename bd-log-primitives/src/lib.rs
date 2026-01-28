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
use bd_macros::proto_serializable;
use bd_proto::protos::logging::payload::data::Data_type;
use bd_proto::protos::logging::payload::{BinaryData, Data, LogType};
#[cfg(test)]
use bd_proto_util::serialization::ProtoMessageDeserialize;
use bd_proto_util::serialization::ProtoMessageSerialize as _;
use bd_time::OffsetDateTimeExt as _;
use flate2::Compression;
use flate2::write::ZlibEncoder;
use ordered_float::NotNan;
use protobuf::rt::WireType;
use protobuf::{CodedInputStream, CodedOutputStream};
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

/// Binary data wrapper for protobuf serialization.
///
/// This newtype wraps a `Vec<u8>` payload and serializes as the protobuf `BinaryData` message
/// (field 2 = payload bytes). The optional `type` field (field 1) is not used.
#[proto_serializable(
  validate_against = "bd_proto::protos::logging::payload::BinaryData",
  validate_partial
)]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LogBinaryData {
  #[field(id = 2)]
  payload: Vec<u8>,
}

impl LogBinaryData {
  /// Creates a new `LogBinaryData` from a byte vector.
  #[must_use]
  pub fn new(payload: Vec<u8>) -> Self {
    Self { payload }
  }

  /// Returns a reference to the payload bytes.
  #[must_use]
  pub fn payload(&self) -> &[u8] {
    &self.payload
  }

  /// Consumes self and returns the inner payload.
  #[must_use]
  pub fn into_payload(self) -> Vec<u8> {
    self.payload
  }

  /// Returns the capacity of the underlying vector.
  #[must_use]
  pub fn capacity(&self) -> usize {
    self.payload.capacity()
  }
}

impl From<Vec<u8>> for LogBinaryData {
  fn from(payload: Vec<u8>) -> Self {
    Self { payload }
  }
}

impl From<&[u8]> for LogBinaryData {
  fn from(slice: &[u8]) -> Self {
    Self {
      payload: slice.to_vec(),
    }
  }
}

impl From<LogBinaryData> for Vec<u8> {
  fn from(data: LogBinaryData) -> Self {
    data.payload
  }
}

impl AsRef<[u8]> for LogBinaryData {
  fn as_ref(&self) -> &[u8] {
    &self.payload
  }
}

impl std::ops::Deref for LogBinaryData {
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    &self.payload
  }
}

/// A union type that allows representing either a UTF-8 string, binary data, or primitive values.
#[proto_serializable(
  validate_against = "bd_proto::protos::logging::payload::Data",
  validate_partial
)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataValue {
  #[field(id = 1, deserialize)]
  String(String),
  #[field(id = 1)]
  SharedString(Arc<str>),
  #[field(id = 1)]
  StaticString(&'static str),
  #[field(id = 2)]
  Bytes(LogBinaryData),
  #[field(id = 6)]
  Boolean(bool),
  #[field(id = 3)]
  U64(u64),
  #[field(id = 5)]
  I64(i64),
  #[field(id = 4)]
  Double(NotNan<f64>),
}

impl DataValue {
  /// Creates a new `DataValue` instance from a static string slice. This is slightly more
  /// efficient than using `SharedString` as it avoids heap allocation.
  #[must_use]
  pub fn from_static_str(s: &'static str) -> Self {
    Self::StaticString(s)
  }

  #[must_use]
  pub fn into_proto(self) -> Data {
    Data {
      data_type: Some(match self {
        Self::String(s) => Data_type::StringData(s),
        Self::StaticString(s) => Data_type::StringData(s.to_string()),
        Self::SharedString(s) => Data_type::StringData((*s).to_string()),
        Self::Bytes(b) => Data_type::BinaryData(BinaryData {
          payload: b.into_payload(),
          ..Default::default()
        }),
        Self::Boolean(b) => Data_type::BoolData(b),
        Self::U64(v) => Data_type::IntData(v),
        Self::I64(v) => Data_type::SintData(v),
        Self::Double(v) => Data_type::DoubleData(*v),
      }),
      ..Default::default()
    }
  }

  pub fn from_proto(data: Data) -> Option<Self> {
    match data.data_type? {
      Data_type::StringData(s) => Some(Self::String(s)),
      Data_type::BinaryData(b) => Some(Self::Bytes(LogBinaryData::new(b.payload))),
      Data_type::BoolData(b) => Some(Self::Boolean(b)),
      Data_type::IntData(v) => Some(Self::U64(v)),
      Data_type::SintData(v) => Some(Self::I64(v)),
      Data_type::DoubleData(v) => Some(Self::Double(NotNan::new(v).ok()?)),
    }
  }

  /// Extracts the underlying str if the enum represents a String, None otherwise.
  #[must_use]
  pub fn as_str(&self) -> Option<&str> {
    match self {
      Self::String(s) => Some(s.as_ref()),
      Self::SharedString(s) => Some(s.as_ref()),
      Self::StaticString(s) => Some(s),
      Self::Bytes(_) | Self::Boolean(_) | Self::U64(_) | Self::I64(_) | Self::Double(_) => None,
    }
  }

  /// Extracts the underlying bytes if the enum represents a Bytes, None otherwise.
  #[must_use]
  pub fn as_bytes(&self) -> Option<&[u8]> {
    match self {
      Self::String(_)
      | Self::SharedString(_)
      | Self::StaticString(_)
      | Self::Boolean(_)
      | Self::U64(_)
      | Self::I64(_)
      | Self::Double(_) => None,
      Self::Bytes(b) => Some(b.as_ref()),
    }
  }
}

impl From<String> for DataValue {
  fn from(s: String) -> Self {
    Self::String(s)
  }
}

impl From<Arc<str>> for DataValue {
  fn from(s: Arc<str>) -> Self {
    Self::SharedString(s)
  }
}

impl From<Vec<u8>> for DataValue {
  fn from(s: Vec<u8>) -> Self {
    Self::Bytes(LogBinaryData::new(s))
  }
}

impl From<&str> for DataValue {
  fn from(s: &str) -> Self {
    Self::String(s.to_string())
  }
}

impl From<&[u8]> for DataValue {
  fn from(slice: &[u8]) -> Self {
    Self::Bytes(LogBinaryData::new(slice.to_vec()))
  }
}

/// A log message is a string or binary value.
pub type LogMessage = DataValue;

impl std::fmt::Display for LogMessage {
  // This trait requires `fmt` with this exact signature.
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::String(s) => write!(f, "{s}"),
      Self::SharedString(s) => write!(f, "{s}"),
      Self::StaticString(s) => write!(f, "{s}"),
      Self::Bytes(b) => {
        write!(f, "bytes:{b:?}")
      },
      Self::Boolean(b) => write!(f, "{b}"),
      Self::U64(v) => write!(f, "{v}"),
      Self::I64(v) => write!(f, "{v}"),
      Self::Double(v) => write!(f, "{v}"),
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

pub type LogFieldValue = DataValue;

//
// LogMessageValue
//

pub type LogMessageValue = DataValue;

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
// LogContentsRef
//

/// A reference wrapper over the message and fields of a log entry.
///
/// This struct corresponds to the `bitdrift_public.protobuf.logging.v1.Log.CompressedContents`
/// message, which is serialized and then zlib-compressed to produce the `compressed_contents`
/// field of a `CompressedLogRef`.
#[derive(Debug, Clone, Copy)]
#[proto_serializable(
  serialize_only,
  validate_against = "bd_proto::protos::logging::payload::log::CompressedContents"
)]
pub struct LogContentsRef<'a> {
  #[field(id = 1)]
  pub message: &'a DataValue,
  #[field(id = 2, repeated)]
  pub fields: &'a LogFields,
}

impl LogContentsRef<'_> {
  /// Compresses this contents struct into zlib-compressed bytes.
  pub fn compress(&self) -> anyhow::Result<Vec<u8>> {
    let size = self.compute_message_size();
    let compressed_output = Vec::with_capacity(size.to_usize_lossy());
    let mut encoder = ZlibEncoder::new(
      compressed_output,
      Compression::new(DEFAULT_MOBILE_ZLIB_COMPRESSION_LEVEL),
    );
    let mut cos = CodedOutputStream::new(&mut encoder);
    self.serialize_message(&mut cos)?;
    drop(cos);
    Ok(encoder.finish()?)
  }
}

//
// Proc-macro generated log serialization structs
//

// TODO(snowp): Ideally we'd be able to do this without maintaining two different structs, but
// allowing additional context (the compressed values) to be optionally serialized based on presence
// is not currently supported by the proc-macro.

/// A reference wrapper for serializing a raw (uncompressed) log entry.
///
/// This struct uses proc-macro-generated serialization code and corresponds to the
/// `bitdrift_public.protobuf.logging.v1.Log` message with fields 3 and 4 (message and fields)
/// present, and field 9 (`compressed_contents`) absent.
#[proto_serializable(
  serialize_only,
  validate_against = "bd_proto::protos::logging::payload::Log",
  validate_partial
)]
pub struct RawLogRef<'a> {
  #[field(id = 1, serialize_as = "u64")]
  pub occurred_at: i64,
  #[field(id = 2)]
  pub log_level: u32,
  #[field(id = 3)]
  pub message: &'a DataValue,
  #[field(id = 4, repeated)]
  pub fields: &'a LogFields,
  #[field(id = 5)]
  pub session_id: &'a str,
  #[field(id = 6, repeated)]
  pub action_ids: &'a [&'a str],
  #[field(id = 7, proto_enum)]
  pub log_type: LogType,
  #[field(id = 8, repeated)]
  pub stream_ids: &'a [&'a str],
}

/// A reference wrapper for serializing a compressed log entry.
///
/// This struct uses proc-macro-generated serialization code and corresponds to the
/// `bitdrift_public.protobuf.logging.v1.Log` message with field 9 (`compressed_contents`) present,
/// and fields 3 and 4 (message and fields) absent.
#[proto_serializable(
  serialize_only,
  validate_against = "bd_proto::protos::logging::payload::Log",
  validate_partial
)]
pub struct CompressedLogRef<'a> {
  #[field(id = 1, serialize_as = "u64")]
  pub occurred_at: i64,
  #[field(id = 2)]
  pub log_level: u32,
  #[field(id = 5)]
  pub session_id: &'a str,
  #[field(id = 6, repeated)]
  pub action_ids: &'a [&'a str],
  #[field(id = 7, proto_enum)]
  pub log_type: LogType,
  #[field(id = 8, repeated)]
  pub stream_ids: &'a [&'a str],
  #[field(id = 9)]
  pub compressed_contents: &'a [u8],
}

//
// Log
//

/// An owned log entry being processed by the workflow engine.
///
/// Serialization is handled via proc-macro-generated `RawLogRef` and `CompressedLogRef` structs,
/// which avoid the overhead of normal protobuf generated code by eliminating copying.
#[derive(Debug, PartialEq, Eq)]
pub struct Log {
  // Remember to update the implementation of the `MemorySized` trait every time the struct is
  // modified!!!
  pub log_level: LogLevel,
  pub log_type: LogType,
  pub message: DataValue,
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

  /// Returns a reference to the compressible contents (message and fields) of this log.
  #[must_use]
  pub fn contents_ref(&self) -> LogContentsRef<'_> {
    LogContentsRef {
      message: &self.message,
      fields: &self.fields,
    }
  }

  /// Creates a `RawLogRef` for serializing this log without compression.
  #[must_use]
  pub fn as_raw_ref<'a>(
    &'a self,
    action_ids: &'a [&'a str],
    stream_ids: &'a [&'a str],
  ) -> RawLogRef<'a> {
    RawLogRef {
      occurred_at: self.occurred_at.unix_timestamp_micros(),
      log_level: self.log_level,
      message: &self.message,
      fields: &self.fields,
      session_id: &self.session_id,
      action_ids,
      log_type: self.log_type,
      stream_ids,
    }
  }

  /// Creates a `CompressedLogRef` for serializing this log with pre-compressed contents.
  #[must_use]
  pub fn as_compressed_ref<'a>(
    &'a self,
    action_ids: &'a [&'a str],
    stream_ids: &'a [&'a str],
    compressed_contents: &'a [u8],
  ) -> CompressedLogRef<'a> {
    CompressedLogRef {
      occurred_at: self.occurred_at.unix_timestamp_micros(),
      log_level: self.log_level,
      session_id: &self.session_id,
      action_ids,
      log_type: self.log_type,
      stream_ids,
      compressed_contents,
    }
  }
}

/// A wrapper around log data that provides efficient protobuf serialization with optional
/// compression.
///
/// `EncodableLog` can work with either owned or borrowed data (using `Cow`) and provides
/// serialization methods that:
/// - Compute sizes and serialize in a single pass when possible
/// - Handle compression of message+fields when size exceeds the threshold
/// - Accept external `action_ids` and `stream_ids` at serialization time
///
/// The serialization uses either `RawLogRef` (uncompressed) or `CompressedLogRef` (compressed)
/// depending on the content size and compression threshold.
pub struct EncodableLog {
  pub log: Log,
  compression_min_size: u64,
  /// Cached state from size computation
  cached: Option<EncodableLogCached>,
}

/// Cached state for `EncodableLog` serialization.
struct EncodableLogCached {
  /// If compression was applied, the compressed bytes
  compressed_contents: Option<Vec<u8>>,
}

impl EncodableLog {
  /// Creates a new `EncodableLog` from owned log data.
  #[must_use]
  pub fn new(log: Log, compression_min_size: u64) -> Self {
    Self {
      log,
      compression_min_size,
      cached: None,
    }
  }

  /// Computes the serialized size of this log with the given action and stream IDs.
  ///
  /// This caches any compression results for use in subsequent `serialize_to_stream` or
  /// `serialize_to_bytes` calls.
  pub fn compute_size(&mut self, action_ids: &[&str], stream_ids: &[&str]) -> anyhow::Result<u64> {
    // Compute core size if not cached
    let cached = self.cached.get_or_insert_with(|| {
      let contents = LogContentsRef {
        message: &self.log.message,
        fields: &self.log.fields,
      };
      let uncompressed_size = contents.compute_message_size();

      // Decide whether to compress
      let compressed_contents =
        if self.compression_min_size > 0 && uncompressed_size >= self.compression_min_size {
          contents.compress().ok()
        } else {
          None
        };
      EncodableLogCached {
        compressed_contents,
      }
    });
    // Use proc-macro-generated size computation
    Ok(
      if let Some(compressed) = &cached.compressed_contents {
        CompressedLogRef {
          occurred_at: self.log.occurred_at.unix_timestamp_micros(),
          log_level: self.log.log_level,
          session_id: &self.log.session_id,
          action_ids,
          log_type: self.log.log_type,
          stream_ids,
          compressed_contents: compressed,
        }
        .compute_message_size()
      } else {
        RawLogRef {
          occurred_at: self.log.occurred_at.unix_timestamp_micros(),
          log_level: self.log.log_level,
          message: &self.log.message,
          fields: &self.log.fields,
          session_id: &self.log.session_id,
          action_ids,
          log_type: self.log.log_type,
          stream_ids,
        }
        .compute_message_size()
      },
    )
  }

  /// Serializes this log to the given output stream.
  ///
  /// Note: `compute_size` must have been called first to populate the cache.
  pub fn serialize_to_stream(
    &self,
    action_ids: &[&str],
    stream_ids: &[&str],
    os: &mut CodedOutputStream<'_>,
  ) -> anyhow::Result<()> {
    let cached = self
      .cached
      .as_ref()
      .ok_or_else(|| anyhow::anyhow!("compute_size must be called before serialize_to_stream"))?;

    // Use proc-macro-generated serialization
    if let Some(compressed) = &cached.compressed_contents {
      CompressedLogRef {
        occurred_at: self.log.occurred_at.unix_timestamp_micros(),
        log_level: self.log.log_level,
        session_id: &self.log.session_id,
        action_ids,
        log_type: self.log.log_type,
        stream_ids,
        compressed_contents: compressed,
      }
      .serialize_message(os)
    } else {
      RawLogRef {
        occurred_at: self.log.occurred_at.unix_timestamp_micros(),
        log_level: self.log.log_level,
        message: &self.log.message,
        fields: &self.log.fields,
        session_id: &self.log.session_id,
        action_ids,
        log_type: self.log.log_type,
        stream_ids,
      }
      .serialize_message(os)
    }
  }

  /// Serializes this log to the given byte buffer.
  ///
  /// Note: `compute_size` must have been called first to populate the cache.
  pub fn serialize_to_bytes(
    &self,
    action_ids: &[&str],
    stream_ids: &[&str],
    buffer: &mut [u8],
  ) -> anyhow::Result<()> {
    let mut os = CodedOutputStream::bytes(buffer);
    self.serialize_to_stream(action_ids, stream_ids, &mut os)
  }

  /// Extracts the timestamp from serialized log bytes.
  ///
  /// Currently this assumes timestamp is encoded first and will fail otherwise.
  #[must_use]
  pub fn extract_timestamp(bytes: &[u8]) -> Option<OffsetDateTime> {
    let mut cis = CodedInputStream::from_bytes(bytes);
    let raw_tag = cis.read_raw_tag_or_eof().ok()??;
    // uint64 timestamp_unix_micro = 1;
    if raw_tag == 8 // field number 1, wire type 0
      && let Some(ts_micros) = cis.read_uint64().ok()
    {
      return OffsetDateTime::from_unix_timestamp_micros(ts_micros.cast_signed()).ok();
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
        // For these variants the string is stored on the heap so we assume that it takes up no
        // space within the enum itself.
        Self::SharedString(_)
        | Self::StaticString(_)
        | Self::Boolean(_)
        | Self::U64(_)
        | Self::I64(_)
        | Self::Double(_) => 0,
        Self::Bytes(b) => b.capacity(),
        Self::Boolean(_) | Self::U64(_) | Self::I64(_) | Self::Double(_) => 0,
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
