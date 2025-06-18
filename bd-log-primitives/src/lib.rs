// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use ahash::AHashMap;
use bd_bounded_buffer::MemorySized;
pub use bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::LogType;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::sync::Arc;

pub const LOG_FIELD_NAME_TYPE: &str = "log_type";
pub const LOG_FIELD_NAME_LEVEL: &str = "log_level";
pub const LOG_FIELD_NAME_MESSAGE: &str = "_message";

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

/// A copy of an incoming log line.
#[derive(Debug, PartialEq, Eq)]
pub struct Log {
  // Remember to update the implementation
  // of the `MemorySized` trait every
  // time the struct is modified!!!
  pub log_level: LogLevel,
  pub log_type: LogType,
  pub message: StringOrBytes<String, Vec<u8>>,
  pub fields: LogFields,
  pub matching_fields: LogFields,
  pub session_id: String,
  pub occurred_at: time::OffsetDateTime,

  pub capture_session: Option<String>,
}

//
// LogRef
//

/// A reference to a log message and its associated fields.
#[derive(Clone, Copy, Debug)]
pub struct LogRef<'a> {
  pub log_type: LogType,
  pub log_level: LogLevel,
  pub message: &'a LogMessage,
  pub fields: &'a FieldsRef<'a>,
  pub session_id: &'a str,
  pub occurred_at: time::OffsetDateTime,

  pub capture_session: Option<&'a str>,
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
  pub fn matching_field_value(&self, key: &str) -> Option<&str> {
    self.matching_fields.get(key)?.as_str()
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
