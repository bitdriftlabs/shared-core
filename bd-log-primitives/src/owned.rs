//
// LogLine
//

use crate::{LogFields, LogLevel, StringOrBytes};
use bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::LogType;

/// A copy of an incoming log line.
///
/// Contrary to `LogLine` it contains group, timestamp
/// and all of the fields that are supposed to be logged with a given log.
#[derive(Debug)]
pub struct LogLine {
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
}
