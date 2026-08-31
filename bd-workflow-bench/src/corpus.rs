// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./corpus_test.rs"]
mod tests;

use crate::data::data_value;
use ahash::AHashMap;
use anyhow::anyhow;
use bd_log_primitives::{Log, LogFields, log_level};
use bd_proto::protos::logging::payload::LogType;
use serde::Deserialize;
use serde_json::Value;
use std::io::BufRead;
use std::path::Path;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

//
// CorpusReader
//

pub struct CorpusReader {
  lines: std::io::Lines<Box<dyn BufRead>>,
  source_line: usize,
}

#[derive(Debug)]
pub struct SourceLog {
  pub source_line: usize,
  pub log_id: String,
  pub log: Log,
}

impl CorpusReader {
  pub fn open(path: &Path) -> anyhow::Result<Self> {
    Ok(Self {
      lines: crate::fixtures::open_reader(path)?.lines(),
      source_line: 0,
    })
  }

  pub fn next_log(&mut self) -> anyhow::Result<Option<SourceLog>> {
    for line in self.lines.by_ref() {
      self.source_line += 1;
      let line = line?;
      if line.trim().is_empty() {
        continue;
      }

      let export: ExportLog = serde_json::from_str(&line)
        .map_err(|e| anyhow!("line {}: invalid JSON log record: {e}", self.source_line))?;
      return export.into_source_log(self.source_line).map(Some);
    }

    Ok(None)
  }
}

//
// ExportLog
//

#[derive(Deserialize)]
struct ExportLog {
  id: String,
  timestamp: String,
  session_id: String,
  log_level: String,
  #[serde(default)]
  log_type: Option<String>,
  message: String,
  fields: ExportFields,
}

#[derive(Deserialize)]
struct ExportFields {
  fields: AHashMap<String, Value>,
}

impl ExportLog {
  fn into_source_log(self, source_line: usize) -> anyhow::Result<SourceLog> {
    let occurred_at = OffsetDateTime::parse(&self.timestamp, &Rfc3339)
      .map_err(|e| anyhow!("line {source_line}: invalid timestamp: {e}"))?;
    let fields = self
      .fields
      .fields
      .into_iter()
      .map(|(key, value)| {
        data_value(value, &format!("line {source_line}: field {key}"))
          .map(|value| (key.into(), value))
      })
      .collect::<anyhow::Result<LogFields>>()?;

    Ok(SourceLog {
      source_line,
      log_id: self.id,
      log: Log {
        log_level: parse_log_level(&self.log_level, source_line)?,
        log_type: parse_log_type(self.log_type.as_deref(), source_line)?,
        message: self.message.into(),
        fields,
        matching_fields: LogFields::default(),
        session_id: self.session_id.into(),
        occurred_at,
        capture_session: None,
      },
    })
  }
}

fn parse_log_level(value: &str, source_line: usize) -> anyhow::Result<u32> {
  match value {
    "TRACE" => Ok(log_level::TRACE),
    "DEBUG" => Ok(log_level::DEBUG),
    "INFO" => Ok(log_level::INFO),
    "WARN" | "WARNING" => Ok(log_level::WARNING),
    "ERROR" => Ok(log_level::ERROR),
    _ => Err(anyhow!("line {source_line}: unknown log level {value:?}")),
  }
}

fn parse_log_type(value: Option<&str>, source_line: usize) -> anyhow::Result<LogType> {
  match value.unwrap_or("NORMAL") {
    "NORMAL" => Ok(LogType::NORMAL),
    "REPLAY" => Ok(LogType::REPLAY),
    "LIFECYCLE" => Ok(LogType::LIFECYCLE),
    "RESOURCE" => Ok(LogType::RESOURCE),
    "INTERNAL_SDK" => Ok(LogType::INTERNAL_SDK),
    "VIEW" => Ok(LogType::VIEW),
    "DEVICE" => Ok(LogType::DEVICE),
    "UX" => Ok(LogType::UX),
    "SPAN" => Ok(LogType::SPAN),
    value => Err(anyhow!("line {source_line}: unknown log type {value:?}")),
  }
}
