// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./report_test.rs"]
mod tests;

use anyhow::anyhow;
use serde::Serialize;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;

//
// PerLogRecord
//

#[derive(Clone, Debug, Serialize)]
pub struct PerLogRecord {
  pub replay_iteration: usize,
  pub source_line: usize,
  pub log_id: String,
  pub evaluation_ns: u64,
  pub engine_outcome: EngineOutcome,
}

//
// EngineOutcome
//

#[derive(Clone, Debug, Default, Serialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct EngineOutcome {
  pub started_new_session: bool,
  pub tracing_active: bool,
  pub tracing_state_changed: bool,
  pub triggered_flush_action_count: usize,
  pub triggered_flush_buffer_count: usize,
  pub capture_screenshot: bool,
  pub injected_log_count: usize,
  pub workflow_debug_state_count: usize,
  pub has_debug_workflows: bool,
}

//
// EngineOutcomeTotals
//

#[derive(Debug, Default, Serialize)]
pub struct EngineOutcomeTotals {
  pub session_start_count: usize,
  pub tracing_state_change_count: usize,
  pub triggered_flush_action_count: usize,
  pub triggered_flush_buffer_count: usize,
  pub screenshot_request_count: usize,
  pub injected_log_count: usize,
  pub workflow_debug_state_count: usize,
  pub debug_workflow_evaluation_count: usize,
}

impl EngineOutcomeTotals {
  pub fn observe(&mut self, outcome: &EngineOutcome) {
    self.session_start_count += usize::from(outcome.started_new_session);
    self.tracing_state_change_count += usize::from(outcome.tracing_state_changed);
    self.triggered_flush_action_count += outcome.triggered_flush_action_count;
    self.triggered_flush_buffer_count += outcome.triggered_flush_buffer_count;
    self.screenshot_request_count += usize::from(outcome.capture_screenshot);
    self.injected_log_count += outcome.injected_log_count;
    self.workflow_debug_state_count += outcome.workflow_debug_state_count;
    self.debug_workflow_evaluation_count += usize::from(outcome.has_debug_workflows);
  }
}

//
// SlowestLogRecord
//

#[derive(Debug, Serialize)]
pub struct SlowestLogRecord {
  pub replay_iteration: usize,
  pub source_line: usize,
  pub log_id: String,
  pub evaluation_ns: u64,
  pub log_type: String,
  pub message: String,
  pub field_count: usize,
  pub engine_outcome: EngineOutcome,
}

pub fn truncate_message(message: &str) -> String {
  const MAX_CHARS: usize = 100;

  let mut chars = message.chars();
  let prefix = chars.by_ref().take(MAX_CHARS).collect::<String>();
  if chars.next().is_none() {
    return prefix;
  }

  let mut truncated = prefix.chars().take(MAX_CHARS - 1).collect::<String>();
  truncated.push('…');
  truncated
}

//
// SlowestLogs
//

pub struct SlowestLogs {
  limit: usize,
  records: Vec<SlowestLogRecord>,
}

impl SlowestLogs {
  pub fn new(limit: usize) -> Self {
    Self {
      limit,
      records: vec![],
    }
  }

  pub fn insert(&mut self, record: SlowestLogRecord) {
    self.records.push(record);
    self.records.sort_unstable_by(|left, right| {
      right
        .evaluation_ns
        .cmp(&left.evaluation_ns)
        .then_with(|| left.source_line.cmp(&right.source_line))
    });
    self.records.truncate(self.limit);
  }

  pub fn records(&self) -> &[SlowestLogRecord] {
    &self.records
  }
}

//
// TimingSummary
//

#[derive(Debug, Serialize)]
pub struct TimingSummary {
  pub total_evaluation_ns: u128,
  pub min_ns: u64,
  pub mean_ns: u64,
  pub p50_ns: u64,
  pub p90_ns: u64,
  pub p95_ns: u64,
  pub p99_ns: u64,
  pub p999_ns: u64,
  pub max_ns: u64,
}

impl TimingSummary {
  pub fn from_durations(durations: &[u64]) -> Self {
    if durations.is_empty() {
      return Self {
        total_evaluation_ns: 0,
        min_ns: 0,
        mean_ns: 0,
        p50_ns: 0,
        p90_ns: 0,
        p95_ns: 0,
        p99_ns: 0,
        p999_ns: 0,
        max_ns: 0,
      };
    }

    let mut sorted = durations.to_vec();
    sorted.sort_unstable();
    let total_evaluation_ns = sorted.iter().map(|duration| u128::from(*duration)).sum();
    let len = sorted.len();
    let Some(min_ns) = sorted.first().copied() else {
      return Self::from_durations(&[]);
    };
    let Some(max_ns) = sorted.last().copied() else {
      return Self::from_durations(&[]);
    };
    Self {
      total_evaluation_ns,
      min_ns,
      mean_ns: u64::try_from(total_evaluation_ns / len as u128).unwrap_or(u64::MAX),
      p50_ns: percentile(&sorted, 500),
      p90_ns: percentile(&sorted, 900),
      p95_ns: percentile(&sorted, 950),
      p99_ns: percentile(&sorted, 990),
      p999_ns: percentile(&sorted, 999),
      max_ns,
    }
  }
}

//
// RunSummary
//

#[derive(Debug, Serialize)]
pub struct RunSummary {
  pub declared_workflow_count: usize,
  pub loaded_workflow_count: usize,
  pub source_log_count: usize,
  pub replay_count: usize,
  pub log_count: usize,
  pub timing: TimingSummary,
  pub engine_outcomes: EngineOutcomeTotals,
}

#[derive(Serialize)]
struct SummaryReport<'a> {
  #[serde(flatten)]
  summary: &'a RunSummary,
  slowest_logs: &'a [SlowestLogRecord],
}

pub fn write_reports(
  output_dir: &Path,
  summary: &RunSummary,
  per_log: &[PerLogRecord],
  slowest_logs: &[SlowestLogRecord],
  overwrite: bool,
) -> anyhow::Result<()> {
  fs::create_dir_all(output_dir)?;
  let per_log_path = output_dir.join("per-log.jsonl");
  let summary_path = output_dir.join("summary.json");
  if !overwrite && (per_log_path.exists() || summary_path.exists()) {
    return Err(anyhow!(
      "report files already exist in {}; pass --overwrite to replace them",
      output_dir.display()
    ));
  }

  let per_log_tmp = output_dir.join("per-log.jsonl.tmp");
  let summary_tmp = output_dir.join("summary.json.tmp");
  write_per_log(&per_log_tmp, per_log)?;
  let report = SummaryReport {
    summary,
    slowest_logs,
  };
  write_json(&summary_tmp, &report)?;
  fs::rename(per_log_tmp, per_log_path)?;
  fs::rename(summary_tmp, summary_path)?;
  Ok(())
}

fn write_per_log(path: &Path, per_log: &[PerLogRecord]) -> anyhow::Result<()> {
  let mut writer = BufWriter::new(File::create(path)?);
  for record in per_log {
    serde_json::to_writer(&mut writer, record)?;
    writer.write_all(b"\n")?;
  }
  writer.flush()?;
  Ok(())
}

fn write_json(path: &Path, value: &impl Serialize) -> anyhow::Result<()> {
  let writer = BufWriter::new(File::create(path)?);
  serde_json::to_writer_pretty(writer, value)?;
  Ok(())
}

fn percentile(sorted: &[u64], permille: usize) -> u64 {
  let rank = (permille * sorted.len()).div_ceil(1000).saturating_sub(1);
  sorted.get(rank).copied().unwrap_or_default()
}
