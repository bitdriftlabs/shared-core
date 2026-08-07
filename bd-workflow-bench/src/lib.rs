// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![deny(
  clippy::expect_used,
  clippy::indexing_slicing,
  clippy::panic,
  clippy::string_slice,
  clippy::todo,
  clippy::unimplemented,
  clippy::unreachable,
  clippy::unwrap_used
)]

mod bootstrap;
mod corpus;
mod data;
mod replay;
mod report;

use anyhow::anyhow;
use clap::Parser;
pub use replay::{BenchmarkCorpus, WorkflowReplay, fixture_paths};
use report::{
  EngineOutcome,
  EngineOutcomeTotals,
  PerLogRecord,
  RunSummary,
  SlowestLogRecord,
  SlowestLogs,
  TimingSummary,
  truncate_message,
  write_reports,
};
use std::path::PathBuf;
use std::time::Instant;

//
// Options
//

#[derive(Debug, Parser)]
#[command(about = "Replay an exported log corpus through the workflow engine")]
pub struct Options {
  /// A JSON API response containing a `configurationUpdate` message.
  #[arg(long)]
  config: PathBuf,

  /// An NDJSON file containing one exported log record per line.
  #[arg(long)]
  logs: PathBuf,

  /// Directory in which to write summary.json and per-log.jsonl.
  #[arg(long)]
  output_dir: PathBuf,

  /// Number of slowest records to retain in the summary report.
  #[arg(long, default_value_t = 100)]
  top_n: usize,

  /// Number of times to replay the complete corpus with a fresh workflow engine.
  #[arg(long, default_value_t = 1)]
  replay_count: usize,

  /// Aggregate timing and slowest-log data without retaining every per-log record.
  #[arg(long)]
  summary_only: bool,

  /// Start Callgrind instrumentation after setup and stop it before report writing.
  #[arg(long, hide = true)]
  callgrind_instrument: bool,

  /// Replace existing report files in the output directory.
  #[arg(long)]
  overwrite: bool,
}

//
// Run
//

pub async fn run(options: Options) -> anyhow::Result<()> {
  if options.top_n == 0 {
    return Err(anyhow!("--top-n must be greater than zero"));
  }
  if options.replay_count == 0 {
    return Err(anyhow!("--replay-count must be greater than zero"));
  }
  if options.callgrind_instrument && !cfg!(target_os = "linux") {
    return Err(anyhow!("--callgrind-instrument is only supported on Linux"));
  }

  let corpus = BenchmarkCorpus::load(&options.config, &options.logs)?;

  log::info!(
    "loaded {} of {} workflow(s)",
    corpus.loaded_workflow_count(),
    corpus.declared_workflow_count()
  );

  let mut timings = Vec::new();
  let mut per_log = Vec::new();
  let mut slowest_logs = SlowestLogs::new(options.top_n);
  let mut engine_outcomes = EngineOutcomeTotals::default();
  for replay_iteration in 1 ..= options.replay_count {
    let mut replay = corpus.new_replay().await;
    let mut previous_session_id = None;
    let mut tracing_active = false;

    for source_log in corpus.logs() {
      let started_new_session =
        previous_session_id.as_deref() != Some(source_log.log.session_id.as_str());
      let started = Instant::now();
      // Bound Callgrind to the engine operation itself. The surrounding aggregation and report
      // bookkeeping, including this benchmarker's timing, are not workflow evaluation work.
      start_callgrind_instrumentation(options.callgrind_instrument);
      let result = replay.process_log(&source_log.log);
      stop_callgrind_instrumentation(options.callgrind_instrument);
      let evaluation_ns = started.elapsed().as_nanos();
      let evaluation_ns = u64::try_from(evaluation_ns)
        .map_err(|_| anyhow!("workflow evaluation duration exceeded u64 nanoseconds"))?;
      let engine_outcome = EngineOutcome {
        started_new_session,
        tracing_active: result.is_tracing_active,
        tracing_state_changed: result.is_tracing_active != tracing_active,
        triggered_flush_action_count: result.triggered_flush_buffers_action_ids.len(),
        triggered_flush_buffer_count: result.triggered_flushes_buffer_ids.len(),
        capture_screenshot: result.capture_screenshot,
        injected_log_count: result.logs_to_inject.iter().count(),
        workflow_debug_state_count: result.workflow_debug_state.len(),
        has_debug_workflows: result.has_debug_workflows,
      };
      let next_tracing_active = engine_outcome.tracing_active;
      drop(result);

      timings.push(evaluation_ns);
      engine_outcomes.observe(&engine_outcome);
      if !options.summary_only {
        per_log.push(PerLogRecord {
          replay_iteration,
          source_line: source_log.source_line,
          log_id: source_log.log_id.clone(),
          evaluation_ns,
          engine_outcome: engine_outcome.clone(),
        });
      }
      let message = source_log.log.message.to_string();
      slowest_logs.insert(SlowestLogRecord {
        replay_iteration,
        source_line: source_log.source_line,
        log_id: source_log.log_id.clone(),
        evaluation_ns,
        log_type: format!("{:?}", source_log.log.log_type),
        message: truncate_message(&message),
        field_count: source_log.log.fields.len(),
        engine_outcome,
      });
      previous_session_id = Some(source_log.log.session_id.clone());
      tracing_active = next_tracing_active;
    }
  }

  let timing = TimingSummary::from_durations(&timings);
  let summary = RunSummary {
    declared_workflow_count: corpus.declared_workflow_count(),
    loaded_workflow_count: corpus.loaded_workflow_count(),
    source_log_count: corpus.log_count(),
    replay_count: options.replay_count,
    log_count: timings.len(),
    timing,
    engine_outcomes,
  };
  write_reports(
    &options.output_dir,
    &summary,
    &per_log,
    slowest_logs.records(),
    options.overwrite,
  )?;
  print_summary(&summary);
  Ok(())
}

fn start_callgrind_instrumentation(enabled: bool) {
  #[cfg(target_os = "linux")]
  if enabled {
    gungraun::client_requests::callgrind::start_instrumentation();
  }

  #[cfg(not(target_os = "linux"))]
  let _ = enabled;
}

fn stop_callgrind_instrumentation(enabled: bool) {
  #[cfg(target_os = "linux")]
  if enabled {
    gungraun::client_requests::callgrind::stop_instrumentation();
  }

  #[cfg(not(target_os = "linux"))]
  let _ = enabled;
}

fn print_summary(summary: &RunSummary) {
  println!(
    "workflows: {}/{}; replays: {}; logs: {}; p50: {} ns; p99: {} ns; max: {} ns",
    summary.loaded_workflow_count,
    summary.declared_workflow_count,
    summary.replay_count,
    summary.log_count,
    summary.timing.p50_ns,
    summary.timing.p99_ns,
    summary.timing.max_ns,
  );
}
