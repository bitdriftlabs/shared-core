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
#[path = "./monitor_test.rs"]
mod tests;

pub mod config_writer;
mod file_watcher;
pub mod global_state;

use bd_artifact_upload::SnappedFeatureFlag;
use bd_client_common::debug_check_lifecycle_less_than;
use bd_client_common::init_lifecycle::{InitLifecycle, InitLifecycleState};
use bd_error_reporter::reporter::handle_unexpected;
use bd_log_primitives::{
  AnnotatedLogField,
  AnnotatedLogFields,
  LogFieldKind,
  LogFields,
  LogLevel,
  LogMessageValue,
  log_level,
};
use bd_proto::flatbuffers::report::bitdrift_public::fbs;
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::{
  Platform,
  Report,
  ReportType,
};
use bd_resilient_kv::TimestampedValue;
use bd_state::StateReader;
use bd_time::OffsetDateTimeExt as _;
use fbs::issue_reporting::v_1::root_as_report;
use itertools::Itertools as _;
use memmap2::Mmap;
use std::ffi::OsStr;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use time::OffsetDateTime;

#[cfg(test)]
#[ctor::ctor]
fn test_global_init() {
  bd_test_helpers::test_global_init();
}

const REPORTS_DIRECTORY: &str = "reports";

pub use config_writer::ConfigWriter;

//
// CrashLog
//

/// A single crash log to be emitted by the crash logger.
pub struct CrashLog {
  pub log_level: LogLevel,
  pub fields: AnnotatedLogFields,
  pub timestamp: OffsetDateTime,
  pub message: LogMessageValue,
}

//
// CrashLogger
//

/// A trait for logging crash reports, allowing the crash handling code to be decoupled from the
/// logging code.
#[cfg_attr(test, mockall::automock)]
pub trait CrashLogger: Send + Sync {
  fn log_crash(&self, report: &[u8]);
}

/// Indicates whether the report is from the previous or current process execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportOrigin {
  Current,
  Previous,
}

//
// Monitor
//

/// Monitors the reports directories for new reports crashes and maintains the configuration file
/// that tells the platform pre-init where to look for reports. Reports are only read on startup
/// but the configuration file may be updated at any time in response to a runtime change.
///
/// This uses the following directory layout (within the SDK directory):
/// - `reports/` - The root directory for all all reports.
/// - `reports/new/` - A directory where new crash reports are placed. The platform layer is
///   responsible for copying the raw files into this directory.
/// - `reports/watcher/current_session/` - When watcher directory exists, native layer will scan for
///   added reports to be processed right away.
#[derive(Clone)]
pub struct Monitor {
  report_directory: PathBuf,
  previous_run_state: bd_resilient_kv::ScopedMaps,
  state: bd_state::Store,
  artifact_client: Arc<dyn bd_artifact_upload::Client>,

  global_state_reader: global_state::Reader,
  pub session: Arc<bd_session::Strategy>,
  monitor: Option<file_watcher::FileWatcher>,
}

impl Monitor {
  pub fn new(
    sdk_directory: &Path,
    store: Arc<bd_device::Store>,
    artifact_client: Arc<dyn bd_artifact_upload::Client>,
    session: Arc<bd_session::Strategy>,
    init_lifecycle: &InitLifecycleState,
    state: bd_state::Store,
    previous_run_state: bd_resilient_kv::ScopedMaps,
    emit_log: impl Fn(CrashLog) -> anyhow::Result<()> + Send + Sync + 'static,
  ) -> Self {
    debug_check_lifecycle_less_than!(
      init_lifecycle,
      InitLifecycle::LogProcessingStarted,
      "Monitor must be created before log processing starts"
    );

    let global_state_reader = global_state::Reader::new(store);

    let mut monitor = Self {
      report_directory: sdk_directory.join(REPORTS_DIRECTORY),
      previous_run_state,
      state,
      artifact_client,
      global_state_reader,
      session,
      monitor: None,
    };

    // Only enable the file watcher if the reports/watcher directory exists. This allows the
    // platform layer to control whether file watching is enabled.
    if Self::is_reports_watcher_enabled(&sdk_directory.join(REPORTS_DIRECTORY)) {
      let monitor_clone = monitor.clone();
      match file_watcher::FileWatcher::new(&sdk_directory.join(REPORTS_DIRECTORY).join("watcher")) {
        Err(e) => {
          handle_unexpected::<(), anyhow::Error>(
            Err(e),
            "Failed to initialize report file watcher",
          );
        },
        Ok((watcher, mut rx)) => {
          tokio::spawn(async move {
            loop {
              let Some(report) = rx.recv().await else {
                log::debug!("Report file watcher channel closed, exiting");
                break;
              };

              log::debug!(
                "Report file watcher detected new report: {}",
                report.path.display()
              );
              if let Some(crash_log) = monitor_clone
                .process_file(&report.path, report.origin)
                .await
              {
                // TODO(snowp): Once we migrate over all logs (including previous process logs),
                // we'll need to ensure that we are setting the correct fields. For current
                // session, it is correct to let freshly evaluated global state be snapped.
                // TODO(snowp): Consider not setting timestamp at all for the current session logs
                // as it is correct to use now().
                if let Err(e) = emit_log(crash_log) {
                  log::warn!(
                    "Failed to emit crash log for report {}: {}",
                    report.path.display(),
                    e
                  );
                }
              }
            }
          });
          monitor.monitor = Some(watcher);
        },
      }
    }

    monitor
  }

  fn read_log_fields(
    report: Report<'_>,
    global_state_fields: &LogFields,
  ) -> (Option<OffsetDateTime>, LogFields) {
    let (crash_time, os_version, platform) = report
      .device_metrics()
      .map(|dev| {
        (
          dev.time(),
          dev.os_build().and_then(|b| b.version()),
          dev.platform(),
        )
      })
      .unwrap_or_default();

    let (app_id, app_version, build_number) = report
      .app_metrics()
      .map(|app| (app.app_id(), app.version(), app.build_number()))
      .unwrap_or_default();

    let timestamp = crash_time.and_then(|t| {
      OffsetDateTime::from_unix_timestamp_nanos(i128::from(
        (t.seconds() * 1_000_000_000) + u64::from(t.nanos()),
      ))
      .ok()
    });
    let mut fields = global_state_fields.clone();
    fields.extend([
      ("os_version".into(), os_version.unwrap_or("unknown").into()),
      (
        "app_version".into(),
        app_version.unwrap_or("unknown").into(),
      ),
    ]);

    if let Some(app_id) = app_id {
      fields.insert("app_id".into(), app_id.into());
    }

    match platform {
      Platform::Android => {
        let version_code =
          build_number.map_or_else(|| "unknown".to_string(), |b| b.version_code().to_string());
        fields.insert("_app_version_code".into(), version_code.into());
      },
      Platform::iOS | Platform::macOS => {
        let bundle_version = build_number
          .and_then(|b| b.cf_bundle_version())
          .unwrap_or("unknown");
        fields.insert("_build_number".into(), bundle_version.into());
      },
      _ => {},
    }
    (timestamp, fields)
  }

  fn read_report_contents(report: &[u8]) -> (Option<String>, Option<String>, Option<Report<'_>>) {
    root_as_report(report).map_or((None, None, None), |bin_report| {
      bin_report
        .errors()
        .and_then(|errs| errs.iter().next())
        .map_or((None, None, Some(bin_report)), |err| {
          (
            err.name().map(str::to_owned),
            err.reason().map(str::to_owned),
            Some(bin_report),
          )
        })
    })
  }

  fn report_type_to_reason(report_type: ReportType) -> &'static str {
    match report_type {
      ReportType::AppNotResponding => "ANR",
      ReportType::NativeCrash => "Native Crash",
      ReportType::JVMCrash => "Crash",
      ReportType::StrictModeViolation => "Strict Mode Violation",
      ReportType::MemoryTermination => "Memory Termination",
      ReportType::JavaScriptFatalError => "Fatal JavaScript Error",
      ReportType::JavaScriptNonFatalError => "Non-Fatal JavaScript Error",
      ReportType::HandledError => "Handled Error",
      _ => "Unknown",
    }
  }

  /// Checks whether the reports/watcher directory exists. This indicates that we want to make use
  /// of file watching to detect new reports during the current session.
  #[must_use]
  pub fn is_reports_watcher_enabled(report_directory: &Path) -> bool {
    report_directory.join("watcher").is_dir()
  }

  /// Processes all pending reports found in the "new" reports directory.
  pub async fn process_all_pending_reports(&self) -> Vec<CrashLog> {
    let mut dir = match tokio::fs::read_dir(&self.report_directory.join("new")).await {
      Ok(dir) => dir,
      Err(e) => {
        // Do some basic error checking to see why we failed to read the directory. If the
        // directory just doesn't exist it is not an error.
        if self.report_directory.join("new").exists() {
          log::warn!(
            "Failed to read report directory: {} ({})",
            self.report_directory.join("new").display(),
            e,
          );
        } else {
          log::debug!(
            "Report directory does not exist: {}",
            self.report_directory.join("new").display()
          );
        }

        return vec![];
      },
    };

    // TODO(snowp): Add smarter handling to avoid duplicate reporting.
    // TODO(snowp): Consider only reporting one of the pending reports if there are multiple.

    let mut logs = vec![];

    while let Ok(Some(entry)) = dir.next_entry().await {
      log::debug!("Considering report file: {}", entry.path().display());
      let path = entry.path();
      let ext = path.extension().and_then(OsStr::to_str);
      if path.is_file()
        && ext == Some("cap")
        && let Some(crash_log) = self.process_file(&path, ReportOrigin::Previous).await
      {
        logs.push(crash_log);
      }
    }

    logs
  }

  fn build_crash_log_fields(
    state_fields: LogFields,
    artifact_id: uuid::Uuid,
    bin_report: &Report<'_>,
    crash_reason: Option<String>,
    crash_details: Option<String>,
  ) -> LogFields {
    let mut fields = state_fields;
    fields.insert("_crash_artifact_id".into(), artifact_id.to_string().into());
    fields.extend([
      (
        "_app_exit_reason".into(),
        Self::report_type_to_reason(bin_report.type_()).into(),
      ),
      (
        "_app_exit_info".into(),
        crash_reason.unwrap_or_else(|| "unknown".to_string()).into(),
      ),
      (
        "_app_exit_details".into(),
        crash_details
          .unwrap_or_else(|| "unknown".to_string())
          .into(),
      ),
      ("_fatal_issue_mechanism".into(), "BUILT_IN".into()),
    ]);
    fields
  }

  async fn get_feature_flags(&self, origin: ReportOrigin) -> Vec<SnappedFeatureFlag> {
    let values: Vec<(String, String, OffsetDateTime)> = match origin {
      ReportOrigin::Previous => self
        .previous_run_state
        .iter()
        .filter(|(scope, ..)| *scope == bd_resilient_kv::Scope::FeatureFlagExposure)
        .filter_map(|(_, name, TimestampedValue { value, timestamp })| {
          value
            .has_string_value()
            .then(|| {
              let variant = value.string_value().to_string();
              Some((
                name.clone(),
                variant,
                OffsetDateTime::from_unix_timestamp_micros((*timestamp).try_into().ok()?).ok()?,
              ))
            })
            .flatten()
        })
        .collect(),
      ReportOrigin::Current => self
        .state
        .read()
        .await
        .as_scoped_maps()
        .iter()
        .filter(|(scope, ..)| *scope == bd_resilient_kv::Scope::FeatureFlagExposure)
        .filter_map(|(_, key, value)| {
          value
            .value
            .has_string_value()
            .then(|| {
              let variant = value.value.string_value().to_string();
              let timestamp =
                OffsetDateTime::from_unix_timestamp_nanos(i128::from(value.timestamp) * 1_000)
                  .ok()?;
              Some((key.clone(), variant, timestamp))
            })
            .flatten()
        })
        .collect(),
    };

    values
      .into_iter()
      .map(|(name, variant, timestamp)| {
        SnappedFeatureFlag::new(name, (!variant.is_empty()).then_some(variant), timestamp)
      })
      .collect_vec()
  }

  fn get_global_state_fields(&self, origin: ReportOrigin) -> LogFields {
    match origin {
      ReportOrigin::Current => self.global_state_reader.global_state_fields(),
      ReportOrigin::Previous => self
        .global_state_reader
        .previous_global_state_fields()
        .cloned()
        .unwrap_or_default(),
    }
  }

  fn get_session_id(&self, origin: ReportOrigin) -> String {
    match origin {
      ReportOrigin::Current => self.session.session_id(),
      ReportOrigin::Previous => self
        .session
        .previous_process_session_id()
        .unwrap_or_default(),
    }
  }

  async fn process_file(&self, file_path: &Path, origin: ReportOrigin) -> Option<CrashLog> {
    if !file_path.exists() || file_path.extension().and_then(OsStr::to_str) != Some("cap") {
      log::debug!("Skipping invalid report file: {}", file_path.display());
      return None;
    }

    let Ok(file) = File::open(file_path) else {
      log::warn!("Failed to open report file: {}", file_path.display());
      return None;
    };

    log::debug!("Processing report file: {}", file_path.display());

    let mapped_file = match unsafe { Mmap::map(&file) } {
      Ok(m) => m,
      Err(e) => {
        log::warn!(
          "Failed to memory-map report file: {} ({e})",
          file_path.display()
        );
        return None;
      },
    };

    let (crash_reason, crash_details, bin_report) = Self::read_report_contents(&mapped_file);

    if crash_reason.is_none() {
      log::warn!(
        "Failed to infer crash reason from report {}, dropping.",
        file_path.display()
      );
      return None;
    }

    let Some(bin_report) = bin_report else {
      log::warn!("Failed to parse report into fbs format, dropping.");
      return None;
    };

    let reporting_feature_flags = self.get_feature_flags(origin).await;
    let global_state_fields = self.get_global_state_fields(origin);
    let session_id = self.get_session_id(origin);
    let (timestamp, state_fields) = Self::read_log_fields(bin_report, &global_state_fields);

    log::debug!("uploading report out of band");

    let Ok(artifact_id) = self.artifact_client.enqueue_upload(
      file,
      state_fields.clone(),
      timestamp,
      session_id.clone(),
      reporting_feature_flags.clone(),
      "client_report".to_string(),
      false, // Don't skip intent negotiation for crash reports
    ) else {
      log::warn!(
        "Failed to enqueue issue report for upload: {}",
        file_path.display()
      );
      return None;
    };

    let fields = Self::build_crash_log_fields(
      state_fields,
      artifact_id,
      &bin_report,
      crash_reason,
      crash_details,
    );

    if let Err(e) = tokio::fs::remove_file(file_path).await {
      log::warn!(
        "Failed to remove issue report: {} ({e})",
        file_path.display()
      );
    }

    Some(CrashLog {
      log_level: log_level::ERROR,
      fields: fields
        .into_iter()
        .map(|(key, value)| {
          (
            key,
            AnnotatedLogField {
              value,
              kind: LogFieldKind::Ootb,
            },
          )
        })
        .collect(),
      timestamp: timestamp.unwrap_or_else(OffsetDateTime::now_utc),
      message: "AppExit".into(),
    })
  }
}
