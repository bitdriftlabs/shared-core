// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./monitor_test.rs"]
mod tests;

pub mod global_state;

use bd_log_primitives::{LogFields, LogMessageValue};
use bd_proto::flatbuffers::report::bitdrift_public::fbs;
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::{
  Platform,
  Report,
  ReportType,
};
use bd_runtime::runtime::{BoolWatch, ConfigLoader};
use fbs::issue_reporting::v_1::root_as_report;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use time::OffsetDateTime;

#[cfg(test)]
#[ctor::ctor]
fn test_global_init() {
  bd_test_helpers::test_global_init();
}

const REPORTS_DIRECTORY: &str = "reports";

//
// CrashLog
//

/// A single crash log to be emitted by the crash logger.
pub struct CrashLog {
  pub fields: LogFields,
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

//
// Monitor
//

/// Monitors the reports directories for new reports crashes and maintains the configuration file
/// that tells the platform pre-init where to look for reports. Reports are only read on startup
/// but the configuration file may be updated at any time in response to a runtime change.
///
/// This uses the following directory layout (within the SDK directory):
/// - `reports/` - The root directory for all all reports.
/// - `reports/config` - A file that contains the configuration to look for prior stored crashes.
//    The platform layer is responsible for reading this file and looking in each of the
///   reports during pre-init.
/// - `reports/new/` - A directory where new crash reports are placed. The platform layer is
///   responsible for copying the raw files into this directory.
pub struct Monitor {
  // TODO(snowp): Now that we load runtime early enough we can redo how this works a bit to make
  // them less special.
  out_of_band_enabled_flag: BoolWatch<bd_runtime::runtime::artifact_upload::Enabled>,

  previous_session_id: String,

  report_directory: PathBuf,
  global_state_reader: global_state::Reader,
  artifact_client: Arc<dyn bd_artifact_upload::Client>,
}

impl Monitor {
  pub fn new(
    runtime: &ConfigLoader,
    sdk_directory: &Path,
    store: Arc<bd_device::Store>,
    artifact_client: Arc<dyn bd_artifact_upload::Client>,
    previous_session_id: String,
  ) -> Self {
    runtime.expect_initialized();

    Self {
      out_of_band_enabled_flag: runtime.register_watch().unwrap(),
      report_directory: sdk_directory.join(REPORTS_DIRECTORY),
      global_state_reader: global_state::Reader::new(store),
      artifact_client,
      previous_session_id,
    }
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

  pub async fn process_new_reports(&self) -> Vec<CrashLog> {
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

    // Read out the global fields from the previous application run. This is used to populate the
    // fields in the crash report to attempt to match the fields state at the time of the crash.
    let global_state_fields = self.global_state_reader.global_state_fields();

    let mut logs = vec![];

    while let Ok(Some(entry)) = dir.next_entry().await {
      let path = entry.path();
      let ext = path.extension().and_then(OsStr::to_str);
      if path.is_file() && ext == Some("cap") {
        log::info!("Processing new reports report: {}", path.display());
        let contents = match tokio::fs::read(&path).await {
          Ok(contents) => contents,
          Err(e) => {
            log::warn!("Failed to read reports report: {} ({e})", path.display());
            continue;
          },
        };

        let (crash_reason, crash_details, bin_report) = Self::read_report_contents(&contents);

        if crash_reason.is_none() {
          log::warn!(
            "Failed to infer crash reason from report {}, dropping.",
            path.display()
          );
          continue;
        }

        let Some(bin_report) = bin_report else {
          log::warn!("Failed to parse report into fbs format, dropping.");
          continue;
        };

        let (timestamp, state_fields) = Self::read_log_fields(bin_report, &global_state_fields);

        let report_type = match bin_report.type_() {
          ReportType::AppNotResponding => "ANR",
          ReportType::NativeCrash => "Native Crash",
          ReportType::JVMCrash => "Crash",
          ReportType::StrictModeViolation => "Strict Mode Violation",
          ReportType::MemoryTermination => "Memory Termination",
          _ => "Unknown",
        };

        let (crash_field_key, crash_field_value) = if *self.out_of_band_enabled_flag.read() {
          log::debug!("uploading report out of band");

          let Ok(artifact_id) = self.artifact_client.enqueue_upload(
            contents,
            state_fields.clone(),
            timestamp,
            self.previous_session_id.clone(),
          ) else {
            // TODO(snowp): Should we fall back to passing it via a field at this point?
            log::warn!(
              "Failed to enqueue crash report for upload: {}",
              path.display()
            );
            continue;
          };

          ("_crash_artifact_id".into(), artifact_id.to_string().into())
        } else {
          log::debug!("uploading report in band with log line");
          ("_crash_artifact".into(), contents.into())
        };

        let mut fields = state_fields.clone();
        fields.insert(crash_field_key, crash_field_value);
        fields.extend(
          [
            ("_app_exit_reason".into(), report_type.into()),
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
          ]
          .into_iter(),
        );

        logs.push(CrashLog {
          fields,
          timestamp: timestamp.unwrap_or_else(OffsetDateTime::now_utc),
          message: "AppExit".into(),
        });
      }

      // Clean up files after processing them. If this fails we'll potentially end up
      // double-reporting in the future.
      if let Err(e) = tokio::fs::remove_file(&path).await {
        log::warn!("Failed to remove crash report: {} ({e})", path.display());
      }
    }

    logs
  }
}
