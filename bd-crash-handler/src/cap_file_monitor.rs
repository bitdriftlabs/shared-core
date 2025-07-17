// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{CrashLog, FileProcessor, global_state};
use bd_log_primitives::LogFields;
use bd_proto::flatbuffers::report::bitdrift_public::fbs;
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::{
  Platform,
  Report,
  ReportType,
};
use fbs::issue_reporting::v_1::root_as_report;
use std::ffi::OsStr;
use std::iter::once;
use std::path::Path;
use std::sync::Arc;
use time::OffsetDateTime;

/// Processes crash reports in the supported formats located in the prescribed
/// directory, typically `{SDK directory}/reports/new`.
pub struct CapFileCrashMonitor {
  previous_session_id: String,
  global_state_reader: global_state::Reader,
  artifact_client: Arc<dyn bd_artifact_upload::Client>,
}

impl CapFileCrashMonitor {
  pub fn new(
    artifact_client: Arc<dyn bd_artifact_upload::Client>,
    store: Arc<bd_device::Store>,
    previous_session_id: String,
  ) -> Self {
    Self {
      artifact_client,
      global_state_reader: global_state::Reader::new(store),
      previous_session_id,
    }
  }

  fn read_report_type(report: &Report<'_>) -> String {
    match report.type_() {
      ReportType::AppNotResponding => "ANR",
      ReportType::JVMCrash => "Crash",
      ReportType::NativeCrash => "Native Crash",
      ReportType::StrictModeViolation => "StrictMode Violation",
      ReportType::HandledError => "Handled Error",
      ReportType::MemoryTermination => "Memory Termination",
      _ => "Unknown",
    }
    .into()
  }

  fn platform_as_str(platform: Platform) -> String {
    platform.variant_name().unwrap_or("unknown").to_lowercase()
  }

  fn read_log_fields(
    &self,
    report: &Report<'_>,
    path: &Path,
  ) -> Option<(OffsetDateTime, LogFields)> {
    let (reason, details) =
      report
        .errors()
        .and_then(|errs| errs.iter().next())
        .map_or((None, None), |err| {
          (
            err.name().map(str::to_owned),
            err.reason().map(str::to_owned),
          )
        });
    let Some(reason) = reason else {
      log::warn!(
        "Failed to infer crash reason from report {}, dropping.",
        path.display()
      );
      return None;
    };
    let (crash_time, model, os_version, platform) = report
      .device_metrics()
      .map(|dev| {
        (
          dev.time(),
          dev.model(),
          dev.os_build().and_then(|b| b.version()),
          dev.platform(),
        )
      })
      .unwrap_or_default();

    let (app_version, build_number) = report
      .app_metrics()
      .map(|app| (app.version(), app.build_number()))
      .unwrap_or_default();

    let timestamp = crash_time
      .and_then(|t| {
        OffsetDateTime::from_unix_timestamp_nanos(i128::from(t.seconds() * 1_000_000_000)).ok()
      })
      .unwrap_or_else(OffsetDateTime::now_utc);

    let global_state_fields = self.global_state_reader.global_state_fields();
    let mut fields = global_state_fields.clone();
    fields.extend([
      (
        "_app_exit_reason".into(),
        Self::read_report_type(report).into(),
      ),
      ("_app_exit_info".into(), reason.into()),
      (
        "_app_exit_details".into(),
        details.unwrap_or_else(|| "unknown".to_string()).into(),
      ),
      ("_fatal_issue_mechanism".into(), "BUILT_IN".into()),
      ("os_version".into(), os_version.unwrap_or("unknown").into()),
      ("model".into(), model.unwrap_or("unknown").into()),
      ("platform".into(), Self::platform_as_str(platform).into()),
      (
        "app_version".into(),
        app_version.unwrap_or("unknown").into(),
      ),
    ]);

    if platform == Platform::Android {
      let version_code =
        build_number.map_or_else(|| "unknown".to_string(), |b| b.version_code().to_string());
      fields.extend([("_app_version_code".into(), version_code.into())]);
    }
    Some((timestamp, fields))
  }
}

impl FileProcessor for CapFileCrashMonitor {
  fn can_process_file(&self, path: &Path) -> bool {
    let ext = path.extension().and_then(OsStr::to_str);
    path.is_file() && ext == Some("cap")
  }

  async fn process_file(&self, path: &Path) -> Option<CrashLog> {
    let ext = path.extension().and_then(OsStr::to_str);
    if !path.is_file() || ext != Some("cap") {
      return None;
    }
    let contents = match tokio::fs::read(&path).await {
      Ok(contents) => contents,
      Err(e) => {
        log::warn!("Failed to read report: {} ({e})", path.display());
        return None;
      },
    };
    let Ok(report) = root_as_report(&contents) else {
      return None;
    };

    let (timestamp, state_fields) = self.read_log_fields(&report, path)?;
    let crash_field = match self.artifact_client.enqueue_upload(
      contents,
      state_fields.clone(),
      Some(timestamp),
      self.previous_session_id.clone(),
    ) {
      Ok(artifact_id) => ("_crash_artifact_id".into(), artifact_id.to_string().into()),
      Err(e) => {
        // TODO(snowp): Should we fall back to passing it via a field at this point?
        log::warn!(
          "Failed to enqueue crash report for upload: {} {e}",
          path.display()
        );
        return None;
      },
    };
    let mut fields = state_fields.clone();
    fields.extend(once(crash_field));
    Some(CrashLog {
      fields,
      timestamp,
      message: "AppExit".into(),
    })
  }
}
