// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Platform hook for attaching platform artifacts to ordinary crash reports.
//!
//! The crash monitor and logger stay platform-neutral: they only hand over reports after ordinary
//! enqueue succeeds. Android supplies the only implementation (`CrashProfileAttribution`), which
//! reads the report's typed `crash_profile_source` field and ignores other reports.

#[cfg(test)]
#[path = "./report_attribution_test.rs"]
mod tests;

use crate::uploader::chunked::read_pending_report;
use crate::{Client, Uploader};
use bd_client_common::artifact::CLIENT_REPORT_ARTIFACT_TYPE_ID;
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::{
  Report,
  root_as_report,
};
use bd_runtime::runtime::ConfigLoader;
use bd_shutdown::ComponentShutdown;
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;

//
// ReportAttribution
//

pub trait ReportAttribution: Send + Sync {
  /// Adds a newly enqueued or restored report. It remains eligible until process exit,
  /// including after its upload ACK.
  fn add_report(&self, report_id: Uuid, report: &Report<'_>, session_id: &str);

  /// The initial previous-run report pass has finished. Earlier platform submissions wait for this;
  /// it does not stop later fresh reports from being added.
  fn mark_ready(&self);

  /// Starts asynchronous adoption with the logger's ordinary upload client.
  fn start(&self, client: Arc<dyn Client>, runtime: &ConfigLoader, shutdown: ComponentShutdown);
}

/// Restores attribution candidates from queued chunked reports before the uploader can retire them.
pub async fn restore_pending_reports(attribution: &dyn ReportAttribution, sdk_directory: &Path) {
  let entries = match Uploader::pending_index(sdk_directory).await {
    Ok(index) => index.chunked_artifact,
    Err(error) => {
      log::warn!("failed to read pending reports for attribution: {error}");
      return;
    },
  };
  let mut restored = 0;
  for entry in entries {
    if entry.type_id != CLIENT_REPORT_ARTIFACT_TYPE_ID {
      continue;
    }
    let Ok(report_id) = Uuid::parse_str(&entry.artifact_id) else {
      continue;
    };
    match read_pending_report(sdk_directory, &entry).await {
      Ok(payload) => match root_as_report(&payload) {
        Ok(report) => {
          attribution.add_report(report_id, &report, &entry.session_id);
          restored += 1;
        },
        Err(error) => log::debug!("queued report {report_id} is unreadable: {error}"),
      },
      Err(error) => log::debug!("queued report {report_id} could not be read: {error}"),
    }
  }
  log::debug!("restored {restored} pending reports for attribution");
}
