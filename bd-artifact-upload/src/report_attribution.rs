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

use crate::uploader::ARTIFACT_UPLOAD_DIRECTORY;
use crate::{Client, Uploader};
use bd_client_common::artifact::CLIENT_REPORT_ARTIFACT_TYPE_ID;
use bd_client_common::file::read_checksummed_data;
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::{
  Report,
  root_as_report,
};
use bd_proto::protos::client::artifact::StorageFormat;
use bd_runtime::runtime::ConfigLoader;
use bd_shutdown::ComponentShutdown;
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;

//
// ReportAttribution
//

pub trait ReportAttribution: Send + Sync {
  /// Adds a report that the ordinary uploader accepted under `report_id`: either a fresh dispatch
  /// during this process, or a report restored from the pending upload index at startup. Reports
  /// remain eligible until process exit, including after their upload ACK.
  fn add_report(&self, report_id: Uuid, report: &Report<'_>, session_id: &str);

  /// The initial previous-run report pass has finished. Earlier platform submissions wait for this;
  /// it does not stop later fresh reports from being added.
  fn mark_ready(&self);

  /// Starts asynchronous adoption with the logger's ordinary upload client.
  fn start(&self, client: Arc<dyn Client>, runtime: &ConfigLoader, shutdown: ComponentShutdown);
}

/// Seeds attribution from reports still in the pending upload index, re-reading each report's
/// queued payload. This must run before the uploader starts, since upload ACK, intent rejection and
/// eviction remove entries and payloads. The pending queue bounds this work; unreadable payloads
/// remain ordinary uploads and are simply not attributable.
pub async fn restore_pending_reports(attribution: &dyn ReportAttribution, sdk_directory: &Path) {
  let entries = match Uploader::pending_artifacts(sdk_directory).await {
    Ok(entries) => entries,
    Err(error) => {
      log::warn!("failed to read pending reports for attribution: {error}");
      return;
    },
  };
  log::debug!(
    "restoring pending attribution reports: artifacts={}",
    entries.len()
  );
  let mut restored = 0;
  for entry in entries {
    if entry.type_id.as_deref() != Some(CLIENT_REPORT_ARTIFACT_TYPE_ID) {
      continue;
    }
    let Ok(report_id) = Uuid::parse_str(&entry.name) else {
      log::debug!(
        "skipping pending attribution report: reason=invalid_report_id, artifact={}",
        entry.name
      );
      continue;
    };
    let path = sdk_directory
      .join(&*ARTIFACT_UPLOAD_DIRECTORY)
      .join(&entry.name);
    let payload = match tokio::fs::read(&path).await {
      Ok(payload) => payload,
      Err(error) => {
        log::debug!("failed to read pending report {report_id}: {error}");
        continue;
      },
    };
    let payload = if entry.storage_format.enum_value_or_default() == StorageFormat::RAW {
      Ok(payload)
    } else {
      read_checksummed_data(&payload)
    };
    let payload = match payload {
      Ok(payload) => payload,
      Err(error) => {
        log::debug!("pending report {report_id} storage decoding failed: {error}");
        continue;
      },
    };
    match root_as_report(&payload) {
      Ok(report) => {
        log::debug!(
          "restoring crash profile attribution candidate from pending report {report_id}"
        );
        attribution.add_report(report_id, &report, &entry.session_id);
        restored += 1;
      },
      Err(error) => log::debug!("pending report {report_id} is not a readable report: {error}"),
    }
  }
  log::debug!("restored {restored} pending reports for attribution");
}
