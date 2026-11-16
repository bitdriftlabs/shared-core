// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./fake_crash_test.rs"]
mod tests;

use crate::cli::FakeCrashCommand;
use bd_client_common::file_system::OwnedFile;
use logger_cli::issue_report::{FeatureFlagExposure, IssueReport};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const REPORTS_DIRECTORY: &str = "reports/new";

pub fn write_pending_report(
  sdk_directory: &Path,
  command: &FakeCrashCommand,
) -> anyhow::Result<PathBuf> {
  let report_dir = sdk_directory.join(
    if command.current_session {
      "reports/watcher/current_session"
    } else {
      REPORTS_DIRECTORY
    },
  );
  if command.current_session && !report_dir.is_dir() {
    anyhow::bail!(
      "current-session reports require a running logger started with \
       --attachment-fixture-directory"
    );
  }
  std::fs::create_dir_all(&report_dir)?;

  let report_path = report_dir.join(normalize_file_name(command.file_name.as_deref()));
  let bytes = build_report(command)?;
  if command.current_session {
    let temporary = report_dir
      .canonicalize()?
      .join(format!("{}.tmp", uuid::Uuid::new_v4()));
    let guard = OwnedFile::new(&temporary)
      .ok_or_else(|| anyhow::anyhow!("temporary report path must be absolute"))?;
    std::fs::write(&temporary, bytes)?;
    std::fs::rename(&temporary, &report_path)?;
    guard.disarm();
  } else {
    std::fs::write(&report_path, bytes)?;
  }
  Ok(report_path)
}

fn normalize_file_name(file_name: Option<&str>) -> String {
  let base_name = file_name.map_or_else(default_file_name, std::string::ToString::to_string);
  if Path::new(&base_name)
    .extension()
    .is_some_and(|extension| extension.eq_ignore_ascii_case("cap"))
  {
    base_name
  } else {
    format!("{base_name}.cap")
  }
}

fn default_file_name() -> String {
  let millis = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap_or_default()
    .as_millis();
  format!("fake-crash-{millis}.cap")
}

fn build_report(command: &FakeCrashCommand) -> anyhow::Result<Vec<u8>> {
  IssueReport {
    kind: command.kind,
    platform: command.platform,
    app_id: command.app_id.clone(),
    app_version: command.app_version.clone(),
    app_build_id: command.app_build_id.clone(),
    name: command.reason.clone(),
    reason: command.detail.clone(),
    frames: Vec::new(),
    feature_flags: command
      .feature_flags
      .iter()
      .map(|flag| FeatureFlagExposure {
        name: flag.name.clone(),
        value: flag.value.clone(),
      })
      .collect(),
    timestamp: time::OffsetDateTime::now_utc(),
  }
  .to_bytes()
}
