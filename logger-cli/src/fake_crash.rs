// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/licenses/strict/1.0.0

#[cfg(test)]
#[path = "./fake_crash_test.rs"]
mod tests;

use crate::cli::FakeCrashCommand;
use logger_cli::issue_report::{FeatureFlagExposure, IssueKind, IssueReport};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const REPORTS_DIRECTORY: &str = "reports/new";

pub fn write_pending_report(
  sdk_directory: &Path,
  command: &FakeCrashCommand,
) -> anyhow::Result<PathBuf> {
  let report_dir = sdk_directory.join(REPORTS_DIRECTORY);
  std::fs::create_dir_all(&report_dir)?;

  let report_path = report_dir.join(normalize_file_name(command.file_name.as_deref()));
  std::fs::write(&report_path, build_report(command)?)?;
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
    kind: IssueKind::NativeCrash,
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
