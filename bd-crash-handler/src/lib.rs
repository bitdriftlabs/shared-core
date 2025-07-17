// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

mod cap_file_monitor;
pub mod global_state;
mod json_extractor;
mod json_file_monitor;

use bd_log_primitives::{LogFields, LogMessageValue};
pub use cap_file_monitor::CapFileCrashMonitor;
pub use json_file_monitor::JSONFileMonitor;
use std::path::Path;
use time::OffsetDateTime;
use tokio::fs::ReadDir;

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
// FileProcessor
//

/// A trait for ingesting new crash reports
pub trait FileProcessor {
  fn process_file(&self, path: &Path)
  -> impl std::future::Future<Output = Option<CrashLog>> + Send;
  fn can_process_file(&self, path: &Path) -> bool;
}

async fn read_report_dir(path: &Path) -> Result<ReadDir, ()> {
  tokio::fs::read_dir(&path).await.map_err(|e| {
    // Do some basic error checking to see why we failed to read the directory. If the
    // directory just doesn't exist it is not an error.
    if path.exists() {
      log::warn!(
        "Failed to read report directory: {} ({})",
        path.display(),
        e,
      );
    } else {
      log::debug!("Report directory does not exist: {}", path.display());
    }
  })
}

pub async fn process_new_reports<T>(monitors: Vec<&T>, sdk_directory: &Path) -> Vec<CrashLog>
where
  T: FileProcessor,
{
  let report_directory = sdk_directory.join(REPORTS_DIRECTORY);
  let Ok(mut dir) = read_report_dir(&report_directory.join("new")).await else {
    return vec![];
  };

  // TODO(snowp): Add smarter handling to avoid duplicate reporting.
  // TODO(snowp): Consider only reporting one of the pending reports if there are multiple.

  let mut logs = vec![];

  while let Ok(Some(entry)) = dir.next_entry().await {
    let path = entry.path();
    if let Some(monitor) = monitors.iter().find(|m| m.can_process_file(&path)) {
      if let Some(log) = monitor.process_file(&path).await {
        logs.push(log);
      }
    }

    // Clean up files after processing them. If this fails we'll potentially end up
    // double-reporting in the future.
    if let Err(e) = tokio::fs::remove_file(&path).await {
      log::warn!("Failed to remove crash report: {} ({e})", path.display());
    }
  }
  logs
}
