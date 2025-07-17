// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

mod config_monitor;
pub mod global_state;
mod json_extractor;

use bd_log_primitives::{LogFields, LogMessageValue};
pub use config_monitor::ConfigMonitor;
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
  fn process_new_reports(&self) -> impl std::future::Future<Output = Vec<CrashLog>> + Send;
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
