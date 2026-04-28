// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_runtime::runtime::{BoolWatch, ConfigLoader, crash_reporting};
use std::path::{Path, PathBuf};

#[cfg(test)]
#[path = "./config_writer_test.rs"]
mod tests;

const REPORT_CONFIG_NAME: &str = "config.csv";

//
// ConfigWriter
//

/// Checks whether config has changed, updating a persisted copy on disk if so
/// This uses the following directory layout (within the SDK directory):
/// - `reports/runtime.csv` - Cache of configuration flags for behavior
pub struct ConfigWriter {
  report_directory: PathBuf,
  config_path: PathBuf,
  crash_reporting_enabled_flag: BoolWatch<crash_reporting::Enabled>,
  shutdown: bd_shutdown::ComponentShutdown,
}

impl ConfigWriter {
  pub fn new(
    runtime: &ConfigLoader,
    sdk_directory: &Path,
    shutdown: bd_shutdown::ComponentShutdown,
  ) -> Self {
    let report_directory = sdk_directory.join(crate::REPORTS_DIRECTORY);
    Self {
      crash_reporting_enabled_flag: runtime.register_bool_watch(),
      config_path: report_directory.join(REPORT_CONFIG_NAME),
      report_directory,
      shutdown,
    }
  }

  pub async fn run(&mut self) {
    if let Ok(exists) = tokio::fs::try_exists(self.config_path.clone()).await
      && !exists
    {
      self.write_config_file().await;
    }

    self.check_for_config_changes().await;
  }

  async fn check_for_config_changes(&mut self) {
    loop {
      tokio::select! {
        _ = self.crash_reporting_enabled_flag.changed() => {},
        () = self.shutdown.cancelled() => return,
      };

      // There is chance here of the value changing during platform read, in
      // which case crash reporting would be disabled for that session

      let _ = self.crash_reporting_enabled_flag.read_mark_update();
      self.write_config_file().await;
    }
  }

  async fn write_config_file(&self) {
    let crash_reporting_enabled = *self.crash_reporting_enabled_flag.read();
    log::debug!(
      "Writing enabled:{crash_reporting_enabled} to report config file {}",
      self.config_path.display()
    );

    self.try_ensure_directories_exist().await;

    let contents = format!("crash_reporting.enabled,{crash_reporting_enabled}");
    if let Err(e) = tokio::fs::write(&self.config_path, contents).await {
      log::warn!(
        "Failed to write report directories config file: {} ({})",
        self.config_path.display(),
        e
      );
    }
  }

  async fn try_ensure_directories_exist(&self) {
    // This can fail and we can't do anything about it, so swallow the error. Everything else needs
    // to be resilient to the directory not existing.
    if let Err(e) = tokio::fs::create_dir_all(self.report_directory.clone()).await {
      log::warn!(
        "Failed to create crash directory: {} ({})",
        self.report_directory.display(),
        e
      );
    }
  }
}
