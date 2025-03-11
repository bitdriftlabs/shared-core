// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./monitor_test.rs"]
mod tests;

use bd_log_primitives::{AnnotatedLogField, LogFields};
use bd_runtime::runtime::{ConfigLoader, StringWatch};
use bd_shutdown::ComponentShutdown;
use itertools::Itertools as _;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tinyjson::JsonValue;

#[cfg(test)]
#[ctor::ctor]
fn test_global_init() {
  bd_test_helpers::test_global_init();
}

const REPORTS_DIRECTORY: &str = "reports";
const REPORTS_DIRECTORY_CONFIG_FILE: &str = "directories";

//
// CrashLog
//

/// A single crash log to be emitted by the crash logger.
pub struct CrashLog {
  pub fields: LogFields,
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
/// - `reports/directories` - A file that contains a list of directories that may contain crash
///   reports. The platform layer is responsible for reading this file and looking in each of the
///   reports during pre-init.
/// - `reports/new/` - A directory where new crash reports are placed. The platform layer is
///   responsible for copying the raw files into this directory.
pub struct Monitor {
  reports_directories_flag: StringWatch<bd_runtime::runtime::crash_handling::CrashDirectories>,
  report_directory: PathBuf,
  shutdown: ComponentShutdown,
}

impl Monitor {
  pub fn new(runtime: &ConfigLoader, sdk_directory: &Path, shutdown: ComponentShutdown) -> Self {
    let crash_directories_flag =
      bd_runtime::runtime::crash_handling::CrashDirectories::register(runtime).unwrap();

    Self {
      reports_directories_flag: crash_directories_flag,
      report_directory: sdk_directory.join(REPORTS_DIRECTORY),
      shutdown,
    }
  }

  pub async fn run(&mut self) -> anyhow::Result<()> {
    self.check_for_config_changes().await
  }

  async fn try_ensure_directories_exist(&self) {
    // This can fail and we can't do anything about it, so swallow the error. Everything else needs
    // to be resilient to the directory not existing.
    if let Err(e) = tokio::fs::create_dir_all(&self.report_directory).await {
      log::warn!(
        "Failed to create crash directory: {:?} ({})",
        self.report_directory,
        e
      );
    }
  }

  fn guess_crash_reason(report: &[u8], candidate_paths: &[&[&str]]) -> Option<String> {
    // The report may come in either as a single JSON object or as a series of JSON objects.
    // Defensively handle both to produce a list of objects that we want to inspect to infer the
    // crash reason.
    if report.first() != Some(&b'{') {
      return None;
    }

    let Ok(report) = std::str::from_utf8(report) else {
      return None;
    };

    let candidates = if let Ok(json) = tinyjson::JsonParser::new(report.chars()).parse() {
      vec![json]
    } else {
      let Ok(candidates) = report
        .lines()
        .map(|line| tinyjson::JsonParser::new(line.chars()).parse())
        .try_collect()
      else {
        return None;
      };

      candidates
    };

    // Given a path like `["a", "b", "c"]` we want to look for a key `c` in the object at the end
    // after traversing the path `a.b`. If we find a string value at that key we'll use it as the
    // crash reason.

    for candidate_paths in candidate_paths {
      let key = candidate_paths.last()?;

      for candidate in &candidates {
        let mut candidate_map = candidate.get::<HashMap<String, JsonValue>>()?;

        for path in candidate_paths.iter().take(candidate_paths.len() - 1) {
          candidate_map = candidate_map.get(&path.to_string())?.get()?;
        }

        let candidate = candidate_map.get(&key.to_string())?;

        if let JsonValue::String(reason) = candidate {
          return Some(reason.clone());
        }
      }
    }

    None
  }

  async fn check_for_config_changes(&mut self) -> anyhow::Result<()> {
    loop {
      tokio::select! {
        _ = self.reports_directories_flag.changed() => {},
        () = self.shutdown.cancelled() => return Ok(()),
      };

      // With the way this is set up there is a chance that we flip-flip the configuration file a
      // bit during SDK startup as we read the initial value of the flag and then update it with
      // the value read from the cache. This may not matter as the platform layer reads this value
      // before the SDK starts, but if we find this problematic we might need to figure out how to
      // avoid reading the runtime value before the cached value is available.


      let crash_directories = self.reports_directories_flag.read_mark_update().clone();
      self.write_config_file(&crash_directories).await;
    }
  }

  async fn write_config_file(&self, report_directories: &str) {
    let config_file = &self.report_directory.join(REPORTS_DIRECTORY_CONFIG_FILE);

    if report_directories.is_empty() {
      log::debug!("No report directories configured, removing file");

      if let Err(e) = tokio::fs::remove_file(&config_file).await {
        log::warn!(
          "Failed to remove report directories config file: {:?} ({})",
          config_file,
          e
        );
      }
    } else {
      log::debug!(
        "Writing {report_directories:?} to report directories config file {config_file:?}",
      );

      self.try_ensure_directories_exist().await;

      if let Err(e) = tokio::fs::write(config_file, report_directories).await {
        log::warn!(
          "Failed to write report directories config file: {:?} ({})",
          config_file,
          e
        );
      }
    }
  }

  pub async fn process_new_reports(&self) -> Vec<CrashLog> {
    let mut dir = match tokio::fs::read_dir(&self.report_directory.join("new")).await {
      Ok(dir) => dir,
      Err(e) => {
        // Do some basic error checking to see why we failed to read the directory. If the
        // directory just doesn't exist it is not an error.
        if self.report_directory.join("new").exists() {
          log::warn!(
            "Failed to read report directory: {:?} ({})",
            self.report_directory.join("new"),
            e,
          );
        } else {
          log::debug!(
            "Report directory does not exist: {:?}",
            self.report_directory.join("new")
          );
        }

        return vec![];
      },
    };

    // TODO(snowp): Add smarter handling to avoid duplicate reporting.
    // TODO(snowp): Consider only reporting one of the pending reports if there are multiple.

    let mut logs = vec![];

    while let Ok(Some(entry)) = dir.next_entry().await {
      let path = entry.path();
      if path.is_file() {
        log::info!("Processing new reports report: {:?}", path);
        let contents = match tokio::fs::read(&path).await {
          Ok(contents) => contents,
          Err(e) => {
            log::warn!("Failed to read reports report: {:?} ({})", path, e);
            continue;
          },
        };
        let crash_reason =
          Self::guess_crash_reason(&contents, &[&["crash", "reason"], &["reason"]]);
        // TODO(snowp): For now everything in here is a crash, eventually we'll need to be able to
        // differentiate.
        // TODO(snowp): Eventually we'll want to upload the report out of band, but for now just
        // chuck it into a log line.
        logs.push(CrashLog {
          fields: vec![
            AnnotatedLogField::new_ootb("_crash_artifact".into(), contents.into()).into(),
            AnnotatedLogField::new_ootb(
              "_crash_reason".into(),
              crash_reason.unwrap_or("unknown".to_string()).into(),
            )
            .into(),
          ],
        });
      }

      // Clean up files after processing them. If this fails we'll potentially end up
      // double-reporting in the future.
      if let Err(e) = tokio::fs::remove_file(&path).await {
        log::warn!("Failed to remove crash report: {:?} ({})", path, e);
      }
    }

    logs
  }
}
