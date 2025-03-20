// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./monitor_test.rs"]
mod tests;

mod json_extractor;

use bd_log_primitives::LogFields;
use bd_runtime::runtime::{ConfigLoader, StringWatch};
use bd_shutdown::ComponentShutdown;
use itertools::Itertools as _;
use json_extractor::{JsonExtractor, JsonPath};
use std::path::{Path, PathBuf};
use time::OffsetDateTime;

#[cfg(test)]
#[ctor::ctor]
fn test_global_init() {
  bd_test_helpers::test_global_init();
}

const REPORTS_DIRECTORY: &str = "reports";
const INGESTION_CONFIG_FILE: &str = "config";
const REASON_INFERENCE_CONFIG_FILE: &str = "reason_inference";
const DETAILS_INFERENCE_CONFIG_FILE: &str = "details_inference";

//
// CrashLog
//

/// A single crash log to be emitted by the crash logger.
pub struct CrashLog {
  pub fields: LogFields,
  pub timestamp: OffsetDateTime,
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
  reports_directories_flag: StringWatch<bd_runtime::runtime::crash_handling::CrashDirectories>,
  crash_reason_paths_flag: StringWatch<bd_runtime::runtime::crash_handling::CrashReasonPaths>,
  crash_details_paths_flag: StringWatch<bd_runtime::runtime::crash_handling::CrashDetailsPaths>,
  report_directory: PathBuf,
  shutdown: ComponentShutdown,
}

impl Monitor {
  pub fn new(runtime: &ConfigLoader, sdk_directory: &Path, shutdown: ComponentShutdown) -> Self {
    let crash_directories_flag =
      bd_runtime::runtime::crash_handling::CrashDirectories::register(runtime).unwrap();
    let crash_reason_paths_flag =
      bd_runtime::runtime::crash_handling::CrashReasonPaths::register(runtime).unwrap();
    let crash_details_flag =
      bd_runtime::runtime::crash_handling::CrashDetailsPaths::register(runtime).unwrap();

    Self {
      reports_directories_flag: crash_directories_flag,
      crash_reason_paths_flag,
      crash_details_paths_flag: crash_details_flag,
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

  fn guess_crash_details(
    report: &[u8],
    candidate_reason_paths: &[JsonPath],
    candidate_details_path: &[JsonPath],
  ) -> (Option<String>, Option<String>) {
    // The report may come in either as a single JSON object or as a series of JSON objects.
    // Defensively handle both to produce a list of objects that we want to inspect to infer the
    // crash reason.
    if report.first() != Some(&b'{') {
      return (None, None);
    }

    let Ok(report) = std::str::from_utf8(report) else {
      return (None, None);
    };

    let candidates = if let Ok(json) = JsonExtractor::new(report) {
      vec![json]
    } else {
      let Ok(candidates) = report.lines().map(JsonExtractor::new).try_collect() else {
        return (None, None);
      };

      candidates
    };

    // For all the candidate files, look for one which matches against a crash reason. Once we find
    // this candidate, we'll look for the details within the same candidate and return None if we
    // can't find any.

    for candidate in candidates {
      for path in candidate_reason_paths {
        let Some(value) = candidate.extract(path) else {
          continue;
        };

        for path in candidate_details_path {
          let Some(details) = candidate.extract(path) else {
            continue;
          };

          return (Some(value.clone()), Some(details.clone()));
        }

        return (Some(value.clone()), None);
      }
    }

    (None, None)
  }

  async fn check_for_config_changes(&mut self) -> anyhow::Result<()> {
    loop {
      tokio::select! {
        _ = self.reports_directories_flag.changed() => {},
        _ = self.crash_reason_paths_flag.changed() => {},
        () = self.shutdown.cancelled() => return Ok(()),
      };

      // With the way this is set up there is a chance that we flip-flip the configuration file a
      // bit during SDK startup as we read the initial value of the flag and then update it with
      // the value read from the cache. This may not matter as the platform layer reads this value
      // before the SDK starts, but if we find this problematic we might need to figure out how to
      // avoid reading the runtime value before the cached value is available.

      let crash_directories = self.reports_directories_flag.read_mark_update().clone();
      self
        .write_config_file(
          &self.report_directory.join(INGESTION_CONFIG_FILE),
          &crash_directories,
        )
        .await;

      let crash_reason_paths = self.crash_reason_paths_flag.read_mark_update().clone();
      self
        .write_config_file(
          &self.report_directory.join(REASON_INFERENCE_CONFIG_FILE),
          &crash_reason_paths,
        )
        .await;

      let crash_details_paths = self.crash_details_paths_flag.read_mark_update().clone();
      self
        .write_config_file(
          &self.report_directory.join(DETAILS_INFERENCE_CONFIG_FILE),
          &crash_details_paths,
        )
        .await;
    }
  }

  async fn write_config_file(&self, file: &Path, value: &str) {
    if value.is_empty() {
      log::debug!("No report directories configured, removing file");

      if let Err(e) = tokio::fs::remove_file(&file).await {
        log::warn!(
          "Failed to remove report directories config file: {:?} ({})",
          file,
          e
        );
      }
    } else {
      log::debug!("Writing {value:?} to report directories config file {file:?}",);

      self.try_ensure_directories_exist().await;

      if let Err(e) = tokio::fs::write(file, value).await {
        log::warn!(
          "Failed to write report directories config file: {:?} ({})",
          file,
          e
        );
      }
    }
  }

  pub async fn process_new_reports(&self) -> Vec<CrashLog> {
    let crash_reason_paths = self.crash_reason_paths().await;
    let crash_details_paths = self.crash_details_paths().await;

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

        let timestamp = path
          .file_name()
          .and_then(|name| name.to_str())
          .and_then(|name| {
            name.split('_').next().and_then(|timestamp| {
              let Ok(timestamp) = timestamp.parse::<i128>() else {
                return None;
              };

              // We expect to see ms since epoch, so convert from ms to ns to satisfy the
              // OffsetDateTime interface. Converting back to seconds would be a lossy operation.
              OffsetDateTime::from_unix_timestamp_nanos(timestamp * 1_000_000).ok()
            })
          })
          .unwrap_or_else(OffsetDateTime::now_utc);

        let (crash_reason, crash_details) =
          Self::guess_crash_details(&contents, &crash_reason_paths, &crash_details_paths);

        // TODO(snowp): For now everything in here is a crash, eventually we'll need to be able to
        // differentiate.
        // TODO(snowp): Eventually we'll want to upload the report out of band, but for now just
        // chuck it into a log line.
        logs.push(CrashLog {
          fields: [
            ("_crash_artifact".into(), contents.into()),
            (
              "_crash_reason".into(),
              crash_reason.unwrap_or_else(|| "unknown".to_string()).into(),
            ),
            (
              "_crash_details".into(),
              crash_details
                .unwrap_or_else(|| "unknown".to_string())
                .into(),
            ),
          ]
          .into(),
          timestamp,
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

  async fn read_json_paths(&self, file: &str) -> Vec<JsonPath> {
    let raw = tokio::fs::read_to_string(&self.report_directory.join(file))
      .await
      .unwrap_or_default();

    raw.split(',').filter_map(JsonPath::parse).collect()
  }

  async fn crash_reason_paths(&self) -> Vec<JsonPath> {
    self.read_json_paths(REASON_INFERENCE_CONFIG_FILE).await
  }

  async fn crash_details_paths(&self) -> Vec<JsonPath> {
    self.read_json_paths(DETAILS_INFERENCE_CONFIG_FILE).await
  }
}
