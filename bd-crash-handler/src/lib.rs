// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./monitor_test.rs"]
mod tests;

pub mod global_state;
mod json_extractor;

use anyhow::bail;
use bd_log_primitives::{LogFieldKey, LogFieldValue, LogFields, LogMessageValue};
use bd_proto::flatbuffers::report::bitdrift_public::fbs;
use bd_runtime::runtime::{BoolWatch, ConfigLoader, StringWatch};
use bd_shutdown::ComponentShutdown;
use fbs::issue_reporting::v_1::root_as_report;
use itertools::Itertools as _;
use json_extractor::{JsonExtractor, JsonPath};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
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
  // TODO(snowp): Now that we load runtime early enough we can redo how this works a bit to make
  // them less special.
  reports_directories_flag: StringWatch<bd_runtime::runtime::crash_handling::CrashDirectories>,
  crash_reason_paths_flag: StringWatch<bd_runtime::runtime::crash_handling::CrashReasonPaths>,
  crash_details_paths_flag: StringWatch<bd_runtime::runtime::crash_handling::CrashDetailsPaths>,
  out_of_band_enabled_flag: BoolWatch<bd_runtime::runtime::artifact_upload::Enabled>,

  report_directory: PathBuf,
  global_state_reader: global_state::Reader,
  artifact_client: Arc<dyn bd_artifact_upload::Client>,
  shutdown: ComponentShutdown,
}

impl Monitor {
  pub fn new(
    runtime: &ConfigLoader,
    sdk_directory: &Path,
    store: Arc<bd_device::Store>,
    artifact_client: Arc<dyn bd_artifact_upload::Client>,
    shutdown: ComponentShutdown,
  ) -> Self {
    runtime.expect_initialized();

    Self {
      reports_directories_flag: runtime.register_watch().unwrap(),
      crash_reason_paths_flag: runtime.register_watch().unwrap(),
      crash_details_paths_flag: runtime.register_watch().unwrap(),
      out_of_band_enabled_flag: runtime.register_watch().unwrap(),
      report_directory: sdk_directory.join(REPORTS_DIRECTORY),
      global_state_reader: global_state::Reader::new(store),
      artifact_client,
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
        "Failed to create crash directory: {} ({})",
        self.report_directory.display(),
        e
      );
    }
  }

  fn guess_crash_details(
    report: &[u8],
    candidate_reason_paths: &[JsonPath],
    candidate_details_path: &[JsonPath],
  ) -> (Option<String>, Option<String>) {
    if let Ok(bin_report) = root_as_report(report) {
      return bin_report
        .errors()
        .and_then(|errs| errs.iter().next())
        .map_or((None, None), |err| {
          (
            err.name().map(str::to_owned),
            err.reason().map(str::to_owned),
          )
        });
    }
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
          "Failed to remove report directories config file: {} ({e})",
          file.display()
        );
      }
    } else {
      log::debug!(
        "Writing {value:?} to report directories config file {}",
        file.display()
      );

      self.try_ensure_directories_exist().await;

      if let Err(e) = tokio::fs::write(file, value).await {
        log::warn!(
          "Failed to write report directories config file: {} ({e})",
          file.display()
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
            "Failed to read report directory: {} ({})",
            self.report_directory.join("new").display(),
            e,
          );
        } else {
          log::debug!(
            "Report directory does not exist: {}",
            self.report_directory.join("new").display()
          );
        }

        return vec![];
      },
    };

    // TODO(snowp): Add smarter handling to avoid duplicate reporting.
    // TODO(snowp): Consider only reporting one of the pending reports if there are multiple.

    // Read out the global fields from the previous application run. This is used to populate the
    // fields in the crash report to attempt to match the fields state at the time of the crash.
    let global_state_fields = self.global_state_reader.global_state_fields();

    let mut logs = vec![];

    while let Ok(Some(entry)) = dir.next_entry().await {
      let path = entry.path();
      if path.is_file() {
        log::info!("Processing new reports report: {}", path.display());
        let contents = match tokio::fs::read(&path).await {
          Ok(contents) => contents,
          Err(e) => {
            log::warn!("Failed to read reports report: {} ({e})", path.display());
            continue;
          },
        };

        let timestamp = path
          .file_name()
          .and_then(|name| name.to_str())
          .and_then(|name| {
            name.split('_').next().and_then(|timestamp| {
              let Ok(timestamp) = timestamp.parse::<i128>() else {
                log::debug!("Failed to parse timestamp from file name: {name:?}");
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

        if crash_reason.is_none() {
          log::warn!(
            "Failed to infer crash reason from report {}, dropping.",
            path.display()
          );
          continue;
        }

        let Ok(fatal_issue_metadata) = get_fatal_issue_metadata(&path) else {
          log::warn!(
            "Failed to get fatal issue metadata for path: {}",
            path.display()
          );
          continue;
        };

        let crash_field = if *self.out_of_band_enabled_flag.read() {
          log::debug!("uploading report out of band");

          let Ok(artifact_id) = self
            .artifact_client
            .enqueue_upload(contents, global_state_fields.clone())
          else {
            // TODO(snowp): Should we fall back to passing it via a field at this point?
            log::warn!(
              "Failed to enqueue crash report for upload: {}",
              path.display()
            );
            continue;
          };

          ("_crash_artifact_id".into(), artifact_id.to_string().into())
        } else {
          log::debug!("uploading report in band with log line");
          ("_crash_artifact".into(), contents.into())
        };

        let message = fatal_issue_metadata.message_value;
        let mut fields = global_state_fields.clone();
        fields.extend(std::iter::once(crash_field));
        fields.extend(
          [
            (
              "_app_exit_reason".into(),
              fatal_issue_metadata.report_type_value,
            ),
            (
              fatal_issue_metadata.reason_key.clone(),
              crash_reason.unwrap_or_else(|| "unknown".to_string()).into(),
            ),
            (
              fatal_issue_metadata.details_key.clone(),
              crash_details
                .unwrap_or_else(|| "unknown".to_string())
                .into(),
            ),
            (
              "_fatal_issue_mechanism".into(),
              fatal_issue_metadata.mechanism_type_value.into(),
            ),
          ]
          .into_iter(),
        );

        logs.push(CrashLog {
          fields,
          timestamp,
          message,
        });
      }

      // Clean up files after processing them. If this fails we'll potentially end up
      // double-reporting in the future.
      if let Err(e) = tokio::fs::remove_file(&path).await {
        log::warn!("Failed to remove crash report: {} ({e})", path.display());
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

//
// FatalIssueMetadata
//

/// Holds the expected log keys/values and message depending on the report file type
#[derive(Debug)]
struct FatalIssueMetadata {
  details_key: LogFieldKey,
  mechanism_type_value: &'static str,
  message_value: LogMessageValue,
  reason_key: LogFieldKey,
  report_type_value: LogFieldValue,
}

fn get_fatal_issue_metadata(path: &Path) -> anyhow::Result<FatalIssueMetadata> {
  let ext = path.extension().and_then(OsStr::to_str);
  let file_name = path
    .file_name()
    .and_then(|f| f.to_str())
    .unwrap_or_default();

  let report_type = if file_name.contains("_anr") {
    "ANR"
  } else if file_name.contains("_native_crash") {
    "Native Crash"
  } else if file_name.contains("_crash") {
    "Crash"
  } else {
    "Unknown"
  };

  match ext {
    Some("envelope" | "json") => Ok(FatalIssueMetadata {
      mechanism_type_value: "INTEGRATION",
      message_value: "App crashed".into(),
      details_key: "_crash_details".into(),
      reason_key: "_crash_reason".into(),
      report_type_value: report_type.into(),
    }),
    Some("cap") => Ok(FatalIssueMetadata {
      mechanism_type_value: "BUILT_IN",
      message_value: "AppExit".into(),
      details_key: "_app_exit_details".into(),
      reason_key: "_app_exit_info".into(),
      report_type_value: report_type.into(),
    }),
    // TODO(FranAguilera): BIT-5414 Clean up tombstone handling
    _ => bail!("Unknown file extension for path: {}", path.display()),
  }
}
