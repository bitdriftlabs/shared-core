// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Per-instance async driver for local data generators, using the same SDK as logger-cli.

#[cfg(test)]
#[path = "driver_test.rs"]
mod tests;

use crate::issue_report::IssueReport;
use crate::logger::{LoggerArgs, LoggerHolder, make_logger};
use bd_artifact_upload::Uploader;
use bd_logger::{
  AnnotatedLogField,
  Block,
  CaptureSession,
  InitializationState,
  LogAttributesOverrides,
  LogFieldKind,
  SdkStatus,
  log_level,
};
use bd_proto::protos::logging::payload::LogType;
use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::Duration;
use time::OffsetDateTime;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

// The stop-time stats upload can be held back by the 30 s explicit-flush minimum interval, in which
// case the next periodic upload (every 60 s by default) sends it.
const STOP_STATS_UPLOAD_DEADLINE: Duration = Duration::from_secs(120);
const STOP_ARTIFACT_UPLOAD_DEADLINE: Duration = Duration::from_secs(120);
const UPLOAD_POLL_INTERVAL: Duration = Duration::from_millis(250);
const REPORT_DEADLINE: Duration = Duration::from_secs(60);
const REPORT_POLL_INTERVAL: Duration = Duration::from_millis(50);
// The SDK deletes a report before it enqueues the `AppExit` log and before its uploader persists
// the queued artifact, and acknowledges neither. Both normally finish well within this settle, but
// a busy uploader can take longer, so the ordering it buys is practical rather than guaranteed.
const REPORT_SETTLE: Duration = Duration::from_millis(250);
// The SDK evicts the oldest queued artifact beyond 10 by default, so hand over a report only while
// fewer uploads than that are pending.
const MAX_PENDING_UPLOADS: usize = 8;

const REPORTS_DIRECTORY: &str = "reports";
// The SDK watches this directory only if it exists at startup, and attributes reports written to
// it to the current session.
const CURRENT_SESSION_REPORTS_DIRECTORY: &str = "reports/watcher/current_session";
const ARTIFACT_UPLOADS_DIRECTORY: &str = "report_uploads";

// The upload index and its staging copy, written before an atomic rename.
fn is_upload_index(name: &str) -> bool {
  name == "report_index.pb" || name == "report_index.tmp"
}

pub struct Driver {
  directory: PathBuf,
  holder: LoggerHolder,
  worker: JoinHandle<anyhow::Result<()>>,
  // The SDK's report watcher keeps at most one pending notification and drops the rest, so reports
  // are handed over one at a time. Holds the next report index.
  reports: Mutex<u64>,
}

impl Driver {
  pub async fn start(directory: &Path, args: &LoggerArgs) -> anyhow::Result<Self> {
    std::fs::create_dir_all(directory.join(CURRENT_SESSION_REPORTS_DIRECTORY))?;
    let holder = make_logger(directory, args).await?;
    let future = holder
      .take_future()
      .ok_or_else(|| anyhow::anyhow!("logger already started"))?;
    Ok(Self {
      directory: directory.to_path_buf(),
      holder,
      worker: tokio::spawn(future),
      reports: Mutex::new(0),
    })
  }

  /// Wait for backend configuration and log processing to start before emitting the fixture.
  pub async fn ready(&self, deadline: Duration) -> anyhow::Result<()> {
    wait_for_configuration(deadline, || self.holder.logger.lock().get_sdk_status()).await
  }

  pub fn begin_session(&self, entity: &str) -> anyhow::Result<String> {
    self.holder.set_entity_id(entity);
    let handle = self.holder.logger.lock().new_logger_handle();
    handle.start_new_session(None)?;
    Ok(handle.session_id()?.to_string())
  }

  /// `OccurredAt` is attached before enqueueing: later clock changes cannot retimestamp this log.
  pub fn log_at(&self, at: OffsetDateTime, message: &str, fields: HashMap<String, String>) {
    let handle = self.holder.logger.lock().new_logger_handle();
    handle.log(
      log_level::INFO,
      LogType::NORMAL,
      message.into(),
      fields
        .into_iter()
        .map(|(key, value)| {
          (
            key.into(),
            AnnotatedLogField {
              value: value.into(),
              kind: LogFieldKind::Custom,
            },
          )
        })
        .collect(),
      [].into(),
      Some(LogAttributesOverrides::OccurredAt(at)),
      &CaptureSession::default(),
    );
  }

  /// Records a feature flag exposure, returning once it is applied. Later issue reports carry
  /// every recorded exposure. Requires [`Self::ready`]: before then the SDK can drop the exposure
  /// and complete the flush without applying it.
  pub async fn set_feature_flag(&self, name: &str, variant: Option<&str>) -> anyhow::Result<()> {
    self.ensure_ready()?;
    self
      .holder
      .set_feature_flag(name.into(), variant.map(Into::into));
    self.flush().await
  }

  fn ensure_ready(&self) -> anyhow::Result<()> {
    let status = self.holder.logger.lock().get_sdk_status();
    anyhow::ensure!(
      status.last_config_delivery_time.is_some()
        && status.initialization_state == InitializationState::Running,
      "the SDK is not ready; await Driver::ready first"
    );
    Ok(())
  }

  /// Hands `report` to the SDK the way the platform layer reports an issue in the running process.
  /// The SDK attributes it to the current session, emits its `AppExit` log and uploads the report
  /// out of band; [`Self::stop`] waits for that upload. Returns after the SDK has taken the report
  /// and a short settle for the log and upload it queues without acknowledgement, so logs and
  /// session changes that follow are ordered after the `AppExit` log in practice, not by
  /// guarantee. Waits while the SDK's upload queue is near the size at which it evicts uploads.
  ///
  /// Requires [`Self::ready`]. Concurrent calls are serialized, and the whole call, including
  /// waiting for earlier calls, is bounded by one deadline. Not cancellation-safe: dropping the
  /// future after the report is published can make the SDK miss the next report.
  pub async fn report_issue(&self, report: &IssueReport) -> anyhow::Result<()> {
    self.ensure_ready()?;
    let bytes = report.to_bytes()?;
    tokio::time::timeout(REPORT_DEADLINE, self.hand_over_report(&bytes))
      .await
      .map_err(|_| anyhow::anyhow!("SDK did not take an issue report within {REPORT_DEADLINE:?}"))?
  }

  async fn hand_over_report(&self, bytes: &[u8]) -> anyhow::Result<()> {
    let mut next = self.reports.lock().await;
    let uploads_directory = self.directory.join(ARTIFACT_UPLOADS_DIRECTORY);
    while pending_files(&uploads_directory, is_upload_index)?.len() >= MAX_PENDING_UPLOADS {
      tokio::time::sleep(UPLOAD_POLL_INTERVAL).await;
    }
    let index = *next;
    *next += 1;
    let staged = self
      .directory
      .join(REPORTS_DIRECTORY)
      .join(format!("staged-{index}.cap"));
    let path = self
      .directory
      .join(CURRENT_SESSION_REPORTS_DIRECTORY)
      .join(format!("report-{index}.cap"));
    // The SDK reads a report as soon as it appears, so publish the complete file with a rename
    // from outside the watched directory.
    tokio::fs::write(&staged, bytes).await?;
    tokio::fs::rename(&staged, &path).await?;
    // The SDK deletes the file once the report is queued for upload.
    while tokio::fs::try_exists(&path).await? {
      tokio::time::sleep(REPORT_POLL_INTERVAL).await;
    }
    tokio::time::sleep(REPORT_SETTLE).await;
    Ok(())
  }

  pub async fn flush(&self) -> anyhow::Result<()> {
    let handle = self.holder.logger.lock().new_logger_handle();
    tokio::task::spawn_blocking(move || {
      handle.flush_state_with_result(Block::Yes {
        timeout: Duration::from_secs(30),
        poll_callback: None,
      })
    })
    .await??;
    Ok(())
  }

  /// Wait until the stats snapshots currently on disk have been uploaded. The SDK deletes a
  /// snapshot only after the backend acknowledges it, so this watches the stats directory.
  /// Snapshots written after the call starts do not hold it.
  pub async fn wait_for_stats_uploads(&self, deadline: Duration) -> anyhow::Result<()> {
    wait_for_uploads(
      &self.directory.join("stats_uploads"),
      |name| name.starts_with("pending_aggregation_index"),
      deadline,
      "stats snapshots",
    )
    .await
  }

  /// Wait until the artifacts persisted when the call starts, such as issue reports and workflow
  /// attachments, have left the SDK's upload queue. The SDK removes an artifact once the backend
  /// acknowledges it or declines it at intent negotiation, and also when it evicts the artifact
  /// from a full queue or cannot read it; uploads the backend answers with an error stay queued
  /// and are retried. Artifacts queued after the call starts do not hold it.
  pub async fn wait_for_artifact_uploads(&self, deadline: Duration) -> anyhow::Result<()> {
    let uploads_directory = self.directory.join(ARTIFACT_UPLOADS_DIRECTORY);
    let uploads = wait_for_uploads(&uploads_directory, is_upload_index, deadline, "artifacts");
    // Workflow attachments keep their payloads outside the upload directory and are tracked only
    // by their upload index entries.
    let attachments = tokio::time::timeout(deadline, async {
      let pending = Uploader::pending_workflow_attachments(&self.directory).await?;
      while !pending.is_empty()
        && !pending.is_disjoint(&Uploader::pending_workflow_attachments(&self.directory).await?)
      {
        tokio::time::sleep(UPLOAD_POLL_INTERVAL).await;
      }
      Ok::<_, anyhow::Error>(())
    });
    let (uploads, attachments) = tokio::join!(uploads, attachments);
    let attachments = attachments
      .map_err(|_| {
        anyhow::anyhow!("workflow attachments were not acknowledged within {deadline:?}")
      })
      .and_then(|result| result);
    combine(uploads, attachments)
  }

  pub async fn stop(self) -> anyhow::Result<()> {
    // Always shut down the worker, including when the flush or upload wait failed. The flush
    // persists stats and starts their upload, which must be acknowledged before shutdown.
    let flushed = self.flush().await;
    let uploaded = if flushed.is_ok() {
      let (stats, artifacts) = tokio::join!(
        self.wait_for_stats_uploads(STOP_STATS_UPLOAD_DEADLINE),
        self.wait_for_artifact_uploads(STOP_ARTIFACT_UPLOAD_DEADLINE),
      );
      combine(stats, artifacts)
    } else {
      Ok(())
    };
    self.holder.logger.lock().shutdown(false);
    drop(self.holder);
    let stopped = stop_worker(self.worker, Duration::from_secs(30)).await;
    flushed?;
    uploaded?;
    stopped?;
    Ok(())
  }
}

// Reports both errors when both waits failed.
fn combine(first: anyhow::Result<()>, second: anyhow::Result<()>) -> anyhow::Result<()> {
  match (first, second) {
    (Err(first), Err(second)) => Err(anyhow::anyhow!("{first:#}; {second:#}")),
    (first, second) => first.and(second),
  }
}

// Waits until none of the upload files pending in `directory` when called remain.
async fn wait_for_uploads(
  directory: &Path,
  is_index: impl Fn(&str) -> bool,
  deadline: Duration,
  uploads: &str,
) -> anyhow::Result<()> {
  tokio::time::timeout(deadline, async {
    let pending = pending_files(directory, &is_index)?;
    while !pending.is_disjoint(&pending_files(directory, &is_index)?) {
      tokio::time::sleep(UPLOAD_POLL_INTERVAL).await;
    }
    Ok(())
  })
  .await
  .map_err(|_| anyhow::anyhow!("{uploads} were not acknowledged within {deadline:?}"))?
}

// Returns the upload file names in `directory`, excluding the SDK's index files.
fn pending_files(
  directory: &Path,
  is_index: impl Fn(&str) -> bool,
) -> anyhow::Result<HashSet<OsString>> {
  let entries = match std::fs::read_dir(directory) {
    Ok(entries) => entries,
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(HashSet::new()),
    Err(error) => return Err(error.into()),
  };
  let mut files = HashSet::new();
  for entry in entries {
    let name = entry?.file_name();
    if !is_index(&name.to_string_lossy()) {
      files.insert(name);
    }
  }
  Ok(files)
}

async fn wait_for_configuration(
  deadline: Duration,
  status: impl Fn() -> SdkStatus,
) -> anyhow::Result<()> {
  tokio::time::timeout(deadline, async {
    loop {
      let status = status();
      anyhow::ensure!(
        status.initialization_state != InitializationState::Disabled,
        "SDK disabled"
      );
      if status.last_config_delivery_time.is_some()
        && status.initialization_state == InitializationState::Running
      {
        return Ok(());
      }
      tokio::time::sleep(Duration::from_millis(100)).await;
    }
  })
  .await?
}

async fn stop_worker(
  mut worker: JoinHandle<anyhow::Result<()>>,
  deadline: Duration,
) -> anyhow::Result<()> {
  if let Ok(result) = tokio::time::timeout(deadline, &mut worker).await {
    result??;
    return Ok(());
  }
  log::warn!("logger worker shutdown timed out; aborting worker");
  worker.abort();
  // Join the cancelled task so shutdown never detaches a live worker.
  let _cancelled = worker.await;
  anyhow::bail!("logger worker shutdown timed out")
}
