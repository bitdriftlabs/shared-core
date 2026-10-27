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

use crate::logger::{LoggerArgs, LoggerHolder, make_logger};
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
use tokio::task::JoinHandle;

// The stop-time stats upload can be held back by the 30 s explicit-flush minimum interval, in which
// case the next periodic upload (every 60 s by default) sends it.
const STOP_STATS_UPLOAD_DEADLINE: Duration = Duration::from_secs(120);
const STATS_UPLOAD_POLL_INTERVAL: Duration = Duration::from_millis(250);

pub struct Driver {
  directory: PathBuf,
  holder: LoggerHolder,
  worker: JoinHandle<anyhow::Result<()>>,
}

impl Driver {
  pub async fn start(directory: &Path, args: &LoggerArgs) -> anyhow::Result<Self> {
    let holder = make_logger(directory, args).await?;
    let future = holder
      .take_future()
      .ok_or_else(|| anyhow::anyhow!("logger already started"))?;
    Ok(Self {
      directory: directory.to_path_buf(),
      holder,
      worker: tokio::spawn(future),
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
    let stats_directory = self.directory.join("stats_uploads");
    let pending = stats_snapshot_files(&stats_directory)?;
    tokio::time::timeout(deadline, async {
      while !pending.is_disjoint(&stats_snapshot_files(&stats_directory)?) {
        tokio::time::sleep(STATS_UPLOAD_POLL_INTERVAL).await;
      }
      Ok(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("stats snapshots were not acknowledged within {deadline:?}"))?
  }

  pub async fn stop(self) -> anyhow::Result<()> {
    // Always shut down the worker, including when the flush or upload wait failed. The flush
    // persists stats and starts their upload, which must be acknowledged before shutdown.
    let flushed = self.flush().await;
    let uploaded = if flushed.is_ok() {
      self
        .wait_for_stats_uploads(STOP_STATS_UPLOAD_DEADLINE)
        .await
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

// Returns the stats snapshot file names in `directory`, excluding the SDK's index files.
fn stats_snapshot_files(directory: &Path) -> anyhow::Result<HashSet<OsString>> {
  let entries = match std::fs::read_dir(directory) {
    Ok(entries) => entries,
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(HashSet::new()),
    Err(error) => return Err(error.into()),
  };
  let mut files = HashSet::new();
  for entry in entries {
    let name = entry?.file_name();
    if !name
      .to_string_lossy()
      .starts_with("pending_aggregation_index")
    {
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
