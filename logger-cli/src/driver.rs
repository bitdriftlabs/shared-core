//! Per-instance async driver for local data generators, using the same SDK as logger-cli.

use crate::logger::{LoggerArgs, LoggerHolder, make_logger};
use bd_logger::{
  AnnotatedLogField,
  Block,
  CaptureSession,
  LogAttributesOverrides,
  LogFieldKind,
  log_level,
};
use bd_proto::protos::logging::payload::LogType;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use time::OffsetDateTime;
use tokio::task::JoinHandle;

pub struct Driver {
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
      holder,
      worker: tokio::spawn(future),
    })
  }

  /// Await backend configuration before emitting the fixture. Real timers remain real.
  pub async fn ready(&self, deadline: Duration) -> anyhow::Result<()> {
    tokio::time::timeout(deadline, async {
      loop {
        let status = self.holder.logger.lock().get_sdk_status();
        anyhow::ensure!(
          status.initialization_state != bd_logger::InitializationState::Disabled,
          "SDK disabled"
        );
        if status.last_config_delivery_time.is_some() {
          return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
      }
    })
    .await?
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
      handle.flush_state(Block::Yes {
        timeout: Duration::from_secs(30),
        poll_callback: None,
      });
    })
    .await?;
    Ok(())
  }

  pub async fn stop(self) -> anyhow::Result<()> {
    self.flush().await?;
    self.holder.logger.lock().shutdown(false);
    drop(self.holder);
    tokio::time::timeout(Duration::from_secs(30), self.worker).await???;
    Ok(())
  }
}
