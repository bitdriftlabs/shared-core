// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::cli::{FieldPairs, LogCommand, Options};
use crate::metadata::Metadata;
use crate::storage::SQLiteStorage;
use bd_logger::{CaptureSession, InitParams, Logger};
use bd_session::{Strategy, fixed};
use bd_test_helpers::metadata_provider::LogMetadata;
use parking_lot::Mutex;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::thread::sleep;
use time::ext::NumericalStdDuration;

pub type LoggerFuture =
  Pin<Box<dyn Future<Output = anyhow::Result<()>> + 'static + std::marker::Send>>;

pub const SESSION_FILE: &str = "session_id";

pub struct LoggerHolder {
  logger: Logger,
  future: Mutex<Option<LoggerFuture>>,
  #[allow(dead_code)] // holding it to avoid drop before the logger itself
  shutdown_trigger: bd_shutdown::ComponentShutdownTrigger,
}

impl LoggerHolder {
  pub fn new(
    logger: Logger,
    future: LoggerFuture,
    shutdown_trigger: bd_shutdown::ComponentShutdownTrigger,
  ) -> Self {
    Self {
      logger,
      future: Mutex::new(Some(future)),
      shutdown_trigger,
    }
  }

  pub fn start(&self) {
    let Some(future) = self.future.lock().take() else {
      return;
    };

    sleep(2.std_seconds()); // some initialization time for the network

    std::thread::spawn(move || {
      tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
          if let Err(e) = future.await {
            log::error!("Failed to start logger: {e}");
          }
        });
    });
  }

  pub fn process_crash_reports(&mut self) -> anyhow::Result<()> {
    self
      .logger
      .process_crash_reports(bd_logger::ReportProcessingSession::PreviousRun)
  }

  pub fn stop(&self) {
    sleep(2.std_seconds());
    self.logger.shutdown(true);
  }

  pub fn log(&self, cmd: &LogCommand, capture_session: bool) {
    let session_capture = if capture_session {
      CaptureSession::capture_with_id("cli command")
    } else {
      CaptureSession::default()
    };
    self.logger.new_logger_handle().log(
      cmd.log_level.clone().into(),
      cmd.log_type.clone().into(),
      cmd.message.clone().into(),
      FieldPairs(cmd.field.clone()).into(),
      [].into(),
      None,
      bd_logger::Block::Yes,
      session_capture,
    );
  }
}

pub struct MaybeStaticSessionGenerator {
  pub config_path: PathBuf,
}

impl MaybeStaticSessionGenerator {
  pub fn cached_session_id(&self) -> anyhow::Result<String> {
    let contents = std::fs::read(self.config_path.clone())?;
    Ok(String::from_utf8(contents)?)
  }
}

impl fixed::Callbacks for MaybeStaticSessionGenerator {
  fn generate_session_id(&self) -> anyhow::Result<String> {
    if let Ok(id) = self.cached_session_id() {
      Ok(id)
    } else {
      let id = fixed::UUIDCallbacks.generate_session_id()?;
      if let Err(e) = std::fs::write(self.config_path.clone(), &id) {
        log::warn!("failed to save session ID to disk: {e}");
      }
      Ok(id)
    }
  }
}

pub fn make_logger(sdk_directory: &Path, config: &Options) -> anyhow::Result<LoggerHolder> {
  let session_callbacks = Arc::new(MaybeStaticSessionGenerator {
    config_path: sdk_directory.join(SESSION_FILE),
  });
  let storage_db = sdk_directory.join("defaults.db");
  let storage = SQLiteStorage::new(&storage_db);
  let store = Arc::new(bd_key_value::Store::new(Box::new(storage)));
  let device = Arc::new(bd_device::Device::new(store.clone()));
  let shutdown_trigger = bd_shutdown::ComponentShutdownTrigger::default();
  let shutdown = shutdown_trigger.make_shutdown();
  let network = bd_hyper_network::HyperNetwork::run_on_thread(&config.api_url, shutdown);

  let static_metadata = Arc::new(Metadata {
    app_id: Some(config.app_id.clone()),
    app_version: Some(config.app_version.clone()),
    platform: config.platform.clone().into(),
    device: device.clone(),
    model: config.model.clone(),
  });

  let (logger, _, future, _) = bd_logger::LoggerBuilder::new(InitParams {
    sdk_directory: sdk_directory.to_path_buf(),
    api_key: config.api_key.clone(),
    session_strategy: Arc::new(Strategy::Fixed(fixed::Strategy::new(
      store.clone(),
      session_callbacks,
    ))),
    metadata_provider: Arc::new(LogMetadata {
      timestamp: time::OffsetDateTime::now_utc().into(),
      ..Default::default()
    }),
    resource_utilization_target: Box::new(bd_test_helpers::resource_utilization::EmptyTarget),
    session_replay_target: Box::new(bd_test_helpers::session_replay::NoOpTarget),
    events_listener_target: Box::new(bd_test_helpers::events::NoOpListenerTarget),
    device,
    store,
    network: Box::new(network),
    static_metadata,
    start_in_sleep_mode: false,
  })
  .build()?;
  Ok(LoggerHolder::new(logger, future, shutdown_trigger))
}
