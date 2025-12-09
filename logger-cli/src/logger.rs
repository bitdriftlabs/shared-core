// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::metadata::Metadata;
use crate::storage::SQLiteStorage;
use crate::types::{Platform, RuntimeValueType};
use bd_logger::{CaptureSession, InitParams, Logger};
use bd_proto::protos::logging::payload::LogType as ProtoLogType;
use bd_session::{Strategy, fixed};
use bd_test_helpers::metadata_provider::LogMetadata;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::thread::sleep;
use time::ext::NumericalStdDuration;

pub type LoggerFuture =
  Pin<Box<dyn Future<Output = anyhow::Result<()>> + 'static + std::marker::Send>>;

pub const SESSION_FILE: &str = "session_id";

pub struct LoggerHolder {
  pub logger: Arc<Mutex<Logger>>,
  future: Mutex<Option<LoggerFuture>>,
  _shutdown_trigger: bd_shutdown::ComponentShutdownTrigger,
}

impl LoggerHolder {
  pub fn new(
    logger: Logger,
    future: LoggerFuture,
    shutdown_trigger: bd_shutdown::ComponentShutdownTrigger,
  ) -> Self {
    Self {
      logger: Arc::new(Mutex::new(logger)),
      future: Mutex::new(Some(future)),
      _shutdown_trigger: shutdown_trigger,
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

  pub fn process_crash_reports(&self) {
    let logger = self.logger.clone();

    // To work around the fact that process_crash_reports calls blocking_recv() we dispatch to the
    // blocking thread pool.
    tokio::task::spawn_blocking(move || {
      logger
        .lock()
        .process_crash_reports(bd_logger::ReportProcessingSession::PreviousRun)
        .inspect_err(|e| log::error!("Failed to process crash reports: {e}"))
    });
  }

  pub fn start_new_session(&self) {
    let handle = self.logger.lock().new_logger_handle();
    handle.start_new_session();
  }

  pub fn set_sleep_mode(&self, enabled: bool) {
    let handle = self.logger.lock().new_logger_handle();
    handle.transition_sleep_mode(enabled);
  }

  pub fn stop(&self) {
    sleep(2.std_seconds());
    self.logger.lock().shutdown(true);
  }

  pub fn get_runtime_value(&self, name: &str, value_type: RuntimeValueType) -> String {
    let snapshot = self.logger.lock().runtime_snapshot();
    match value_type {
      RuntimeValueType::Bool => format!("{}", snapshot.get_bool(name, false)),
      RuntimeValueType::String => format!("'{}'", snapshot.get_string(name, String::new())),
      RuntimeValueType::Int => format!("{}", snapshot.get_integer(name, 0)),
      RuntimeValueType::Duration => {
        format!(
          "{}",
          snapshot.get_duration(name, time::Duration::seconds(0))
        )
      },
    }
  }

  pub fn log(
    &self,
    log_level: bd_logger::LogLevel,
    log_type: ProtoLogType,
    message: String,
    fields: HashMap<String, String>,
    capture_session: bool,
  ) {
    let session_capture = if capture_session {
      CaptureSession::capture_with_id("cli command")
    } else {
      CaptureSession::default()
    };
    self.logger.lock().new_logger_handle().log(
      log_level,
      log_type,
      message.into(),
      fields
        .into_iter()
        .map(|(k, v)| {
          (
            k.into(),
            bd_logger::AnnotatedLogField {
              value: v.into(),
              kind: bd_logger::LogFieldKind::Ootb,
            },
          )
        })
        .collect(),
      [].into(),
      None,
      bd_logger::Block::Yes(1.std_seconds()),
      &session_capture,
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

#[derive(serde::Serialize)]
struct DeviceCodeRequest {
  device_id: String,
}

#[derive(serde::Deserialize)]
struct DeviceCodeResponse {
  code: String,
}

async fn fetch_device_code(args: &LoggerArgs, device_id: &str) -> anyhow::Result<String> {
  let client = reqwest::Client::new();
  let request = DeviceCodeRequest {
    device_id: device_id.to_string(),
  };

  let url = format!("{}/v1/device/code", args.api_url);
  let response = client
    .post(&url)
    .header("x-bitdrift-api-key", &args.api_key)
    .json(&request)
    .send()
    .await?;

  let status = response.status();
  if !status.is_success() {
    let response_text = response
      .text()
      .await
      .unwrap_or_else(|_| "<unable to read body>".to_string());
    anyhow::bail!("HTTP {status} - {response_text}");
  }

  let device_code_response: DeviceCodeResponse = response.json().await?;
  Ok(device_code_response.code)
}

pub struct LoggerArgs {
  pub api_url: String,
  pub api_key: String,
  pub app_id: String,
  pub platform: Platform,
  pub app_version: String,
  pub app_version_code: String,
  pub model: String,
}

pub async fn make_logger(sdk_directory: &Path, args: &LoggerArgs) -> anyhow::Result<LoggerHolder> {
  let session_callbacks = Arc::new(MaybeStaticSessionGenerator {
    config_path: sdk_directory.join(SESSION_FILE),
  });
  let storage_db = sdk_directory.join("defaults.db");
  let storage = SQLiteStorage::new(&storage_db);
  let store = Arc::new(bd_key_value::Store::new(Box::new(storage)));
  let device = Arc::new(bd_device::Device::new(store.clone()));

  // Fetch and log device code
  let device_id = device.id();
  match fetch_device_code(args, &device_id).await {
    Ok(code) => {
      log::info!("Device code: {code}");
    },
    Err(e) => {
      log::warn!("Failed to fetch device code: {e}");
    },
  }

  let shutdown_trigger = bd_shutdown::ComponentShutdownTrigger::default();
  let shutdown = shutdown_trigger.make_shutdown();
  let network = bd_hyper_network::HyperNetwork::run_on_thread(&args.api_url, shutdown);

  let static_metadata = Arc::new(Metadata {
    app_id: Some(args.app_id.clone()),
    app_version: Some(args.app_version.clone()),
    platform: args.platform.clone().into(),
    device: device.clone(),
    model: args.model.clone(),
  });

  let (logger, _, future, _) = bd_logger::LoggerBuilder::new(InitParams {
    sdk_directory: sdk_directory.to_path_buf(),
    api_key: args.api_key.clone(),
    session_strategy: Arc::new(Strategy::Fixed(fixed::Strategy::new(
      store.clone(),
      session_callbacks,
    ))),
    metadata_provider: Arc::new(LogMetadata {
      timestamp: time::OffsetDateTime::now_utc().into(),
      ootb_fields: [(
        "_app_version_code".into(),
        args.app_version_code.clone().into(),
      )]
      .into(),
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
