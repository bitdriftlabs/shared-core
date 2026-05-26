// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::logger::{LoggerArgs, LoggerHolder};
use crate::types::{LogLevel, LogType, RuntimeValueType};
use bd_shutdown::real_graceful_shutdown;
use futures::future;
use futures::prelude::*;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::path::Path;
use std::process::exit;
use std::sync::Arc;
use std::time::Instant;
use tarpc::server::Channel;
use tarpc::tokio_serde::formats::Json;

#[tarpc::service]
pub trait Remote {
  async fn breakpoint();
  async fn stop();
  async fn log(
    log_level: LogLevel,
    log_type: LogType,
    message: String,
    fields: HashMap<String, String>,
    capture_session: bool,
    block: bool,
  );
  async fn process_crash_reports();
  async fn get_runtime_value(name: String, value_type: RuntimeValueType) -> String;
  async fn get_api_url() -> String;
  async fn get_current_session_id() -> Option<String>;
  async fn start_new_session();
  async fn set_sleep_mode(enabled: bool);
  async fn set_feature_flag(name: String, variant: Option<String>);
  async fn set_entity_id(entity_id: String);
}

#[derive(Clone)]
struct Server {
  #[allow(unused)]
  addr: SocketAddr,
  api_url: String,
}

async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
  tokio::spawn(fut);
}

static LOGGER: parking_lot::Mutex<Option<Arc<LoggerHolder>>> = parking_lot::Mutex::new(None);

fn current_logger() -> Option<Arc<LoggerHolder>> {
  LOGGER.lock().clone()
}

fn log_current_session(logger: &LoggerHolder) {
  match logger.current_session_id() {
    Ok(session_id) => {
      log::info!("current session ID: {session_id}");
    },
    Err(e) => {
      log::warn!("failed to load current session ID: {e}");
    },
  }
}

fn log_current_entity(logger: &LoggerHolder) {
  match logger.current_entity_id() {
    Some(entity_id) => {
      log::info!("current entity ID: {entity_id}");
    },
    None => {
      log::info!("current entity ID: <unset>");
    },
  }
}

fn log_previous_session(logger: &LoggerHolder) {
  match logger.previous_process_session_id() {
    Some(session_id) => {
      log::info!("previous session ID: {session_id}");
    },
    None => {
      log::info!("previous session ID: <unset>");
    },
  }
}

fn spawn_session_update_logging(logger: Arc<LoggerHolder>) {
  let mut updates = logger.subscribe_session_updates();
  tokio::spawn(async move {
    while updates.changed().await.is_ok() {
      match logger.try_current_session_id() {
        Ok(session_id) => {
          log::info!("current session ID: {session_id}");
        },
        Err(e) => {
          log::warn!("failed to read current session ID after update: {e}");
        },
      }
    }
  });
}

fn shutdown_logger() {
  if let Some(logger) = LOGGER.lock().take() {
    logger.flush_and_stop();
  }
}

pub async fn start(sdk_directory: &Path, args: &LoggerArgs, port: u16) -> anyhow::Result<()> {
  let startup_started_at = Instant::now();
  let logger = Arc::new(crate::logger::make_logger(sdk_directory, args).await?);
  logger.start();
  log_current_session(logger.as_ref());
  logger.log_sdk_start(startup_started_at.elapsed().try_into().unwrap_or_default());
  log_current_entity(logger.as_ref());
  log_previous_session(logger.as_ref());
  spawn_session_update_logging(logger.clone());
  LOGGER.lock().replace(logger);

  let server_addr = (IpAddr::V6(Ipv6Addr::LOCALHOST), port);
  let mut listener = tarpc::serde_transport::tcp::listen(&server_addr, Json::default).await?;

  listener.config_mut().max_frame_length(usize::MAX);
  let listener = listener
    .filter_map(|r| future::ready(r.ok())) // Ignore accept errors.
    .map(tarpc::server::BaseChannel::with_defaults)
    .map(|channel| {
      let server = Server {
        addr: channel.transport().peer_addr().unwrap(),
        api_url: args.api_url.clone(),
      };
      channel.execute(server.serve()).for_each(spawn)
    })
    // Max 10 channels.
    .buffer_unordered(10)
    .for_each(|()| async {});
  tokio::pin!(listener);

  // Mirror the SDK shutdown flow by racing the server loop against process termination and
  // flushing buffered state before shutting the logger down.
  let should_exit = tokio::select! {
    () = &mut listener => false,
    () = real_graceful_shutdown() => true,
  };

  shutdown_logger();

  if should_exit {
    exit(0);
  }

  Ok(())
}

impl Remote for Server {
  async fn breakpoint(self, _: tarpc::context::Context) {
    #[allow(unused)]
    if current_logger().is_some() {
      unsafe {
        libc::raise(libc::SIGTRAP);
      }
    }
  }

  async fn stop(self, _: tarpc::context::Context) {
    shutdown_logger();
    exit(0);
  }

  async fn set_sleep_mode(self, _: tarpc::context::Context, enabled: bool) {
    if let Some(logger) = current_logger() {
      logger.set_sleep_mode(enabled);
    }
  }

  async fn log(
    self,
    _: ::tarpc::context::Context,
    log_level: LogLevel,
    log_type: LogType,
    message: String,
    fields: HashMap<String, String>,
    capture_session: bool,
    block: bool,
  ) {
    if let Some(logger) = current_logger() {
      logger.log(
        log_level.into(),
        log_type.into(),
        message,
        fields,
        capture_session,
        block,
      );
    }
  }

  async fn process_crash_reports(self, _: ::tarpc::context::Context) {
    if let Some(logger) = current_logger() {
      logger.process_crash_reports();
    }
  }

  async fn get_runtime_value(
    self,
    _: ::tarpc::context::Context,
    name: String,
    value_type: RuntimeValueType,
  ) -> String {
    current_logger().as_ref().map_or_else(
      || "<unset>".to_owned(),
      |logger| logger.get_runtime_value(&name, value_type),
    )
  }

  async fn get_api_url(self, _: ::tarpc::context::Context) -> String {
    self.api_url
  }

  async fn get_current_session_id(self, _: ::tarpc::context::Context) -> Option<String> {
    current_logger().and_then(|logger| match logger.current_session_id() {
      Ok(session_id) => Some(session_id),
      Err(e) => {
        log::warn!("failed to get current session ID: {e}");
        None
      },
    })
  }

  async fn start_new_session(self, _: ::tarpc::context::Context) {
    if let Some(logger) = current_logger() {
      logger.start_new_session();
    }
  }

  async fn set_feature_flag(
    self,
    _: ::tarpc::context::Context,
    name: String,
    variant: Option<String>,
  ) {
    if let Some(logger) = current_logger() {
      logger.set_feature_flag(name, variant);
    }
  }

  async fn set_entity_id(self, _: ::tarpc::context::Context, entity_id: String) {
    if let Some(logger) = current_logger() {
      logger.set_entity_id(&entity_id);
      log_current_entity(logger.as_ref());
    }
  }
}
