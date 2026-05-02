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
  async fn start_new_session();
  async fn set_sleep_mode(enabled: bool);
  async fn set_feature_flag(name: String, variant: Option<String>);
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

static LOGGER: parking_lot::Mutex<Option<LoggerHolder>> = parking_lot::Mutex::new(None);

fn shutdown_logger() {
  if let Some(logger) = LOGGER.lock().take() {
    logger.flush_and_stop();
  }
}

pub async fn start(sdk_directory: &Path, args: &LoggerArgs, port: u16) -> anyhow::Result<()> {
  let logger = crate::logger::make_logger(sdk_directory, args).await?;
  logger.start();
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
    if let Some(holder) = &*LOGGER.lock() {
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
    if let Some(logger) = &*LOGGER.lock() {
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
    if let Some(logger) = &*LOGGER.lock() {
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
    if let Some(logger) = &mut *LOGGER.lock() {
      logger.process_crash_reports();
    }
  }

  async fn get_runtime_value(
    self,
    _: ::tarpc::context::Context,
    name: String,
    value_type: RuntimeValueType,
  ) -> String {
    (*LOGGER.lock()).as_ref().map_or_else(
      || "<unset>".to_owned(),
      |logger| logger.get_runtime_value(&name, value_type),
    )
  }

  async fn get_api_url(self, _: ::tarpc::context::Context) -> String {
    self.api_url
  }

  async fn start_new_session(self, _: ::tarpc::context::Context) {
    let logger = {
      let guard = LOGGER.lock();
      guard.as_ref().map(|logger| logger.logger.clone())
    };

    if let Some(logger) = logger {
      let handle = logger.lock().new_logger_handle();
      handle.start_new_session().unwrap();
    }
  }

  async fn set_feature_flag(
    self,
    _: ::tarpc::context::Context,
    name: String,
    variant: Option<String>,
  ) {
    if let Some(logger) = &*LOGGER.lock() {
      logger.set_feature_flag(name, variant);
    }
  }
}
