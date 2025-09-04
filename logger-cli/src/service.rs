// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::cli::{CliLogType, RuntimeValueType};
use crate::logger::LoggerHolder;
use futures::future;
use futures::lock::Mutex;
use futures::prelude::*;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::process::exit;
use std::sync::LazyLock;
use tarpc::server::Channel;
use tarpc::tokio_serde::formats::Json;

#[tarpc::service]
pub trait Remote {
  async fn breakpoint();
  async fn stop();
  async fn log(
    log_level: bd_logger::LogLevel,
    log_type: CliLogType,
    message: String,
    fields: Vec<String>,
    capture_session: bool,
  );
  async fn process_crash_reports();
  async fn get_runtime_value(name: String, value_type: RuntimeValueType) -> String;
  async fn get_api_url() -> String;
  async fn start_new_session();
  async fn set_sleep_mode(enabled: bool);
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

static LOGGER: LazyLock<Mutex<Option<LoggerHolder>>> = LazyLock::new(|| Mutex::new(None));

pub async fn start(
  sdk_directory: &Path,
  config: &crate::cli::StartCommand,
  port: u16,
) -> anyhow::Result<()> {
  let logger = crate::logger::make_logger(sdk_directory, config)?;
  logger.start();
  LOGGER.lock().await.replace(logger);

  let server_addr = (IpAddr::V6(Ipv6Addr::LOCALHOST), port);
  let mut listener = tarpc::serde_transport::tcp::listen(&server_addr, Json::default).await?;
  listener.config_mut().max_frame_length(usize::MAX);
  listener
      .filter_map(|r| future::ready(r.ok())) // Ignore accept errors.
      .map(tarpc::server::BaseChannel::with_defaults)
      .map(|channel| {
          let server = Server {
            addr: channel.transport().peer_addr().unwrap(),
            api_url: config.api_url.clone(),
          };
          channel.execute(server.serve()).for_each(spawn)
      })
      // Max 10 channels.
      .buffer_unordered(10)
      .for_each(|_| async {})
      .await;

  Ok(())
}

impl Remote for Server {
  async fn breakpoint(self, _: tarpc::context::Context) {
    #[allow(unused)]
    if let Some(holder) = LOGGER.lock().await.deref() {
      unsafe {
        libc::raise(libc::SIGTRAP);
      }
    }
  }

  async fn stop(self, _: tarpc::context::Context) {
    if let Some(logger) = LOGGER.lock().await.deref() {
      logger.stop();
      exit(0);
    }
  }

  async fn set_sleep_mode(self, _: tarpc::context::Context, enabled: bool) {
    if let Some(logger) = LOGGER.lock().await.deref() {
      logger.set_sleep_mode(enabled);
    }
  }

  async fn log(
    self,
    _: ::tarpc::context::Context,
    log_level: bd_logger::LogLevel,
    log_type: CliLogType,
    message: String,
    fields: Vec<String>,
    capture_session: bool,
  ) {
    if let Some(logger) = LOGGER.lock().await.deref() {
      logger.log(log_level, log_type.into(), message, fields, capture_session);
    }
  }

  async fn process_crash_reports(self, _: ::tarpc::context::Context) {
    if let Some(logger) = LOGGER.lock().await.deref_mut() {
      if let Err(e) = logger.process_crash_reports() {
        log::error!("failed to process reports: {e}");
      }
    }
  }

  async fn get_runtime_value(
    self,
    _: ::tarpc::context::Context,
    name: String,
    value_type: RuntimeValueType,
  ) -> String {
    if let Some(logger) = LOGGER.lock().await.deref() {
      logger.get_runtime_value(name, value_type)
    } else {
      "<unset>".to_owned()
    }
  }

  async fn get_api_url(self, _: ::tarpc::context::Context) -> String {
    self.api_url
  }

  async fn start_new_session(self, _: ::tarpc::context::Context) {
    if let Some(logger) = LOGGER.lock().await.deref() {
      logger.start_new_session();
    }
  }
}
