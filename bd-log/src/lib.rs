// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

pub mod rate_limit_log;

pub use parking_lot::Mutex as ParkingLotMutex;
use std::sync::Mutex;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::reload::Handle as ReloadHandle;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

const DEFAULT_FILTER_RULES: &str = "info,aws_config=warn,aws_smithy_http_tower=warn";

//
// SwapLogger
//

// An implementation of log:Log which allows for atomically swapping the underlying implementation.
#[derive(Default)]
pub struct SwapLogger {
  handle: Mutex<Option<ReloadHandle<EnvFilter, Registry>>>,
}

impl SwapLogger {
  const fn new() -> Self {
    Self {
      handle: Mutex::new(None),
    }
  }

  // Get the static instance of the logger.
  fn get() -> &'static Self {
    static LOGGER: SwapLogger = SwapLogger::new();

    &LOGGER
  }

  // Initialize the logger to the default. This can only be called once and should be called as
  // early as possible in the program.
  pub fn initialize() {
    // Gate ANSI on whether BD_LOG_ANSI is set. This avoids using this feature by default (e.g.
    // in k8s) but allows it to be enabled for local development should the user want it.
    let stderr = tracing_subscriber::fmt::layer()
      .with_writer(std::io::stderr)
      .with_ansi(std::env::var("BD_LOG_ANSI").is_ok())
      .with_line_number(true)
      .with_thread_ids(true)
      .compact();

    let filter = EnvFilter::new(
      std::env::var("RUST_LOG")
        .as_deref()
        .unwrap_or(DEFAULT_FILTER_RULES),
    );

    let (filter, reload_handle) = tracing_subscriber::reload::Layer::new(filter);
    *Self::get().handle.lock().unwrap() = Some(reload_handle);

    Registry::default().with(filter).with(stderr).init();
  }

  // Swap in a new logger with the provided RUST_LOG string.
  pub fn swap(new_rust_log: &str) -> anyhow::Result<()> {
    Self::get()
      .handle
      .lock()
      .unwrap()
      .as_mut()
      .unwrap()
      .reload(new_rust_log)?;

    // During init the log level is set based on the initial RUST_LOG value. We need to manually
    // update it each time we reload the config as tracing_subscriber does not do this for us.
    log::set_max_level(tracing_log::AsLog::as_log(
      &tracing_subscriber::filter::LevelFilter::current(),
    ));

    Ok(())
  }
}
