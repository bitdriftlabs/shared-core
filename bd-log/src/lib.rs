// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! `bd-log` installs a single `tracing` subscriber stack that can drive two different outputs:
//! a local human-readable formatter and, when configured, an OTEL span exporter.
//!
//! The routing rules are easier to reason about if you separate them into three pieces:
//!
//! 1. The global filter, which comes from `RUST_LOG` by default or from [`LogConfig::log_filter`].
//! 2. The local formatter layer, which writes compact logs to stderr by default or to stdout when
//!    [`LogConfig::output`] is set to [`LogOutput::Stdout`].
//! 3. The optional OTEL layer, which only receives spans and events emitted on [`OTEL_TARGET`].
//!
//! Ordinary `log` macros are bridged into `tracing` through `LogTracer`, so `log::info!`,
//! ordinary `tracing` events, and `#[tracing::instrument]` spans all go through the same
//! subscriber stack and are filtered by the same top-level rules.
//!
//! With the default configuration (`SwapLogger::initialize()`):
//!
//! - `RUST_LOG` controls which events reach the subscriber. If `RUST_LOG` is unset, the crate uses
//!   `DEFAULT_FILTER_RULES`.
//! - Output goes to stderr.
//! - ANSI color is disabled unless `BD_LOG_ANSI` is set.
//! - Set `BD_LOG_DEBUG_DIRECT_OTEL_SPANS=1` to emit local `new`/`close` span lifecycle lines for
//!   OTEL-targeted spans. This is intended for local debugging and test runs.
//! - No OTEL exporter is installed, so every event that passes the filter only goes to the local
//!   formatter.
//!
//! With OTEL disabled, spans or events emitted with the helper macros in [`otel`] are not treated
//! specially unless `BD_LOG_DEBUG_DIRECT_OTEL_SPANS` is set. They still use the dedicated target
//! name, but they only follow the normal local output path and are filtered like any other target.
//! When `BD_LOG_DEBUG_DIRECT_OTEL_SPANS` is set, `bd-log` also widens the global filter with
//! `bd_log::otel=trace` so those spans are visible locally even if `RUST_LOG` would not normally
//! match that target.
//!
//! With OTEL enabled (`LogConfig { otel: Some(..), .. }`):
//!
//! - The OTEL layer receives only items whose target is [`OTEL_TARGET`].
//! - The global filter is widened with `bd_log::otel=trace` so direct-to-OTEL spans are not
//!   accidentally dropped just because the ordinary `RUST_LOG` filter is narrower.
//! - All non-OTEL-targeted logs still follow the normal `log_filter` rules and only go to the local
//!   formatter.
//!
//! `mirror_to_output` controls whether OTEL-targeted items are also written to the local output:
//!
//! - `mirror_to_output = false`: direct-to-OTEL spans and events go only to the OTEL exporter and
//!   are hidden from stdout or stderr.
//! - `mirror_to_output = true`: direct-to-OTEL spans and events go to both the OTEL exporter and
//!   the local formatter.
//!
//! `BD_LOG_DEBUG_DIRECT_OTEL_SPANS` is separate from `mirror_to_output`: it only prints OTEL-
//! targeted span lifecycle records locally, and it works even when no OTEL exporter is configured.
//!
//! Initialization is intentionally two-stage. Call [`SwapLogger::initialize`] early if you want
//! local stderr logging during startup, then call [`SwapLogger::initialize_with_config`] or
//! [`SwapLogger::configure`] later once real config has loaded. The second call does not install a
//! second global subscriber; it reloads the active filter and layer set in place so output routing
//! can change from "local only" to "local plus OTEL" safely.
//!
//! [`SwapLogger::swap`] is narrower than reconfiguration: it only updates the active filter string.
//! It does not change the output destination or attach or detach the OTEL exporter.

#[cfg(test)]
#[path = "./lib_test.rs"]
mod tests;

pub mod otel;
pub mod rate_limit_log;

use anyhow::anyhow;
use opentelemetry_sdk::trace::SdkTracerProvider;
pub use otel::{LogConfig, LogOutput, OTEL_TARGET, OtelCollectorConfig, OtelCollectorProtocol};
pub use parking_lot::Mutex as ParkingLotMutex;
use std::fmt;
use std::sync::Mutex;
use tracing_error::ErrorLayer;
use tracing_subscriber::field::RecordFields;
use tracing_subscriber::filter::{LevelFilter, filter_fn};
use tracing_subscriber::fmt::FormatFields;
use tracing_subscriber::fmt::format::{DefaultFields, FmtSpan, Writer};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, Registry};

const DEFAULT_FILTER_RULES: &str = "info,aws_config=warn,aws_smithy_http_tower=warn";
pub const DEBUG_DIRECT_OTEL_SPANS_ENV: &str = "BD_LOG_DEBUG_DIRECT_OTEL_SPANS";

type RegistryLayer = Box<dyn Layer<Registry> + Send + Sync + 'static>;
type ReloadFilterFn = Box<dyn FnMut(EnvFilter) -> anyhow::Result<()> + Send>;
type ReloadLayersFn = Box<dyn FnMut(Vec<RegistryLayer>) -> anyhow::Result<()> + Send>;

// The local debug-only OTEL span view uses a second fmt layer alongside the normal output layer.
// `tracing-subscriber` stores formatted span fields in extensions keyed by the formatter type, so
// if both layers use `DefaultFields` they will append into the same stored field buffer. That is
// what caused the duplicated `foo=... foo=...` output on synthesized close events. Give the debug
// layer its own field formatter type so its cached fields are isolated from the normal fmt layer.
#[derive(Debug, Default)]
struct DirectOtelDebugFields(DefaultFields);

impl<'writer> FormatFields<'writer> for DirectOtelDebugFields {
  fn format_fields<R: RecordFields>(&self, writer: Writer<'writer>, fields: R) -> fmt::Result {
    self.0.format_fields(writer, fields)
  }
}

//
// LoggerState
//

#[derive(Default)]
struct LoggerState {
  reload_filter: Option<ReloadFilterFn>,
  reload_layers: Option<ReloadLayersFn>,
  otel_provider: Option<SdkTracerProvider>,
  direct_otel_enabled: bool,
}

//
// SwapLogger
//

// An implementation of log:Log which allows for atomically swapping the underlying implementation.
#[derive(Default)]
pub struct SwapLogger {
  state: Mutex<LoggerState>,
}

impl SwapLogger {
  const fn new() -> Self {
    Self {
      state: Mutex::new(LoggerState {
        reload_filter: None,
        reload_layers: None,
        otel_provider: None,
        direct_otel_enabled: false,
      }),
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
    let config = LogConfig::default();
    Self::initialize_with_config(&config).unwrap();
  }

  // Initialize the logger with explicit output and OTEL configuration. If the logger has already
  // been installed, this reloads the active configuration in place so callers can initialize
  // stderr logging early and attach OTEL later once config has loaded.
  pub fn initialize_with_config(config: &LogConfig) -> anyhow::Result<()> {
    Self::configure(config)
  }

  // Install the logger once, or reload the active configuration if it is already installed.
  pub fn configure(config: &LogConfig) -> anyhow::Result<()> {
    if Self::get().state.lock().unwrap().reload_filter.is_some() {
      Self::reload_config(config)
    } else {
      Self::install(config)
    }
  }

  fn install(config: &LogConfig) -> anyhow::Result<()> {
    let direct_otel_enabled = direct_otel_output_enabled(config);
    let (filter, reload_handle) = tracing_subscriber::reload::Layer::new(EnvFilter::new(
      otel::global_filter_rules(&config.log_filter, direct_otel_enabled),
    ));
    let (layers, otel_provider) = build_registry_layers(config)?;
    let (layers, layers_handle) = tracing_subscriber::reload::Layer::new(layers);

    Registry::default()
      .with(layers)
      .with(ErrorLayer::default())
      .with(filter)
      .try_init()?;

    {
      let mut state = Self::get().state.lock().unwrap();
      state.reload_filter = Some(Box::new(move |filter| {
        reload_handle.reload(filter)?;
        Ok(())
      }));
      state.reload_layers = Some(Box::new(move |layers| {
        layers_handle.reload(layers)?;
        Ok(())
      }));
      state.otel_provider = otel_provider;
      state.direct_otel_enabled = direct_otel_enabled;
    }

    Self::update_log_max_level(&config.log_filter);

    Ok(())
  }

  fn reload_config(config: &LogConfig) -> anyhow::Result<()> {
    let direct_otel_enabled = direct_otel_output_enabled(config);
    let (layers, new_otel_provider) = build_registry_layers(config)?;
    let filter = EnvFilter::new(otel::global_filter_rules(
      &config.log_filter,
      direct_otel_enabled,
    ));

    let old_otel_provider = {
      let mut state = Self::get().state.lock().unwrap();
      state
        .reload_layers
        .as_mut()
        .ok_or_else(|| anyhow!("logger has not been initialized"))?(layers)?;
      state
        .reload_filter
        .as_mut()
        .ok_or_else(|| anyhow!("logger has not been initialized"))?(filter)?;

      state.direct_otel_enabled = direct_otel_enabled;
      std::mem::replace(&mut state.otel_provider, new_otel_provider)
    };

    Self::update_log_max_level(&config.log_filter);

    if let Some(provider) = old_otel_provider {
      provider.shutdown()?;
    }

    Ok(())
  }

  // Swap in a new logger with the provided RUST_LOG string.
  pub fn swap(new_rust_log: &str) -> anyhow::Result<()> {
    let mut state = Self::get().state.lock().unwrap();
    let filter = EnvFilter::new(otel::global_filter_rules(
      new_rust_log,
      state.direct_otel_enabled,
    ));
    state
      .reload_filter
      .as_mut()
      .ok_or_else(|| anyhow!("logger has not been initialized"))?(filter)?;

    Self::update_log_max_level(new_rust_log);

    Ok(())
  }

  // Flush and stop the OTEL exporter if one was installed.
  pub fn shutdown() -> anyhow::Result<()> {
    let provider = {
      let mut state = Self::get().state.lock().unwrap();
      state.otel_provider.take()
    };

    if let Some(provider) = provider {
      provider.shutdown()?;
    }

    Ok(())
  }

  fn update_log_max_level(filter_rules: &str) {
    let filter = EnvFilter::new(filter_rules);
    let max_level = filter.max_level_hint().unwrap_or(LevelFilter::TRACE);
    log::set_max_level(tracing_log::AsLog::as_log(&max_level));
  }
}

#[must_use]
pub fn direct_otel_span_debug_enabled() -> bool {
  std::env::var_os(DEBUG_DIRECT_OTEL_SPANS_ENV).is_some()
}

fn direct_otel_output_enabled(config: &LogConfig) -> bool {
  config.otel.is_some() || direct_otel_span_debug_enabled()
}

fn build_registry_layers(
  config: &LogConfig,
) -> anyhow::Result<(Vec<RegistryLayer>, Option<SdkTracerProvider>)> {
  let mut layers = vec![build_output_layer(config)];
  if direct_otel_span_debug_enabled() {
    layers.push(build_direct_otel_debug_layer(config));
  }
  let otel_provider = match config.otel.as_ref() {
    Some(otel_config) => {
      let (layer, provider) = otel::build_otel_layer(otel_config)?;
      layers.push(layer);
      Some(provider)
    },
    None => None,
  };

  Ok((layers, otel_provider))
}

fn build_direct_otel_debug_layer(config: &LogConfig) -> RegistryLayer {
  match config.output {
    LogOutput::Stdout => tracing_subscriber::fmt::layer()
      // This debug layer intentionally reuses the compact human-readable format, but it must not
      // reuse the normal layer's `DefaultFields` storage or close events will print duplicated
      // span attributes when both fmt layers observe the same OTEL-targeted span.
      .fmt_fields(DirectOtelDebugFields::default())
      .with_writer(std::io::stdout)
      .with_ansi(config.ansi)
      .with_line_number(true)
      .with_thread_ids(true)
      .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
      .compact()
      .with_filter(filter_fn(otel::is_direct_otel_target))
      .boxed(),
    LogOutput::Stderr => tracing_subscriber::fmt::layer()
      // Keep the debug-only span view isolated from the ordinary output layer for the same reason
      // as the stdout path above: separate cached field storage avoids duplicated attributes in the
      // synthesized `new`/`close` events.
      .fmt_fields(DirectOtelDebugFields::default())
      .with_writer(std::io::stderr)
      .with_ansi(config.ansi)
      .with_line_number(true)
      .with_thread_ids(true)
      .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
      .compact()
      .with_filter(filter_fn(otel::is_direct_otel_target))
      .boxed(),
  }
}

fn build_output_layer(config: &LogConfig) -> RegistryLayer {
  let exclude_direct_otel = config
    .otel
    .as_ref()
    .is_some_and(|otel| !otel.mirror_to_output);

  match (config.output, exclude_direct_otel) {
    (LogOutput::Stdout, true) => tracing_subscriber::fmt::layer()
      .with_writer(std::io::stdout)
      .with_ansi(config.ansi)
      .with_line_number(true)
      .with_thread_ids(true)
      .compact()
      .with_filter(filter_fn(otel::is_not_direct_otel_target))
      .boxed(),
    (LogOutput::Stdout, false) => tracing_subscriber::fmt::layer()
      .with_writer(std::io::stdout)
      .with_ansi(config.ansi)
      .with_line_number(true)
      .with_thread_ids(true)
      .compact()
      .boxed(),
    (LogOutput::Stderr, true) => tracing_subscriber::fmt::layer()
      .with_writer(std::io::stderr)
      .with_ansi(config.ansi)
      .with_line_number(true)
      .with_thread_ids(true)
      .compact()
      .with_filter(filter_fn(otel::is_not_direct_otel_target))
      .boxed(),
    (LogOutput::Stderr, false) => tracing_subscriber::fmt::layer()
      .with_writer(std::io::stderr)
      .with_ansi(config.ansi)
      .with_line_number(true)
      .with_thread_ids(true)
      .compact()
      .boxed(),
  }
}
