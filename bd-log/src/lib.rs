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
pub use otel::{
  LogConfig,
  LogOutput,
  OTEL_TARGET,
  OtelCollectorConfig,
  OtelCollectorProtocol,
  TRACEPARENT_HEADER,
  TRACESTATE_HEADER,
  TraceContextHeaders,
  current_trace_context_headers,
  current_trace_request_id,
  set_remote_parent,
};
pub use parking_lot::Mutex as ParkingLotMutex;
use std::marker::PhantomData;
use std::sync::Mutex;
use std::{fmt, io};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata};
use tracing_subscriber::field::RecordFields;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::fmt::FormatFields;
use tracing_subscriber::fmt::format::{Compact, DefaultFields, FmtSpan, Writer};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, Registry};

const DEFAULT_FILTER_RULES: &str = "info,aws_config=warn,aws_smithy_http_tower=warn";
pub const DEBUG_DIRECT_OTEL_SPANS_ENV: &str = "BD_LOG_DEBUG_DIRECT_OTEL_SPANS";

type RegistryLayer = Box<dyn Layer<Registry> + Send + Sync + 'static>;
type ReloadFilterFn = Box<dyn FnMut(EnvFilter) -> anyhow::Result<()> + Send>;
type ReloadLayersFn = Box<dyn FnMut(Vec<RegistryLayer>) -> anyhow::Result<()> + Send>;
type ConsoleFmtLayer = tracing_subscriber::fmt::Layer<
  Registry,
  DefaultFields,
  tracing_subscriber::fmt::format::Format<Compact>,
  ConsoleWriter,
>;
type MetadataPredicate = fn(&Metadata<'_>) -> bool;
use std::any::TypeId;

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
// ConsoleWriter
//

// Keep the console writer type stable so the fmt layer builder can be shared across stdout and
// stderr. This lets the output destination change during reload without duplicating the rest of
// the formatter configuration.
#[derive(Clone, Copy, Debug)]
enum ConsoleWriter {
  Stdout,
  Stderr,
}

enum ConsoleWriterGuard {
  Stdout(io::Stdout),
  Stderr(io::Stderr),
}

impl io::Write for ConsoleWriterGuard {
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    match self {
      Self::Stdout(writer) => writer.write(buf),
      Self::Stderr(writer) => writer.write(buf),
    }
  }

  fn flush(&mut self) -> io::Result<()> {
    match self {
      Self::Stdout(writer) => writer.flush(),
      Self::Stderr(writer) => writer.flush(),
    }
  }
}

impl<'writer> tracing_subscriber::fmt::writer::MakeWriter<'writer> for ConsoleWriter {
  type Writer = ConsoleWriterGuard;

  fn make_writer(&'writer self) -> Self::Writer {
    match self {
      Self::Stdout => ConsoleWriterGuard::Stdout(io::stdout()),
      Self::Stderr => ConsoleWriterGuard::Stderr(io::stderr()),
    }
  }
}

fn console_writer(output: LogOutput) -> ConsoleWriter {
  match output {
    LogOutput::Stdout => ConsoleWriter::Stdout,
    LogOutput::Stderr => ConsoleWriter::Stderr,
  }
}

//
// TargetedLayer
//

// `tracing-subscriber` cannot safely reload a freshly-constructed `Filtered` layer because the
// replacement never gets a new `FilterId`. See tokio-rs/tracing#1629 for the concrete panic and
// tokio-rs/tracing#2101 for the upstream API limitation that makes this hard to fix before a
// breaking release. We still want per-layer routing, so keep the routing predicate inside an
// ordinary layer instead of relying on `.with_filter(...)`.
//
// This is intentionally small and mechanical: it delegates the lifecycle methods only for spans
// and events whose metadata matched the predicate at creation time. The marker stored in span
// extensions makes later callbacks cheap and avoids re-checking the span target string on every
// enter/exit/record/close path.
struct TargetedLayer<L, Marker> {
  inner: L,
  predicate: MetadataPredicate,
  marker: PhantomData<fn() -> Marker>,
}

struct SpanRouteMarker<Marker>(PhantomData<fn() -> Marker>);

struct DirectOtelRoute;
struct NonDirectOtelRoute;

impl<L, Marker> TargetedLayer<L, Marker> {
  fn new(inner: L, predicate: MetadataPredicate) -> Self {
    Self {
      inner,
      predicate,
      marker: PhantomData,
    }
  }
}

impl<L, Marker> Layer<Registry> for TargetedLayer<L, Marker>
where
  L: Layer<Registry>,
  Marker: 'static,
{
  fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, Registry>) {
    if !(self.predicate)(attrs.metadata()) {
      return;
    }

    if let Some(span) = ctx.span(id) {
      span
        .extensions_mut()
        .insert(SpanRouteMarker::<Marker>(PhantomData));
    }

    self.inner.on_new_span(attrs, id, ctx);
  }

  fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, Registry>) {
    if span_matches_route::<Marker>(&ctx, id) {
      self.inner.on_record(id, values, ctx);
    }
  }

  fn on_follows_from(&self, id: &Id, follows: &Id, ctx: Context<'_, Registry>) {
    if span_matches_route::<Marker>(&ctx, id) {
      self.inner.on_follows_from(id, follows, ctx);
    }
  }

  fn on_event(&self, event: &Event<'_>, ctx: Context<'_, Registry>) {
    if (self.predicate)(event.metadata()) {
      self.inner.on_event(event, ctx);
    }
  }

  fn on_enter(&self, id: &Id, ctx: Context<'_, Registry>) {
    if span_matches_route::<Marker>(&ctx, id) {
      self.inner.on_enter(id, ctx);
    }
  }

  fn on_exit(&self, id: &Id, ctx: Context<'_, Registry>) {
    if span_matches_route::<Marker>(&ctx, id) {
      self.inner.on_exit(id, ctx);
    }
  }

  fn on_close(&self, id: Id, ctx: Context<'_, Registry>) {
    if span_matches_route::<Marker>(&ctx, &id) {
      self.inner.on_close(id, ctx);
    }
  }

  unsafe fn downcast_raw(&self, id: TypeId) -> Option<*const ()> {
    if id == TypeId::of::<Self>() {
      Some(std::ptr::from_ref(self).cast())
    } else {
      unsafe { self.inner.downcast_raw(id) }
    }
  }
}

fn span_matches_route<Marker: 'static>(ctx: &Context<'_, Registry>, id: &Id) -> bool {
  ctx
    .span(id)
    .is_some_and(|span| span.extensions().get::<SpanRouteMarker<Marker>>().is_some())
}

pub(crate) fn box_direct_otel_layer<L>(inner: L) -> RegistryLayer
where
  L: Layer<Registry> + Send + Sync + 'static,
{
  TargetedLayer::<L, DirectOtelRoute>::new(inner, otel::is_direct_otel_target).boxed()
}

fn box_non_direct_otel_layer<L>(inner: L) -> RegistryLayer
where
  L: Layer<Registry> + Send + Sync + 'static,
{
  TargetedLayer::<L, NonDirectOtelRoute>::new(inner, otel::is_not_direct_otel_target).boxed()
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

    Registry::default().with(layers).with(filter).try_init()?;

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
  // The layer order stays stable across startup and later reconfiguration. We always keep local
  // console logging first so early initialization has a usable human-readable sink, and later
  // reloads only add or adjust the OTEL-related routing around that local output.
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

fn build_console_fmt_layer(config: &LogConfig) -> ConsoleFmtLayer {
  tracing_subscriber::fmt::layer()
    .with_writer(console_writer(config.output))
    .with_ansi(config.ansi)
    .with_line_number(true)
    .with_thread_ids(true)
    .compact()
}

fn build_direct_otel_debug_layer(config: &LogConfig) -> RegistryLayer {
  // Keep the debug-only span view isolated from the ordinary output layer: separate cached field
  // storage avoids duplicated attributes in the synthesized `new`/`close` events.
  box_direct_otel_layer(
    build_console_fmt_layer(config)
      .fmt_fields(DirectOtelDebugFields::default())
      .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE),
  )
}

fn build_output_layer(config: &LogConfig) -> RegistryLayer {
  let exclude_direct_otel = config
    .otel
    .as_ref()
    .is_some_and(|otel| !otel.mirror_to_output);
  let layer = build_console_fmt_layer(config);

  if exclude_direct_otel {
    box_non_direct_otel_layer(layer)
  } else {
    layer.boxed()
  }
}
