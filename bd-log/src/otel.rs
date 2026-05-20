// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(all(test, feature = "otel"))]
#[path = "./otel_test.rs"]
mod tests;

use crate::DEFAULT_FILTER_RULES;
#[cfg(feature = "otel")]
use crate::RegistryLayer;
#[cfg(feature = "otel")]
use anyhow::anyhow;
#[cfg(feature = "otel")]
use http::{HeaderMap, HeaderName, HeaderValue};
#[cfg(feature = "otel")]
use opentelemetry::KeyValue;
#[cfg(feature = "otel")]
use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator};
#[cfg(feature = "otel")]
use opentelemetry::trace::{SpanContext, TraceContextExt, TracerProvider as _};
#[cfg(feature = "otel")]
use opentelemetry_otlp::tonic_types::metadata::MetadataMap;
#[cfg(feature = "otel")]
use opentelemetry_otlp::{
  Protocol,
  SpanExporter,
  WithExportConfig,
  WithHttpConfig,
  WithTonicConfig,
};
#[cfg(feature = "otel")]
use opentelemetry_sdk::Resource;
#[cfg(feature = "otel")]
use opentelemetry_sdk::propagation::TraceContextPropagator;
#[cfg(feature = "otel")]
use opentelemetry_sdk::runtime::{Tokio, TokioCurrentThread};
#[cfg(feature = "otel")]
use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor;
#[cfg(feature = "otel")]
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
#[cfg(feature = "otel")]
use std::collections::{BTreeMap, HashMap};
#[cfg(feature = "otel")]
use tokio::runtime::{Handle, RuntimeFlavor};
#[cfg(feature = "otel")]
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub const OTEL_TARGET: &str = "bd_log::otel";
pub const TRACEPARENT_HEADER: &str = "traceparent";
pub const TRACESTATE_HEADER: &str = "tracestate";

//
// TraceContextHeaders
//

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TraceContextHeaders {
  pub traceparent: String,
  pub tracestate: Option<String>,
}

impl TraceContextHeaders {
  #[must_use]
  pub fn is_empty(&self) -> bool {
    self.traceparent.is_empty() && self.tracestate.as_deref().unwrap_or_default().is_empty()
  }
}

//
// LogOutput
//

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum LogOutput {
  Stdout,
  #[default]
  Stderr,
}

//
// LogConfig
//

#[derive(Clone, Debug)]
pub struct LogConfig {
  pub log_filter: String,
  pub output: LogOutput,
  pub ansi: bool,
  pub otel: Option<OtelCollectorConfig>,
}

impl Default for LogConfig {
  fn default() -> Self {
    Self {
      log_filter: std::env::var("RUST_LOG").unwrap_or_else(|_| DEFAULT_FILTER_RULES.to_string()),
      output: LogOutput::Stderr,
      ansi: std::env::var("BD_LOG_ANSI").is_ok(),
      otel: None,
    }
  }
}

//
// OtelCollectorProtocol
//

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum OtelCollectorProtocol {
  #[default]
  Grpc,
  HttpBinary,
  HttpJson,
}

#[cfg(feature = "otel")]
impl OtelCollectorProtocol {
  fn export_protocol(self) -> Protocol {
    match self {
      Self::Grpc => Protocol::Grpc,
      Self::HttpBinary => Protocol::HttpBinary,
      Self::HttpJson => Protocol::HttpJson,
    }
  }
}

//
// OtelCollectorConfig
//

#[derive(Clone, Debug)]
pub struct OtelCollectorConfig {
  pub endpoint: String,
  pub protocol: OtelCollectorProtocol,
  pub service_name: String,
  pub tracer_name: String,
  #[cfg(feature = "otel")]
  pub headers: BTreeMap<String, String>,
  #[cfg(not(feature = "otel"))]
  pub headers: std::collections::BTreeMap<String, String>,
  #[cfg(feature = "otel")]
  pub resource_attributes: BTreeMap<String, String>,
  #[cfg(not(feature = "otel"))]
  pub resource_attributes: std::collections::BTreeMap<String, String>,
  pub timeout: std::time::Duration,
  pub mirror_to_output: bool,
  pub max_attributes_per_span: u32,
  pub max_events_per_span: u32,
}

impl OtelCollectorConfig {
  pub fn new(service_name: impl Into<String>, endpoint: impl Into<String>) -> Self {
    let service_name = service_name.into();

    Self {
      endpoint: endpoint.into(),
      protocol: OtelCollectorProtocol::Grpc,
      tracer_name: service_name.clone(),
      service_name,
      headers: Default::default(),
      resource_attributes: Default::default(),
      timeout: std::time::Duration::from_secs(3),
      mirror_to_output: false,
      max_attributes_per_span: 16,
      max_events_per_span: 64,
    }
  }
}

pub(crate) fn is_direct_otel_target(metadata: &tracing::Metadata<'_>) -> bool {
  metadata.target() == OTEL_TARGET
}

pub(crate) fn is_not_direct_otel_target(metadata: &tracing::Metadata<'_>) -> bool {
  !is_direct_otel_target(metadata)
}

#[doc(hidden)]
#[must_use]
#[inline]
pub fn current_span_is_direct_otel() -> bool {
  tracing::Span::current()
    .metadata()
    .is_some_and(is_direct_otel_target)
}

pub(crate) fn global_filter_rules(base_rules: &str, enable_direct_otel: bool) -> String {
  if !enable_direct_otel {
    return base_rules.to_string();
  }

  if base_rules.is_empty() {
    format!("{OTEL_TARGET}=trace")
  } else {
    format!("{base_rules},{OTEL_TARGET}=trace")
  }
}

// --- OTel-only implementation ---

#[cfg(feature = "otel")]
struct TraceContextHeadersInjector<'a> {
  headers: &'a mut TraceContextHeaders,
}

#[cfg(feature = "otel")]
impl Injector for TraceContextHeadersInjector<'_> {
  fn set(&mut self, key: &str, value: String) {
    match key {
      TRACEPARENT_HEADER => self.headers.traceparent = value,
      TRACESTATE_HEADER => self.headers.tracestate = Some(value),
      _ => {},
    }
  }
}

#[cfg(feature = "otel")]
struct TraceContextHeadersExtractor<'a> {
  headers: &'a TraceContextHeaders,
}

#[cfg(feature = "otel")]
impl Extractor for TraceContextHeadersExtractor<'_> {
  fn get(&self, key: &str) -> Option<&str> {
    match key {
      TRACEPARENT_HEADER if !self.headers.traceparent.is_empty() => Some(&self.headers.traceparent),
      TRACESTATE_HEADER => self.headers.tracestate.as_deref(),
      _ => None,
    }
  }

  fn keys(&self) -> Vec<&str> {
    let mut keys = vec![];

    if !self.headers.traceparent.is_empty() {
      keys.push(TRACEPARENT_HEADER);
    }

    if self.headers.tracestate.is_some() {
      keys.push(TRACESTATE_HEADER);
    }

    keys
  }
}

#[cfg(feature = "otel")]
fn trace_context_propagator() -> TraceContextPropagator {
  TraceContextPropagator::new()
}

#[cfg(feature = "otel")]
fn remote_span_context(headers: &TraceContextHeaders) -> Option<SpanContext> {
  if headers.traceparent.is_empty() {
    return None;
  }

  let extracted_context =
    trace_context_propagator().extract(&TraceContextHeadersExtractor { headers });
  let span_context = extracted_context.span().span_context().clone();

  span_context.is_valid().then_some(span_context)
}

#[cfg(feature = "otel")]
#[must_use]
pub fn current_trace_context_headers() -> Option<TraceContextHeaders> {
  let current_context = opentelemetry::Context::current();
  let span_context = current_context.span().span_context().clone();

  if !span_context.is_valid() {
    return None;
  }

  let mut headers = TraceContextHeaders::default();
  trace_context_propagator().inject_context(
    &current_context,
    &mut TraceContextHeadersInjector {
      headers: &mut headers,
    },
  );

  if headers.traceparent.is_empty() {
    return None;
  }

  Some(headers)
}

#[cfg(not(feature = "otel"))]
#[must_use]
pub fn current_trace_context_headers() -> Option<TraceContextHeaders> {
  None
}

#[cfg(feature = "otel")]
#[must_use]
pub fn current_trace_request_id() -> Option<String> {
  let span_context = opentelemetry::Context::current()
    .span()
    .span_context()
    .clone();

  if !span_context.is_valid() {
    return None;
  }

  Some(format!(
    "{}-{}",
    span_context.trace_id(),
    span_context.span_id()
  ))
}

#[cfg(not(feature = "otel"))]
#[must_use]
pub fn current_trace_request_id() -> Option<String> {
  None
}

#[cfg(feature = "otel")]
#[must_use]
pub fn set_remote_parent(span: &tracing::Span, headers: &TraceContextHeaders) -> bool {
  let Some(span_context) = remote_span_context(headers) else {
    return false;
  };

  span
    .set_parent(opentelemetry::Context::new().with_remote_span_context(span_context))
    .is_ok()
}

#[cfg(not(feature = "otel"))]
#[must_use]
pub fn set_remote_parent(_span: &tracing::Span, _headers: &TraceContextHeaders) -> bool {
  false
}

#[cfg(feature = "otel")]
#[must_use]
pub fn add_trace_link(span: &tracing::Span, headers: &TraceContextHeaders) -> bool {
  let Some(span_context) = remote_span_context(headers) else {
    return false;
  };

  span.add_link(span_context);
  true
}

#[cfg(not(feature = "otel"))]
#[must_use]
pub fn add_trace_link(_span: &tracing::Span, _headers: &TraceContextHeaders) -> bool {
  false
}

#[cfg(feature = "otel")]
pub(crate) fn build_otel_layer(
  config: &OtelCollectorConfig,
) -> anyhow::Result<(RegistryLayer, SdkTracerProvider)> {
  let exporter = build_span_exporter(config)?;
  let runtime_flavor = active_tokio_runtime_flavor()?;
  let provider_builder = SdkTracerProvider::builder()
    .with_sampler(Sampler::AlwaysOn)
    .with_max_attributes_per_span(config.max_attributes_per_span)
    .with_max_events_per_span(config.max_events_per_span)
    .with_resource(build_resource(config));
  let provider = match runtime_flavor {
    RuntimeFlavor::CurrentThread => provider_builder
      .with_span_processor(BatchSpanProcessor::builder(exporter, TokioCurrentThread).build())
      .build(),
    RuntimeFlavor::MultiThread => provider_builder
      .with_span_processor(BatchSpanProcessor::builder(exporter, Tokio).build())
      .build(),
    other => {
      return Err(anyhow!(
        "unsupported tokio runtime flavor for OTEL batch exporter: {other:?}"
      ));
    },
  };
  let tracer = provider.tracer(config.tracer_name.clone());

  let layer = build_direct_otel_layer(tracer);

  Ok((layer, provider))
}

#[cfg(feature = "otel")]
fn active_tokio_runtime_flavor() -> anyhow::Result<RuntimeFlavor> {
  Ok(Handle::try_current()?.runtime_flavor())
}

#[cfg(feature = "otel")]
pub(crate) fn build_direct_otel_layer<T>(tracer: T) -> RegistryLayer
where
  T: opentelemetry::trace::Tracer + Send + Sync + 'static,
  T::Span: Send + Sync,
{
  crate::box_direct_otel_layer(
    tracing_opentelemetry::layer()
      .with_tracer(tracer)
      .with_level(true)
      .with_location(false)
      .with_threads(false)
      .with_target(false),
  )
}

#[cfg(feature = "otel")]
fn build_span_exporter(config: &OtelCollectorConfig) -> anyhow::Result<SpanExporter> {
  match config.protocol {
    OtelCollectorProtocol::Grpc => {
      let mut builder = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(config.endpoint.clone())
        .with_timeout(config.timeout);

      if !config.headers.is_empty() {
        builder = builder.with_metadata(build_tonic_metadata(&config.headers)?);
      }

      builder.build().map_err(Into::into)
    },
    OtelCollectorProtocol::HttpBinary | OtelCollectorProtocol::HttpJson => SpanExporter::builder()
      .with_http()
      .with_protocol(config.protocol.export_protocol())
      .with_endpoint(config.endpoint.clone())
      .with_timeout(config.timeout)
      .with_headers(
        config
          .headers
          .clone()
          .into_iter()
          .collect::<HashMap<_, _>>(),
      )
      .build()
      .map_err(Into::into),
  }
}

#[cfg(feature = "otel")]
fn build_resource(config: &OtelCollectorConfig) -> Resource {
  let mut attributes = Vec::with_capacity(config.resource_attributes.len() + 1);
  attributes.push(KeyValue::new("service.name", config.service_name.clone()));
  attributes.extend(
    config
      .resource_attributes
      .iter()
      .map(|(key, value)| KeyValue::new(key.clone(), value.clone())),
  );

  Resource::builder_empty()
    .with_attributes(attributes)
    .build()
}

#[cfg(feature = "otel")]
fn build_tonic_metadata(headers: &BTreeMap<String, String>) -> anyhow::Result<MetadataMap> {
  let mut header_map = HeaderMap::with_capacity(headers.len());

  for (key, value) in headers {
    let header_name = HeaderName::from_bytes(key.as_bytes())
      .map_err(|error| anyhow!("invalid OTEL header name {key}: {error}"))?;
    let header_value = HeaderValue::from_str(value)
      .map_err(|error| anyhow!("invalid OTEL header value for {key}: {error}"))?;
    header_map.insert(header_name, header_value);
  }

  Ok(MetadataMap::from_headers(header_map))
}

// --- Macros (always available, no OTel deps needed) ---

#[macro_export]
macro_rules! otel_span {
  ($level:expr, $name:expr) => {
    tracing::span!(target: $crate::OTEL_TARGET, $level, $name)
  };
  ($level:expr, $name:expr, $($fields:tt)*) => {
    tracing::span!(target: $crate::OTEL_TARGET, $level, $name, $($fields)*)
  };
}

#[macro_export]
macro_rules! otel_trace_span {
  ($name:expr) => {
    $crate::otel_span!(tracing::Level::TRACE, $name)
  };
  ($name:expr, $($fields:tt)*) => {
    $crate::otel_span!(tracing::Level::TRACE, $name, $($fields)*)
  };
}

#[macro_export]
macro_rules! otel_span_if_parent {
  ($level:expr, $name:expr) => {{
    if $crate::otel::current_span_is_direct_otel() {
      $crate::otel_span!($level, $name)
    } else {
      tracing::Span::none()
    }
  }};
  ($level:expr, $name:expr, $($fields:tt)*) => {{
    if $crate::otel::current_span_is_direct_otel() {
      $crate::otel_span!($level, $name, $($fields)*)
    } else {
      tracing::Span::none()
    }
  }};
}

#[macro_export]
macro_rules! otel_trace_span_if_parent {
  ($name:expr) => {
    $crate::otel_span_if_parent!(tracing::Level::TRACE, $name)
  };
  ($name:expr, $($fields:tt)*) => {
    $crate::otel_span_if_parent!(tracing::Level::TRACE, $name, $($fields)*)
  };
}

#[macro_export]
macro_rules! otel_debug_span_if_parent {
  ($name:expr) => {
    $crate::otel_span_if_parent!(tracing::Level::DEBUG, $name)
  };
  ($name:expr, $($fields:tt)*) => {
    $crate::otel_span_if_parent!(tracing::Level::DEBUG, $name, $($fields)*)
  };
}

#[macro_export]
macro_rules! otel_info_span_if_parent {
  ($name:expr) => {
    $crate::otel_span_if_parent!(tracing::Level::INFO, $name)
  };
  ($name:expr, $($fields:tt)*) => {
    $crate::otel_span_if_parent!(tracing::Level::INFO, $name, $($fields)*)
  };
}

#[macro_export]
macro_rules! otel_warn_span_if_parent {
  ($name:expr) => {
    $crate::otel_span_if_parent!(tracing::Level::WARN, $name)
  };
  ($name:expr, $($fields:tt)*) => {
    $crate::otel_span_if_parent!(tracing::Level::WARN, $name, $($fields)*)
  };
}

#[macro_export]
macro_rules! otel_error_span_if_parent {
  ($name:expr) => {
    $crate::otel_span_if_parent!(tracing::Level::ERROR, $name)
  };
  ($name:expr, $($fields:tt)*) => {
    $crate::otel_span_if_parent!(tracing::Level::ERROR, $name, $($fields)*)
  };
}

#[macro_export]
macro_rules! otel_debug_span {
  ($name:expr) => {
    $crate::otel_span!(tracing::Level::DEBUG, $name)
  };
  ($name:expr, $($fields:tt)*) => {
    $crate::otel_span!(tracing::Level::DEBUG, $name, $($fields)*)
  };
}

#[macro_export]
macro_rules! otel_info_span {
  ($name:expr) => {
    $crate::otel_span!(tracing::Level::INFO, $name)
  };
  ($name:expr, $($fields:tt)*) => {
    $crate::otel_span!(tracing::Level::INFO, $name, $($fields)*)
  };
}

#[macro_export]
macro_rules! otel_warn_span {
  ($name:expr) => {
    $crate::otel_span!(tracing::Level::WARN, $name)
  };
  ($name:expr, $($fields:tt)*) => {
    $crate::otel_span!(tracing::Level::WARN, $name, $($fields)*)
  };
}

#[macro_export]
macro_rules! otel_error_span {
  ($name:expr) => {
    $crate::otel_span!(tracing::Level::ERROR, $name)
  };
  ($name:expr, $($fields:tt)*) => {
    $crate::otel_span!(tracing::Level::ERROR, $name, $($fields)*)
  };
}

#[macro_export]
macro_rules! otel_event {
  ($level:expr, $($fields:tt)*) => {
    tracing::event!(target: $crate::OTEL_TARGET, $level, $($fields)*)
  };
}

#[macro_export]
macro_rules! otel_trace {
  ($($fields:tt)*) => {
    $crate::otel_event!(tracing::Level::TRACE, $($fields)*)
  };
}

#[macro_export]
macro_rules! otel_debug {
  ($($fields:tt)*) => {
    $crate::otel_event!(tracing::Level::DEBUG, $($fields)*)
  };
}

#[macro_export]
macro_rules! otel_info {
  ($($fields:tt)*) => {
    $crate::otel_event!(tracing::Level::INFO, $($fields)*)
  };
}

#[macro_export]
macro_rules! otel_warn {
  ($($fields:tt)*) => {
    $crate::otel_event!(tracing::Level::WARN, $($fields)*)
  };
}

#[macro_export]
macro_rules! otel_error {
  ($($fields:tt)*) => {
    $crate::otel_event!(tracing::Level::ERROR, $($fields)*)
  };
}

#[macro_export]
macro_rules! otel_instrument {
  ($future:expr, $level:expr, $name:expr) => {{
    use tracing::Instrument as _;
    ($future).instrument($crate::otel_span!($level, $name))
  }};
  ($future:expr, $level:expr, $name:expr, $($fields:tt)*) => {{
    use tracing::Instrument as _;
    ($future).instrument($crate::otel_span!($level, $name, $($fields)*))
  }};
}

#[macro_export]
macro_rules! otel_instrument_if_parent {
  ($future:expr, $level:expr, $name:expr) => {{
    use tracing::Instrument as _;
    ($future).instrument($crate::otel_span_if_parent!($level, $name))
  }};
  ($future:expr, $level:expr, $name:expr, $($fields:tt)*) => {{
    use tracing::Instrument as _;
    ($future).instrument($crate::otel_span_if_parent!($level, $name, $($fields)*))
  }};
}
