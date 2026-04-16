// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{DEFAULT_FILTER_RULES, RegistryLayer};
use anyhow::anyhow;
use http::{HeaderMap, HeaderName, HeaderValue};
use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::tonic_types::metadata::MetadataMap;
use opentelemetry_otlp::{
  Protocol,
  SpanExporter,
  WithExportConfig,
  WithHttpConfig,
  WithTonicConfig,
};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

pub const OTEL_TARGET: &str = "bd_log::otel";

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
  pub headers: BTreeMap<String, String>,
  pub resource_attributes: BTreeMap<String, String>,
  pub timeout: Duration,
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
      headers: BTreeMap::new(),
      resource_attributes: BTreeMap::new(),
      timeout: Duration::from_secs(3),
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

pub(crate) fn global_filter_rules(base_rules: &str, enable_direct_otel: bool) -> String {
  if !enable_direct_otel {
    return base_rules.to_string();
  }

  if base_rules.is_empty() {
    format!("{OTEL_TARGET}=trace")
  } else {
    // Force the dedicated OTEL target through the global gate so it reaches the collector even
    // when the ordinary log filter is narrower.
    format!("{base_rules},{OTEL_TARGET}=trace")
  }
}

pub(crate) fn build_otel_layer(
  config: &OtelCollectorConfig,
) -> anyhow::Result<(RegistryLayer, SdkTracerProvider)> {
  let exporter = build_span_exporter(config)?;
  let provider = SdkTracerProvider::builder()
    .with_batch_exporter(exporter)
    .with_sampler(Sampler::AlwaysOn)
    .with_max_attributes_per_span(config.max_attributes_per_span)
    .with_max_events_per_span(config.max_events_per_span)
    .with_resource(build_resource(config))
    .build();
  let tracer = provider.tracer(config.tracer_name.clone());

  let layer = build_direct_otel_layer(tracer);

  Ok((layer, provider))
}

pub(crate) fn build_direct_otel_layer<T>(tracer: T) -> RegistryLayer
where
  T: opentelemetry::trace::Tracer + Send + Sync + 'static,
  T::Span: Send + Sync,
{
  // Keep routing outside `tracing-opentelemetry`'s own `Filtered` wrapper so the layer can be
  // replaced safely during the second-stage logger configuration. This is the same upstream reload
  // limitation described in tokio-rs/tracing#1629 and tokio-rs/tracing#2101.
  crate::box_direct_otel_layer(
    tracing_opentelemetry::layer()
      .with_tracer(tracer)
      .with_level(true)
      .with_location(false)
      .with_threads(false)
      .with_target(false),
  )
}

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
