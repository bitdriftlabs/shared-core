// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! API-compatible no-OTEL implementation used when `bd-log/otel` is disabled.

use std::collections::BTreeMap;
use std::time::Duration;

pub const OTEL_TARGET: &str = "bd_log::otel";
pub const TRACEPARENT_HEADER: &str = "traceparent";
pub const TRACESTATE_HEADER: &str = "tracestate";

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

#[must_use]
pub fn current_trace_context_headers() -> Option<TraceContextHeaders> {
  None
}

#[must_use]
pub fn current_trace_request_id() -> Option<String> {
  None
}

#[must_use]
pub fn set_remote_parent(_: &tracing::Span, _: &TraceContextHeaders) -> bool {
  false
}

#[must_use]
pub fn add_trace_link(_: &tracing::Span, _: &TraceContextHeaders) -> bool {
  false
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum LogOutput {
  Stdout,
  #[default]
  Stderr,
}

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
      log_filter: std::env::var("RUST_LOG")
        .unwrap_or_else(|_| crate::DEFAULT_FILTER_RULES.to_string()),
      output: LogOutput::Stderr,
      ansi: std::env::var("BD_LOG_ANSI").is_ok(),
      otel: None,
    }
  }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum OtelCollectorProtocol {
  #[default]
  Grpc,
  HttpBinary,
}

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
    OTEL_TARGET.to_string()
  } else {
    format!("{base_rules},{OTEL_TARGET}=trace")
  }
}
