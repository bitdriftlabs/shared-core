// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{LogConfig, RegistryLayer, otel};
use opentelemetry::trace::{TraceContextExt, TracerProvider as _};
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
use std::future::Future;
use std::sync::{Arc, Mutex};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::Registry;
use tracing_subscriber::layer::SubscriberExt;

#[derive(Clone, Default, Debug)]
struct TestExporter(Arc<Mutex<Vec<SpanData>>>);

impl TestExporter {
  fn exported_spans(&self) -> Vec<SpanData> {
    self.0.lock().unwrap().clone()
  }
}

impl SpanExporter for TestExporter {
  async fn export(&self, mut batch: Vec<SpanData>) -> OTelSdkResult {
    self.0.lock().unwrap().append(&mut batch);
    Ok(())
  }
}

fn build_test_otel(service_name: &str) -> (TestExporter, SdkTracerProvider, RegistryLayer) {
  let exporter = TestExporter::default();
  let provider = SdkTracerProvider::builder()
    .with_simple_exporter(exporter.clone())
    .build();
  let tracer = provider.tracer(service_name.to_string());

  (exporter, provider, otel::build_direct_otel_layer(tracer))
}

pub struct TestTraceContext {
  exporter: TestExporter,
  provider: SdkTracerProvider,
  dispatch: tracing::Dispatch,
}

impl TestTraceContext {
  #[must_use]
  pub fn new(service_name: &str) -> Self {
    let (exporter, provider, otel_layer) = build_test_otel(service_name);
    let subscriber = Registry::default().with(otel_layer);

    Self {
      exporter,
      provider,
      dispatch: tracing::Dispatch::new(subscriber),
    }
  }

  #[must_use]
  pub fn dispatch(&self) -> tracing::Dispatch {
    self.dispatch.clone()
  }

  #[must_use]
  pub fn exported_spans(&self) -> Vec<SpanData> {
    self.exporter.exported_spans()
  }

  pub fn request_span(&self) -> (tracing::dispatcher::DefaultGuard, tracing::Span, String) {
    let guard = tracing::dispatcher::set_default(&self.dispatch);
    let span = crate::otel_info_span!("test_request");
    let span_context = {
      let _enter = span.enter();
      span.context().span().span_context().clone()
    };

    assert!(span_context.is_valid());

    (
      guard,
      span,
      format!("{}-{}", span_context.trace_id(), span_context.span_id()),
    )
  }
}

impl Drop for TestTraceContext {
  fn drop(&mut self) {
    self.provider.force_flush().unwrap();
  }
}

fn test_layers(config: &LogConfig, otel_layer: Option<RegistryLayer>) -> Vec<RegistryLayer> {
  let mut layers = vec![crate::build_output_layer(config)];

  if crate::direct_otel_span_debug_enabled() {
    layers.push(crate::build_direct_otel_debug_layer(config));
  }

  if let Some(otel_layer) = otel_layer.filter(|_| config.otel.is_some()) {
    layers.push(otel_layer);
  }

  layers
}

#[must_use]
pub fn test_otel_log_config(service_name: impl Into<String>) -> LogConfig {
  LogConfig {
    otel: Some(crate::OtelCollectorConfig::new(
      service_name,
      "http://127.0.0.1:4317",
    )),
    ..LogConfig::default()
  }
}

pub fn with_two_phase_test_otel<R>(
  service_name: &str,
  future: impl Future<Output = R>,
) -> (Vec<SpanData>, R) {
  let initial_config = LogConfig::default();
  let later_config = test_otel_log_config(service_name);
  let initial_direct_otel = crate::direct_otel_output_enabled(&initial_config);
  // Mirror the production logger topology as closely as possible: keep the global filter behind
  // `reload::Layer`, but use the custom reloadable layer stack for the actual layer list so
  // tracing-opentelemetry's `WithContext` downcasts still work after the second-stage reload.
  let (layers, layers_handle) =
    crate::ReloadableLayerStack::new(test_layers(&initial_config, None));
  let (filter, filter_handle) =
    tracing_subscriber::reload::Layer::new(tracing_subscriber::EnvFilter::new(
      otel::global_filter_rules(&initial_config.log_filter, initial_direct_otel),
    ));
  let subscriber = Registry::default().with(layers).with(filter);

  let (exporter, provider, otel_layer) = build_test_otel(service_name);
  let runtime = tokio::runtime::Runtime::new().unwrap();

  let result = tracing::subscriber::with_default(subscriber, || {
    let direct_otel_enabled = crate::direct_otel_output_enabled(&later_config);
    layers_handle.reload(test_layers(&later_config, Some(otel_layer)));
    filter_handle
      .reload(tracing_subscriber::EnvFilter::new(
        otel::global_filter_rules(&later_config.log_filter, direct_otel_enabled),
      ))
      .unwrap();

    runtime.block_on(future)
  });

  provider.force_flush().unwrap();

  (exporter.exported_spans(), result)
}
