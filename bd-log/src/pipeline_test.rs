// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use crate::conditional_export::ExportControl;
use crate::test::TestExporter;
use crate::{LogConfig, OtelCollectorConfig, RegistryLayer, ReloadableLayerStack, otel};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider, SimpleSpanProcessor, SpanData};
use std::future::Future;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{EnvFilter, Registry};

fn layers(config: &LogConfig, otel_layer: Option<RegistryLayer>) -> Vec<RegistryLayer> {
  let mut layers = vec![crate::build_output_layer(config)];
  if crate::direct_otel_span_debug_enabled() {
    layers.push(crate::build_direct_otel_debug_layer(config));
  }
  layers.extend(otel_layer);
  layers
}

pub fn two_stage_otel(future: impl Future<Output = ()>) -> Vec<SpanData> {
  let initial = LogConfig::default();
  let configured = LogConfig {
    otel: Some(OtelCollectorConfig::new(
      "bd-log-pipeline-test",
      "http://127.0.0.1:4317",
    )),
    ..LogConfig::default()
  };
  let (layer_stack, layers_handle) = ReloadableLayerStack::new(layers(&initial, None));
  let (filter, filter_handle) =
    tracing_subscriber::reload::Layer::new(EnvFilter::new(otel::global_filter_rules(
      &initial.log_filter,
      crate::direct_otel_output_enabled(&initial),
    )));
  let subscriber = Registry::default().with(layer_stack).with(filter);
  let exporter = TestExporter::default();
  let control = ExportControl::default();
  let provider = SdkTracerProvider::builder()
    .with_sampler(Sampler::AlwaysOn)
    .with_max_attributes_per_span(16)
    .with_span_processor(control.processor(SimpleSpanProcessor::new(exporter.clone())))
    .build();
  let tracer = ExportControl::tracer(provider.tracer("bd-log-pipeline-test"));
  let otel_layer = otel::build_direct_otel_layer(tracer, control);

  // Keep the entire runtime and all its spans inside this registry's synchronous scope.
  // No global logger, provider, or propagator is installed or reconfigured by this fixture.
  tracing::subscriber::with_default(subscriber, || {
    let runtime = tokio::runtime::Builder::new_current_thread()
      .enable_all()
      .build()
      .unwrap();
    layers_handle.reload(layers(&configured, Some(otel_layer)));
    filter_handle
      .reload(EnvFilter::new(otel::global_filter_rules(
        &configured.log_filter,
        crate::direct_otel_output_enabled(&configured),
      )))
      .unwrap();
    runtime.block_on(future);
    drop(runtime);
    provider.force_flush().unwrap();
    provider.shutdown().unwrap();
  });

  exporter.exported_spans()
}
