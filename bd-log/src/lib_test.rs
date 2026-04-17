// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{LogConfig, otel};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
use std::future::Future;
use std::sync::{Arc, Mutex};
use tracing::span::{Attributes, Id};
use tracing::{Event, Subscriber};
use tracing_subscriber::Registry;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

#[derive(Clone, Default, Debug)]
struct TestExporter(Arc<Mutex<Vec<SpanData>>>);

impl TestExporter {
  fn exported_span_names(&self) -> Vec<String> {
    self
      .0
      .lock()
      .unwrap()
      .iter()
      .map(|span| span.name.to_string())
      .collect()
  }
}

impl SpanExporter for TestExporter {
  async fn export(&self, mut batch: Vec<SpanData>) -> OTelSdkResult {
    self.0.lock().unwrap().append(&mut batch);
    Ok(())
  }
}

#[derive(Clone, Default)]
struct TargetCaptureLayer {
  spans: Arc<Mutex<Vec<String>>>,
  events: Arc<Mutex<Vec<String>>>,
}

impl TargetCaptureLayer {
  fn span_targets(&self) -> Vec<String> {
    self.spans.lock().unwrap().clone()
  }

  fn event_targets(&self) -> Vec<String> {
    self.events.lock().unwrap().clone()
  }
}

impl<S> tracing_subscriber::Layer<S> for TargetCaptureLayer
where
  S: Subscriber + for<'span> LookupSpan<'span>,
{
  fn on_new_span(&self, attrs: &Attributes<'_>, _: &Id, _: Context<'_, S>) {
    self
      .spans
      .lock()
      .unwrap()
      .push(attrs.metadata().target().to_string());
  }

  fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
    self
      .events
      .lock()
      .unwrap()
      .push(event.metadata().target().to_string());
  }
}

fn build_test_otel() -> (TestExporter, SdkTracerProvider, crate::RegistryLayer) {
  let exporter = TestExporter::default();
  let provider = SdkTracerProvider::builder()
    .with_simple_exporter(exporter.clone())
    .build();
  let tracer = provider.tracer("bd-log-test");

  (exporter, provider, otel::build_direct_otel_layer(tracer))
}

fn later_log_config() -> LogConfig {
  LogConfig {
    otel: Some(crate::OtelCollectorConfig::new(
      "bd-log-test",
      "http://127.0.0.1:4317",
    )),
    ..LogConfig::default()
  }
}

fn exported_span_names_after_two_stage_init(future: impl Future<Output = ()>) -> Vec<String> {
  let initial_config = LogConfig::default();
  let later_config = later_log_config();
  let (layers, layers_handle) =
    tracing_subscriber::reload::Layer::new(vec![crate::build_output_layer(&initial_config)]);
  let (filter, filter_handle) =
    tracing_subscriber::reload::Layer::new(tracing_subscriber::EnvFilter::new(
      otel::global_filter_rules(&initial_config.log_filter, false),
    ));
  let subscriber = Registry::default().with(layers).with(filter);

  let (exporter, provider, otel_layer) = build_test_otel();
  let runtime = tokio::runtime::Runtime::new().unwrap();

  tracing::subscriber::with_default(subscriber, || {
    layers_handle
      .reload(vec![crate::build_output_layer(&later_config), otel_layer])
      .unwrap();
    filter_handle
      .reload(tracing_subscriber::EnvFilter::new(
        otel::global_filter_rules(&later_config.log_filter, true),
      ))
      .unwrap();

    runtime.block_on(future);
  });

  provider.force_flush().unwrap();

  exporter.exported_span_names()
}

#[test]
fn two_stage_init_keeps_plain_root_instrumented_spans_out_of_otel() {
  #[tracing::instrument(skip_all)]
  async fn plain_root_span() {}

  let span_names = exported_span_names_after_two_stage_init(plain_root_span());

  assert!(
    span_names.is_empty(),
    "unexpected exported spans: {span_names:?}"
  );
}

#[test]
fn two_stage_init_still_exports_direct_otel_spans() {
  let span_names = exported_span_names_after_two_stage_init(async {
    let span = crate::otel_info_span!("direct_otel_root");
    let _entered = span.enter();
  });

  assert_eq!(span_names, vec!["direct_otel_root".to_string()]);
}

#[test]
fn shared_target_routing_only_forwards_matching_targets() {
  let direct_capture = TargetCaptureLayer::default();
  let non_direct_capture = TargetCaptureLayer::default();
  let (layers, _) = tracing_subscriber::reload::Layer::new(vec![
    crate::box_direct_otel_layer(direct_capture.clone()),
    crate::box_non_direct_otel_layer(non_direct_capture.clone()),
  ]);
  let subscriber = Registry::default().with(layers);

  tracing::subscriber::with_default(subscriber, || {
    let direct_span = crate::otel_info_span!("direct_span");
    let _direct_entered = direct_span.enter();
    crate::otel_info!(message = "direct_event");

    let plain_span = tracing::info_span!("plain_span");
    let _plain_entered = plain_span.enter();
    tracing::info!("plain_event");
  });

  assert_eq!(
    direct_capture.span_targets(),
    vec![crate::OTEL_TARGET.to_string()]
  );
  assert_eq!(
    direct_capture.event_targets(),
    vec![crate::OTEL_TARGET.to_string()]
  );
  assert_eq!(
    non_direct_capture.span_targets(),
    vec![module_path!().to_string()]
  );
  assert_eq!(
    non_direct_capture.event_targets(),
    vec![module_path!().to_string()]
  );
}

#[test]
fn conditional_otel_span_returns_null_without_direct_otel_parent() {
  let span_names = exported_span_names_after_two_stage_init(async {
    let span = crate::otel_info_span_if_parent!("conditional_root");
    assert!(span.metadata().is_none());

    let plain_parent = tracing::info_span!("plain_parent");
    let _plain_parent_entered = plain_parent.enter();
    let nested_span = crate::otel_info_span_if_parent!("conditional_nested");
    assert!(nested_span.metadata().is_none());
  });

  assert!(
    span_names.is_empty(),
    "unexpected exported spans: {span_names:?}"
  );
}

#[test]
fn conditional_otel_span_creates_child_for_direct_otel_parent() {
  let span_names = exported_span_names_after_two_stage_init(async {
    let parent = crate::otel_info_span!("direct_parent");
    let _parent_entered = parent.enter();

    let child = crate::otel_info_span_if_parent!("direct_child", answer = 42);
    assert_eq!(
      child.metadata().map(tracing::Metadata::target),
      Some(crate::OTEL_TARGET)
    );

    let _child_entered = child.enter();
  });

  assert!(span_names.iter().any(|name| name == "direct_parent"));
  assert!(span_names.iter().any(|name| name == "direct_child"));
}

#[test]
fn conditional_otel_instrument_only_creates_span_for_direct_otel_parent() {
  fn instrumented_work() -> impl Future<Output = ()> {
    std::future::ready(())
  }

  let span_names = exported_span_names_after_two_stage_init(async {
    crate::otel_instrument_if_parent!(instrumented_work(), tracing::Level::INFO, "without_parent")
      .await;

    let parent = crate::otel_info_span!("direct_parent");
    let _parent_entered = parent.enter();
    crate::otel_instrument_if_parent!(instrumented_work(), tracing::Level::INFO, "with_parent")
      .await;
  });

  assert!(span_names.iter().any(|name| name == "direct_parent"));
  assert!(span_names.iter().any(|name| name == "with_parent"));
  assert!(!span_names.iter().any(|name| name == "without_parent"));
}
