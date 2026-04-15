// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{DEBUG_DIRECT_OTEL_SPANS_ENV, LogConfig, LogOutput, OTEL_TARGET, otel};
use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::span::{Attributes, Id};
use tracing::{Event, Subscriber, field};
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::fmt::writer::MakeWriter;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{Layer, Registry};

//
// TargetCaptureLayer
//

#[derive(Clone, Default)]
struct TargetCaptureLayer {
  spans: Arc<Mutex<Vec<String>>>,
  events: Arc<Mutex<Vec<String>>>,
}

impl TargetCaptureLayer {
  fn recorded_spans(&self) -> Vec<String> {
    self.spans.lock().unwrap().clone()
  }

  fn recorded_events(&self) -> Vec<String> {
    self.events.lock().unwrap().clone()
  }
}

impl<S> Layer<S> for TargetCaptureLayer
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

static ENV_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone, Default)]
struct CapturedWriter {
  output: Arc<Mutex<Vec<u8>>>,
}

impl CapturedWriter {
  fn contents(&self) -> String {
    String::from_utf8(self.output.lock().unwrap().clone()).unwrap()
  }
}

struct CapturedWriterGuard {
  output: Arc<Mutex<Vec<u8>>>,
}

impl io::Write for CapturedWriterGuard {
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    self.output.lock().unwrap().extend_from_slice(buf);
    Ok(buf.len())
  }

  fn flush(&mut self) -> io::Result<()> {
    Ok(())
  }
}

impl<'a> MakeWriter<'a> for CapturedWriter {
  type Writer = CapturedWriterGuard;

  fn make_writer(&'a self) -> Self::Writer {
    CapturedWriterGuard {
      output: self.output.clone(),
    }
  }
}

fn with_direct_otel_span_debug_env<T>(value: Option<&str>, f: impl FnOnce() -> T) -> T {
  let _guard = ENV_LOCK.lock().unwrap();
  let previous = std::env::var_os(DEBUG_DIRECT_OTEL_SPANS_ENV);

  // Rust 2024 makes process environment mutation unsafe; serialize it for these tests.
  unsafe {
    match value {
      Some(value) => std::env::set_var(DEBUG_DIRECT_OTEL_SPANS_ENV, value),
      None => std::env::remove_var(DEBUG_DIRECT_OTEL_SPANS_ENV),
    }
  }

  let result = f();

  // Restore the original process environment before releasing the lock.
  unsafe {
    match previous {
      Some(value) => std::env::set_var(DEBUG_DIRECT_OTEL_SPANS_ENV, value),
      None => std::env::remove_var(DEBUG_DIRECT_OTEL_SPANS_ENV),
    }
  }

  result
}

#[test]
fn default_log_config_matches_existing_behavior() {
  with_direct_otel_span_debug_env(None, || {
    let config = LogConfig::default();

    assert_eq!(config.output, LogOutput::Stderr);
  });
}

#[test]
fn global_filter_rules_force_the_direct_otel_target() {
  let filter = otel::global_filter_rules("info", true);

  assert_eq!(filter, format!("info,{OTEL_TARGET}=trace"));
}

#[test]
fn build_registry_layers_supports_dual_stage_configuration() {
  with_direct_otel_span_debug_env(None, || {
    let early_config = LogConfig::default();
    let (early_layers, early_provider) = crate::build_registry_layers(&early_config).unwrap();

    assert_eq!(early_layers.len(), 1);
    assert!(early_provider.is_none());

    let later_config = LogConfig {
      otel: Some(crate::OtelCollectorConfig {
        endpoint: "http://127.0.0.1:4317".to_string(),
        protocol: crate::OtelCollectorProtocol::Grpc,
        service_name: "bd-log-test".to_string(),
        tracer_name: "bd-log-test".to_string(),
        headers: BTreeMap::default(),
        resource_attributes: BTreeMap::default(),
        timeout: Duration::from_secs(1),
        mirror_to_output: false,
        max_attributes_per_span: 16,
        max_events_per_span: 64,
      }),
      ..LogConfig::default()
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (later_layers, later_provider) = runtime
      .block_on(async {
        let _entered = runtime.handle().enter();
        crate::build_registry_layers(&later_config)
      })
      .unwrap();

    assert_eq!(later_layers.len(), 2);
    assert!(later_provider.is_some());
  });
}

#[test]
fn build_registry_layers_supports_local_direct_otel_span_debugging() {
  with_direct_otel_span_debug_env(Some("1"), || {
    let config = LogConfig::default();
    let (layers, provider) = crate::build_registry_layers(&config).unwrap();

    assert_eq!(layers.len(), 2);
    assert!(provider.is_none());
    assert!(crate::direct_otel_span_debug_enabled());
  });
}

#[test]
fn direct_otel_debug_formatter_does_not_duplicate_close_fields_with_two_fmt_layers() {
  let output_capture = CapturedWriter::default();
  let debug_capture = CapturedWriter::default();
  let subscriber = Registry::default()
    .with(
      tracing_subscriber::fmt::layer()
        .with_writer(output_capture)
        .with_ansi(false)
        .with_level(false)
        .with_target(false)
        .with_thread_ids(false)
        .without_time(),
    )
    .with(
      tracing_subscriber::fmt::layer()
        .fmt_fields(crate::DirectOtelDebugFields::default())
        .with_writer(debug_capture.clone())
        .with_ansi(false)
        .with_level(false)
        .with_target(false)
        .with_thread_ids(false)
        .without_time()
        .with_span_events(FmtSpan::CLOSE)
        .compact()
        .with_filter(filter_fn(otel::is_direct_otel_target)),
    );

  tracing::subscriber::with_default(subscriber, || {
    let span = crate::otel_info_span!(
      "clickhouse.query",
      clickhouse.retry_count = field::Empty,
      clickhouse.row_count = field::Empty,
      clickhouse.duration_ms = field::Empty,
      otel.status_code = field::Empty,
    );
    let _entered = span.enter();
    span.record("clickhouse.retry_count", 0);
    span.record("clickhouse.row_count", 0);
    span.record("clickhouse.duration_ms", 9);
    span.record("otel.status_code", "OK");
  });

  let output = debug_capture.contents();

  assert_eq!(output.matches("clickhouse.retry_count=0").count(), 1);
  assert_eq!(output.matches("clickhouse.row_count=0").count(), 1);
  assert_eq!(output.matches("clickhouse.duration_ms=9").count(), 1);
  assert_eq!(output.matches("otel.status_code=\"OK\"").count(), 1);
}

#[test]
fn otel_helper_macros_emit_the_direct_target() {
  let capture = TargetCaptureLayer::default();
  let subscriber = Registry::default().with(capture.clone());

  tracing::subscriber::with_default(subscriber, || {
    let span = crate::otel_info_span!("collector_span", otel.kind = "client");
    let _entered = span.enter();

    crate::otel_info!(message = "collector_event");
  });

  assert_eq!(capture.recorded_spans(), vec![OTEL_TARGET.to_string()]);
  assert_eq!(capture.recorded_events(), vec![OTEL_TARGET.to_string()]);
}

#[test]
fn otel_instrument_wraps_an_async_future_in_the_direct_target() {
  let capture = TargetCaptureLayer::default();
  let subscriber = Registry::default().with(capture.clone());
  let runtime = tokio::runtime::Runtime::new().unwrap();

  tracing::subscriber::with_default(subscriber, || {
    runtime.block_on(crate::otel_instrument!(
      async {
        crate::otel_info!(message = "inside_future");
      },
      tracing::Level::INFO,
      "collector_future",
      otel.kind = "internal"
    ));
  });

  assert_eq!(capture.recorded_spans(), vec![OTEL_TARGET.to_string()]);
  assert_eq!(capture.recorded_events(), vec![OTEL_TARGET.to_string()]);
}
