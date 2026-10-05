// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use super::{
  CaptureEndGuard,
  CaptureGroup,
  END_CAPTURE,
  ExportControl,
  MAX_GLOBAL_BYTES,
  TraceCaptureLimits,
  dropped_capture_count,
  value_bytes,
};
use crate::test::TestExporter;
use opentelemetry::trace::{Span as _, Status, Tracer as _, TracerProvider as _};
use opentelemetry::{Array, Context, Key, KeyValue, StringValue, Value};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::runtime::{RuntimeChannel, Tokio, TokioCurrentThread};
use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor;
use opentelemetry_sdk::trace::{
  BatchConfigBuilder,
  Sampler,
  SdkTracerProvider,
  Span,
  SpanData,
  SpanExporter,
  SpanProcessor,
};
use parking_lot::Mutex;
use std::mem::size_of;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

//
// ProcessorProbe
//

#[derive(Debug, Default)]
struct ProcessorProbe {
  starts: AtomicUsize,
  ended: Mutex<Vec<SpanData>>,
  flushes: AtomicUsize,
  shutdowns: AtomicUsize,
  resource: Mutex<Option<Resource>>,
}

//
// ProbeProcessor
//

#[derive(Debug)]
struct ProbeProcessor(Arc<ProcessorProbe>);

impl SpanProcessor for ProbeProcessor {
  fn on_start(&self, _: &mut Span, _: &Context) {
    self.0.starts.fetch_add(1, Ordering::Relaxed);
  }

  fn on_end(&self, span: SpanData) {
    self.0.ended.lock().push(span);
  }

  fn force_flush(&self) -> OTelSdkResult {
    self.0.flushes.fetch_add(1, Ordering::Relaxed);
    Ok(())
  }

  fn shutdown_with_timeout(&self, _: Duration) -> OTelSdkResult {
    self.0.shutdowns.fetch_add(1, Ordering::Relaxed);
    Ok(())
  }

  fn set_resource(&mut self, resource: &Resource) {
    *self.0.resource.lock() = Some(resource.clone());
  }
}

#[test]
fn conditional_export_filters_before_downstream_processor_and_delegates_lifecycle() {
  let control = ExportControl::default();
  let probe = Arc::new(ProcessorProbe::default());
  let provider = SdkTracerProvider::builder()
    .with_sampler(Sampler::AlwaysOn)
    .with_max_attributes_per_span(0)
    .with_resource(
      Resource::builder_empty()
        .with_attribute(KeyValue::new("service.name", "test"))
        .build(),
    )
    .with_span_processor(control.processor(ProbeProcessor(probe.clone())))
    .build();
  let tracer = ExportControl::tracer(provider.tracer("test"));

  drop(tracer.start("ordinary"));
  let success = CaptureGroup::new(&control, TraceCaptureLimits::default());
  drop(tracer.start_with_context("success", &Context::new().with_value(success.clone())));
  success.decide(false);
  success.close_member();
  assert_eq!(success.0.state.lock().spans, []);
  let group = CaptureGroup::new(&control, TraceCaptureLimits::default());
  let mut failure = tracer.start_with_context("failure", &Context::new().with_value(group.clone()));
  failure.set_attribute(KeyValue::new("more", "attributes"));
  failure.set_status(Status::error("final error"));
  failure.end();
  failure.end();
  drop(failure);
  group.decide(true);
  group.close_member();
  provider.force_flush().unwrap();
  assert_eq!(control.0.lock().bytes, 0);

  let ended = probe.ended.lock();
  assert_eq!(ended.len(), 2);
  assert_eq!(ended[0].name, "ordinary");
  assert_eq!(ended[1].status, Status::error("final error"));
  assert_eq!(ended[1].attributes, []);
  drop(ended);
  assert_eq!(probe.starts.load(Ordering::Relaxed), 3);
  provider.force_flush().unwrap();
  assert_eq!(probe.flushes.load(Ordering::Relaxed), 2);
  assert_eq!(
    probe
      .resource
      .lock()
      .as_ref()
      .unwrap()
      .get(&Key::new("service.name")),
    Some(Value::from("test"))
  );
  provider.shutdown().unwrap();
  assert_eq!(probe.shutdowns.load(Ordering::Relaxed), 1);
}

#[test]
fn conditional_export_cleans_up_shutdown_and_nonrecording_spans() {
  for sampler in [Sampler::AlwaysOn, Sampler::AlwaysOff] {
    let control = ExportControl::default();
    let probe = Arc::new(ProcessorProbe::default());
    let provider = SdkTracerProvider::builder()
      .with_sampler(sampler.clone())
      .with_span_processor(control.processor(ProbeProcessor(probe.clone())))
      .build();
    let tracer = ExportControl::tracer(provider.tracer("test"));
    let group = CaptureGroup::new(&control, TraceCaptureLimits::default());
    let span = tracer.start_with_context("pending", &Context::new().with_value(group.clone()));
    assert_eq!(span.is_recording(), !matches!(sampler, Sampler::AlwaysOff));
    provider.shutdown().unwrap();
    group.decide(true);
    drop(span);
    group.close_member();
    assert!(END_CAPTURE.with(|slot| slot.borrow().is_none()));
    assert_eq!(control.0.lock().bytes, 0);
    assert!(probe.ended.lock().is_empty());
  }
}

#[test]
fn conditional_export_limits_discard_whole_groups_and_future_members() {
  let limits = [
    TraceCaptureLimits {
      max_spans: 1,
      ..TraceCaptureLimits::default()
    },
    TraceCaptureLimits {
      max_bytes: 1,
      ..TraceCaptureLimits::default()
    },
    TraceCaptureLimits {
      max_duration: Duration::ZERO,
      ..TraceCaptureLimits::default()
    },
    TraceCaptureLimits::default(),
  ];
  for (index, limits) in limits.into_iter().enumerate() {
    let control = ExportControl::default();
    let probe = Arc::new(ProcessorProbe::default());
    let provider = SdkTracerProvider::builder()
      .with_span_processor(control.processor(ProbeProcessor(probe.clone())))
      .build();
    let tracer = ExportControl::tracer(provider.tracer("test"));
    let group = CaptureGroup::new(&control, limits);
    let parent = Context::new().with_value(group.clone());
    let before = dropped_capture_count();
    if index == 3 {
      // Reserve the global budget without allocating a large synthetic payload.
      control.0.lock().bytes = MAX_GLOBAL_BYTES;
    }
    group.add_member();
    drop(tracer.start_with_context("first_child", &parent));
    group.close_member();
    group.add_member();
    drop(tracer.start_with_context("late_child", &parent));
    group.close_member();
    group.decide(true);
    drop(tracer.start_with_context("root", &parent));
    group.close_member();
    assert!(group.0.state.lock().dropped);
    assert!(dropped_capture_count() > before);
    assert_eq!(probe.ended.lock().as_slice(), []);
    assert_eq!(
      control.0.lock().bytes,
      if index == 3 { MAX_GLOBAL_BYTES } else { 0 }
    );
    control.0.lock().bytes = 0;
    drop(parent);
    drop(group);
    assert!(control.0.lock().groups.is_empty());
    provider.shutdown().unwrap();
  }
}

#[test]
fn conditional_export_shutdown_clears_buffers_even_without_active_sdk_spans() {
  let control = ExportControl::default();
  let probe = Arc::new(ProcessorProbe::default());
  let provider = SdkTracerProvider::builder()
    .with_span_processor(control.processor(ProbeProcessor(probe.clone())))
    .build();
  let tracer = ExportControl::tracer(provider.tracer("test"));
  let group = CaptureGroup::new(&control, TraceCaptureLimits::default());
  drop(tracer.start_with_context("completed_child", &Context::new().with_value(group.clone())));
  assert!(END_CAPTURE.with(|slot| slot.borrow().is_none()));
  assert!(control.0.lock().bytes > 0);
  provider.force_flush().unwrap();
  assert_eq!(probe.ended.lock().as_slice(), []);
  provider.shutdown().unwrap();
  assert_eq!(control.0.lock().bytes, 0);
  assert_eq!(group.0.state.lock().spans, []);
  assert_eq!(group.0.state.lock().spans.capacity(), 0);
  group.decide(true);
  group.close_member();
  assert_eq!(probe.ended.lock().as_slice(), []);
}

#[test]
fn conditional_export_discard_releases_capacity_with_surviving_members() {
  let control = ExportControl::default();
  let probe = Arc::new(ProcessorProbe::default());
  let provider = SdkTracerProvider::builder()
    .with_span_processor(control.processor(ProbeProcessor(probe.clone())))
    .build();
  let tracer = ExportControl::tracer(provider.tracer("test"));
  let group = CaptureGroup::new(&control, TraceCaptureLimits::default());
  let parent = Context::new().with_value(group.clone());
  drop(tracer.start_with_context("completed_child", &parent));
  assert!(group.0.state.lock().spans.capacity() > 0);
  group.decide(false);
  assert_eq!(group.0.state.lock().spans.capacity(), 0);
  assert_eq!(control.0.lock().bytes, 0);
  group.close_member();
  provider.shutdown().unwrap();
  assert!(!group.0.state.lock().dropped);
  assert_eq!(probe.ended.lock().as_slice(), []);
}

#[test]
fn conditional_export_shutdown_does_not_count_completed_or_successful_captures() {
  let control = ExportControl::default();
  let probe = Arc::new(ProcessorProbe::default());
  let provider = SdkTracerProvider::builder()
    .with_span_processor(control.processor(ProbeProcessor(probe.clone())))
    .build();
  let tracer = ExportControl::tracer(provider.tracer("test"));
  let completed = CaptureGroup::new(&control, TraceCaptureLimits::default());
  drop(tracer.start_with_context("retained", &Context::new().with_value(completed.clone())));
  completed.decide(true);
  completed.close_member();
  let successful = CaptureGroup::new(&control, TraceCaptureLimits::default());
  successful.decide(false);
  let unfinished = CaptureGroup::new(&control, TraceCaptureLimits::default());
  provider.shutdown().unwrap();
  assert!(!completed.0.state.lock().dropped);
  assert!(!successful.0.state.lock().dropped);
  assert!(unfinished.0.state.lock().dropped);
  assert_eq!(probe.ended.lock().len(), 1);
  successful.close_member();
  unfinished.close_member();
}

#[test]
fn conditional_export_ordinary_spans_do_not_lock_capture_control() {
  let control = ExportControl::default();
  let probe = Arc::new(ProcessorProbe::default());
  let provider = SdkTracerProvider::builder()
    .with_span_processor(control.processor(ProbeProcessor(probe.clone())))
    .build();
  let tracer = ExportControl::tracer(provider.tracer("test"));
  let group = CaptureGroup::new(&control, TraceCaptureLimits::default());
  let captured = tracer.start_with_context("captured", &Context::new().with_value(group.clone()));
  let control_lock = control.0.lock();
  drop(tracer.start("ordinary"));
  assert_eq!(probe.ended.lock().len(), 1);
  drop(control_lock);
  drop(captured);
  group.decide(false);
  group.close_member();
  assert!(END_CAPTURE.with(|slot| slot.borrow().is_none()));
  provider.shutdown().unwrap();
}

#[test]
fn conditional_export_end_guard_restores_outer_callback_and_ignores_other_spans() {
  let control = ExportControl::default();
  let probe = Arc::new(ProcessorProbe::default());
  let provider = SdkTracerProvider::builder()
    .with_span_processor(control.processor(ProbeProcessor(probe.clone())))
    .build();
  let tracer = ExportControl::tracer(provider.tracer("test"));
  let group = CaptureGroup::new(&control, TraceCaptureLimits::default());
  let captured = tracer.start_with_context("captured", &Context::new().with_value(group.clone()));
  let outer = CaptureEndGuard::new(captured.span_context(), &group);
  drop(tracer.start("nested_ordinary"));
  assert_eq!(probe.ended.lock().len(), 1);
  drop(captured);
  assert!(END_CAPTURE.with(|slot| slot.borrow().is_some()));
  drop(outer);
  assert!(END_CAPTURE.with(|slot| slot.borrow().is_none()));
  group.decide(true);
  group.close_member();
  assert_eq!(probe.ended.lock().len(), 2);
  provider.shutdown().unwrap();
}

#[test]
fn conditional_export_value_accounting_uses_typed_storage_sizes() {
  assert_eq!(value_bytes(&Value::Bool(true)), 8);
  assert_eq!(value_bytes(&Value::I64(i64::MIN)), 8);
  assert_eq!(value_bytes(&Value::F64(f64::INFINITY)), 8);
  assert_eq!(value_bytes(&Value::from("a string")), 8);
  assert_eq!(value_bytes(&Value::Array(Array::I64(vec![1, 2, 3]))), 24);
  assert_eq!(value_bytes(&Value::Array(Array::F64(vec![0.0, 1.0]))), 16);
  assert_eq!(
    value_bytes(&Value::Array(Array::Bool(vec![true, false]))),
    2
  );
  let strings = vec!["one".into(), "two".into()];
  let expected = strings.capacity() * size_of::<StringValue>() + 6;
  assert_eq!(value_bytes(&Value::Array(Array::String(strings))), expected);
}

#[test]
fn conditional_export_budget_pressure_reclaims_idle_expired_groups() {
  for busy in [false, true] {
    let control = ExportControl::default();
    let probe = Arc::new(ProcessorProbe::default());
    let provider = SdkTracerProvider::builder()
      .with_span_processor(control.processor(ProbeProcessor(probe.clone())))
      .build();
    let tracer = ExportControl::tracer(provider.tracer("test"));
    let expired = CaptureGroup::new(&control, TraceCaptureLimits::default());
    let expired_parent = Context::new().with_value(expired.clone());
    drop(tracer.start_with_context("expired_child", &expired_parent));
    {
      let mut state = expired.0.state.lock();
      state.started = Instant::now().checked_sub(Duration::from_secs(61)).unwrap();
      // Account for a full provider without allocating 64 MiB of synthetic attributes.
      state.bytes = MAX_GLOBAL_BYTES;
      control.0.lock().bytes = MAX_GLOBAL_BYTES;
    }
    let busy_lock = busy.then(|| expired.0.state.lock());
    let fresh = CaptureGroup::new(&control, TraceCaptureLimits::default());
    drop(tracer.start_with_context("fresh_failure", &Context::new().with_value(fresh.clone())));
    fresh.decide(true);
    fresh.close_member();
    assert_eq!(fresh.0.state.lock().dropped, busy);
    assert_eq!(probe.ended.lock().len(), usize::from(!busy));
    drop(busy_lock);
    if busy {
      // Once idle, the same expired capture can be reclaimed by the next admission.
      let retry = CaptureGroup::new(&control, TraceCaptureLimits::default());
      drop(tracer.start_with_context("fresh_retry", &Context::new().with_value(retry.clone())));
      retry.decide(true);
      retry.close_member();
    }
    assert!(expired.0.state.lock().dropped);
    assert_eq!(expired.0.state.lock().spans.capacity(), 0);
    assert_eq!(control.0.lock().bytes, 0);
    assert_eq!(probe.ended.lock().len(), 1);
    expired.decide(true);
    drop(tracer.start_with_context("late_expired_child", &expired_parent));
    expired.close_member();
    assert_eq!(probe.ended.lock().len(), 1);
    provider.shutdown().unwrap();
  }
}

//
// GatedExporter
//

#[derive(Debug)]
struct GatedExporter {
  exporter: TestExporter,
  gate: Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
  completed: mpsc::UnboundedSender<()>,
  shutdowns: Arc<AtomicUsize>,
}

impl SpanExporter for GatedExporter {
  async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
    let gate = self.gate.lock().take();
    if let Some((started, release)) = gate {
      started.send(()).unwrap();
      release.await.unwrap();
    }
    self.exporter.export(batch).await?;
    self.completed.send(()).unwrap();
    Ok(())
  }

  fn shutdown_with_timeout(&self, _: Duration) -> OTelSdkResult {
    self.shutdowns.fetch_add(1, Ordering::Relaxed);
    Ok(())
  }
}

async fn saturated_batch_captures<R: RuntimeChannel>(runtime: R) {
  let control = ExportControl::default();
  let exporter = TestExporter::default();
  let (started, exporting) = oneshot::channel();
  let (release, blocked) = oneshot::channel();
  let (completed, mut exported) = mpsc::unbounded_channel();
  let shutdowns = Arc::new(AtomicUsize::new(0));
  let config = BatchConfigBuilder::default()
    .with_max_queue_size(2)
    .with_max_export_batch_size(1)
    .with_scheduled_delay(Duration::from_secs(3_600))
    .build();
  let processor = BatchSpanProcessor::builder(
    GatedExporter {
      exporter: exporter.clone(),
      gate: Mutex::new(Some((started, blocked))),
      completed,
      shutdowns: shutdowns.clone(),
    },
    runtime,
  )
  .with_batch_config(config)
  .build();
  let provider = SdkTracerProvider::builder()
    .with_span_processor(control.processor(processor))
    .build();
  let tracer = ExportControl::tracer(provider.tracer("saturated-captures"));
  drop(tracer.start("ordinary_first"));
  exporting.await.unwrap();
  // The worker is inside export and cannot drain its queue until the gate is released.
  let successful = CaptureGroup::new(&control, TraceCaptureLimits::default());
  for _ in 0 .. 32 {
    drop(tracer.start_with_context("suppressed", &Context::new().with_value(successful.clone())));
  }
  successful.decide(false);
  successful.close_member();
  drop(tracer.start("ordinary_queued_first"));
  drop(tracer.start("ordinary_queued_second"));
  let retained = CaptureGroup::new(&control, TraceCaptureLimits::default());
  for _ in 0 .. 32 {
    drop(tracer.start_with_context(
      "retained_burst",
      &Context::new().with_value(retained.clone()),
    ));
  }
  retained.decide(true);
  retained.close_member();
  assert_eq!(control.0.lock().bytes, 0);
  release.send(()).unwrap();
  for _ in 0 .. 3 {
    exported.recv().await.unwrap();
  }
  assert_eq!(exporter.exported_spans().len(), 3);
  assert!(
    exporter
      .exported_spans()
      .iter()
      .all(|span| span.name.starts_with("ordinary_"))
  );
  drop(tracer.start("ordinary_after_saturation"));
  exported.recv().await.unwrap();
  provider.force_flush().unwrap();
  let unfinished = CaptureGroup::new(&control, TraceCaptureLimits::default());
  drop(tracer.start_with_context("unfinished", &Context::new().with_value(unfinished.clone())));
  provider.shutdown().unwrap();
  assert!(unfinished.0.state.lock().dropped);
  assert_eq!(unfinished.0.state.lock().spans.capacity(), 0);
  assert_eq!(control.0.lock().bytes, 0);
  assert_eq!(shutdowns.load(Ordering::Relaxed), 1);
  unfinished.decide(true);
  unfinished.close_member();
  assert_eq!(exporter.exported_spans().len(), 4);
}

#[tokio::test]
async fn conditional_export_batch_saturation_current_thread() {
  saturated_batch_captures(TokioCurrentThread).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conditional_export_batch_saturation_multi_thread() {
  saturated_batch_captures(Tokio).await;
}
