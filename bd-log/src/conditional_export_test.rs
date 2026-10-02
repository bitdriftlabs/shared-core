// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use super::{
  CaptureGroup,
  ExportControl,
  MAX_GLOBAL_BYTES,
  TraceCaptureLimits,
  dropped_capture_count,
};
use opentelemetry::trace::{Span as _, Status, Tracer as _, TracerProvider as _};
use opentelemetry::{Context, Key, KeyValue, Value};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider, Span, SpanData, SpanProcessor};
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

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
  let tracer = control.tracer(provider.tracer("test"));

  drop(tracer.start("ordinary"));
  let success = CaptureGroup::new(&control, TraceCaptureLimits::default());
  drop(tracer.start_with_context("success", &Context::new().with_value(success.clone())));
  success.decide(false);
  success.close_member();
  assert!(control.0.lock().spans.is_empty());
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
  assert!(control.0.lock().spans.is_empty());

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
      .with_sampler(sampler)
      .with_span_processor(control.processor(ProbeProcessor(probe.clone())))
      .build();
    let tracer = control.tracer(provider.tracer("test"));
    let group = CaptureGroup::new(&control, TraceCaptureLimits::default());
    let span = tracer.start_with_context("pending", &Context::new().with_value(group.clone()));
    assert_eq!(
      control.0.lock().spans.len(),
      usize::from(span.is_recording())
    );
    provider.shutdown().unwrap();
    group.decide(true);
    drop(span);
    group.close_member();
    assert!(control.0.lock().spans.is_empty());
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
    let tracer = control.tracer(provider.tracer("test"));
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
    assert_eq!(dropped_capture_count(), before + 1);
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
  let tracer = control.tracer(provider.tracer("test"));
  let group = CaptureGroup::new(&control, TraceCaptureLimits::default());
  drop(tracer.start_with_context("completed_child", &Context::new().with_value(group.clone())));
  assert!(control.0.lock().spans.is_empty());
  assert!(control.0.lock().bytes > 0);
  provider.force_flush().unwrap();
  assert_eq!(probe.ended.lock().as_slice(), []);
  provider.shutdown().unwrap();
  assert_eq!(control.0.lock().bytes, 0);
  assert_eq!(group.0.state.lock().spans, []);
  group.decide(true);
  group.close_member();
  assert_eq!(probe.ended.lock().as_slice(), []);
}
