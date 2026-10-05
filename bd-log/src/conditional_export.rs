// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Bounded, opt-in tail capture for a local operation and its explicitly routed descendants.
//!
//! A typed context value carries capture membership through SDK parent contexts. Registry
//! extensions separately count tracing spans at creation, before the bridge lazily starts their SDK
//! spans. The final root outcome controls the group; individual statuses never decide retention.
//!
//! Completed data is buffered until the root decision and every registered member's close callback
//! are complete. An SDK span's synchronous end call supplies its group to the processor through a
//! scoped callback guard, not the ambient context. SDK IDs prevent nested ordinary ends from being
//! mistaken for capture members. Register only the wrapped processor to avoid bypassing the buffer.
//!
//! Limits discard whole groups, including future descendants, rather than exporting partial data.
//! Expiry is checked on capture activity and provider budget pressure. Idle buffers can otherwise
//! remain until the group closes or its provider shuts down. The downstream batch queue retains its
//! normal loss semantics. This does not collect library spans or remote-service spans.

#[cfg(test)]
#[path = "./conditional_export_test.rs"]
mod tests;

use opentelemetry::trace::{Span, SpanBuilder, SpanContext, SpanId, Status, TraceId, Tracer};
use opentelemetry::{Array, Context, KeyValue, StringValue, Value};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{Span as SdkSpan, SpanData, SpanProcessor};
use parking_lot::Mutex;
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::mem::size_of;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant, SystemTime};
use tracing::span::{Attributes, Id};
use tracing_subscriber::Registry;
use tracing_subscriber::layer::Context as LayerContext;
use tracing_subscriber::registry::LookupSpan as _;

type SpanKey = (TraceId, SpanId);
const MAX_GLOBAL_BYTES: usize = 64 * 1_024 * 1_024;
static DROPPED_CAPTURES: AtomicU64 = AtomicU64::new(0);
thread_local! {
  static END_CAPTURE: RefCell<Option<(SpanKey, CaptureGroup)>> = const { RefCell::new(None) };
}

/// Process-wide count of entire captures discarded because a resource limit or shutdown was
/// reached.
pub fn dropped_capture_count() -> u64 {
  DROPPED_CAPTURES.load(Ordering::Relaxed)
}

//
// TraceCaptureLimits
//

/// Limits on one local capture. Byte accounting conservatively includes typed value storage and
/// fixed span overhead; it is an estimate, not an allocator-level measurement.
/// Defaults: 1,024 spans, 4 MiB, and 60 seconds. Providers share a 64 MiB buffer budget across
/// their captures. Age is checked on membership changes, completed spans, and provider budget
/// pressure, without a background timer.
#[derive(Clone, Copy, Debug)]
pub struct TraceCaptureLimits {
  pub max_spans: usize,
  pub max_bytes: usize,
  pub max_duration: Duration,
}

impl Default for TraceCaptureLimits {
  fn default() -> Self {
    Self {
      max_spans: 1_024,
      max_bytes: 4 * 1_024 * 1_024,
      max_duration: Duration::from_secs(60),
    }
  }
}

//
// CaptureState
//

#[derive(Debug)]
struct CaptureState {
  started: Instant,
  live: usize,
  created: usize,
  decision: Option<bool>,
  dropped: bool,
  bytes: usize,
  spans: Vec<SpanData>,
}

//
// CaptureGroup
//

#[derive(Clone, Debug)]
pub struct CaptureGroup(Arc<CaptureInner>);

#[derive(Debug)]
struct CaptureInner {
  id: u64,
  state: Mutex<CaptureState>,
  limits: TraceCaptureLimits,
  control: ExportControl,
}

impl CaptureGroup {
  fn new(control: &ExportControl, limits: TraceCaptureLimits) -> Self {
    let id = {
      let mut state = control.0.lock();
      state.next_id += 1;
      state.next_id
    };
    let group = Self(Arc::new(CaptureInner {
      id,
      state: Mutex::new(CaptureState {
        started: Instant::now(),
        live: 1,
        created: 1,
        decision: None,
        dropped: false,
        bytes: 0,
        spans: Vec::new(),
      }),
      limits,
      control: control.clone(),
    }));
    control.0.lock().groups.insert(id, Arc::downgrade(&group.0));
    group
  }

  fn discard_buffer(&self, state: &mut CaptureState) {
    self.0.control.0.lock().bytes -= state.bytes;
    state.bytes = 0;
    state.spans = Vec::new();
  }

  fn drop_capture(&self, state: &mut CaptureState) {
    if !state.dropped {
      state.dropped = true;
      self.discard_buffer(state);
      DROPPED_CAPTURES.fetch_add(1, Ordering::Relaxed);
      log::debug!("discarding conditional trace: capture limit or provider shutdown");
    }
  }

  fn check_limits(&self, state: &mut CaptureState) {
    if state.dropped || state.decision == Some(false) {
      return;
    }
    if state.created > self.0.limits.max_spans
      || state.started.elapsed() >= self.0.limits.max_duration
      || self.0.control.0.lock().shutdown
    {
      self.drop_capture(state);
    }
  }

  fn add_member(&self) {
    let mut state = self.0.state.lock();
    state.live += 1;
    state.created += 1;
    self.check_limits(&mut state);
    log::trace!(
      "conditional trace {} registered member: live={}",
      self.0.id,
      state.live
    );
  }

  pub(crate) fn decide(&self, retain: bool) {
    let mut state = self.0.state.lock();
    state.decision.get_or_insert(retain);
    if state.decision == Some(false) {
      self.discard_buffer(&mut state);
    }
  }

  fn buffer(&self, span: SpanData) {
    let mut state = self.0.state.lock();
    self.check_limits(&mut state);
    if state.dropped || state.decision == Some(false) {
      return;
    }
    let bytes = estimated_span_bytes(&span);
    if bytes > self.0.limits.max_bytes.saturating_sub(state.bytes) {
      self.drop_capture(&mut state);
      return;
    }
    let mut control = self.0.control.0.lock();
    if bytes > MAX_GLOBAL_BYTES.saturating_sub(control.bytes) {
      drop(control);
      self.0.control.reclaim_expired(self.0.id);
      control = self.0.control.0.lock();
    }
    if control.shutdown || bytes > MAX_GLOBAL_BYTES.saturating_sub(control.bytes) {
      drop(control);
      self.drop_capture(&mut state);
      return;
    }
    control.bytes += bytes;
    state.bytes += bytes;
    state.spans.push(span);
  }

  pub(crate) fn close_member(&self) {
    let mut state = self.0.state.lock();
    state.live -= 1;
    self.check_limits(&mut state);
    log::trace!(
      "conditional trace {} closed member: live={}, decision={:?}",
      self.0.id,
      state.live,
      state.decision
    );
    if state.live == 0 {
      self.0.control.0.lock().bytes -= state.bytes;
      state.bytes = 0;
      let spans = if state.decision == Some(true) && !state.dropped {
        std::mem::take(&mut state.spans)
      } else {
        state.spans = Vec::new();
        Vec::new()
      };
      drop(state);
      log::trace!(
        "closing conditional trace capture: {} retained spans",
        spans.len()
      );
      self.0.control.export(spans);
    }
  }
}

impl Drop for CaptureInner {
  fn drop(&mut self) {
    let mut control = self.control.0.lock();
    control.bytes -= self.state.get_mut().bytes;
    control.groups.remove(&self.id);
  }
}

fn estimated_span_bytes(span: &SpanData) -> usize {
  let attribute_bytes = |attributes: &[KeyValue]| {
    attributes
      .iter()
      .map(|attribute| {
        64_usize
          .saturating_add(attribute.key.as_str().len())
          .saturating_add(value_bytes(&attribute.value))
      })
      .fold(0, usize::saturating_add)
  };
  let status_bytes = match &span.status {
    Status::Error { description } => description.len(),
    _ => 0,
  };
  512_usize
    .saturating_add(span.name.len())
    .saturating_add(status_bytes)
    .saturating_add(attribute_bytes(&span.attributes))
    .saturating_add(
      span
        .events
        .iter()
        .map(|event| {
          128_usize
            .saturating_add(event.name.len())
            .saturating_add(attribute_bytes(&event.attributes))
        })
        .fold(0, usize::saturating_add),
    )
    .saturating_add(
      span
        .links
        .iter()
        .map(|link| 128_usize.saturating_add(attribute_bytes(&link.attributes)))
        .fold(0, usize::saturating_add),
    )
}

fn value_bytes(value: &Value) -> usize {
  match value {
    Value::Bool(_) | Value::I64(_) | Value::F64(_) => 8,
    Value::String(value) => value.as_str().len(),
    Value::Array(Array::Bool(values)) => values.capacity(),
    Value::Array(Array::I64(values)) => values.capacity().saturating_mul(size_of::<i64>()),
    Value::Array(Array::F64(values)) => values.capacity().saturating_mul(size_of::<f64>()),
    Value::Array(Array::String(values)) => values.iter().map(|value| value.as_str().len()).fold(
      values.capacity().saturating_mul(size_of::<StringValue>()),
      usize::saturating_add,
    ),
    _ => usize::MAX,
  }
}

//
// CaptureEndGuard
//

/// SDK processors run synchronously inside end; scope the decision to that exact callback. The
/// guard restores any outer end during nested callbacks or unwinding and cannot cross threads.
struct CaptureEndGuard {
  previous: Option<(SpanKey, CaptureGroup)>,
  thread: PhantomData<Rc<()>>,
}

impl CaptureEndGuard {
  fn new(context: &SpanContext, group: &CaptureGroup) -> Self {
    let key = (context.trace_id(), context.span_id());
    Self {
      previous: END_CAPTURE.with(|slot| slot.replace(Some((key, group.clone())))),
      thread: PhantomData,
    }
  }
}

impl Drop for CaptureEndGuard {
  fn drop(&mut self) {
    END_CAPTURE.with(|slot| slot.replace(self.previous.take()));
  }
}

//
// ControlState
//

#[derive(Debug, Default)]
struct ControlState {
  groups: HashMap<u64, Weak<CaptureInner>>,
  processor: Option<Weak<dyn SpanProcessor>>,
  next_id: u64,
  bytes: usize,
  shutdown: bool,
}

//
// ExportControl
//

#[derive(Clone, Debug, Default)]
pub struct ExportControl(Arc<Mutex<ControlState>>);

impl ExportControl {
  fn reclaim_expired(&self, current_id: u64) {
    let groups: Vec<_> = self
      .0
      .lock()
      .groups
      .iter()
      .filter(|(id, _)| **id != current_id)
      .filter_map(|(_, group)| group.upgrade().map(CaptureGroup))
      .collect();
    // The caller holds its own group lock. Never wait for another group, and never acquire a
    // group lock while holding the provider lock: concurrent buffer admissions use the same path.
    for group in groups {
      if let Some(mut state) = group.0.state.try_lock()
        && !state.dropped
        && state.decision != Some(false)
        && state.live > 0
        && state.started.elapsed() >= group.0.limits.max_duration
      {
        group.drop_capture(&mut state);
      }
    }
  }

  pub(crate) fn tracer<T>(inner: T) -> ExportControlledTracer<T> {
    ExportControlledTracer { inner }
  }

  pub(crate) fn processor<P: SpanProcessor + 'static>(
    &self,
    inner: P,
  ) -> ExportControlledSpanProcessor<P> {
    let inner = Arc::new(inner);
    let erased: Arc<dyn SpanProcessor> = inner.clone();
    self.0.lock().processor = Some(Arc::downgrade(&erased));
    ExportControlledSpanProcessor {
      inner,
      control: self.clone(),
    }
  }

  fn export(&self, spans: Vec<SpanData>) {
    let processor = {
      let state = self.0.lock();
      if state.shutdown {
        None
      } else {
        state.processor.as_ref().and_then(Weak::upgrade)
      }
    };
    if let Some(processor) = processor {
      for span in spans {
        processor.on_end(span);
      }
    }
  }

  pub(crate) fn register_root(
    &self,
    span: &tracing::Span,
    limits: TraceCaptureLimits,
  ) -> Option<CaptureGroup> {
    span.with_subscriber(|(id, dispatch)| {
      let registry = dispatch.downcast_ref::<Registry>()?;
      let root = registry.span(id)?;
      let group = CaptureGroup::new(self, limits);
      root.extensions_mut().insert(group.clone());
      Some(group)
    })?
  }

  pub(crate) fn register_child(attrs: &Attributes<'_>, id: &Id, ctx: &LayerContext<'_, Registry>) {
    let parent = if attrs.is_contextual() {
      ctx.lookup_current()
    } else {
      attrs.parent().and_then(|id| ctx.span(id))
    };
    let group = parent.and_then(|parent| parent.extensions().get::<CaptureGroup>().cloned());
    if let Some(group) = group
      && let Some(span) = ctx.span(id)
    {
      group.add_member();
      span.extensions_mut().insert(group);
    }
  }
}

//
// ExportControlledTracer
//

pub struct ExportControlledTracer<T> {
  inner: T,
}

impl<T: Tracer> Tracer for ExportControlledTracer<T> {
  type Span = ExportControlledSpan<T::Span>;

  fn build_with_context(&self, builder: SpanBuilder, parent: &Context) -> Self::Span {
    let inner = self.inner.build_with_context(builder, parent);
    ExportControlledSpan {
      inner,
      group: parent.get::<CaptureGroup>().cloned(),
    }
  }
}

//
// ExportControlledSpan
//

pub struct ExportControlledSpan<S: Span> {
  inner: S,
  group: Option<CaptureGroup>,
}

impl<S: Span> ExportControlledSpan<S> {
  fn end_guard(&self) -> Option<CaptureEndGuard> {
    self
      .group
      .as_ref()
      .filter(|_| self.inner.is_recording())
      .map(|group| CaptureEndGuard::new(self.inner.span_context(), group))
  }
}

impl<S: Span> Span for ExportControlledSpan<S> {
  fn add_event_with_timestamp<T>(
    &mut self,
    name: T,
    timestamp: SystemTime,
    attributes: Vec<KeyValue>,
  ) where
    T: Into<Cow<'static, str>>,
  {
    self
      .inner
      .add_event_with_timestamp(name, timestamp, attributes);
  }

  fn span_context(&self) -> &SpanContext {
    self.inner.span_context()
  }

  fn is_recording(&self) -> bool {
    self.inner.is_recording()
  }

  fn set_attribute(&mut self, attribute: KeyValue) {
    self.inner.set_attribute(attribute);
  }

  fn set_status(&mut self, status: Status) {
    self.inner.set_status(status);
  }

  fn update_name<T>(&mut self, name: T)
  where
    T: Into<Cow<'static, str>>,
  {
    self.inner.update_name(name);
  }

  fn add_link(&mut self, context: SpanContext, attributes: Vec<KeyValue>) {
    self.inner.add_link(context, attributes);
  }

  fn end_with_timestamp(&mut self, timestamp: SystemTime) {
    let _guard = self.end_guard();
    self.inner.end_with_timestamp(timestamp);
    self.group.take();
  }
}

impl<S: Span> Drop for ExportControlledSpan<S> {
  fn drop(&mut self) {
    // Explicitly end before cleanup rather than letting the inner field end after this Drop body.
    // SDK end is idempotent, so a span explicitly ended earlier will not be processed twice.
    let _guard = self.end_guard();
    self.inner.end();
    self.group.take();
  }
}

//
// ExportControlledSpanProcessor
//

/// Buffers capture members before the downstream processor can enqueue or export them.
#[derive(Debug)]
pub struct ExportControlledSpanProcessor<P> {
  inner: Arc<P>,
  control: ExportControl,
}

impl<P: SpanProcessor + 'static> SpanProcessor for ExportControlledSpanProcessor<P> {
  fn on_start(&self, span: &mut SdkSpan, context: &Context) {
    self.inner.on_start(span, context);
  }

  fn on_end(&self, span: SpanData) {
    let group = END_CAPTURE.with(|slot| {
      slot
        .borrow()
        .as_ref()
        .filter(|(key, _)| *key == (span.span_context.trace_id(), span.span_context.span_id()))
        .map(|(_, group)| group.clone())
    });
    if let Some(group) = group {
      group.buffer(span);
    } else {
      self.inner.on_end(span);
    }
  }

  // Export policy changes only on_end. Preserve downstream flush, shutdown and resource handling;
  // those operations do not decide whether still-active candidate spans should be retained.
  fn force_flush(&self) -> OTelSdkResult {
    self.inner.force_flush()
  }

  fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
    let groups = {
      let mut state = self.control.0.lock();
      state.shutdown = true;
      state
        .groups
        .values()
        .filter_map(Weak::upgrade)
        .map(CaptureGroup)
        .collect::<Vec<_>>()
    };
    for group in groups {
      let mut state = group.0.state.lock();
      if state.live > 0 && state.decision != Some(false) {
        group.drop_capture(&mut state);
      }
    }
    self.inner.shutdown_with_timeout(timeout)
  }

  fn set_resource(&mut self, resource: &Resource) {
    // Resource setup precedes span creation. Remove our weak callback while mutating the sole
    // processor owner, then restore it for registry close callbacks.
    let mut state = self.control.0.lock();
    state.processor = None;
    Arc::get_mut(&mut self.inner)
      .expect("processor cannot be shared during resource setup")
      .set_resource(resource);
    let erased: Arc<dyn SpanProcessor> = self.inner.clone();
    state.processor = Some(Arc::downgrade(&erased));
  }
}
