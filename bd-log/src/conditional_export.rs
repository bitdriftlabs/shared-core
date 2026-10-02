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
//! are complete. The tracer and processor share SDK IDs only to route end callbacks, which cannot
//! rely on the ambient context. Register only the wrapped processor to avoid bypassing the buffer.
//!
//! Limits discard whole groups, including future descendants, rather than exporting partial data.
//! Expiry is checked on capture activity; idle groups retain only bounded storage until they close
//! or their provider shuts down. This does not collect library spans or remote-service spans.

#[cfg(test)]
#[path = "./conditional_export_test.rs"]
mod tests;

use opentelemetry::trace::{Span, SpanBuilder, SpanContext, SpanId, Status, TraceId, Tracer};
use opentelemetry::{Context, KeyValue};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{Span as SdkSpan, SpanData, SpanProcessor};
use parking_lot::Mutex;
use std::borrow::Cow;
use std::collections::HashMap;
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

/// Process-wide count of entire captures discarded because a resource limit or shutdown was
/// reached.
pub fn dropped_capture_count() -> u64 {
  DROPPED_CAPTURES.load(Ordering::Relaxed)
}

//
// TraceCaptureLimits
//

/// Limits on one local capture. Byte accounting conservatively includes serialized values and
/// fixed span overhead; it is an estimate, not an allocator-level measurement.
/// Defaults: 1,024 spans, 4 MiB, and 60 seconds. Providers share a 64 MiB buffer budget across
/// their captures. Age is checked on membership changes and completed spans, without a background
/// timer.
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
  started: Instant,
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
        live: 1,
        created: 1,
        decision: None,
        dropped: false,
        bytes: 0,
        spans: Vec::new(),
      }),
      started: Instant::now(),
      limits,
      control: control.clone(),
    }));
    control.0.lock().groups.insert(id, Arc::downgrade(&group.0));
    group
  }

  fn discard_buffer(&self, state: &mut CaptureState) {
    self.0.control.0.lock().bytes -= state.bytes;
    state.bytes = 0;
    state.spans.clear();
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
      || self.0.started.elapsed() >= self.0.limits.max_duration
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
    let mut control = self.0.control.0.lock();
    if bytes > self.0.limits.max_bytes.saturating_sub(state.bytes)
      || bytes > MAX_GLOBAL_BYTES.saturating_sub(control.bytes)
    {
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
        state.spans.clear();
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
      .map(|attribute| 64 + attribute.key.as_str().len() + attribute.value.to_string().len())
      .sum::<usize>()
  };
  let status_bytes = match &span.status {
    Status::Error { description } => description.len(),
    _ => 0,
  };
  512
    + span.name.len()
    + status_bytes
    + attribute_bytes(&span.attributes)
    + span
      .events
      .iter()
      .map(|event| 128 + event.name.len() + attribute_bytes(&event.attributes))
      .sum::<usize>()
    + span
      .links
      .iter()
      .map(|link| 128 + attribute_bytes(&link.attributes))
      .sum::<usize>()
}

//
// ControlState
//

#[derive(Debug, Default)]
struct ControlState {
  spans: HashMap<SpanKey, Weak<CaptureInner>>,
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
  pub(crate) fn tracer<T>(&self, inner: T) -> ExportControlledTracer<T> {
    ExportControlledTracer {
      inner,
      control: self.clone(),
    }
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
  control: ExportControl,
}

impl<T: Tracer> Tracer for ExportControlledTracer<T> {
  type Span = ExportControlledSpan<T::Span>;

  fn build_with_context(&self, builder: SpanBuilder, parent: &Context) -> Self::Span {
    let inner = self.inner.build_with_context(builder, parent);
    let key = parent
      .get::<CaptureGroup>()
      .filter(|_| inner.is_recording())
      .map(|group| {
        let context = inner.span_context();
        let key = (context.trace_id(), context.span_id());
        self
          .control
          .0
          .lock()
          .spans
          .insert(key, Arc::downgrade(&group.0));
        key
      });
    ExportControlledSpan {
      inner,
      key,
      group: parent.get::<CaptureGroup>().cloned(),
      control: self.control.clone(),
    }
  }
}

//
// ExportControlledSpan
//

pub struct ExportControlledSpan<S: Span> {
  inner: S,
  key: Option<SpanKey>,
  group: Option<CaptureGroup>,
  control: ExportControl,
}

impl<S: Span> ExportControlledSpan<S> {
  fn remove_policy(&mut self) {
    // Normally on_end already consumed the entry. This also cleans up when provider shutdown
    // prevents the SDK from calling processors, and taking the key makes repeated cleanup harmless.
    if let Some(key) = self.key.take() {
      self.control.0.lock().spans.remove(&key);
    }
    self.group.take();
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
    // Ending the SDK span synchronously invokes on_end. Removing membership first would make the
    // processor mistake this span for an ordinary span and export it by default.
    self.inner.end_with_timestamp(timestamp);
    self.remove_policy();
  }
}

impl<S: Span> Drop for ExportControlledSpan<S> {
  fn drop(&mut self) {
    // Explicitly end before cleanup rather than letting the inner field end after this Drop body.
    // SDK end is idempotent, so a span explicitly ended earlier will not be processed twice.
    self.inner.end();
    self.remove_policy();
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
    let context = &span.span_context;
    let group = self
      .control
      .0
      .lock()
      .spans
      .remove(&(context.trace_id(), context.span_id()));
    if let Some(group) = group {
      if let Some(group) = group.upgrade() {
        CaptureGroup(group).buffer(span);
      }
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
      group.drop_capture(&mut group.0.state.lock());
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
