// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Assign fixture ownership without replacing the subscriber or changing OTEL parentage.
//!
//! Tokio scopes ownership during polling and cancellation. Registry extensions carry that ownership
//! through instrumented descendants, including work spawned without a task-local scope. The SDK
//! bridge can start a span much later, so its builder also needs a token identifying the original
//! owner. The actual tracing-to-OTEL conversion and conditional export remain production code.

use super::{CaptureInner, TraceCaptureHandle};
use crate::conditional_export::{
  CaptureGroup,
  ExportControl,
  ExportControlledSpan,
  WeakCaptureGroup,
};
use crate::{LogConfig, RegistryLayer, SwapLogger, otel};
use opentelemetry::trace::{SpanBuilder, Tracer};
use opentelemetry::{Context as OtelContext, KeyValue, Value};
use opentelemetry_sdk::trace::Span as SdkSpan;
use parking_lot::Mutex;
use std::any::TypeId;
use std::borrow::Cow;
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use tracing::Event;
use tracing::span::{Attributes, Id, Record};
use tracing_subscriber::layer::{Context as LayerContext, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, Registry};

tokio::task_local! {
  static CURRENT_CAPTURE: ScopedWork;
  static BUILDING_SPAN: i64;
}

// Registry spans retain their owner. This lookup only bridges the SDK's delayed builder activation;
// it must not independently keep providers alive after their tracing spans have closed.
fn routes() -> &'static Mutex<HashMap<i64, Weak<CaptureInner>>> {
  static ROUTES: OnceLock<Mutex<HashMap<i64, Weak<CaptureInner>>>> = OnceLock::new();
  ROUTES.get_or_init(Mutex::default)
}

/// Install the test logger once. Captures own providers, not the global registry or its filters.
pub fn initialize() {
  static INITIALIZED: OnceLock<()> = OnceLock::new();
  INITIALIZED.get_or_init(|| {
    let config = LogConfig::default();
    let layer = tracing_opentelemetry::layer()
      .with_tracer(CaptureTracer)
      .with_level(true)
      .with_location(false)
      .with_threads(false)
      .with_target(false);
    let layers: Vec<RegistryLayer> = vec![
      crate::build_output_layer(&config),
      Box::new(CaptureLayer {
        inner: crate::box_direct_otel_layer(layer),
        control: ExportControl::default(),
      }),
    ];
    Registry::default()
      .with(layers)
      .with(EnvFilter::new(otel::global_filter_rules(
        &config.log_filter,
        true,
      )))
      .try_init()
      .expect("test logging must be initialized before installing another global subscriber");
    SwapLogger::update_log_max_level(&config.log_filter);
  });
}

//
// ScopedWork
//

// Count scopes when constructed, not first polled: finish() must also reject unpolled futures.
// Tokio drops the inner future in its ownership scope before releasing this task-local value.
struct ScopedWork(TraceCaptureHandle);

impl ScopedWork {
  fn new(handle: TraceCaptureHandle) -> Self {
    {
      let mut state = handle.0.state.lock();
      assert!(!state.finished, "trace capture has already finished");
      state.scopes += 1;
    }
    Self(handle)
  }
}

impl Drop for ScopedWork {
  fn drop(&mut self) {
    self.0.0.state.lock().scopes -= 1;
  }
}

pub(super) fn in_scope<R>(handle: &TraceCaptureHandle, work: impl FnOnce() -> R) -> R {
  CURRENT_CAPTURE.sync_scope(ScopedWork::new(handle.clone()), work)
}

pub(super) fn scope<F: Future>(
  handle: TraceCaptureHandle,
  future: F,
) -> impl Future<Output = F::Output> {
  CURRENT_CAPTURE.scope(ScopedWork::new(handle), future)
}

//
// CaptureTracer
//

// The bridge lazily starts SDK spans, sometimes through OpenTelemetrySpanExt rather than a layer
// callback. Bind the provider at builder creation, not whichever task later starts it. The bridge
// exposes no typed builder extensions, so carry a private token and strip it before SDK sampling,
// attribute limits, or export can observe it.
struct CaptureTracer;

const CAPTURE_TOKEN: &str = "bd_log.test.capture";

impl Tracer for CaptureTracer {
  type Span = ExportControlledSpan<SdkSpan>;

  fn span_builder<T>(&self, name: T) -> SpanBuilder
  where
    T: Into<Cow<'static, str>>,
  {
    let token = BUILDING_SPAN
      .try_with(|token| *token)
      .expect("captured span builder must have an owner");
    SpanBuilder::from_name(name).with_attributes([KeyValue::new(CAPTURE_TOKEN, token)])
  }

  fn build_with_context(&self, mut builder: SpanBuilder, parent: &OtelContext) -> Self::Span {
    let token = builder
      .attributes
      .as_mut()
      .expect("captured span must have a routing token")
      .remove(0);
    assert_eq!(token.key.as_str(), CAPTURE_TOKEN);
    let Value::I64(token) = token.value else {
      panic!("invalid trace capture routing token")
    };
    let capture = routes()
      .lock()
      .get(&token)
      .and_then(Weak::upgrade)
      .expect("captured span provider must outlive its tracing span");
    ExportControl::tracer(capture.tracer.clone()).build_with_context(builder, parent)
  }
}

//
// CaptureLayer
//

// Ordinary tracing spans carry ownership too, so non-OTEL instrumentation does not break
// inheritance.
#[derive(Clone)]
struct OwnedSpan {
  capture: TraceCaptureHandle,
  token: Option<i64>,
}

struct CaptureLayer {
  inner: RegistryLayer,
  control: ExportControl,
}

fn owner(ctx: &LayerContext<'_, Registry>, id: &Id) -> Option<OwnedSpan> {
  ctx
    .span(id)
    .and_then(|span| span.extensions().get::<OwnedSpan>().cloned())
}

impl Layer<Registry> for CaptureLayer {
  fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: LayerContext<'_, Registry>) {
    // Parentless conditional roots still belong to an entered, instrumented task's fixture.
    // This lookup only assigns ownership; the original attributes keep tracing/OTEL parentage.
    let parent = attrs
      .parent()
      .map_or_else(|| ctx.lookup_current(), |parent| ctx.span(parent));
    // An explicit fixture scope wins over the parent: two captures may participate in one trace.
    // Ownership is stored separately from SDK parent contexts, which remote reparenting can
    // replace.
    let capture = CURRENT_CAPTURE
      .try_with(|work| work.0.clone())
      .ok()
      .or_else(|| {
        parent.and_then(|span| {
          span
            .extensions()
            .get::<OwnedSpan>()
            .map(|owner| owner.capture.clone())
        })
      });
    let Some(capture) = capture else { return };
    capture.assert_open();
    capture
      .0
      .state
      .lock()
      .live
      .insert(id.clone(), attrs.metadata().name());
    let token = if otel::is_direct_otel_target(attrs.metadata()) {
      static NEXT_ID: AtomicI64 = AtomicI64::new(1);
      let token = NEXT_ID.fetch_add(1, Ordering::Relaxed);
      assert!(
        routes()
          .lock()
          .insert(token, Arc::downgrade(&capture.0))
          .is_none()
      );
      Some(token)
    } else {
      None
    };
    let span = ctx.span(id).expect("new span must be registered");
    {
      let mut extensions = span.extensions_mut();
      extensions.insert(OwnedSpan {
        capture: capture.clone(),
        token,
      });
      extensions.insert(capture.0.control.clone());
    }
    drop(span);
    if let Some(token) = token {
      ExportControl::register_child(attrs, id, &ctx);
      if let Some(group) = ctx
        .span(id)
        .and_then(|span| span.extensions().get::<CaptureGroup>().cloned())
      {
        let mut state = capture.0.state.lock();
        state.groups.retain(WeakCaptureGroup::is_pending);
        state.groups.push(group.downgrade());
      }
      BUILDING_SPAN.sync_scope(token, || self.inner.on_new_span(attrs, id, ctx));
    }
  }

  fn on_record(&self, id: &Id, values: &Record<'_>, ctx: LayerContext<'_, Registry>) {
    self.inner.on_record(id, values, ctx);
  }

  fn on_event(&self, event: &Event<'_>, ctx: LayerContext<'_, Registry>) {
    self.inner.on_event(event, ctx);
  }

  fn on_enter(&self, id: &Id, ctx: LayerContext<'_, Registry>) {
    self.inner.on_enter(id, ctx);
  }

  fn on_exit(&self, id: &Id, ctx: LayerContext<'_, Registry>) {
    self.inner.on_exit(id, ctx);
  }

  fn on_follows_from(&self, id: &Id, follows: &Id, ctx: LayerContext<'_, Registry>) {
    self.inner.on_follows_from(id, follows, ctx);
  }

  fn on_close(&self, id: Id, ctx: LayerContext<'_, Registry>) {
    let Some(owner) = owner(&ctx, &id) else {
      return;
    };
    let group = ctx
      .span(&id)
      .and_then(|span| span.extensions().get::<CaptureGroup>().cloned());
    // Closing may activate a lazy SDK span. Keep its route until SDK end has buffered/exported it,
    // then close conditional membership so the root cannot release an incomplete recording.
    self.inner.on_close(id.clone(), ctx);
    if let Some(group) = group {
      group.close_member();
    }
    if let Some(token) = owner.token {
      routes().lock().remove(&token);
    }
    owner.capture.0.state.lock().live.remove(&id);
  }

  unsafe fn downcast_raw(&self, id: TypeId) -> Option<*const ()> {
    // Conditional macros discover ExportControl through the subscriber; register_root then selects
    // the span's fixture-local control. Forward other downcasts for the bridge's span-context APIs.
    if id == TypeId::of::<ExportControl>() {
      Some(std::ptr::from_ref(&self.control).cast())
    } else if id == TypeId::of::<Self>() {
      Some(std::ptr::from_ref(self).cast())
    } else {
      unsafe { self.inner.downcast_raw(id) }
    }
  }
}
