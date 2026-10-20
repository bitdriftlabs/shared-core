// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Independent trace captures share one permanent registry, not subscriber configuration.
//! Scope root work with [`TraceCapture::scope`] or [`TraceCapture::in_scope`]. Enabled instrumented
//! descendants inherit ownership without changing their OTEL parentage. Join background work and
//! release spans before calling [`TraceCapture::finish`].
//!
//! ```rust
//! let capture = bd_log::test::TraceCapture::new();
//! capture.in_scope(|| drop(bd_log::otel_info_span!("operation")));
//! let spans = capture.finish();
//! assert_eq!(spans.len(), 1);
//! ```
//!
//! Async work uses `capture.scope(future).await`. Use a cloned [`TraceCaptureHandle`] at service
//! boundaries or for detached roots; ordinary Tokio spawning does not inherit capture scope.
//! Filtered-out spans cannot carry ownership; use `scope` or `spawn` when filters may disable them.
//! Each capture owns its SDK provider, including attribute limits and optional batching, so
//! concurrent tests can exercise different configurations without changing global logging.
//!
//! A permanent registry avoids closing spans against another fixture's subscriber when library
//! tasks move between runtime workers. Capture-local providers still exercise the real SDK and
//! conditional-export pipeline; ownership routing must not substitute for that code under test.

mod routing;

#[cfg(test)]
#[path = "./test_test.rs"]
mod tests;

use crate::conditional_export::{ExportControl, WeakCaptureGroup, estimated_span_bytes};
use opentelemetry::trace::{TraceContextExt as _, TracerProvider as _};
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::runtime::{Tokio, TokioCurrentThread};
use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor;
use opentelemetry_sdk::trace::{
  BatchConfig,
  Sampler,
  SdkTracer,
  SdkTracerProvider,
  SimpleSpanProcessor,
  SpanData,
  SpanExporter,
};
use parking_lot::Mutex;
pub use routing::initialize;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use tokio::runtime::{Builder, Handle, RuntimeFlavor};
use tokio::task::JoinHandle;
use tracing::Span;
use tracing::span::Id;
use tracing_opentelemetry::OpenTelemetrySpanExt;

//
// TestExporter
//

#[derive(Debug, Default)]
struct ExportState {
  spans: Vec<SpanData>,
  bytes: usize,
  failure: Option<String>,
}

// The SDK's in-memory exporter is unbounded and clears recordings on shutdown. Keep this small
// exporter so runaway tracing fails the test explicitly rather than retaining unlimited data.
#[derive(Clone, Debug)]
pub(crate) struct TestExporter {
  state: Arc<Mutex<ExportState>>,
  max_spans: usize,
  max_bytes: usize,
}

impl Default for TestExporter {
  fn default() -> Self {
    Self {
      state: Arc::default(),
      max_spans: 16_384,
      max_bytes: 16 * 1_024 * 1_024,
    }
  }
}

impl TestExporter {
  pub(crate) fn exported_spans(&self) -> Vec<SpanData> {
    let state = self.state.lock();
    assert!(
      state.failure.is_none(),
      "trace capture failed: {:?}",
      state.failure
    );
    state.spans.clone()
  }

  fn take_spans(&self) -> Vec<SpanData> {
    let mut state = self.state.lock();
    assert!(
      state.failure.is_none(),
      "trace capture failed: {:?}",
      state.failure
    );
    std::mem::take(&mut state.spans)
  }
}

impl SpanExporter for TestExporter {
  fn export(&self, batch: Vec<SpanData>) -> impl Future<Output = OTelSdkResult> {
    let mut state = self.state.lock();
    let bytes = batch
      .iter()
      .map(estimated_span_bytes)
      .fold(0, usize::saturating_add);
    if state.failure.is_none()
      && (batch.len() > self.max_spans.saturating_sub(state.spans.len())
        || bytes > self.max_bytes.saturating_sub(state.bytes))
    {
      let failure = format!(
        "trace capture exceeded its buffer limit ({} spans, {} bytes)",
        self.max_spans, self.max_bytes
      );
      log::error!("{failure}");
      state.failure = Some(failure);
      state.spans.clear();
    }
    let result = if let Some(failure) = &state.failure {
      Err(OTelSdkError::InternalFailure(failure.clone()))
    } else {
      state.bytes += bytes;
      state.spans.extend(batch);
      Ok(())
    };
    std::future::ready(result)
  }
}

//
// TraceCapture
//

#[derive(Default)]
struct CaptureState {
  live: HashMap<Id, &'static str>,
  // A foreign conditional root can still hold this capture's data after all local spans close.
  groups: Vec<WeakCaptureGroup>,
  scopes: usize,
  finished: bool,
}

struct CaptureInner {
  exporter: TestExporter,
  provider: SdkTracerProvider,
  tracer: SdkTracer,
  control: ExportControl,
  state: Mutex<CaptureState>,
}

/// Owns a bounded recording and its provider. Finish explicitly after shutting down traced work.
/// Creating a capture never changes the process-wide logger or introduces an OTEL parent.
#[must_use]
pub struct TraceCapture {
  handle: TraceCaptureHandle,
  finished: bool,
}

impl Default for TraceCapture {
  fn default() -> Self {
    Self::new()
  }
}

impl TraceCapture {
  pub fn new() -> Self {
    Self::builder().build()
  }

  pub fn builder() -> TraceCaptureBuilder {
    TraceCaptureBuilder::default()
  }

  pub fn handle(&self) -> TraceCaptureHandle {
    self.handle.clone()
  }

  pub fn in_scope<R>(&self, work: impl FnOnce() -> R) -> R {
    self.handle.in_scope(work)
  }

  pub fn scope<F: Future>(&self, future: F) -> impl Future<Output = F::Output> + use<F> {
    self.handle.scope(future)
  }

  pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
  where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
  {
    self.handle.spawn(future)
  }

  /// A snapshot for assertions made while an operation is still running.
  #[must_use]
  pub fn spans(&self) -> Vec<SpanData> {
    self.handle.0.exporter.exported_spans()
  }

  pub fn flush(&self) {
    self.handle.0.provider.force_flush().unwrap();
  }

  #[must_use]
  pub fn request_span(&self) -> (Span, String) {
    self.in_scope(|| {
      let span = crate::otel_info_span!("test_request");
      let context = span.context().span().span_context().clone();
      assert!(context.is_valid());
      let request_id = format!("{}-{}", context.trace_id(), context.span_id());
      (span, request_id)
    })
  }

  /// Flush and close this capture only. Live spans or scoped work indicate missing test teardown,
  /// not something flushing or waiting for an arbitrary delay can repair.
  #[must_use]
  pub fn finish(mut self) -> Vec<SpanData> {
    {
      let mut state = self.handle.0.state.lock();
      assert!(
        state.live.is_empty() && state.scopes == 0,
        "trace capture still owns {} scoped futures and live spans: {:?}",
        state.scopes,
        state.live
      );
      assert!(
        !self.handle.0.control.has_pending_captures()
          && !state.groups.iter().any(WeakCaptureGroup::is_pending),
        "trace capture still belongs to an unfinished conditional trace"
      );
      state.finished = true;
    }
    self.finished = true;
    self.handle.0.provider.force_flush().unwrap();
    self.handle.0.provider.shutdown().unwrap();
    self.handle.0.exporter.take_spans()
  }
}

impl Drop for TraceCapture {
  fn drop(&mut self) {
    assert!(
      self.finished || std::thread::panicking(),
      "TraceCapture::finish() must be called after shutting down traced work"
    );
  }
}

//
// TraceCaptureHandle
//

/// Cloneable ownership for service boundaries and explicitly detached root work.
#[derive(Clone)]
#[must_use]
pub struct TraceCaptureHandle(Arc<CaptureInner>);

impl TraceCaptureHandle {
  pub fn in_scope<R>(&self, work: impl FnOnce() -> R) -> R {
    routing::in_scope(self, work)
  }

  pub fn scope<F: Future>(&self, future: F) -> impl Future<Output = F::Output> + use<F> {
    routing::scope(self.clone(), future)
  }

  pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
  where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
  {
    tokio::spawn(self.scope(future))
  }

  fn assert_open(&self) {
    assert!(
      !self.0.state.lock().finished,
      "trace capture has already finished"
    );
  }
}

//
// TraceCaptureBuilder
//

#[must_use]
pub struct TraceCaptureBuilder {
  max_attributes: u32,
  batch: Option<BatchConfig>,
  exporter: TestExporter,
}

impl Default for TraceCaptureBuilder {
  fn default() -> Self {
    Self {
      max_attributes: 16,
      batch: None,
      exporter: TestExporter::default(),
    }
  }
}

impl TraceCaptureBuilder {
  pub fn max_attributes(mut self, limit: u32) -> Self {
    self.max_attributes = limit;
    self
  }

  pub fn max_spans(mut self, limit: usize) -> Self {
    self.exporter.max_spans = limit;
    self
  }

  pub fn max_bytes(mut self, limit: usize) -> Self {
    self.exporter.max_bytes = limit;
    self
  }

  pub fn batch_config(mut self, config: BatchConfig) -> Self {
    self.batch = Some(config);
    self
  }

  pub fn build(self) -> TraceCapture {
    initialize();
    let control = ExportControl::default();
    let builder = SdkTracerProvider::builder()
      .with_sampler(Sampler::AlwaysOn)
      .with_max_attributes_per_span(self.max_attributes);
    let provider = if let Some(config) = self.batch {
      match Handle::current().runtime_flavor() {
        RuntimeFlavor::CurrentThread => builder
          .with_span_processor(
            control.processor(
              BatchSpanProcessor::builder(self.exporter.clone(), TokioCurrentThread)
                .with_batch_config(config)
                .build(),
            ),
          )
          .build(),
        RuntimeFlavor::MultiThread => builder
          .with_span_processor(
            control.processor(
              BatchSpanProcessor::builder(self.exporter.clone(), Tokio)
                .with_batch_config(config)
                .build(),
            ),
          )
          .build(),
        flavor => panic!("unsupported test runtime: {flavor:?}"),
      }
    } else {
      builder
        .with_span_processor(control.processor(SimpleSpanProcessor::new(self.exporter.clone())))
        .build()
    };
    let tracer = provider.tracer("test");
    TraceCapture {
      handle: TraceCaptureHandle(Arc::new(CaptureInner {
        provider,
        tracer,
        control,
        exporter: self.exporter,
        state: Mutex::default(),
      })),
      finished: false,
    }
  }
}

/// Run a synchronous test's async work with an independently owned capture and current-thread
/// runtime. Tasks are torn down inside their capture before the final recording is returned.
pub fn capture_traces<F: Future>(future: F) -> (Vec<SpanData>, F::Output) {
  let capture = TraceCapture::new();
  let runtime = Builder::new_current_thread().enable_all().build().unwrap();
  // Keep runtime destruction inside the ownership scope too: cancellation can close lazy spans.
  let result = capture.in_scope(|| runtime.block_on(capture.scope(future)));
  capture.in_scope(|| drop(runtime));
  (capture.finish(), result)
}
