// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use super::{CaptureInner, TestExporter, TraceCapture, TraceCaptureHandle, capture_traces};
use crate::conditional_export::{ExportControl, TraceCaptureLimits};
use opentelemetry::Context;
use opentelemetry::trace::{SpanId, TraceContextExt, TracerProvider as _};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{
  Sampler,
  SdkTracerProvider,
  SimpleSpanProcessor,
  Span,
  SpanData,
  SpanProcessor,
};
use parking_lot::Mutex;
use std::future::pending;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::Command;
use std::sync::mpsc::{SyncSender, sync_channel};
use std::sync::{Arc, Barrier};
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::oneshot;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

//
// TraceOnDrop
//

struct TraceOnDrop;

impl Drop for TraceOnDrop {
  fn drop(&mut self) {
    drop(crate::otel_info_span!("cancelled"));
  }
}

//
// GatedProcessor
//

#[derive(Debug)]
struct GatedProcessor {
  inner: SimpleSpanProcessor<TestExporter>,
  started: SyncSender<()>,
  release: Arc<Barrier>,
}

impl SpanProcessor for GatedProcessor {
  fn on_start(&self, span: &mut Span, context: &Context) {
    self.inner.on_start(span, context);
  }

  fn on_end(&self, span: SpanData) {
    log::debug!("deferred span reached delivery gate: {}", span.name);
    self.started.send(()).unwrap();
    self.release.wait();
    self.inner.on_end(span);
  }

  fn force_flush(&self) -> OTelSdkResult {
    self.inner.force_flush()
  }

  fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
    self.inner.shutdown_with_timeout(timeout)
  }

  fn set_resource(&mut self, resource: &Resource) {
    self.inner.set_resource(resource);
  }
}

#[test]
fn captures_work_under_concurrent_libtest() {
  assert!(
    std::env::var_os("BD_LOG_LIBTEST_CHILD").is_none(),
    "concurrent libtest must skip its subprocess launcher"
  );
  let output = Command::new(std::env::current_exe().unwrap())
    .args([
      "--test-threads=8",
      "--skip",
      "captures_work_under_concurrent_libtest",
    ])
    .env("BD_LOG_LIBTEST_CHILD", "1")
    .output()
    .unwrap();
  assert!(
    output.status.success(),
    "concurrent libtest failed:\n{}\n{}",
    String::from_utf8_lossy(&output.stdout),
    String::from_utf8_lossy(&output.stderr)
  );
  assert!(String::from_utf8_lossy(&output.stdout).contains("test result: ok."));
}

#[test]
fn captures_run_concurrently_in_one_process() {
  let barrier = Arc::new(Barrier::new(8));
  let threads: Vec<_> = (0 .. 8)
    .map(|index| {
      let barrier = barrier.clone();
      std::thread::spawn(move || {
        let capture = TraceCapture::new();
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        barrier.wait();
        runtime.block_on(capture.scope(async {
          let root = crate::otel_info_span!("root", index);
          async {
            tokio::task::yield_now().await;
            drop(crate::otel_info_span_if_parent!("child", index));
          }
          .instrument(root)
          .await;
        }));
        let spans = capture.finish();
        assert_eq!(spans.len(), 2);
        for span in &spans {
          assert!(span.attributes.iter().any(|attribute| {
            attribute.key.as_str() == "index" && attribute.value.to_string() == index.to_string()
          }));
        }
        let root = spans.iter().find(|span| span.name == "root").unwrap();
        let child = spans.iter().find(|span| span.name == "child").unwrap();
        assert_eq!(root.parent_span_id, SpanId::INVALID);
        assert_eq!(child.parent_span_id, root.span_context.span_id());
      })
    })
    .collect();
  for thread in threads {
    thread.join().unwrap();
  }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn captures_share_registry_and_survive_independent_shutdown() {
  let first = TraceCapture::new();
  let second = TraceCapture::builder().max_attributes(0).build();
  let registry = tracing::dispatcher::get_default(|dispatch| {
    std::ptr::from_ref(
      dispatch
        .downcast_ref::<tracing_subscriber::Registry>()
        .unwrap(),
    )
  });
  let first_span = first.in_scope(|| crate::otel_info_span!("first", value = 1));
  let second_span = second.in_scope(|| crate::otel_info_span!("second", value = 2));
  // SDK activation outside any capture scope must still select the span's original provider.
  assert!(first_span.context().span().span_context().is_valid());
  assert!(second_span.context().span().span_context().is_valid());
  let child = first_span.in_scope(|| crate::otel_info_span_if_parent!("first_child"));
  drop(first_span);
  tokio::spawn(async move { drop(child) }).await.unwrap();
  let first_spans = first.finish();
  assert_eq!(first_spans.len(), 2);
  assert!(
    first_spans
      .iter()
      .find(|span| span.name == "first")
      .unwrap()
      .attributes
      .iter()
      .any(|attribute| attribute.key.as_str() == "value" && attribute.value.to_string() == "1")
  );
  assert!(first_spans.iter().all(|span| {
    span
      .attributes
      .iter()
      .all(|attribute| !attribute.key.as_str().starts_with("bd_log.test"))
  }));
  tokio::spawn(async move {
    async {
      tokio::task::yield_now().await;
      drop(crate::otel_info_span_if_parent!("second_child"));
    }
    .instrument(second_span)
    .await;
  })
  .await
  .unwrap();
  let second_spans = second.finish();
  assert_eq!(second_spans.len(), 2);
  assert!(second_spans.iter().all(|span| span.attributes.is_empty()));
  tracing::dispatcher::get_default(|dispatch| {
    assert_eq!(
      registry,
      std::ptr::from_ref(
        dispatch
          .downcast_ref::<tracing_subscriber::Registry>()
          .unwrap()
      )
    );
  });
}

#[test]
fn capture_scope_does_not_create_a_parent_or_retain_successes() {
  let (spans, ()) = capture_traces(async {
    assert!(crate::otel_info_span_if_parent!("without_parent").is_disabled());
    crate::otel::instrument_on_error(
      async { Ok::<_, &str>(()) },
      crate::otel_span_on_error!("successful_operation"),
    )
    .await
    .unwrap();
    crate::otel::instrument_on_error(
      async { Err::<(), _>("failed") },
      crate::otel_span_on_error!("failed_operation"),
    )
    .await
    .unwrap_err();
  });
  assert_eq!(spans.len(), 1);
  assert_eq!(spans[0].name, "failed_operation");
  assert_eq!(spans[0].parent_span_id, SpanId::INVALID);
}

#[test]
fn remote_reparenting_preserves_capture_ownership() {
  let first = TraceCapture::new();
  let second = TraceCapture::new();
  let (parent, _) = first.request_span();
  let headers = parent.in_scope(|| crate::current_trace_context_headers().unwrap());
  let child = second.in_scope(|| crate::otel_info_span!("remote_child"));
  assert!(crate::set_remote_parent(&child, &headers));
  let context = child.context().span().span_context().clone();
  drop(child);
  drop(parent);
  let parents = first.finish();
  let children = second.finish();
  assert_eq!(parents.len(), 1);
  assert_eq!(children.len(), 1);
  assert_eq!(children[0].span_context, context);
  assert_eq!(
    children[0].span_context.trace_id(),
    parents[0].span_context.trace_id()
  );
  assert_eq!(
    children[0].parent_span_id,
    parents[0].span_context.span_id()
  );
}

#[tokio::test]
async fn nested_capture_scopes_restore_ownership_without_leaking_to_other_futures() {
  let first = TraceCapture::new();
  let second = TraceCapture::new();
  tokio::join!(
    first.scope(async {
      drop(crate::otel_info_span!("first"));
      second
        .scope(async {
          tokio::task::yield_now().await;
          drop(crate::otel_info_span!("second"));
        })
        .await;
      drop(crate::otel_info_span!("first_restored"));
    }),
    async {
      drop(crate::otel_info_span!("outside_before_yield"));
      tokio::task::yield_now().await;
      drop(crate::otel_info_span!("outside_after_yield"));
    }
  );
  let first_spans = first.finish();
  assert_eq!(
    first_spans
      .iter()
      .map(|span| span.name.as_ref())
      .collect::<Vec<_>>(),
    ["first", "first_restored"]
  );
  let second_spans = second.finish();
  assert_eq!(second_spans.len(), 1);
  assert_eq!(second_spans[0].name, "second");
}

#[test]
fn synchronous_capture_scopes_restore_ownership_after_unwinding() {
  let first = TraceCapture::new();
  let second = TraceCapture::new();
  first.in_scope(|| {
    let result = catch_unwind(AssertUnwindSafe(|| {
      second.in_scope(|| panic!("unwind the inner capture"));
    }));
    assert!(result.is_err());
    drop(crate::otel_info_span!("outer_restored"));
  });
  let spans = first.finish();
  assert_eq!(spans.len(), 1);
  assert_eq!(spans[0].name, "outer_restored");
  assert_eq!(second.finish(), []);
}

#[test]
fn capture_owns_unpolled_future_destruction_inside_another_capture() {
  let first = TraceCapture::new();
  let second = TraceCapture::new();
  let on_drop = TraceOnDrop;
  let future = first.scope(async move {
    let _on_drop = on_drop;
    pending::<()>().await;
  });
  second.in_scope(|| {
    drop(future);
    drop(crate::otel_info_span!("second_restored"));
  });
  let first_spans = first.finish();
  assert_eq!(first_spans.len(), 1);
  assert_eq!(first_spans[0].name, "cancelled");
  let second_spans = second.finish();
  assert_eq!(second_spans.len(), 1);
  assert_eq!(second_spans[0].name, "second_restored");
}

#[tokio::test]
async fn capture_owns_span_creation_during_future_cancellation() {
  let capture = TraceCapture::new();
  let (started, ready) = oneshot::channel();
  let task = capture.spawn(async move {
    let _on_drop = TraceOnDrop;
    started.send(()).unwrap();
    pending::<()>().await;
  });
  ready.await.unwrap();
  task.abort();
  assert!(task.await.unwrap_err().is_cancelled());
  let spans = capture.finish();
  assert_eq!(spans.len(), 1);
  assert_eq!(spans[0].name, "cancelled");
}

#[test]
#[should_panic(expected = "trace capture still owns")]
fn finishing_reports_unpolled_scoped_work() {
  let capture = TraceCapture::new();
  let _future = capture.scope(pending::<()>());
  drop(capture.finish());
}

#[test]
#[should_panic(expected = "trace capture still owns")]
fn finishing_reports_live_spans() {
  let capture = TraceCapture::new();
  let _span = capture.in_scope(|| crate::otel_info_span!("not_shut_down"));
  drop(capture.finish());
}

#[test]
#[should_panic(expected = "exceeded its buffer limit")]
fn capture_overflow_is_a_test_failure() {
  let capture = TraceCapture::builder().max_spans(0).build();
  capture.in_scope(|| drop(crate::otel_info_span!("too_many")));
  drop(capture.finish());
}

#[test]
fn conditional_captures_have_independent_providers_and_budgets() {
  let first = TraceCapture::new();
  let second = TraceCapture::new();
  let limits = TraceCaptureLimits {
    max_spans: 1,
    ..TraceCaptureLimits::default()
  };
  let runtime = Builder::new_current_thread().enable_all().build().unwrap();
  runtime.block_on(async {
    first
      .scope(crate::otel::instrument_on_error(
        async { Err::<(), _>("first") },
        first.in_scope(|| crate::otel_span_on_error!(limits: limits, "first")),
      ))
      .await
      .unwrap_err();
    second
      .scope(async {
        crate::otel::instrument_on_error(
          async {
            drop(crate::otel_info_span_if_parent!("second_child"));
            Err::<(), _>("second")
          },
          crate::otel_span_on_error!("second"),
        )
        .await
        .unwrap_err();
      })
      .await;
  });
  assert_eq!(first.finish().len(), 1);
  assert_eq!(second.finish().len(), 2);
}

#[test]
fn conditional_trace_routes_each_member_to_its_own_capture() {
  let first = TraceCapture::new();
  let second = TraceCapture::new();
  let runtime = Builder::new_current_thread().enable_all().build().unwrap();
  runtime.block_on(first.scope(async {
    crate::otel::instrument_on_error(
      async {
        second
          .scope(async {
            drop(crate::otel_info_span_if_parent!("second_capture_child"));
          })
          .await;
        assert_eq!(second.spans(), []);
        Err::<(), _>("retain the whole operation")
      },
      crate::otel_span_on_error!("first_capture_root"),
    )
    .await
    .unwrap_err();
  }));
  let first_spans = first.finish();
  let second_spans = second.finish();
  assert_eq!(first_spans.len(), 1);
  assert_eq!(second_spans.len(), 1);
  assert_eq!(first_spans[0].name, "first_capture_root");
  assert_eq!(second_spans[0].name, "second_capture_child");
  assert_eq!(
    second_spans[0].parent_span_id,
    first_spans[0].span_context.span_id()
  );
}

#[test]
fn foreign_capture_cannot_finish_during_deferred_span_delivery() {
  let first = TraceCapture::new();
  let exporter = TestExporter::default();
  let control = ExportControl::default();
  let (started, delivering) = sync_channel(0);
  let release = Arc::new(Barrier::new(2));
  let provider = SdkTracerProvider::builder()
    .with_sampler(Sampler::AlwaysOn)
    .with_max_attributes_per_span(16)
    .with_span_processor(control.processor(GatedProcessor {
      inner: SimpleSpanProcessor::new(exporter.clone()),
      started,
      release: release.clone(),
    }))
    .build();
  let second = TraceCapture {
    handle: TraceCaptureHandle(Arc::new(CaptureInner {
      tracer: provider.tracer("gated-capture"),
      provider,
      control,
      exporter,
      state: Mutex::default(),
    })),
    finished: false,
  };
  let root = first.in_scope(|| crate::otel_span_on_error!("retained_root"));
  second.in_scope(|| {
    root.in_scope(|| drop(crate::otel_info_span_if_parent!("deferred_child")));
  });
  assert_eq!(second.spans(), []);
  root.retain();
  let closer = std::thread::spawn(move || drop(root));
  delivering.recv().unwrap();

  // Exercise finish's rejection without losing the original owner to the expected panic.
  let premature = TraceCapture {
    handle: second.handle(),
    finished: false,
  };
  let result = catch_unwind(AssertUnwindSafe(|| premature.finish()));
  let still_open = !second.handle.0.state.lock().finished;
  // Always release and join before asserting, so a failed regression cannot strand the closer.
  release.wait();
  closer.join().unwrap();
  assert!(
    result.is_err(),
    "finish accepted spans still being delivered"
  );
  assert!(
    still_open,
    "finish shut down the provider before delivery completed"
  );
  let spans = second.finish();
  assert_eq!(spans.len(), 1);
  assert_eq!(spans[0].name, "deferred_child");
  let roots = first.finish();
  assert_eq!(roots.len(), 1);
  assert_eq!(roots[0].name, "retained_root");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn instrumented_spawn_inherits_ownership_for_parentless_conditional_roots() {
  let capture = TraceCapture::new();
  let unowned = tracing::info_span!("unowned_parent");
  let owned = capture.in_scope(|| tracing::info_span!("task_owner"));
  tokio::spawn(
    async move {
      // An explicit, unowned parent must not fall back to the entered task owner's capture.
      drop(tracing::info_span!(
        target: crate::OTEL_TARGET,
        parent: &unowned,
        "unowned_child"
      ));
      crate::otel::instrument_on_error(
        async {
          tokio::task::yield_now().await;
          drop(crate::otel_info_span_if_parent!("failed_child"));
          Err::<(), &str>("retain the failed operation")
        },
        crate::otel_span_on_error!("failed_root"),
      )
      .await
      .unwrap_err();
      crate::otel::instrument_on_error(
        async {
          drop(crate::otel_info_span_if_parent!("successful_child"));
          Ok::<(), &str>(())
        },
        crate::otel_span_on_error!("successful_root"),
      )
      .await
      .unwrap();
    }
    .instrument(owned),
  )
  .await
  .unwrap();
  let spans = capture.finish();
  assert_eq!(spans.len(), 2);
  let root = spans
    .iter()
    .find(|span| span.name == "failed_root")
    .unwrap();
  let child = spans
    .iter()
    .find(|span| span.name == "failed_child")
    .unwrap();
  assert_eq!(root.parent_span_id, SpanId::INVALID);
  assert_eq!(child.parent_span_id, root.span_context.span_id());
}

#[test]
#[should_panic(expected = "unfinished conditional trace")]
fn finishing_reports_a_foreign_conditional_trace_that_still_owns_buffered_spans() {
  let first = TraceCapture::new();
  let second = TraceCapture::new();
  let runtime = Builder::new_current_thread().enable_all().build().unwrap();
  runtime.block_on(first.scope(async {
    crate::otel::instrument_on_error(
      async {
        second.in_scope(|| drop(crate::otel_info_span_if_parent!("buffered_child")));
        drop(second.finish());
        Ok::<_, &str>(())
      },
      crate::otel_span_on_error!("unfinished_root"),
    )
    .await
    .unwrap();
  }));
  drop(first.finish());
}

#[test]
#[should_panic(expected = "exceeded its buffer limit")]
fn capture_byte_overflow_is_a_test_failure() {
  let capture = TraceCapture::builder().max_bytes(1).build();
  capture.in_scope(|| drop(crate::otel_info_span!("too_large")));
  drop(capture.finish());
}
