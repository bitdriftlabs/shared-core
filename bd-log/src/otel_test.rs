// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use super::active_tokio_runtime_flavor;
use crate::test::{TestTraceContext, with_two_phase_test_otel};
use opentelemetry::trace::{SpanId, Status};
use std::future::{pending, ready};
use tokio::runtime::{Builder, RuntimeFlavor};
use tracing::Instrument as _;
use tracing::instrument::WithSubscriber as _;
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[tokio::test]
async fn conditional_export_buffers_only_controlled_descendants_until_failure() {
  let context = TestTraceContext::new("conditional-children");
  let dispatch = context.dispatch();
  let _guard = tracing::dispatcher::set_default(&dispatch);
  let root = crate::otel_span_on_error!("root");
  crate::otel::instrument_on_error(
    async {
      let child = crate::otel_info_span_if_parent!("child", value = 42);
      {
        let _entered = child.enter();
        crate::otel_info!("child event");
        let grandchild = crate::otel_debug_span_if_parent!("grandchild");
        drop(grandchild);
        let excluded = tracing::info_span!("library_child");
        drop(excluded);
      }
      drop(child);
      assert_eq!(context.exported_spans(), []);
      Err::<(), _>("final failure")
    },
    root,
  )
  .await
  .unwrap_err();
  let spans = context.exported_spans();
  assert_eq!(spans.len(), 3);
  let root = spans.iter().find(|span| span.name == "root").unwrap();
  let child = spans.iter().find(|span| span.name == "child").unwrap();
  let grandchild = spans.iter().find(|span| span.name == "grandchild").unwrap();
  assert_eq!(root.parent_span_id, SpanId::INVALID);
  assert_eq!(child.parent_span_id, root.span_context.span_id());
  assert_eq!(grandchild.parent_span_id, child.span_context.span_id());
  assert_eq!(
    grandchild.span_context.trace_id(),
    root.span_context.trace_id()
  );
  assert_eq!(child.events.len(), 1);
  assert!(
    child
      .attributes
      .iter()
      .any(|attribute| attribute.key.as_str() == "value")
  );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conditional_export_waits_for_lazy_and_detached_children() {
  let context = TestTraceContext::new("conditional-children");
  let dispatch = context.dispatch();
  let _guard = tracing::dispatcher::set_default(&dispatch);
  let (send, receive) = tokio::sync::oneshot::channel();
  let root = crate::otel_span_on_error!("root");
  crate::otel::instrument_on_error(
    async {
      let lazy = crate::otel_info_span_if_parent!("lazy");
      let child = crate::otel_info_span_if_parent!("detached");
      send.send((lazy, child)).unwrap();
      Err::<(), _>("failed")
    },
    root,
  )
  .await
  .unwrap_err();
  assert_eq!(context.exported_spans(), []);
  let (lazy, child) = receive.await.unwrap();
  // Start a descendant after the root outcome, through an explicitly propagated child.
  let grandchild = {
    let _entered = child.enter();
    crate::otel_info_span_if_parent!("late_grandchild")
  };
  drop(child);
  drop(lazy);
  assert_eq!(context.exported_spans(), []);
  drop(grandchild);
  assert_eq!(context.exported_spans().len(), 4);
}

#[tokio::test]
async fn conditional_export_discards_children_on_success_and_cancellation() {
  let context = TestTraceContext::with_max_attributes("conditional-children", 0);
  let dispatch = context.dispatch();
  let _guard = tracing::dispatcher::set_default(&dispatch);
  crate::otel::instrument_on_error(
    async {
      let retry = crate::otel_info_span_if_parent!("failed_attempt");
      retry.set_status(Status::error("retryable"));
      drop(retry);
      let recovery = crate::otel_info_span_if_parent!("recovered_attempt");
      drop(recovery);
      Ok::<_, &str>(())
    },
    crate::otel_span_on_error!("success"),
  )
  .await
  .unwrap();
  let mut request = Box::pin(crate::otel::instrument_on_error(
    async {
      let child = crate::otel_info_span_if_parent!("cancelled_child");
      let _entered = child.enter();
      pending::<Result<(), &str>>().await
    },
    crate::otel_span_on_error!("cancelled"),
  ));
  assert!(
    std::future::poll_fn(|task| std::task::Poll::Ready(request.as_mut().poll(task)))
      .await
      .is_pending()
  );
  drop(request);
  assert_eq!(context.exported_spans(), []);
}

#[tokio::test]
async fn conditional_export_limit_suppresses_buffered_and_unstarted_children() {
  let context = TestTraceContext::new("conditional-limits");
  let dispatch = context.dispatch();
  let _guard = tracing::dispatcher::set_default(&dispatch);
  let limits = crate::otel::TraceCaptureLimits {
    max_spans: 2,
    ..crate::otel::TraceCaptureLimits::default()
  };
  let root = crate::otel_span_on_error!(limits: limits, "limited");
  crate::otel::instrument_on_error(
    async {
      let buffered = crate::otel_info_span_if_parent!("buffered");
      drop(buffered);
      let overflow = crate::otel_info_span_if_parent!("overflow");
      drop(overflow);
      let late = crate::otel_info_span_if_parent!("late");
      drop(late);
      Err::<(), _>("failed")
    },
    root,
  )
  .await
  .unwrap_err();
  assert_eq!(context.exported_spans(), []);
  let ordinary = crate::otel_info_span!("ordinary");
  drop(ordinary);
  assert_eq!(context.exported_spans().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conditional_export_propagates_to_spawned_controlled_children() {
  let context = TestTraceContext::with_max_attributes("conditional-task", 0);
  let dispatch = context.dispatch();
  let _guard = tracing::dispatcher::set_default(&dispatch);
  crate::otel::instrument_on_error(
    async {
      let child = crate::otel_info_span_if_parent!("task");
      tokio::spawn(
        async move {
          async {
            let grandchild = crate::otel_info_span_if_parent!("task_child");
            drop(grandchild);
            tokio::task::yield_now().await;
          }
          .instrument(child)
          .await;
        }
        .with_subscriber(dispatch.clone()),
      )
      .await
      .unwrap();
      assert_eq!(context.exported_spans(), []);
      Err::<(), _>("failed")
    },
    crate::otel_span_on_error!("root"),
  )
  .await
  .unwrap_err();
  assert_eq!(context.exported_spans().len(), 3);
}

#[tokio::test]
async fn conditional_export_nested_roots_have_independent_decisions() {
  let context = TestTraceContext::new("conditional-nested");
  let dispatch = context.dispatch();
  let _guard = tracing::dispatcher::set_default(&dispatch);
  crate::otel::instrument_on_error(
    async {
      let outer_child = crate::otel_info_span_if_parent!("outer_child");
      drop(outer_child);
      crate::otel::instrument_on_error(
        async {
          let child = crate::otel_info_span_if_parent!("inner_child");
          drop(child);
          Err::<(), _>("inner failed")
        },
        crate::otel_span_on_error!("inner"),
      )
      .await
      .unwrap_err();
      Ok::<_, &str>(())
    },
    crate::otel_span_on_error!("outer"),
  )
  .await
  .unwrap();
  let spans = context.exported_spans();
  assert_eq!(spans.len(), 2);
  let root = spans.iter().find(|span| span.name == "inner").unwrap();
  let child = spans
    .iter()
    .find(|span| span.name == "inner_child")
    .unwrap();
  assert_eq!(root.links.len(), 1);
  assert_eq!(root.parent_span_id, SpanId::INVALID);
  assert_eq!(child.parent_span_id, root.span_context.span_id());
  assert_ne!(
    root.links[0].span_context.trace_id(),
    root.span_context.trace_id()
  );
}

#[tokio::test]
async fn conditional_export_cancellation_is_final_with_a_surviving_handle() {
  let context = TestTraceContext::new("conditional-cancellation");
  let dispatch = context.dispatch();
  let _guard = tracing::dispatcher::set_default(&dispatch);
  let root = crate::otel_span_on_error!("cancelled");
  let mut request = Box::pin(crate::otel::instrument_on_error(
    async {
      let child = crate::otel_info_span_if_parent!("child");
      drop(child);
      pending::<Result<(), &str>>().await
    },
    root.clone(),
  ));
  assert!(
    std::future::poll_fn(|task| std::task::Poll::Ready(request.as_mut().poll(task)))
      .await
      .is_pending()
  );
  drop(request);
  root.retain();
  drop(root);
  assert_eq!(context.exported_spans(), []);
}

#[tokio::test]
async fn conditional_export_only_retains_final_errors_with_any_attribute_budget() {
  for limit in [0, 1, 16] {
    let context = TestTraceContext::with_max_attributes("conditional-export-test", limit);
    let dispatch = context.dispatch();
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let success = crate::otel_span_on_error!("success", first = 1, second = 2);
    assert!(
      crate::otel::instrument_on_error(ready(Ok::<_, &str>(())), success)
        .await
        .is_ok()
    );
    let failure = crate::otel_span_on_error!("failure", first = 1, second = 2);
    // Entering creates the SDK span before the retention field is updated.
    {
      let _entered = failure.enter();
    }
    assert!(
      crate::otel::instrument_on_error(ready(Err::<(), _>("final error")), failure)
        .await
        .is_err()
    );
    let ordinary = crate::otel_info_span!("ordinary");
    {
      let _entered = ordinary.enter();
    }
    drop(ordinary);
    let spans = context.exported_spans();
    assert_eq!(spans.len(), 2);
    let failure = spans.iter().find(|span| span.name == "failure").unwrap();
    assert_eq!(failure.status, Status::error("final error"));
    assert_eq!(failure.parent_span_id, SpanId::INVALID);
    assert!(
      !failure
        .attributes
        .iter()
        .any(|attribute| attribute.key.as_str() == "bd_log.export")
    );
    assert!(spans.iter().any(|span| span.name == "ordinary"));
  }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conditional_export_discards_cancelled_and_recovered_errors() {
  let context = TestTraceContext::new("conditional-export-test");
  let dispatch = context.dispatch();
  let _guard = tracing::dispatcher::set_default(&dispatch);
  let span = crate::otel_span_on_error!("cancelled");
  let mut request = Box::pin(crate::otel::instrument_on_error(
    async {
      crate::otel_error!("intermediate error");
      pending::<Result<(), &str>>().await
    },
    span,
  ));
  assert!(
    std::future::poll_fn(|task| std::task::Poll::Ready(request.as_mut().poll(task)))
      .await
      .is_pending()
  );
  drop(request);
  let span = crate::otel_span_on_error!("recovered");
  crate::otel::instrument_on_error(
    async {
      crate::otel_error!("retryable error");
      tokio::task::yield_now().await;
      Ok::<_, &str>(())
    },
    span,
  )
  .await
  .unwrap();
  assert_eq!(context.exported_spans(), []);
}

#[test]
fn conditional_export_survives_two_stage_logger_setup_and_links_parent() {
  let (spans, ()) = with_two_phase_test_otel("conditional-export-test", async {
    let parent = crate::otel_info_span!("parent");
    let failure = {
      let _entered = parent.enter();
      crate::otel_span_on_error!("linked_failure")
    };
    crate::otel::instrument_on_error(ready(Err::<(), _>("failed")), failure)
      .await
      .unwrap_err();
    crate::otel::instrument_on_error(
      ready(Ok::<_, &str>(())),
      crate::otel_span_on_error!("success"),
    )
    .await
    .unwrap();
  });
  assert_eq!(spans.len(), 2);
  let parent = spans.iter().find(|span| span.name == "parent").unwrap();
  let failure = spans
    .iter()
    .find(|span| span.name == "linked_failure")
    .unwrap();
  assert_eq!(failure.parent_span_id, SpanId::INVALID);
  assert_eq!(failure.links.len(), 1);
  assert_eq!(
    failure.links[0].span_context.trace_id(),
    parent.span_context.trace_id()
  );
  assert_eq!(
    failure.links[0].span_context.span_id(),
    parent.span_context.span_id()
  );
  assert_ne!(
    failure.span_context.trace_id(),
    parent.span_context.trace_id()
  );
}

#[test]
fn detects_current_thread_runtime_flavor() {
  let runtime = Builder::new_current_thread().enable_all().build().unwrap();

  let flavor = runtime.block_on(async { active_tokio_runtime_flavor().unwrap() });

  assert_eq!(RuntimeFlavor::CurrentThread, flavor);
}

#[test]
fn detects_multi_thread_runtime_flavor() {
  let runtime = Builder::new_multi_thread()
    .worker_threads(2)
    .enable_all()
    .build()
    .unwrap();

  let flavor = runtime.block_on(async { active_tokio_runtime_flavor().unwrap() });

  assert_eq!(RuntimeFlavor::MultiThread, flavor);
}

#[test]
fn rejects_missing_tokio_runtime() {
  assert!(active_tokio_runtime_flavor().is_err());
}
