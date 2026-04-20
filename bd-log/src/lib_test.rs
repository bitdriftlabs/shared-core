// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use opentelemetry::trace::TraceContextExt;
use std::future::Future;
use std::sync::{Arc, Mutex};
use tracing::span::{Attributes, Id};
use tracing::{Event, Subscriber};
use tracing_subscriber::Registry;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

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

fn exported_span_names_after_two_stage_init(future: impl Future<Output = ()>) -> Vec<String> {
  let (spans, ()) = crate::test::with_two_phase_test_otel("bd-log-test", future);
  spans.iter().map(|span| span.name.to_string()).collect()
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
  let (layers, _) = crate::ReloadableLayerStack::new(vec![
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

#[test]
fn trace_context_headers_round_trip_remote_parent() {
  let (spans, ()) = crate::test::with_two_phase_test_otel("bd-log-test", async {
    let headers = {
      let parent = crate::otel_info_span!("parent");
      let _parent_entered = parent.enter();
      crate::current_trace_context_headers().unwrap()
    };

    let child = crate::otel_info_span!("child");
    assert!(crate::set_remote_parent(&child, &headers));
    let _child_entered = child.enter();
  });
  let parent = spans.iter().find(|span| span.name == "parent").unwrap();
  let child = spans.iter().find(|span| span.name == "child").unwrap();

  assert_eq!(child.parent_span_id, parent.span_context.span_id());
  assert_eq!(
    child.span_context.trace_id(),
    parent.span_context.trace_id()
  );
}

#[test]
fn trace_context_headers_round_trip_trace_link() {
  let (spans, (parent_trace_id, parent_span_id, linked_trace_id)) =
    crate::test::with_two_phase_test_otel("bd-log-test", async {
      let (headers, parent_trace_id, parent_span_id) = {
        let parent = crate::otel_info_span!("parent");
        let _parent_entered = parent.enter();
        let span_context = opentelemetry::Context::current()
          .span()
          .span_context()
          .clone();

        (
          crate::current_trace_context_headers().unwrap(),
          span_context.trace_id(),
          span_context.span_id(),
        )
      };

      let linked = crate::otel_info_span!("linked");
      assert!(crate::add_trace_link(&linked, &headers));
      let _linked_entered = linked.enter();

      let invalid = crate::otel_info_span!("invalid");
      assert!(!crate::add_trace_link(
        &invalid,
        &crate::TraceContextHeaders::default(),
      ));
      let _invalid_entered = invalid.enter();

      assert_ne!(
        opentelemetry::Context::current()
          .span()
          .span_context()
          .trace_id(),
        parent_trace_id
      );
      (
        parent_trace_id,
        parent_span_id,
        opentelemetry::Context::current()
          .span()
          .span_context()
          .trace_id(),
      )
    });
  let linked = spans.iter().find(|span| span.name == "linked").unwrap();

  assert_eq!(linked.links.len(), 1);
  assert_eq!(linked.links[0].span_context.trace_id(), parent_trace_id);
  assert_eq!(linked.links[0].span_context.span_id(), parent_span_id);
  assert_eq!(linked.span_context.trace_id(), linked_trace_id);
  assert_ne!(linked.span_context.trace_id(), parent_trace_id);
  assert_ne!(linked.parent_span_id, parent_span_id);

  let invalid = spans.iter().find(|span| span.name == "invalid").unwrap();
  assert!(invalid.links.is_empty());
}
