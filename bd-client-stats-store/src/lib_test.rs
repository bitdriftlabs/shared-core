// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/licenses/strict/1.0.0

use crate::{Collector, MetricDataRef, NameType};
use bd_stats_common::{Counter as _, labels};
use std::collections::BTreeMap;

#[test]
fn dual_type() {
  let collector = Collector::default();
  let _counter = collector.dynamic_counter(BTreeMap::new(), "same_id");
  let _histogram = collector.dynamic_histogram(BTreeMap::new(), "same_id");
}

#[test]
fn retain() {
  let collector = Collector::default();
  let c = collector.scope("").counter("c");
  let h = collector.scope("").histogram("h");
  assert!(
    collector
      .find_counter(&NameType::Global("c".to_string()), &labels!())
      .is_some()
  );
  assert!(
    collector
      .find_histogram(&NameType::Global("h".to_string()), &labels!())
      .is_some()
  );

  // Dropping c and h should leave them in the map until we retain.
  drop(c);
  drop(h);
  assert!(
    collector
      .find_counter(&NameType::Global("c".to_string()), &labels!())
      .is_some()
  );
  assert!(
    collector
      .find_histogram(&NameType::Global("h".to_string()), &labels!())
      .is_some()
  );

  collector.retain(|_, _, metric| match metric {
    MetricDataRef::Counter(c) => c.multiple_references(),
    MetricDataRef::Histogram(h) => h.multiple_references(),
  });

  assert!(
    collector
      .find_counter(&NameType::Global("c".to_string()), &labels!())
      .is_none()
  );
  assert!(
    collector
      .find_histogram(&NameType::Global("h".to_string()), &labels!())
      .is_none()
  );
}

#[test]
fn product_counters_have_emission_origin_but_operational_and_workflow_metrics_do_not() {
  for environment in [None, Some("{}"), Some(r#"{"channel":"production"}"#)] {
    let collector = Collector::default().with_product_metadata(
      environment
        .map(|value| ("__bd_labels".to_string(), value.to_string()))
        .into_iter()
        .collect(),
    );
    let scope = collector.scope("nested").scope("product");
    scope.product_counter("counter").inc();
    let expected = environment
      .map(|value| labels! { "__bd_labels" => value })
      .unwrap_or_default();
    assert_eq!(
      collector
        .find_counter(
          &NameType::Global("nested:product:counter".to_string()),
          &expected
        )
        .unwrap()
        .get(),
      1,
    );
    scope.counter("operational").inc();
    assert_eq!(
      collector
        .find_counter(
          &NameType::Global("nested:product:operational".to_string()),
          &labels! {}
        )
        .unwrap()
        .get(),
      1,
    );
    collector
      .dynamic_counter(labels! {}, "workflow")
      .unwrap()
      .inc();
    assert_eq!(
      collector
        .find_counter(&NameType::ActionId("workflow".to_string()), &labels! {})
        .unwrap()
        .get(),
      1,
    );
  }
}

#[test]
fn product_counter_handles_snapshot_origin_on_each_increment() {
  let collector = Collector::default();
  let scope = collector.scope("product");
  let counter = scope.product_counter("counter");
  scope.set_product_origin("__bd_labels", "{}".to_string());
  counter.inc_by(2);
  scope.set_product_origin("__bd_labels", r#"{"channel":"qa"}"#.to_string());
  counter.inc_by(3);
  counter.inc_with_origin(labels! { "__bd_labels" => r#"{"channel":"historical"}"# });
  for (origin, expected) in [("{}", 2), (r#"{"channel":"qa"}"#, 3)] {
    assert_eq!(
      collector
        .find_counter(
          &NameType::Global("product:counter".to_string()),
          &labels! { "__bd_labels" => origin },
        )
        .unwrap()
        .get(),
      expected
    );
  }
  assert_eq!(
    collector
      .find_counter(
        &NameType::Global("product:counter".to_string()),
        &labels! { "__bd_labels" => r#"{"channel":"historical"}"# }
      )
      .unwrap()
      .get(),
    1
  );
}
