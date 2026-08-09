// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use super::*;
use crate::test::util::stats::Helper;
use prometheus::labels;

#[test]
fn float_counter_records_fractional_values() {
  let helper = Helper::new();
  let counter = helper.collector().scope("float").float_counter("counter");

  counter.inc_by(0.5);
  counter.inc_by(1.25);

  let value = helper
    .find_counter("float:counter", &labels!())
    .expect("float counter is registered")
    .value();
  assert!(
    (value - 1.75).abs() < f64::EPSILON,
    "expected float counter value 1.75, got {value}",
  );
}

#[test]
fn labeled_float_counter_records_fractional_values() {
  let helper = Helper::new();
  let counter = helper
    .collector()
    .scope("float")
    .float_counter_vec("counter_vec", &["kind"])
    .with_label_values(&["read"]);

  counter.inc_by(0.5);

  let value = helper
    .find_counter("float:counter_vec", &labels!("kind" => "read"))
    .expect("labeled float counter is registered")
    .value();
  assert!(
    (value - 0.5).abs() < f64::EPSILON,
    "expected labeled float counter value 0.5, got {value}",
  );
}

#[test]
fn contribution_gauges_aggregate_and_remove_only_their_own_values() {
  let helper = Helper::new();
  let gauge = helper.collector().scope("contribution").gauge("value");
  let first = ContributionGauge::new(gauge.clone());
  let first_clone = first.clone();
  let second = ContributionGauge::new(gauge);

  first.inc_by(3);
  second.inc_by(5);
  helper.assert_gauge_eq(8, "contribution:value", &labels!());

  first_clone.dec_by(2);
  helper.assert_gauge_eq(6, "contribution:value", &labels!());

  first.set(-4);
  helper.assert_gauge_eq(1, "contribution:value", &labels!());

  second.set(-2);
  helper.assert_gauge_eq(-6, "contribution:value", &labels!());

  first_clone.inc_by(5);
  helper.assert_gauge_eq(-1, "contribution:value", &labels!());

  second.dec_by(4);
  helper.assert_gauge_eq(-5, "contribution:value", &labels!());

  first.clear();
  helper.assert_gauge_eq(-6, "contribution:value", &labels!());

  first_clone.set(2);
  helper.assert_gauge_eq(-4, "contribution:value", &labels!());

  drop(first);
  helper.assert_gauge_eq(-4, "contribution:value", &labels!());

  drop(first_clone);
  helper.assert_gauge_eq(-6, "contribution:value", &labels!());

  drop(second);
  helper.assert_gauge_eq(0, "contribution:value", &labels!());
}

#[test]
fn contribution_gauge_saturates_large_signed_adjustments() {
  let helper = Helper::new();
  let gauge = ContributionGauge::new(helper.collector().scope("contribution").gauge("value"));

  gauge.inc_by(u64::MAX);
  helper.assert_gauge_eq(i64::MAX, "contribution:value", &labels!());

  gauge.dec_by(u64::MAX);
  helper.assert_gauge_eq(i64::MIN, "contribution:value", &labels!());

  gauge.clear();
  helper.assert_gauge_eq(0, "contribution:value", &labels!());
}

// Verify basic label tracking functionality.
#[test]
fn label_tracker() {
  let helper = Helper::new_with_collector(Collector::new_with_label_limit(3));
  let scope = helper.collector().scope("foo");
  let labeled_metric_builder = scope.labeled_metric_builder(&["bar", "baz"]);
  let labeled_counter = labeled_metric_builder.build_counter("counter");
  let mut counter1 = labeled_counter.new_metric();
  let mut counter2 = labeled_counter.new_metric();
  let mut counter3 = labeled_counter.new_metric();

  // Increment counter 1, using 2 label slots.
  counter1.get(&vec!["bar1", "baz1"]).inc();

  // Increment counter 2, using 1 existing label and 1 new label.
  counter2.get(&vec!["bar1", "baz2"]).inc();

  // Increment 3, using 1 existing label and 1 new label. This will overflow.
  counter3.get(&vec!["bar1", "baz3"]).inc();

  helper.assert_counter_eq(1, "foo:counter", &labels!("bar" => "bar1", "baz" => "baz1"));
  helper.assert_counter_eq(1, "foo:counter", &labels!("bar" => "bar1", "baz" => "baz2"));
  helper.assert_counter_eq(
    1,
    "foo:counter",
    &labels!("bar" => "label_overflow", "baz" => "label_overflow"),
  );
  helper.assert_counter_eq(1, "stats:label_tracker:label_overflow", &labels!());

  // Make sure gauges use the same label tracker.
  let labeled_gauge = labeled_metric_builder.build_gauge("gauge");
  let mut gauge1: LateInitializedGauge = labeled_gauge.new_metric();
  gauge1.get(&vec!["bar1", "baz1"]).inc();
  let mut gauge2: LateInitializedGauge = labeled_gauge.new_metric();
  gauge2.get(&vec!["bar1", "baz3"]).inc();

  helper.assert_gauge_eq(1, "foo:gauge", &labels!("bar" => "bar1", "baz" => "baz1"));
  helper.assert_gauge_eq(
    1,
    "foo:gauge",
    &labels!("bar" => "label_overflow", "baz" => "label_overflow"),
  );
  helper.assert_counter_eq(2, "stats:label_tracker:label_overflow", &labels!());

  // Make sure histograms use the same label tracker.
  let labeled_histogram = labeled_metric_builder.build_histogram("histogram");
  let mut histogram1: LateInitializedHistogram = labeled_histogram.new_metric();
  histogram1.get(&vec!["bar1", "baz1"]).observe(1.0);

  helper.assert_histogram_count(
    1,
    "foo:histogram",
    &labels!("bar" => "bar1", "baz" => "baz1"),
  );
}
