// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::*;
use crate::test::util::stats::Helper;
use prometheus::labels;

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
