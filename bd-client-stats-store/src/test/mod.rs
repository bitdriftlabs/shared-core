// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{BoundedCollector, Collector, HistogramInner, MetricData};
use std::collections::BTreeMap;

pub trait StatsHelper {
  fn assert_counter_eq(&self, value: u64, name: &str, labels: BTreeMap<String, String>);
  fn assert_histogram_observed(&self, value: f64, name: &str, labels: BTreeMap<String, String>);
}

impl StatsHelper for Collector {
  #[allow(clippy::needless_pass_by_value)]
  fn assert_counter_eq(&self, value: u64, name: &str, labels: BTreeMap<String, String>) {
    self.inner().assert_counter_eq(value, name, labels);
  }

  #[allow(clippy::needless_pass_by_value)]
  fn assert_histogram_observed(&self, value: f64, name: &str, labels: BTreeMap<String, String>) {
    self.inner().assert_histogram_observed(value, name, labels);
  }
}

impl StatsHelper for BoundedCollector {
  #[allow(clippy::needless_pass_by_value)]
  fn assert_counter_eq(&self, value: u64, name: &str, labels: BTreeMap<String, String>) {
    assert_eq!(
      value,
      self
        .find_counter(name, labels.clone())
        .unwrap_or_else(|| panic!("Counter not found: {name} {labels:?}"))
        .get()
    );
  }

  fn assert_histogram_observed(&self, value: f64, name: &str, labels: BTreeMap<String, String>) {
    let histogram_values = match self
      .find_histogram(name, labels.clone())
      .unwrap_or_else(|| panic!("Histogram not found: {name} {labels:?}"))
      .snap()
      .unwrap()
    {
      MetricData::Histogram(h) => match *h.inner.lock() {
        HistogramInner::Inline(ref v) => v.clone(),
        HistogramInner::DDSketch(_) => panic!("Unexpected histogram type"),
      },
      MetricData::Counter(_) => panic!("Unexpected metric type"),
    };

    assert!(histogram_values.contains(&value));
  }
}
