// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::stats;
use bd_time::TimeDurationExt;
use prometheus::proto::{Counter, Gauge, Histogram, Metric};
use std::collections::HashMap;
use time::ext::NumericalDuration;

//
// Helper
//

// General helper for verifying emitted stats in tests.
#[derive(Clone)]
pub struct Helper {
  collector: stats::Collector,
}

impl Default for Helper {
  fn default() -> Self {
    Self::new()
  }
}

impl Helper {
  // Make a new stats helper with a default collector.
  #[must_use]
  pub fn new() -> Self {
    Self::new_with_collector(stats::Collector::default())
  }

  // Make a new stats helper given a collector.
  #[must_use]
  pub const fn new_with_collector(collector: stats::Collector) -> Self {
    Self { collector }
  }

  // Get the backing collector.
  #[must_use]
  pub const fn collector(&self) -> &stats::Collector {
    &self.collector
  }

  // Determine whether a given metric matches a set of labels.
  #[must_use]
  fn labels_equal(metric: &Metric, labels: &HashMap<&str, &str>) -> bool {
    if metric.get_label().len() != labels.len() {
      return false;
    }

    for label_pair in metric.get_label() {
      if !labels.contains_key(label_pair.name())
        || *labels.get(label_pair.name()).unwrap() != label_pair.value()
      {
        return false;
      }
    }

    true
  }

  // Find a metric with the right name and labels.
  #[must_use]
  fn find_metric(&self, name: &str, labels: &HashMap<&str, &str>) -> Option<Metric> {
    for metric_family in self.collector.gather() {
      if metric_family.name() == name {
        for metric in metric_family.get_metric() {
          if Self::labels_equal(metric, labels) {
            return Some(metric.clone());
          }
        }
      }
    }

    None
  }

  // Find a counter by name.
  #[must_use]
  pub fn find_counter(&self, name: &str, labels: &HashMap<&str, &str>) -> Option<Counter> {
    self
      .find_metric(name, labels)
      .and_then(|metric| metric.counter.into_option())
  }

  // Find a gauge by name.
  #[must_use]
  pub fn find_gauge(&self, name: &str, labels: &HashMap<&str, &str>) -> Option<Gauge> {
    self
      .find_metric(name, labels)
      .and_then(|metric| metric.gauge.into_option())
  }

  // Find a histogram by name.
  #[must_use]
  pub fn find_histogram(&self, name: &str, labels: &HashMap<&str, &str>) -> Option<Histogram> {
    self
      .find_metric(name, labels)
      .and_then(|metric| metric.histogram.into_option())
  }

  // Wait for a counter to be equal to some value, for up to 10s.
  pub async fn wait_for_counter_eq(&self, value: u64, name: &str, labels: &HashMap<&str, &str>) {
    self
      .wait_for_counter_eq_with_timeout(value, name, labels, 10.seconds())
      .await;
  }

  // Wait for a counter to be equal to some value, up to a timeout.
  #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
  pub async fn wait_for_counter_eq_with_timeout(
    &self,
    value: u64,
    name: &str,
    labels: &HashMap<&str, &str>,
    duration: time::Duration,
  ) {
    let now = tokio::time::Instant::now();
    let mut latest_value = None;
    loop {
      assert!(
        now.elapsed() < duration,
        "wait for counter '{name}' with labels '{labels:?}' timeout. Latest value: \
         '{latest_value:?}'",
      );
      latest_value.clone_from(&self.find_counter(name, labels));

      if latest_value
        .as_ref()
        .is_some_and(|counter| counter.value() as u64 == value)
      {
        return;
      }
      10.milliseconds().sleep().await;
    }
  }

  // Assert that a counter is equal to a value.
  #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
  pub fn assert_counter_eq(&self, value: u64, name: &str, labels: &HashMap<&str, &str>) {
    let actual = self
      .find_counter(name, labels)
      .unwrap_or_else(|| panic!("counter '{name}' with labels '{labels:?}' not found"))
      .value() as u64;
    assert_eq!(
      value, actual,
      "counter '{name}' with labels '{labels:?}' not equal to '{value}', was '{actual}'",
    );
  }

  // Assert that a gauge is equal to a value.
  #[allow(clippy::cast_possible_truncation)]
  pub fn assert_gauge_eq(&self, value: i64, name: &str, labels: &HashMap<&str, &str>) {
    assert_eq!(
      value,
      self
        .find_gauge(name, labels)
        .unwrap_or_else(|| panic!("gauge '{name}' with labels '{labels:?}' not found"))
        .value() as i64
    );
  }

  // Assert that a gauge is equal to a float value.
  pub fn assert_gauge_float_eq(&self, value: f64, name: &str, labels: &HashMap<&str, &str>) {
    assert_eq!(
      value,
      self
        .find_gauge(name, labels)
        .unwrap_or_else(|| panic!("gauge '{name}' with labels '{labels:?}' not found"))
        .value()
    );
  }

  // Assert that a histogram has a certain number of samples.
  pub fn assert_histogram_count(&self, value: u64, name: &str, labels: &HashMap<&str, &str>) {
    assert_eq!(
      value,
      self
        .find_histogram(name, labels)
        .unwrap_or_else(|| panic!("histogram '{name}' with labels '{labels:?}' not found"))
        .get_sample_count()
    );
  }
}

//
// TestLabelProvider
//

pub struct TestLabelProvider {}

impl stats::LabelProvider for TestLabelProvider {
  fn labels(&self) -> Vec<&str> {
    vec!["unknown"]
  }
}

//
// FakeMetricData
//

pub enum FakeMetricData {
  // Delta
  Counter(u64),
  // Samples
  Histogram(Vec<f64>),
}

//
// FakeMetric
//

pub struct FakeMetric {
  pub name: String,
  pub tags: HashMap<String, String>,
  pub metric: FakeMetricData,
}

impl FakeMetric {
  fn new(name: &str, tags: Vec<(&str, &str)>, metric: FakeMetricData) -> Self {
    Self {
      name: name.to_string(),
      tags: tags
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect(),
      metric,
    }
  }
}

// Verify histogram contents.
pub fn assert_histogram(histogram: &Histogram, count: u64, sum: f64, buckets: &[(f64, u64)]) {
  assert_eq!(histogram.get_sample_count(), count);
  assert!((histogram.get_sample_sum() - sum).abs() < f64::EPSILON);
  assert!(
    histogram.get_bucket().len() == buckets.len(),
    "buckets={}, expected={}",
    histogram.get_bucket().len(),
    buckets.len()
  );

  // TODO(mattklein123): Replace with iterator::eq_by() when stabilized.
  for (bucket, verify) in itertools::zip_eq(histogram.get_bucket(), buckets) {
    assert!((bucket.upper_bound() - verify.0).abs() < f64::EPSILON);
    assert_eq!(bucket.cumulative_count(), verify.1);
  }
}

// Create a fake histogram.
#[must_use]
pub fn fake_histogram(name: &str, tags: Vec<(&str, &str)>, samples: Vec<f64>) -> FakeMetric {
  FakeMetric::new(name, tags, FakeMetricData::Histogram(samples))
}

// Create a fake counter.
#[must_use]
pub fn fake_counter(name: &str, tags: Vec<(&str, &str)>, delta: u64) -> FakeMetric {
  FakeMetric::new(name, tags, FakeMetricData::Counter(delta))
}
