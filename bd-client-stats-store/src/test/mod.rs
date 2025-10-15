// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// Test code only.
#![allow(clippy::unwrap_used, clippy::panic)]

use crate::{Collector, HistogramInner, MetricData, NameType};
use std::collections::BTreeMap;

#[async_trait::async_trait]
pub trait StatsHelper {
  fn assert_counter_eq(&self, value: u64, name: &str, labels: BTreeMap<String, String>);
  async fn wait_for_counter_eq(
    &self,
    value: u64,
    name: &str,
    labels: BTreeMap<String, String>,
  ) -> bool;
  fn assert_workflow_counter_eq(
    &self,
    value: u64,
    action_id: &str,
    labels: BTreeMap<String, String>,
  );
  fn assert_workflow_histogram_observed(
    &self,
    value: f64,
    action_id: &str,
    labels: BTreeMap<String, String>,
  );
}

#[async_trait::async_trait]
impl StatsHelper for Collector {
  async fn wait_for_counter_eq(
    &self,
    value: u64,
    name: &str,
    labels: BTreeMap<String, String>,
  ) -> bool {
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(50));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    for _ in 0 .. 20 {
      interval.tick().await;
      if let Some(counter) = self.find_counter(&NameType::Global(name.to_string()), &labels) {
        if counter.get() == value {
          return true;
        }
      }
    }

    if let Some(counter) = self.find_counter(&NameType::Global(name.to_string()), &labels) {
      if counter.get() == value {
        return true;
      } else {
        panic!(
          "Counter value did not match: {name} {labels:?} expected {value} got {}",
          counter.get()
        );
      }
    }

    panic!("Counter not found or value not matched: {name} {labels:?}");
  }

  #[allow(clippy::needless_pass_by_value)]
  fn assert_counter_eq(&self, value: u64, name: &str, labels: BTreeMap<String, String>) {
    assert_eq!(
      value,
      self
        .find_counter(&NameType::Global(name.to_string()), &labels)
        .unwrap_or_else(|| panic!("Counter not found: {name} {labels:?}"))
        .get()
    );
  }

  #[allow(clippy::needless_pass_by_value)]
  fn assert_workflow_counter_eq(
    &self,
    value: u64,
    action_id: &str,
    labels: BTreeMap<String, String>,
  ) {
    assert_eq!(
      value,
      self
        .find_counter(&NameType::ActionId(action_id.to_string()), &labels)
        .unwrap_or_else(|| panic!("Counter not found: {action_id} {labels:?}"))
        .get()
    );
  }

  fn assert_workflow_histogram_observed(
    &self,
    value: f64,
    action_id: &str,
    labels: BTreeMap<String, String>,
  ) {
    let histogram_values = match self
      .find_histogram(&NameType::ActionId(action_id.to_string()), &labels)
      .unwrap_or_else(|| panic!("Histogram not found: {action_id} {labels:?}"))
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
