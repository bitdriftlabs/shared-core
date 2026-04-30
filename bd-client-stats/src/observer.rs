// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

#[derive(Clone, Debug)]
pub struct ObservedMetric {
  pub action_id: String,
  pub labels: BTreeMap<String, String>,
  pub value: ObservedMetricValue,
}

#[derive(Clone, Debug)]
pub enum ObservedMetricValue {
  Counter(u64),
  InlineHistogram(Vec<f64>),
  DdSketchHistogram { encoded_len: usize },
}

#[derive(Clone, Debug)]
pub struct SnapshotObservation {
  pub metrics: Vec<ObservedMetric>,
}

#[derive(Clone, Debug)]
pub struct UploadAttemptObservation {
  pub upload_uuid: String,
  pub upload_reason: String,
  pub metrics: Vec<ObservedMetric>,
}

#[derive(Clone, Debug)]
pub struct UploadAckObservation {
  pub upload_uuid: String,
  pub success: bool,
}

pub trait StatsObserver: Send + Sync {
  fn on_snapshot(&self, observation: SnapshotObservation);

  fn on_upload_attempt(&self, observation: UploadAttemptObservation);

  fn on_upload_ack(&self, observation: UploadAckObservation);
}

static OBSERVER: LazyLock<RwLock<Option<Arc<dyn StatsObserver>>>> =
  LazyLock::new(|| RwLock::new(None));

pub fn set_observer(observer: Option<Arc<dyn StatsObserver>>) {
  *OBSERVER.write() = observer;
}

pub(crate) fn with_observer(f: impl FnOnce(&dyn StatsObserver)) {
  let observer = OBSERVER.read().clone();
  if let Some(observer) = observer {
    f(observer.as_ref());
  }
}
