// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./lib_test.rs"]
mod lib_test;

pub mod test;

use bd_proto::protos::client::metric::metric::Data as ProtoMetricData;
use bd_proto::protos::client::metric::{
  Counter as ProtoCounter,
  DDSketchHistogram,
  InlineHistogramValues,
};
use bd_stats_common::{DynCounter, MetricType, NameType, make_client_sketch};
use parking_lot::Mutex;
use sketches_rust::DDSketch;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::watch;

#[cfg(test)]
#[ctor::ctor]
fn test_global_init() {
  bd_test_helpers::test_global_init();
}

const SEP: &str = ":";

// This was determined without any real science. An empty sketch is 17 bytes encoded (though we
// would never send an empty sketch). A sketch with 1 value is 21 bytes encoded. A sketch with
// 10 of the same value is 22 bytes encoded. A sketch with 10 values ranging in steps from 10 to
// 100 is 39 bytes encoded. Given that 5 double sent directly is 40 bytes, starting with 5 seems
// reasonable and we can further refine later.
const MAX_INLINE_HISTOGRAM_VALUES: usize = 5;

//
// Error
//

#[derive(thiserror::Error, Debug)]
pub enum Error {
  #[error("Overflow")]
  Overflow,
}

pub type Result<T> = std::result::Result<T, Error>;

//
// CounterWrapper
//

#[derive(Debug)]
pub struct CounterWrapper(Counter);

impl CounterWrapper {
  #[must_use]
  pub fn make_dyn(counter: Counter) -> DynCounter {
    Arc::new(Self(counter))
  }
}

impl bd_stats_common::Counter for CounterWrapper {
  fn inc(&self) {
    self.0.inc();
  }

  fn inc_by(&self, value: u64) {
    self.0.inc_by(value);
  }
}

//
// Counter
//

#[derive(Clone, Debug, Default)]
pub struct Counter {
  value: Arc<AtomicU64>,
}

impl Counter {
  pub fn inc(&self) {
    self.value.fetch_add(1, Ordering::Relaxed);
  }

  pub fn inc_by(&self, value: u64) {
    self.value.fetch_add(value, Ordering::Relaxed);
  }

  #[must_use]
  pub fn get(&self) -> u64 {
    self.value.load(Ordering::Relaxed)
  }

  fn snap(&self) -> Option<MetricData> {
    let previous = self.value.swap(0, Ordering::Relaxed);
    if previous == 0 {
      None
    } else {
      Some(MetricData::Counter(Self {
        value: Arc::new(AtomicU64::new(previous)),
      }))
    }
  }

  fn multiple_references(&self) -> bool {
    Arc::strong_count(&self.value) > 1
  }
}

//
// HistogramTimer
//

pub struct HistogramTimer {
  start: Instant,
  histogram: Histogram,
}

impl Drop for HistogramTimer {
  fn drop(&mut self) {
    self
      .histogram
      .inner
      .lock()
      .accept(self.start.elapsed().as_secs_f64());
  }
}

//
// HistogramInner
//

enum HistogramInner {
  Inline(Vec<f64>),
  DDSketch(DDSketch),
}

impl Default for HistogramInner {
  fn default() -> Self {
    Self::Inline(Vec::new())
  }
}

impl HistogramInner {
  fn accept(&mut self, value: f64) {
    match self {
      Self::Inline(values) => {
        if values.len() >= MAX_INLINE_HISTOGRAM_VALUES {
          log::trace!("switching from inline to DDSketch");
          self.switch_to_sketch(Some(value));
        } else {
          values.push(value);
        }
      },
      Self::DDSketch(sketch) => sketch.accept(value),
    }
  }

  fn switch_to_sketch(&mut self, additional_value: Option<f64>) {
    match self {
      Self::Inline(values) => {
        let mut sketch = make_client_sketch();
        for value in values {
          sketch.accept(*value);
        }
        if let Some(additional_value) = additional_value {
          sketch.accept(additional_value);
        }
        *self = Self::DDSketch(sketch);
      },
      Self::DDSketch(_) => (),
    }
  }
}

//
// Histogram
//

#[derive(Default, Clone)]
pub struct Histogram {
  inner: Arc<Mutex<HistogramInner>>,
}

impl Histogram {
  #[must_use]
  pub fn start_timer(&self) -> HistogramTimer {
    HistogramTimer {
      start: Instant::now(),
      histogram: self.clone(),
    }
  }

  pub fn observe(&self, value: f64) {
    self.inner.lock().accept(value);
  }

  pub fn merge_from(&self, other: &Self) {
    let mut self_inner = self.inner.lock();
    let other = other.inner.lock();
    match &*other {
      HistogramInner::Inline(values) => {
        // In this case regardless of the format of the self histogram we can just accept the
        // values.
        for value in values {
          self_inner.accept(*value);
        }
      },
      HistogramInner::DDSketch(sketch) => {
        // In this case we need to make sure that the self histogram is in the DDSketch format.
        self_inner.switch_to_sketch(None);
        if let HistogramInner::DDSketch(self_sketch) = &mut *self_inner {
          self_sketch.merge_with(sketch).unwrap();
        } else {
          unreachable!();
        }
      },
    }
  }

  fn snap(&self) -> Option<MetricData> {
    let mut inner = self.inner.lock();
    match &mut *inner {
      HistogramInner::Inline(values) => {
        if values.is_empty() {
          None
        } else {
          let metric = MetricData::Histogram(Self {
            inner: Arc::new(Mutex::new(HistogramInner::Inline(values.clone()))),
          });
          values.clear();
          Some(metric)
        }
      },
      HistogramInner::DDSketch(sketch) => {
        if sketch.is_empty() {
          None
        } else {
          let metric = MetricData::Histogram(Self {
            inner: Arc::new(Mutex::new(HistogramInner::DDSketch(sketch.clone()))),
          });
          sketch.clear();
          Some(metric)
        }
      },
    }
  }

  fn multiple_references(&self) -> bool {
    Arc::strong_count(&self.inner) > 1
  }
}

impl Debug for Histogram {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Histogram").finish()
  }
}

//
// Scope
//

#[derive(Clone)]
pub struct Scope {
  name: String,
  collector: Collector,
}

impl Scope {
  fn get_common_global(
    inner: &mut CollectorInner,
    name: String,
    metric_type: MetricType,
    labels: BTreeMap<String, String>,
    constructor: impl FnOnce() -> MetricData,
  ) -> &mut MetricData {
    let by_name = inner
      .metrics_by_name
      .entry(NameType::Global(metric_type, name))
      .or_default();

    match by_name.entry(labels) {
      Entry::Occupied(entry) => entry.into_mut(),
      Entry::Vacant(entry) => entry.insert(constructor()),
    }
  }

  #[must_use]
  pub fn counter_with_labels(&self, name: &str, labels: BTreeMap<String, String>) -> Counter {
    let mut inner = self.collector.inner.lock();
    match Self::get_common_global(
      &mut inner,
      self.metric_name(name),
      MetricType::Counter,
      labels,
      || {
        MetricData::Counter(Counter {
          value: Arc::new(AtomicU64::new(0)),
        })
      },
    ) {
      MetricData::Counter(counter) => counter.clone(),
      MetricData::Histogram(_) => unreachable!(),
    }
  }

  #[must_use]
  pub fn counter(&self, name: &str) -> Counter {
    self.counter_with_labels(name, BTreeMap::new())
  }

  #[must_use]
  pub fn histogram(&self, name: &str) -> Histogram {
    self.histogram_with_labels(name, BTreeMap::new())
  }

  #[must_use]
  pub fn histogram_with_labels(&self, name: &str, labels: BTreeMap<String, String>) -> Histogram {
    let mut inner = self.collector.inner.lock();
    match Self::get_common_global(
      &mut inner,
      self.metric_name(name),
      MetricType::Histogram,
      labels,
      || MetricData::Histogram(Histogram::default()),
    ) {
      MetricData::Histogram(histogram) => histogram.clone(),
      MetricData::Counter(_) => unreachable!(),
    }
  }

  #[must_use]
  pub fn scope(&self, name: &str) -> Self {
    Self {
      name: self.metric_name(name),
      collector: self.collector.clone(),
    }
  }

  // Build the final metric name from the current scope.
  fn metric_name(&self, name: &str) -> String {
    if self.name.is_empty() {
      name.to_string()
    } else {
      format!("{}{SEP}{name}", self.name)
    }
  }
}

//
// MetricData
//

#[derive(Debug)]
pub enum MetricData {
  Counter(Counter),
  Histogram(Histogram),
}

impl MetricData {
  pub fn from_proto(proto: ProtoMetricData) -> Option<Self> {
    match proto {
      ProtoMetricData::Counter(ProtoCounter { value, .. }) => Some(Self::Counter(Counter {
        value: Arc::new(AtomicU64::new(value)),
      })),
      ProtoMetricData::DdsketchHistogram(histogram) => {
        let mut sketch = make_client_sketch();
        sketch.decode_and_merge_with(&histogram.serialized).ok()?;
        Some(Self::Histogram(Histogram {
          inner: Arc::new(Mutex::new(HistogramInner::DDSketch(sketch))),
        }))
      },
      ProtoMetricData::InlineHistogramValues(InlineHistogramValues { values, .. }) => {
        Some(Self::Histogram(Histogram {
          inner: Arc::new(Mutex::new(HistogramInner::Inline(values))),
        }))
      },
    }
  }

  #[must_use]
  pub fn to_proto(&self) -> ProtoMetricData {
    match self {
      Self::Counter(counter) => ProtoMetricData::Counter(ProtoCounter {
        value: counter.get(),
        ..Default::default()
      }),
      Self::Histogram(histogram) => {
        let inner = histogram.inner.lock();
        match &*inner {
          HistogramInner::Inline(values) => {
            ProtoMetricData::InlineHistogramValues(InlineHistogramValues {
              values: values.clone(),
              ..Default::default()
            })
          },
          HistogramInner::DDSketch(sketch) => {
            ProtoMetricData::DdsketchHistogram(DDSketchHistogram {
              serialized: sketch.encode().unwrap(),
              ..Default::default()
            })
          },
        }
      },
    }
  }

  #[must_use]
  pub fn snap(&self) -> Option<Self> {
    match self {
      Self::Counter(counter) => counter.snap(),
      Self::Histogram(histogram) => histogram.snap(),
    }
  }

  #[must_use]
  pub fn multiple_references(&self) -> bool {
    match self {
      Self::Counter(counter) => counter.multiple_references(),
      Self::Histogram(histogram) => histogram.multiple_references(),
    }
  }
}

//
// Collector
//

pub type MetricsByName = HashMap<NameType, HashMap<BTreeMap<String, String>, MetricData>>;
struct CollectorInner {
  metrics_by_name: MetricsByName,
  limit: Option<watch::Receiver<u32>>,
}
#[derive(Clone)]
pub struct Collector {
  inner: Arc<Mutex<CollectorInner>>,
}

impl Default for Collector {
  fn default() -> Self {
    Self::new(None)
  }
}

impl Collector {
  #[must_use]
  pub fn new(limit: Option<watch::Receiver<u32>>) -> Self {
    Self {
      inner: Arc::new(Mutex::new(CollectorInner {
        metrics_by_name: HashMap::new(),
        limit,
      })),
    }
  }

  #[must_use]
  pub fn limit(&self) -> Option<u32> {
    self
      .inner
      .lock()
      .limit
      .as_ref()
      .map(|limit| *limit.borrow())
  }

  #[must_use]
  pub fn scope(&self, name: &str) -> Scope {
    Scope {
      name: name.to_string(),
      collector: self.clone(),
    }
  }

  #[must_use]
  pub fn find_counter(
    &self,
    name: &NameType,
    labels: &BTreeMap<String, String>,
  ) -> Option<Counter> {
    match self
      .inner
      .lock()
      .metrics_by_name
      .get(name)
      .and_then(|metrics| metrics.get(labels))
    {
      Some(MetricData::Counter(counter)) => Some(counter.clone()),
      _ => None,
    }
  }

  #[must_use]
  pub fn find_histogram(
    &self,
    name: &NameType,
    labels: &BTreeMap<String, String>,
  ) -> Option<Histogram> {
    match self
      .inner
      .lock()
      .metrics_by_name
      .get(name)
      .and_then(|metrics| metrics.get(labels))
    {
      Some(MetricData::Histogram(histogram)) => Some(histogram.clone()),
      _ => None,
    }
  }

  // Similar to HashMap, iterate over all metrics and decide whether to retain them in the map
  // or not.
  pub fn retain(
    &self,
    mut f: impl FnMut(&NameType, &BTreeMap<String, String>, &MetricData) -> bool,
  ) {
    self.inner.lock().metrics_by_name.retain(|name, metrics| {
      metrics.retain(|labels, metric| {
        let retain = f(name, labels, metric);
        if !retain {
          log::trace!("removing metric: {}/{labels:?}", name.as_str());
        }
        retain
      });

      !metrics.is_empty()
    });
  }

  fn get_common_workflow(
    inner: &mut CollectorInner,
    action_id: String,
    metric_type: MetricType,
    labels: BTreeMap<String, String>,
    constructor: impl FnOnce() -> MetricData,
  ) -> Result<&mut MetricData> {
    let by_name = inner
      .metrics_by_name
      .entry(NameType::ActionId(metric_type, action_id))
      .or_default();

    let by_name_len = by_name.len();
    match by_name.entry(labels) {
      Entry::Occupied(entry) => Ok(entry.into_mut()),
      Entry::Vacant(entry) => {
        if inner
          .limit
          .as_ref()
          .is_none_or(|limit| by_name_len < (*limit.borrow()) as usize)
        {
          Ok(entry.insert(constructor()))
        } else {
          Err(Error::Overflow)
        }
      },
    }
  }

  pub fn dynamic_counter(&self, tags: BTreeMap<String, String>, id: &str) -> Result<Counter> {
    let mut inner = self.inner.lock();
    match Self::get_common_workflow(
      &mut inner,
      id.to_string(),
      MetricType::Counter,
      tags,
      || {
        MetricData::Counter(Counter {
          value: Arc::new(AtomicU64::new(0)),
        })
      },
    )? {
      MetricData::Counter(counter) => Ok(counter.clone()),
      MetricData::Histogram(_) => unreachable!(),
    }
  }

  pub fn dynamic_histogram(&self, tags: BTreeMap<String, String>, id: &str) -> Result<Histogram> {
    let mut inner = self.inner.lock();
    match Self::get_common_workflow(
      &mut inner,
      id.to_string(),
      MetricType::Histogram,
      tags,
      || MetricData::Histogram(Histogram::default()),
    )? {
      MetricData::Histogram(histogram) => Ok(histogram.clone()),
      MetricData::Counter(_) => unreachable!(),
    }
  }
}
