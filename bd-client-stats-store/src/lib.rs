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
use bd_stats_common::{DynCounter, Id};
use parking_lot::Mutex;
use sketches_rust::DDSketch;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
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

#[must_use]
pub fn make_sketch() -> DDSketch {
  // For this sketch, the index parameters must be consistent in order to perform merges.
  // However, the maximum number of buckets do not have to be the same. This means that
  // we can use less buckets on the client and more on the server if we like as long as
  // the other index parameters are the same. For now using the logarithmic collapsing
  // variant seems the best. 0.02 relative accuracy is arbitrary as well as a max of 128
  // buckets. We can tune this later as we have more time to devote to understanding the
  // math.
  DDSketch::logarithmic_collapsing_lowest_dense(0.02, 128).unwrap()
}

//
// Error
//

#[derive(thiserror::Error, Debug)]
pub enum Error {
  #[error("Changed type")]
  ChangedType,

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
        let mut sketch = make_sketch();
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
// BoundedScope
//

#[derive(Clone)]
pub struct BoundedScope {
  name: String,
  collector: BoundedCollector,
}

impl BoundedScope {
  fn get_common(
    inner: &mut BoundedCollectorInner,
    name: String,
    labels: BTreeMap<String, String>,
    constructor: impl FnOnce() -> MetricData,
  ) -> Result<&mut MetricData> {
    let can_insert = inner
      .limit
      .as_ref()
      .is_none_or(|limit| inner.metrics.len() < (*limit.borrow()) as usize);

    match inner.metrics.entry(Id::new(name, labels)) {
      Entry::Occupied(entry) => Ok(entry.into_mut()),
      Entry::Vacant(entry) => {
        if !can_insert {
          return Err(Error::Overflow);
        }
        Ok(entry.insert(constructor()))
      },
    }
  }

  pub fn counter_with_labels(
    &self,
    name: &str,
    labels: BTreeMap<String, String>,
  ) -> Result<Counter> {
    let mut inner = self.collector.inner.lock();
    match Self::get_common(&mut inner, self.metric_name(name), labels, || {
      MetricData::Counter(Counter {
        value: Arc::new(AtomicU64::new(0)),
      })
    })? {
      MetricData::Counter(counter) => Ok(counter.clone()),
      MetricData::Histogram(_) => Err(Error::ChangedType),
    }
  }

  pub fn counter(&self, name: &str) -> Result<Counter> {
    self.counter_with_labels(name, BTreeMap::new())
  }

  pub fn histogram(&self, name: &str) -> Result<Histogram> {
    let mut inner = self.collector.inner.lock();
    match Self::get_common(&mut inner, self.metric_name(name), BTreeMap::new(), || {
      MetricData::Histogram(Histogram::default())
    })? {
      MetricData::Histogram(histogram) => Ok(histogram.clone()),
      MetricData::Counter(_) => Err(Error::ChangedType),
    }
  }

  pub fn histogram_with_labels(
    &self,
    name: &str,
    labels: BTreeMap<String, String>,
  ) -> Result<Histogram> {
    let mut inner = self.collector.inner.lock();
    match Self::get_common(&mut inner, self.metric_name(name), labels, || {
      MetricData::Histogram(Histogram::default())
    })? {
      MetricData::Histogram(histogram) => Ok(histogram.clone()),
      MetricData::Counter(_) => Err(Error::ChangedType),
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

pub enum MetricData {
  Counter(Counter),
  Histogram(Histogram),
}

impl MetricData {
  pub fn from_proto(proto: ProtoMetricData) -> Option<Self> {
    match proto {
      ProtoMetricData::Counter(ProtoCounter {
        value,
        value_deprecated,
        ..
      }) => Some(Self::Counter(Counter {
        value: Arc::new(AtomicU64::new(value + u64::from(value_deprecated))),
      })),
      ProtoMetricData::DdsketchHistogram(histogram) => {
        let mut sketch = make_sketch();
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
      ProtoMetricData::FixedBucketHistogramDeprecated(_) => None,
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
// BoundedCollector
//

struct BoundedCollectorInner {
  metrics: HashMap<Id, MetricData>,
  limit: Option<watch::Receiver<u32>>,
}
#[derive(Clone)]
pub struct BoundedCollector {
  inner: Arc<Mutex<BoundedCollectorInner>>,
}

impl BoundedCollector {
  #[must_use]
  pub fn new(limit: Option<watch::Receiver<u32>>) -> Self {
    Self {
      inner: Arc::new(Mutex::new(BoundedCollectorInner {
        metrics: HashMap::new(),
        limit,
      })),
    }
  }

  #[must_use]
  pub fn scope(&self, name: &str) -> BoundedScope {
    BoundedScope {
      name: name.to_string(),
      collector: self.clone(),
    }
  }

  #[must_use]
  pub fn find_counter(&self, name: &str, labels: BTreeMap<String, String>) -> Option<Counter> {
    match self
      .inner
      .lock()
      .metrics
      .get(&Id::new(name.to_string(), labels))
    {
      Some(MetricData::Counter(counter)) => Some(counter.clone()),
      _ => None,
    }
  }

  #[must_use]
  pub fn find_histogram(&self, name: &str, labels: BTreeMap<String, String>) -> Option<Histogram> {
    match self
      .inner
      .lock()
      .metrics
      .get(&Id::new(name.to_string(), labels))
    {
      Some(MetricData::Histogram(histogram)) => Some(histogram.clone()),
      _ => None,
    }
  }

  // Similar to HashMap, iterate over all metrics and decide whether to retain them in the map
  // or not.
  pub fn retain(&self, mut f: impl FnMut(&Id, &MetricData) -> bool) {
    self.inner.lock().metrics.retain(|id, metric| {
      let retain = f(id, metric);
      if !retain {
        log::trace!("removing metric: {id:?}");
      }
      retain
    });
  }
}

//
// Collector
//

#[derive(Clone)]
pub struct Collector(BoundedCollector);

impl Default for Collector {
  fn default() -> Self {
    Self(BoundedCollector::new(None))
  }
}

impl Collector {
  #[must_use]
  pub const fn inner(&self) -> &BoundedCollector {
    &self.0
  }

  #[must_use]
  pub fn scope(&self, name: &str) -> Scope {
    Scope(self.0.scope(name))
  }
}

//
// Scope
//

#[derive(Clone)]
pub struct Scope(BoundedScope);

impl Scope {
  #[must_use]
  pub fn scope(&self, name: &str) -> Self {
    Self(self.0.scope(name))
  }

  #[must_use]
  pub fn counter(&self, name: &str) -> Counter {
    self.0.counter(name).expect("unbounded")
  }

  #[must_use]
  pub fn counter_with_labels(&self, name: &str, labels: BTreeMap<String, String>) -> Counter {
    self.0.counter_with_labels(name, labels).expect("unbounded")
  }

  #[must_use]
  pub fn histogram(&self, name: &str) -> Histogram {
    self.0.histogram(name).expect("unbounded")
  }
}
