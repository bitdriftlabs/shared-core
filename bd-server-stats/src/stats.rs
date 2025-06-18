// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./stats_test.rs"]
mod stats_test;

use bd_stats_common::{DynCounter, Id};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use parking_lot::Mutex;
use prometheus::core::Collector as PromCollector;
use prometheus::proto::MetricFamily;
use prometheus::{
  Encoder,
  Histogram,
  HistogramOpts,
  HistogramVec,
  IntCounter,
  IntCounterVec,
  IntGauge,
  IntGaugeVec,
  Opts,
  Registry,
  TextEncoder,
  opts,
  register_histogram_vec_with_registry,
  register_histogram_with_registry,
  register_int_counter_vec_with_registry,
  register_int_counter_with_registry,
  register_int_gauge_vec_with_registry,
  register_int_gauge_with_registry,
};
use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// TODO(mattklein123): Potentially add a mechanism to remove unused label variants in long lived
//                     processes. It's unclear if this will ever actually be an issue in practice.

//
// LabelTracker
//

// The maximum number of labels that a label tracker will store. This is arbitrary and can be
// tuned later.
const MAX_LABELS: u64 = 1000;

// Tracks a set of labels to guard against unlimited cardinality explosion.
struct LabelTracker {
  // Using DashMap vs. DashSet to get access to the entry() API.
  labels: DashMap<String, ()>,
  label_count: AtomicU64,
  max_labels: u64,
  label_overflow: IntCounter,
}

impl LabelTracker {
  // Create a new label tracker with a maximum label count.
  fn new(max_labels: u64, registry: &Registry) -> Arc<Self> {
    Arc::new(Self {
      labels: DashMap::new(),
      label_count: AtomicU64::new(0),
      max_labels,
      #[allow(clippy::ignored_unit_patterns)]
      label_overflow: register_int_counter_with_registry!(
        "stats:label_tracker:label_overflow",
        "-",
        registry
      )
      .unwrap(),
    })
  }

  // Track the requested labels to make sure that we don't explode cardinality accidentally. This
  // either return the passed in labels or return a set of fallback labels to use.
  fn track_labels<'a>(
    &self,
    label_names: &[&str],
    label_provider: &'a dyn LabelProvider,
  ) -> Vec<&'a str> {
    let provided_labels = label_provider.labels();
    debug_assert_eq!(
      label_names.len(),
      provided_labels.len(),
      "{label_names:?} does not match length {provided_labels:?}",
    );

    let mut use_fallback_labels = false;
    for (index, label_name) in label_names.iter().enumerate() {
      let final_label = concat_string::concat_string!(label_name, "_", provided_labels[index]);
      if let Entry::Vacant(entry) = self.labels.entry(final_label) {
        // See if there is space. This is not exact as we check the load only which can race, but
        // it's close enough.
        if self.label_count.load(Ordering::Relaxed) < self.max_labels {
          entry.insert(());
          self.label_count.fetch_add(1, Ordering::Relaxed);
        } else {
          use_fallback_labels = true;
          self.label_overflow.inc();
          break;
        }
      }
    }

    if use_fallback_labels {
      return vec!["label_overflow"; provided_labels.len()];
    }
    provided_labels
  }
}

//
// LabeledMetricBuilder
//

// Facilitates building labeled metrics with less parameter passing.
pub struct LabeledMetricBuilder {
  label_tracker: Arc<LabelTracker>,
  scope: Scope,
  label_names: Arc<Vec<&'static str>>,
}

impl LabeledMetricBuilder {
  // Create a new labeled counter with a specific name.
  #[must_use]
  pub fn build_counter(&self, name: &str) -> LabeledMetric<IntCounterVec> {
    self.build_generic(name, |name: &str| {
      IntCounterVec::new(
        Opts::new(self.scope.metric_name(name), "-"),
        &self.label_names,
      )
      .unwrap()
    })
  }

  // Create a new labeled gauge with a specific name.
  #[must_use]
  pub fn build_gauge(&self, name: &str) -> LabeledMetric<IntGaugeVec> {
    self.build_generic(name, |name: &str| {
      IntGaugeVec::new(
        Opts::new(self.scope.metric_name(name), "-"),
        &self.label_names,
      )
      .unwrap()
    })
  }

  // Common helper for building a histogram.
  fn build_histogram_helper(
    &self,
    name: &str,
    buckets: Option<&[f64]>,
  ) -> LabeledMetric<HistogramVec> {
    self.build_generic(name, |name: &str| {
      let mut histogram_opts = HistogramOpts::new(self.scope.metric_name(name), "-");
      if let Some(buckets) = buckets {
        histogram_opts = histogram_opts.buckets(buckets.to_vec());
      }
      HistogramVec::new(histogram_opts, &self.label_names).unwrap()
    })
  }

  // Create a new labeled histogram with a specific name and with specified buckets.
  #[must_use]
  pub fn build_histogram_with_buckets(
    &self,
    name: &str,
    buckets: &[f64],
  ) -> LabeledMetric<HistogramVec> {
    self.build_histogram_helper(name, Some(buckets))
  }

  // Create a new labeled histogram with a specific name.
  #[must_use]
  pub fn build_histogram(&self, name: &str) -> LabeledMetric<HistogramVec> {
    self.build_histogram_helper(name, None)
  }

  // Helper for building and registering a labeled metric.
  fn build_generic<
    MetricVecType: Clone + PromCollector + 'static,
    CreateFunc: Fn(&str) -> MetricVecType,
  >(
    &self,
    name: &str,
    create: CreateFunc,
  ) -> LabeledMetric<MetricVecType> {
    let metric_vec = create(name);

    self
      .scope
      .collector
      .inner
      .registry
      .register(Box::new(metric_vec.clone()))
      .unwrap();

    LabeledMetric {
      label_tracker: self.label_tracker.clone(),
      label_names: self.label_names.clone(),
      metric: metric_vec,
    }
  }
}

//
// LabeledMetric
//

// A metric with a specific set of label names that can produce late initialized labeled stats.
#[derive(Clone)]
pub struct LabeledMetric<MetricVecType: Clone> {
  label_tracker: Arc<LabelTracker>,
  label_names: Arc<Vec<&'static str>>,
  metric: MetricVecType,
}

impl<MetricVecType: Clone> LabeledMetric<MetricVecType> {
  // Create a new late initialized metric for later use.
  #[must_use]
  pub fn new_metric<MetricType>(&self) -> LateInitializedMetric<MetricVecType, MetricType> {
    LateInitializedMetric {
      labeled_metric: self.clone(),
      real_metric: None,
    }
  }
}

pub type LabeledCounter = LabeledMetric<IntCounterVec>;
pub type LabeledGauge = LabeledMetric<IntGaugeVec>;
pub type LabeledHistogram = LabeledMetric<HistogramVec>;

//
// LabelProvider
//

// Trait to provide labels to a LateInitializedMetric.
pub trait LabelProvider: Send + Sync {
  // Returns the current set of label names. The length of the vector and the order must match
  // that used to create the LabeledMetric.
  fn labels(&self) -> Vec<&str>;
}

impl LabelProvider for String {
  fn labels(&self) -> Vec<&str> {
    vec![self]
  }
}

impl LabelProvider for Vec<&str> {
  fn labels(&self) -> Vec<&str> {
    self.clone()
  }
}

//
// GetMetricWithLabels
//

// Trait to wrap with_label_values() for each prometheus metric vec type.
pub trait GetMetricWithLabels<MetricType> {
  fn with_label_values(&self, labels: &[&str]) -> MetricType;
}

impl GetMetricWithLabels<IntCounter> for IntCounterVec {
  fn with_label_values(&self, labels: &[&str]) -> IntCounter {
    self.with_label_values(labels)
  }
}

impl GetMetricWithLabels<IntGauge> for IntGaugeVec {
  fn with_label_values(&self, labels: &[&str]) -> IntGauge {
    self.with_label_values(labels)
  }
}

impl GetMetricWithLabels<Histogram> for HistogramVec {
  fn with_label_values(&self, labels: &[&str]) -> Histogram {
    self.with_label_values(labels)
  }
}

//
// LateInitializedMetric
//

// This is a metric backed by a LabeledMetric that has its real stat initialized on first use.
// This allows the labels to be provided at that time, since labels may not be known at
// initialization time.
pub struct LateInitializedMetric<MetricVecType: Clone, MetricType> {
  labeled_metric: LabeledMetric<MetricVecType>,
  real_metric: Option<MetricType>,
}

impl<MetricVecType: Clone + GetMetricWithLabels<MetricType>, MetricType>
  LateInitializedMetric<MetricVecType, MetricType>
{
  // Get the metric. Labels will be initialized on first use. Note that re-initialization is
  // not currently performed if the labels change for some reason.
  pub fn get(&mut self, label_provider: &dyn LabelProvider) -> &MetricType {
    self.real_metric.get_or_insert_with(|| {
      let final_labels = self
        .labeled_metric
        .label_tracker
        .track_labels(&self.labeled_metric.label_names, label_provider);

      self.labeled_metric.metric.with_label_values(&final_labels)
    })
  }
}

pub type LateInitializedCounter = LateInitializedMetric<IntCounterVec, IntCounter>;
pub type LateInitializedGauge = LateInitializedMetric<IntGaugeVec, IntGauge>;
pub type LateInitializedHistogram = LateInitializedMetric<HistogramVec, Histogram>;

//
// CounterWrapper
//

#[derive(Debug)]
pub struct CounterWrapper(IntCounter);

impl CounterWrapper {
  #[must_use]
  pub fn make_dyn(counter: IntCounter) -> DynCounter {
    Arc::new(Self(counter))
  }
}

impl bd_stats_common::Counter for CounterWrapper {
  fn inc(&self) {
    self.0.inc_by(1);
  }

  fn inc_by(&self, value: u64) {
    self.0.inc_by(value);
  }
}

//
// Scope
//

// Named metrics scope used to create metrics.
#[derive(Clone)]
#[allow(clippy::struct_field_names)]
pub struct Scope {
  name: String,
  collector: Collector,
  labels: HashMap<String, String>,
}

const SEP: &str = ":";

impl Scope {
  // Create final labels from both scope labels and input labels.
  fn final_labels(&self, labels: HashMap<String, String>) -> HashMap<String, String> {
    let mut new_labels = self.labels.clone();
    new_labels.extend(labels);
    new_labels
  }

  // Create a sub-scope.
  #[must_use]
  pub fn scope(&self, extend: &str) -> Self {
    self.scope_with_labels(extend, HashMap::new())
  }

  // Create a sub-scope with a set of fixed labels.
  #[must_use]
  pub fn scope_with_labels(&self, extend: &str, labels: HashMap<String, String>) -> Self {
    let name = if extend.is_empty() {
      self.name.clone()
    } else {
      self.metric_name(extend)
    };

    Self {
      name,
      collector: self.collector.clone(),
      labels: self.final_labels(labels),
    }
  }

  // Create a new labeled metric builder.
  #[must_use]
  pub fn labeled_metric_builder(&self, label_names: &[&'static str]) -> LabeledMetricBuilder {
    LabeledMetricBuilder {
      label_tracker: self.collector.inner.label_tracker.clone(),
      scope: self.clone(),
      label_names: Arc::new(label_names.to_vec()),
    }
  }

  // Create a new counter with scope labels.
  #[must_use]
  pub fn counter(&self, name: &str) -> IntCounter {
    self.counter_with_labels(name, HashMap::new())
  }

  // Create a new counter with scope and provided labels.
  #[must_use]
  pub fn counter_with_labels(&self, name: &str, labels: HashMap<String, String>) -> IntCounter {
    let name = self.metric_name(name);
    let labels = self.final_labels(labels);
    self
      .collector
      .inner
      .counters
      .entry(Id::new(name.clone(), labels.clone().into_iter().collect()))
      .or_insert_with(|| {
        let opts = opts!(name, "-").const_labels(labels);
        #[allow(clippy::ignored_unit_patterns)]
        register_int_counter_with_registry!(opts, self.collector.inner.registry).unwrap()
      })
      .clone()
  }

  // Create a new counter vec that can be used to produce labeled counters.
  #[must_use]
  pub fn counter_vec(&self, name: &str, labels: &[&str]) -> IntCounterVec {
    #[allow(clippy::ignored_unit_patterns)]
    register_int_counter_vec_with_registry!(
      self.metric_name(name),
      "-",
      labels,
      self.collector.inner.registry
    )
    .unwrap()
  }

  // Create a new gauge with scope labels.
  #[must_use]
  pub fn gauge(&self, name: &str) -> IntGauge {
    let name = self.metric_name(name);
    self
      .collector
      .inner
      .gauges
      .entry(Id::new(
        name.clone(),
        self.labels.clone().into_iter().collect(),
      ))
      .or_insert_with(|| {
        let opts = opts!(name, "-").const_labels(self.labels.clone());
        #[allow(clippy::ignored_unit_patterns)]
        register_int_gauge_with_registry!(opts, self.collector.inner.registry).unwrap()
      })
      .clone()
  }

  // Create a new gauge vec that can be used to produce labeled counters.
  #[must_use]
  pub fn gauge_vec(&self, name: &str, labels: &[&str]) -> IntGaugeVec {
    #[allow(clippy::ignored_unit_patterns)]
    register_int_gauge_vec_with_registry!(
      self.metric_name(name),
      "-",
      labels,
      self.collector.inner.registry
    )
    .unwrap()
  }

  // Create a new histogram with scope labels.
  #[must_use]
  pub fn histogram(&self, name: &str) -> Histogram {
    self.histogram_ex(name, None)
  }

  fn histogram_ex(&self, name: &str, buckets: Option<&[f64]>) -> Histogram {
    let name = self.metric_name(name);
    self
      .collector
      .inner
      .histograms
      .entry(Id::new(
        name.clone(),
        self.labels.clone().into_iter().collect(),
      ))
      .or_insert_with(|| {
        let mut histogram_opts = HistogramOpts::new(name, "-");
        histogram_opts = histogram_opts.const_labels(self.labels.clone());
        histogram_opts = if let Some(buckets) = buckets {
          histogram_opts.buckets(buckets.to_vec())
        } else {
          histogram_opts
        };
        #[allow(clippy::ignored_unit_patterns)]
        register_histogram_with_registry!(histogram_opts, self.collector.inner.registry).unwrap()
      })
      .clone()
  }

  // Create a new histogram vec that can be used to produce labeled counters.
  #[must_use]
  pub fn histogram_vec(&self, name: &str, labels: &[&str]) -> HistogramVec {
    #[allow(clippy::ignored_unit_patterns)]
    register_histogram_vec_with_registry!(
      self.metric_name(name),
      "-",
      labels,
      self.collector.inner.registry
    )
    .unwrap()
  }

  // Create a new histogram with custom buckets and scope labels.
  #[must_use]
  pub fn histogram_with_buckets(&self, name: &str, buckets: &[f64]) -> Histogram {
    self.histogram_ex(name, Some(buckets))
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
// GatherMetrics
//

// Trait that wraps a source for gathering metrics.
pub trait GatherMetrics: Send + Sync {
  // Gather all metrics from the source.
  fn gather(&self) -> Vec<MetricFamily>;
}

//
// Collector
//

type OnGatherCallback = Box<dyn Fn() + Send>;

// Wrapper around a prometheus collector.
struct CollectorInner {
  registry: Registry,
  label_tracker: Arc<LabelTracker>,
  chained_collectors: Mutex<Vec<Arc<dyn GatherMetrics>>>,
  on_gather_callbacks: Mutex<Vec<OnGatherCallback>>,
  counters: DashMap<Id, IntCounter>,
  gauges: DashMap<Id, IntGauge>,
  histograms: DashMap<Id, Histogram>,
}
#[derive(Clone)]
pub struct Collector {
  inner: Arc<CollectorInner>,
}

impl Default for Collector {
  // Create a new collector with default settings.
  fn default() -> Self {
    Self::new_with_label_limit(MAX_LABELS)
  }
}

impl Collector {
  // Create a new collector with a specific label limit.
  #[must_use]
  pub fn new_with_label_limit(label_limit: u64) -> Self {
    let registry = Registry::default();
    let label_tracker = LabelTracker::new(label_limit, &registry);
    Self {
      inner: Arc::new(CollectorInner {
        registry,
        label_tracker,
        chained_collectors: Mutex::default(),
        on_gather_callbacks: Mutex::default(),
        counters: DashMap::new(),
        gauges: DashMap::new(),
        histograms: DashMap::new(),
      }),
    }
  }

  // Create a named scope.
  #[must_use]
  pub fn scope(&self, name: &str) -> Scope {
    Scope {
      name: name.to_string(),
      collector: self.clone(),
      labels: HashMap::new(),
    }
  }

  // Dump output for debugging stat names.
  #[must_use]
  pub fn debug_output(&self) -> String {
    let mut output = String::new();

    for family in self.gather_all() {
      for metric in &family.metric {
        let labels = metric
          .label
          .iter()
          .map(|label| format!("{}: {}", label.name(), label.value()))
          .collect::<Vec<_>>()
          .join(", ");
        writeln!(&mut output, "{}: ({})", family.name(), labels).unwrap();
      }
    }

    output
  }

  // Dump prometheus output.
  #[must_use]
  pub fn prometheus_output(&self) -> Vec<u8> {
    let output = self.gather_all();
    let encoder = TextEncoder::new();
    let mut buffer = vec![];

    encoder.encode(&output, &mut buffer).unwrap();
    buffer
  }

  // Gather all metrics in proto form.
  #[must_use]
  pub fn gather(&self) -> Vec<MetricFamily> {
    self.gather_all()
  }

  // Add a callback that will be called before metrics are gathered during scrape. This can be used
  // to update on demand metrics.
  pub fn add_gather_callback(&self, callback: impl Fn() + Send + 'static) {
    self
      .inner
      .on_gather_callbacks
      .lock()
      .push(Box::new(callback));
  }

  // Chain a collector into this collector. The passed collector with be gathered and concatenated
  // to this collector during output.
  pub fn chain(&self, collector: Arc<dyn GatherMetrics>) {
    self.inner.chained_collectors.lock().push(collector);
  }

  // Gather all metrics including chained collectors.
  fn gather_all(&self) -> Vec<MetricFamily> {
    for callback in &*self.inner.on_gather_callbacks.lock() {
      callback();
    }

    // Note: This code makes no attempt to check whether chained collectors have overlapping (same
    // name metric families). This is guaranteed in the current codebase but is admittedly fragile.
    let mut output = self.inner.registry.gather();
    for collector in &*self.inner.chained_collectors.lock() {
      output.append(&mut collector.gather());
    }
    output
  }
}

//
// StackAutoGauge
//

// Helper to increment a gauge and decrement it on destruction.
pub struct StackAutoGauge<'a> {
  gauge: &'a IntGauge,
}

impl<'a> StackAutoGauge<'a> {
  // Create a new AutoGauge that will increment on construction and decrement on
  // destruction.
  #[must_use]
  pub fn new(gauge: &'a IntGauge) -> Self {
    gauge.add(1);
    Self { gauge }
  }
}

impl Drop for StackAutoGauge<'_> {
  fn drop(&mut self) {
    debug_assert!(self.gauge.get() > 0);
    self.gauge.sub(1);
  }
}

//
// AutoGauge
//

// Helper to increment a gauge and decrement it on destruction.
pub struct AutoGauge {
  gauge: IntGauge,
}

impl AutoGauge {
  // Create a new AutoGauge that will increment on construction and decrement on
  // destruction.
  #[must_use]
  pub fn new(gauge: IntGauge) -> Self {
    gauge.add(1);
    Self { gauge }
  }
}

impl Drop for AutoGauge {
  fn drop(&mut self) {
    debug_assert!(self.gauge.get() > 0);
    self.gauge.sub(1);
  }
}
