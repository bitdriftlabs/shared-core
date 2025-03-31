// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_proto::protos::client::api::stats_upload_request::snapshot::Snapshot_type;
use bd_proto::protos::client::api::StatsUploadRequest;
use bd_proto::protos::client::metric::metric::Data;
use bd_proto::protos::client::metric::{Metric, MetricsList};
use std::collections::{BTreeMap, HashMap};

#[macro_export]
macro_rules! float_eq {
  ($rhs:expr, $lhs:expr) => {
    ($rhs - $lhs).abs() < f64::EPSILON
  };
}

/// Counts the total number of stats present in the list of metric families.
#[must_use]
pub fn total_metrics(metrics: &[prometheus::proto::MetricFamily]) -> usize {
  metrics.iter().map(|f| f.get_metric().len()).sum()
}

// Attempts to extract a metric with the specific name and tags from the provided metrics familiy
// vec.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
#[must_use]
pub fn get_stat_value(
  metrics: &[prometheus::proto::MetricFamily],
  name: &str,
  tags: Vec<(&str, &str)>,
) -> Option<u64> {
  let tags = HashMap::from_iter(tags);
  metrics
    .iter()
    .find_map(|f| {
      (f.name() == name).then(|| {
        f.get_metric()
          .iter()
          .find(|m| {
            m.get_label()
              .iter()
              .map(|label| (label.name(), label.value()))
              .collect::<HashMap<&str, &str>>()
              == tags
          })
          .map(|m| m.get_counter().value() as u64)
      })
    })
    .flatten()
}

#[allow(clippy::needless_pass_by_value)]
#[must_use]
pub fn get_required_stat_value(
  metrics: &[prometheus::proto::MetricFamily],
  name: &str,
  tags: Vec<(&str, &str)>,
) -> u64 {
  get_stat_value(metrics, name, tags.clone())
    .unwrap_or_else(|| panic!("{name} {tags:?} not in {metrics:?}"))
}

pub struct StatRequestHelper {
  request: bd_proto::protos::client::api::StatsUploadRequest,
}

impl StatRequestHelper {
  pub const fn new(request: bd_proto::protos::client::api::StatsUploadRequest) -> Self {
    Self { request }
  }

  #[allow(clippy::needless_pass_by_value)]
  pub fn get_metric(&self, name: &str, fields: HashMap<&str, &str>) -> Option<u64> {
    assert_eq!(self.request.snapshot.len(), 1);
    let fields_str = fields
      .iter()
      .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
      .collect();

    self.request.snapshot[0]
      .metrics()
      .metric
      .iter()
      .find(|metric| metric.name == name && metric.tags == fields_str)
      .map(|m| m.counter().value)
  }
}

//
// ProstStatRequestHelper
//

pub struct StatsRequestHelper {
  request: StatsUploadRequest,
}

impl StatsRequestHelper {
  #[must_use]
  pub const fn new(request: StatsUploadRequest) -> Self {
    Self { request }
  }

  #[allow(clippy::needless_pass_by_value)]
  fn get_metric(&self, name: &str, fields: BTreeMap<&str, &str>) -> Option<&Metric> {
    assert_eq!(self.request.snapshot.len(), 1);

    let fields_str = fields
      .iter()
      .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
      .collect();

    self
      .metrics()
      .metric
      .iter()
      .find(|metric| metric.name == name && metric.tags == fields_str)
  }

  #[allow(clippy::needless_pass_by_value)]
  #[must_use]
  pub fn get_inline_histogram(&self, name: &str, fields: BTreeMap<&str, &str>) -> Option<Vec<f64>> {
    self.get_metric(name, fields).and_then(|metric| {
      metric.data.as_ref().map(|data| match data {
        Data::InlineHistogramValues(h) => h.values.clone(),
        Data::Counter(_) | Data::FixedBucketHistogramDeprecated(_) | Data::DdsketchHistogram(_) => {
          panic!("not an inline histogram")
        },
      })
    })
  }

  #[allow(clippy::needless_pass_by_value)]
  #[must_use]
  pub fn get_counter(&self, name: &str, fields: BTreeMap<&str, &str>) -> Option<u64> {
    self.get_metric(name, fields).and_then(|metric| {
      metric.data.as_ref().map(|data| match data {
        Data::Counter(c) => c.value,
        Data::FixedBucketHistogramDeprecated(_)
        | Data::DdsketchHistogram(_)
        | Data::InlineHistogramValues(_) => {
          panic!("not a counter")
        },
      })
    })
  }

  fn metrics(&self) -> &MetricsList {
    self
      .request
      .snapshot
      .first()
      .and_then(|snapshot| {
        snapshot
          .snapshot_type
          .as_ref()
          .map(|snapshot| match snapshot {
            Snapshot_type::Metrics(m) => m,
          })
      })
      .unwrap()
  }
}
