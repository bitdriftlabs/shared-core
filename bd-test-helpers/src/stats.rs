// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use assert_matches::assert_matches;
use bd_proto::protos::client::api::stats_upload_request::snapshot::{
  Aggregated,
  Occurred_at,
  Snapshot_type,
};
use bd_proto::protos::client::api::StatsUploadRequest;
use bd_proto::protos::client::metric::metric::Data;
use bd_proto::protos::client::metric::{Metric, MetricsList};
use bd_stats_common::{make_client_sketch, NameType};
use bd_time::TimestampExt;
use std::collections::{BTreeMap, HashMap};
use time::OffsetDateTime;

#[macro_export]
macro_rules! float_eq {
  ($rhs:expr, $lhs:expr) => {
    ($rhs - $lhs).abs() < f64::EPSILON
  };
  ($rhs:expr, $lhs:expr, $type:ident) => {
    ($rhs - $lhs).abs() < $type::EPSILON
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

//
// StatRequestHelper
//

pub struct StatsRequestHelper {
  pub request: StatsUploadRequest,
}

impl StatsRequestHelper {
  #[must_use]
  pub const fn new(request: StatsUploadRequest) -> Self {
    Self { request }
  }

  #[allow(clippy::needless_pass_by_value)]
  fn get_metric(&self, name: NameType, fields: BTreeMap<&str, &str>) -> Option<&Metric> {
    assert_eq!(self.request.snapshot.len(), 1);

    let fields_str = fields
      .iter()
      .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
      .collect();

    self
      .metrics()
      .metric
      .iter()
      .find(|metric| match &name {
        NameType::Global(name) => metric.name() == name,
        NameType::ActionId(id) => metric.metric_id() == id,
      } && metric.tags == fields_str)
  }

  #[allow(clippy::needless_pass_by_value)]
  #[must_use]
  pub fn get_inline_histogram(&self, id: &str, fields: BTreeMap<&str, &str>) -> Option<Vec<f64>> {
    self
      .get_metric(NameType::ActionId(id.to_string()), fields)
      .and_then(|metric| {
        metric.data.as_ref().map(|data| match data {
          Data::InlineHistogramValues(h) => h.values.clone(),
          Data::Counter(_) | Data::DdsketchHistogram(_) => {
            panic!("not an inline histogram")
          },
        })
      })
  }

  #[allow(clippy::float_cmp, clippy::cast_precision_loss)]
  pub fn expect_ddsketch_histogram(
    &self,
    name: &str,
    fields: BTreeMap<&str, &str>,
    sum: f64,
    count: u64,
  ) {
    let histogram = self
      .get_metric(NameType::Global(name.to_string()), fields)
      .and_then(|c| c.has_ddsketch_histogram().then(|| c.ddsketch_histogram()))
      .expect("missing histogram");

    let mut sketch = make_client_sketch();
    sketch.decode_and_merge_with(&histogram.serialized).unwrap();
    assert_eq!(sketch.get_count(), count as f64);
    assert!(
      float_eq!(sketch.get_sum().unwrap(), sum),
      "sum: {}, sketch sum: {}",
      sum,
      sketch.get_sum().unwrap()
    );

    // TODO(mattklein123): Add verification for the actual quantiles?
  }

  pub fn expect_inline_histogram(&self, name: &str, fields: BTreeMap<&str, &str>, values: &[f64]) {
    let histogram = self
      .get_metric(NameType::Global(name.to_string()), fields)
      .and_then(|c| {
        c.has_inline_histogram_values()
          .then(|| c.inline_histogram_values())
      })
      .expect("missing histogram");

    assert_eq!(histogram.values, values);
  }

  #[allow(clippy::needless_pass_by_value)]
  #[must_use]
  pub fn get_counter(&self, name: &str, fields: BTreeMap<&str, &str>) -> Option<u64> {
    self.get_counter_inner(NameType::Global(name.to_string()), fields)
  }

  #[allow(clippy::needless_pass_by_value)]
  #[must_use]
  pub fn get_workflow_counter(&self, id: &str, fields: BTreeMap<&str, &str>) -> Option<u64> {
    self.get_counter_inner(NameType::ActionId(id.to_string()), fields)
  }

  #[allow(clippy::needless_pass_by_value)]
  fn get_counter_inner(&self, name: NameType, fields: BTreeMap<&str, &str>) -> Option<u64> {
    self.get_metric(name, fields).and_then(|metric| {
      metric.data.as_ref().map(|data| match data {
        Data::Counter(c) => c.value,
        Data::DdsketchHistogram(_) | Data::InlineHistogramValues(_) => {
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

  pub fn overflows(&self) -> &HashMap<String, u64> {
    &self.request.snapshot.first().unwrap().metric_id_overflows
  }

  pub fn aggregation_window_start(&self) -> OffsetDateTime {
    assert_eq!(self.request.snapshot.len(), 1);
    assert_matches!(
      &self.request.snapshot[0].occurred_at,
      Some(Occurred_at::Aggregated(Aggregated {
        period_start,
        ..
      })) => period_start.to_offset_date_time()
    )
  }

  pub fn aggregation_window_end(&self) -> OffsetDateTime {
    assert_eq!(self.request.snapshot.len(), 1);
    assert_matches!(
      &self.request.snapshot[0].occurred_at,
      Some(Occurred_at::Aggregated(Aggregated {
        period_end,
        ..
      })) => period_end.to_offset_date_time()
    )
  }

  pub fn number_of_metrics(&self) -> usize {
    assert!(self.request.snapshot.len() <= 1);
    if self.request.snapshot.is_empty() {
      return 0;
    }

    self.request.snapshot[0].metrics().metric.len()
  }
}
