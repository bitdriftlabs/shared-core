// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./otlp_test.rs"]
mod tests;

use crate::metric::{CounterType, Metric as ModelMetric, MetricType, MetricValue};
use crate::protos::common::any_value::Value;
use crate::protos::common::{AnyValue, KeyValue};
use crate::protos::metrics::metric::Data;
use crate::protos::metrics::summary_data_point::ValueAtQuantile;
use crate::protos::metrics::{
  AggregationTemporality,
  ExponentialHistogram,
  ExponentialHistogramDataPoint,
  Gauge,
  Histogram,
  HistogramDataPoint,
  Metric,
  NumberDataPoint,
  ResourceMetrics,
  ScopeMetrics,
  Sum,
  Summary,
  SummaryDataPoint,
  number_data_point,
};
use crate::protos::metrics_service::ExportMetricsServiceRequest;
use bytes::Bytes;
use protobuf::{Chars, Message};
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::str;

//
// OtlpCompression
//

#[derive(Clone, Copy)]
pub enum OtlpCompression {
  None,
  Snappy,
}

//
// OtlpMetric
//

/// Per-point wire metadata. Model timestamps are seconds; interval starts are nanoseconds.
/// Numeric values retain the model's f64 precision; callers must check integer-to-float conversion.
#[derive(Clone, Debug)]
pub struct OtlpMetric {
  pub metric: ModelMetric,
  pub start_time_unix_nano: u64,
  pub is_monotonic: bool,
}

//
// OtlpPoint
//

#[derive(Clone, Debug)]
pub enum OtlpPoint {
  Metric(OtlpMetric),
  ExponentialHistogram {
    name: Chars,
    point: ExponentialHistogramDataPoint,
    temporality: CounterType,
  },
}

impl From<OtlpMetric> for OtlpPoint {
  fn from(metric: OtlpMetric) -> Self {
    Self::Metric(metric)
  }
}

fn validate_exponential_histogram(point: &ExponentialHistogramDataPoint) -> anyhow::Result<()> {
  if point.start_time_unix_nano >= point.time_unix_nano {
    anyhow::bail!("OTLP interval start must precede its end");
  }
  if !(-4 ..= 5).contains(&point.scale)
    || !point.zero_threshold.is_finite()
    || point.zero_threshold < 0.0
  {
    anyhow::bail!("invalid OTLP exponential histogram scale or zero threshold");
  }
  let mut count = point.zero_count;
  for buckets in point.positive.iter().chain(point.negative.iter()) {
    if buckets.bucket_counts.len() > 256
      || buckets
        .offset
        .checked_add(i32::try_from(buckets.bucket_counts.len())?)
        .is_none()
    {
      anyhow::bail!("OTLP exponential histogram bucket span exceeds its limit");
    }
    for bucket_count in &buckets.bucket_counts {
      count = count
        .checked_add(*bucket_count)
        .ok_or_else(|| anyhow::anyhow!("OTLP exponential histogram count overflow"))?;
    }
  }
  if count != point.count {
    anyhow::bail!("inconsistent OTLP exponential histogram counts");
  }
  if point.sum.is_some_and(|sum| {
    !sum.is_finite()
      || sum < 0.0
      || (point.count == 0 && sum != 0.0)
      || point
        .negative
        .iter()
        .any(|buckets| buckets.bucket_counts.iter().any(|count| *count > 0))
  }) {
    anyhow::bail!("OTLP exponential histogram sum requires finite nonnegative observations");
  }
  let mut attributes = HashSet::new();
  for attribute in &point.attributes {
    if !attributes.insert(&attribute.key) {
      anyhow::bail!("OTLP point attributes must be unique");
    }
  }
  Ok(())
}

fn validate_otlp_metric(sample: &OtlpMetric) -> anyhow::Result<()> {
  let id = sample.metric.get_id();
  str::from_utf8(id.name())?;
  for tag in id.tags() {
    str::from_utf8(&tag.tag)?;
    str::from_utf8(&tag.value)?;
  }
  let end = sample
    .metric
    .timestamp
    .checked_mul(1_000_000_000)
    .ok_or_else(|| anyhow::anyhow!("OTLP timestamp exceeds the nanosecond range"))?;
  if sample.start_time_unix_nano >= end {
    anyhow::bail!("OTLP interval start must precede its end");
  }
  let kind = id.mtype().unwrap_or(MetricType::Gauge);
  if sample.is_monotonic && !matches!(kind, MetricType::Counter(_)) {
    anyhow::bail!("OTLP monotonicity applies only to counters");
  }
  match (kind, &sample.metric.value) {
    (
      MetricType::Counter(_) | MetricType::Gauge | MetricType::DirectGauge,
      MetricValue::Simple(value),
    ) => {
      if !value.is_finite() || (sample.is_monotonic && *value < 0.0) {
        anyhow::bail!("OTLP numeric value must be finite and nonnegative for monotonic counters");
      }
    },
    (MetricType::Summary, MetricValue::Summary(summary)) => {
      if !summary.sample_count.is_finite()
        || summary.sample_count < 0.0
        || summary.sample_count >= 18_446_744_073_709_551_616.0
        || summary.sample_count.fract() != 0.0
      {
        anyhow::bail!("OTLP summary count must be an integral value in the u64 range");
      }
      if !summary.sample_sum.is_finite()
        || (summary.sample_count == 0.0 && summary.sample_sum != 0.0)
      {
        anyhow::bail!("OTLP summary sum must be finite and zero when count is zero");
      }
      if summary.quantiles.iter().any(|quantile| {
        !quantile.quantile.is_finite()
          || !(0.0 ..= 1.0).contains(&quantile.quantile)
          || !quantile.value.is_finite()
          || quantile.value < 0.0
      }) {
        anyhow::bail!("OTLP summary quantiles must be in [0, 1] with finite nonnegative values");
      }
      let mut quantiles: Vec<_> = summary
        .quantiles
        .iter()
        .map(|quantile| quantile.quantile)
        .collect();
      quantiles.sort_by(f64::total_cmp);
      quantiles.dedup();
      if quantiles.len() != summary.quantiles.len() {
        anyhow::bail!("OTLP summary quantiles must be unique");
      }
    },
    _ => anyhow::bail!("unsupported OTLP interval metric or mismatched value type"),
  }
  Ok(())
}

fn tags_to_key_value(metric: &ModelMetric) -> Vec<KeyValue> {
  metric
    .get_id()
    .tags()
    .iter()
    .filter_map(|tag| {
      Some(KeyValue {
        key: Chars::from_bytes(tag.tag.clone()).ok()?,
        value: Some(AnyValue {
          value: Some(Value::StringValue(
            Chars::from_bytes(tag.value.clone()).ok()?,
          )),
          ..Default::default()
        })
        .into(),
        ..Default::default()
      })
    })
    .collect()
}

fn make_simple_metric(
  samples: Vec<OtlpMetric>,
  name: Bytes,
  mtype: MetricType,
  is_monotonic: bool,
) -> Option<Metric> {
  let data_points = samples
    .into_iter()
    .map(|sample| NumberDataPoint {
      attributes: tags_to_key_value(&sample.metric),
      start_time_unix_nano: sample.start_time_unix_nano,
      time_unix_nano: sample.metric.timestamp * 1_000_000_000,
      value: Some(number_data_point::Value::AsDouble(
        sample.metric.value.to_simple(),
      )),
      ..Default::default()
    })
    .collect();

  Some(Metric {
    name: Chars::from_bytes(name).ok()?,
    data: Some(match mtype {
      MetricType::Gauge | MetricType::DirectGauge => Data::Gauge(Gauge {
        data_points,
        ..Default::default()
      }),
      MetricType::Counter(counter_type) => Data::Sum(Sum {
        data_points,
        is_monotonic,
        aggregation_temporality: match counter_type {
          CounterType::Absolute => {
            AggregationTemporality::AGGREGATION_TEMPORALITY_CUMULATIVE.into()
          },
          CounterType::Delta => AggregationTemporality::AGGREGATION_TEMPORALITY_DELTA.into(),
        },
        ..Default::default()
      }),
      _ => unreachable!(),
    }),
    ..Default::default()
  })
}

fn make_histogram_metric(samples: Vec<OtlpMetric>, name: Bytes) -> Option<Metric> {
  let data_points = samples
    .into_iter()
    .map(|sample| {
      let histogram = sample.metric.value.to_histogram();
      let mut bucket_counts: Vec<u64> = histogram
        .buckets
        .iter()
        .enumerate()
        .map(|(index, bucket)| {
          let count = if index == 0 {
            bucket.count
          } else {
            bucket.count - histogram.buckets[index - 1].count
          };
          #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
          {
            count as u64
          }
        })
        .collect();
      if let Some(last_bucket) = histogram.buckets.last() {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        bucket_counts.push((histogram.sample_count - last_bucket.count) as u64);
      }

      #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
      let count = histogram.sample_count as u64;
      HistogramDataPoint {
        attributes: tags_to_key_value(&sample.metric),
        start_time_unix_nano: sample.start_time_unix_nano,
        time_unix_nano: sample.metric.timestamp * 1_000_000_000,
        count,
        sum: Some(histogram.sample_sum),
        bucket_counts,
        explicit_bounds: histogram.buckets.iter().map(|bucket| bucket.le).collect(),
        ..Default::default()
      }
    })
    .collect();

  Some(Metric {
    name: Chars::from_bytes(name).ok()?,
    data: Some(Data::Histogram(Histogram {
      data_points,
      aggregation_temporality: AggregationTemporality::AGGREGATION_TEMPORALITY_CUMULATIVE.into(),
      ..Default::default()
    })),
    ..Default::default()
  })
}

fn make_summary_metric(
  samples: Vec<OtlpMetric>,
  name: Bytes,
  sort_quantiles: bool,
) -> Option<Metric> {
  let data_points = samples
    .into_iter()
    .map(|sample| {
      let summary = sample.metric.value.to_summary();
      #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
      let count = summary.sample_count as u64;
      let mut quantile_values: Vec<_> = summary
        .quantiles
        .iter()
        .map(|quantile| ValueAtQuantile {
          quantile: quantile.quantile,
          value: quantile.value,
          ..Default::default()
        })
        .collect();
      if sort_quantiles {
        quantile_values.sort_by(|left, right| left.quantile.total_cmp(&right.quantile));
      }
      SummaryDataPoint {
        attributes: tags_to_key_value(&sample.metric),
        start_time_unix_nano: sample.start_time_unix_nano,
        time_unix_nano: sample.metric.timestamp * 1_000_000_000,
        count,
        sum: summary.sample_sum,
        quantile_values,
        ..Default::default()
      }
    })
    .collect();

  Some(Metric {
    name: Chars::from_bytes(name).ok()?,
    data: Some(Data::Summary(Summary {
      data_points,
      ..Default::default()
    })),
    ..Default::default()
  })
}

#[must_use]
pub fn encode_otlp_metrics(samples: Vec<ModelMetric>, compression: OtlpCompression) -> Bytes {
  encode_samples(
    samples
      .into_iter()
      .map(|metric| OtlpMetric {
        metric,
        start_time_unix_nano: 0,
        is_monotonic: false,
      })
      .collect(),
    compression,
    false,
  )
}

pub fn encode_otlp_metrics_with_metadata(
  samples: Vec<OtlpMetric>,
  compression: OtlpCompression,
) -> anyhow::Result<Bytes> {
  for sample in &samples {
    validate_otlp_metric(sample)?;
  }
  Ok(encode_samples(samples, compression, true))
}

pub fn encode_otlp_points(
  points: Vec<OtlpPoint>,
  compression: OtlpCompression,
) -> anyhow::Result<Bytes> {
  let mut samples = Vec::new();
  let mut histograms: HashMap<Chars, (CounterType, Vec<ExponentialHistogramDataPoint>)> =
    HashMap::new();
  let mut kinds = HashMap::new();
  for point in points {
    let (name, kind, temporality) = match &point {
      OtlpPoint::Metric(sample) => {
        validate_otlp_metric(sample)?;
        (
          Chars::from_bytes(sample.metric.get_id().name().clone())?,
          Some((sample.metric.get_id().mtype(), sample.is_monotonic)),
          None,
        )
      },
      OtlpPoint::ExponentialHistogram {
        name,
        point,
        temporality,
      } => {
        validate_exponential_histogram(point)?;
        (name.clone(), None, Some(*temporality))
      },
    };
    if name.is_empty() {
      anyhow::bail!("OTLP metric name must not be empty");
    }
    if let Some(previous) = kinds.insert(name, (kind, temporality))
      && previous != (kind, temporality)
    {
      anyhow::bail!("conflicting OTLP metric types under the same name");
    }
    match point {
      OtlpPoint::Metric(sample) => samples.push(sample),
      OtlpPoint::ExponentialHistogram {
        name,
        point,
        temporality,
      } => {
        histograms
          .entry(name)
          .or_insert_with(|| (temporality, Vec::new()))
          .1
          .push(point);
      },
    }
  }
  let mut metrics = make_metrics(samples, true);
  metrics.extend(
    histograms
      .into_iter()
      .map(|(name, (temporality, data_points))| Metric {
        name,
        data: Some(Data::ExponentialHistogram(ExponentialHistogram {
          data_points,
          aggregation_temporality: match temporality {
            CounterType::Delta => AggregationTemporality::AGGREGATION_TEMPORALITY_DELTA.into(),
            CounterType::Absolute => {
              AggregationTemporality::AGGREGATION_TEMPORALITY_CUMULATIVE.into()
            },
          },
          ..Default::default()
        })),
        ..Default::default()
      }),
  );
  Ok(encode_request(metrics, compression))
}

fn encode_samples(
  samples: Vec<OtlpMetric>,
  compression: OtlpCompression,
  sort_quantiles: bool,
) -> Bytes {
  encode_request(make_metrics(samples, sort_quantiles), compression)
}

fn make_metrics(samples: Vec<OtlpMetric>, sort_quantiles: bool) -> Vec<Metric> {
  let metrics_by_name_and_type: HashMap<(Bytes, MetricType, bool), Vec<OtlpMetric>> = samples
    .into_iter()
    .fold(HashMap::new(), |mut metrics, sample| {
      let key = (
        sample.metric.get_id().name().clone(),
        sample.metric.get_id().mtype().unwrap_or(MetricType::Gauge),
        sample.is_monotonic,
      );
      metrics.entry(key).or_default().push(sample);
      metrics
    });

  metrics_by_name_and_type
    .into_iter()
    .filter_map(|((name, mtype, is_monotonic), samples)| match mtype {
      MetricType::Gauge | MetricType::DirectGauge | MetricType::Counter(_) => {
        make_simple_metric(samples, name, mtype, is_monotonic)
      },
      MetricType::Histogram => make_histogram_metric(samples, name),
      MetricType::Summary => make_summary_metric(samples, name, sort_quantiles),
      MetricType::DeltaGauge | MetricType::Timer | MetricType::BulkTimer => {
        log::warn!("unsupported OTLP metric type: {mtype:?}");
        None
      },
    })
    .collect()
}

fn encode_request(metrics: Vec<Metric>, compression: OtlpCompression) -> Bytes {
  let request = ExportMetricsServiceRequest {
    resource_metrics: vec![ResourceMetrics {
      scope_metrics: vec![ScopeMetrics {
        metrics,
        ..Default::default()
      }],
      ..Default::default()
    }],
    ..Default::default()
  };
  log::trace!("ExportMetricsServiceRequest batched and ready to send: {request}");

  let uncompressed = request.write_to_bytes().unwrap();
  match compression {
    OtlpCompression::None => uncompressed.into(),
    OtlpCompression::Snappy => {
      let mut compressed = Vec::new();
      snap::write::FrameEncoder::new(&mut compressed)
        .write_all(&uncompressed)
        .unwrap();
      compressed.into()
    },
  }
}

#[must_use]
pub fn deserialize_otlp_metrics_request(compressed: &[u8], compression: OtlpCompression) -> String {
  let decompressed = match compression {
    OtlpCompression::None => compressed.to_vec(),
    OtlpCompression::Snappy => {
      let mut decompressed = Vec::new();
      if let Err(error) = std::io::copy(
        &mut snap::read::FrameDecoder::new(compressed),
        &mut decompressed,
      ) {
        return format!("failed to decompress request: {error}");
      }
      decompressed
    },
  };
  match ExportMetricsServiceRequest::parse_from_bytes(&decompressed) {
    Ok(request) => request.to_string(),
    Err(error) => format!("failed to parse ExportMetricsServiceRequest: {error}"),
  }
}
