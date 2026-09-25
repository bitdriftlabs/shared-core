// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::metric::{CounterType, Metric as ModelMetric, MetricType};
use crate::protos::common::any_value::Value;
use crate::protos::common::{AnyValue, KeyValue};
use crate::protos::metrics::metric::Data;
use crate::protos::metrics::summary_data_point::ValueAtQuantile;
use crate::protos::metrics::{
  AggregationTemporality,
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
use std::collections::HashMap;
use std::io::Write;

//
// OtlpCompression
//

#[derive(Clone, Copy)]
pub enum OtlpCompression {
  None,
  Snappy,
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

fn make_simple_metric(samples: Vec<ModelMetric>, name: Bytes, mtype: MetricType) -> Option<Metric> {
  let data_points = samples
    .into_iter()
    .map(|sample| NumberDataPoint {
      attributes: tags_to_key_value(&sample),
      time_unix_nano: sample.timestamp * 1_000_000_000,
      value: Some(number_data_point::Value::AsDouble(sample.value.to_simple())),
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

fn make_histogram_metric(samples: Vec<ModelMetric>, name: Bytes) -> Option<Metric> {
  let data_points = samples
    .into_iter()
    .map(|sample| {
      let histogram = sample.value.to_histogram();
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
        attributes: tags_to_key_value(&sample),
        time_unix_nano: sample.timestamp * 1_000_000_000,
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

fn make_summary_metric(samples: Vec<ModelMetric>, name: Bytes) -> Option<Metric> {
  let data_points = samples
    .into_iter()
    .map(|sample| {
      let summary = sample.value.to_summary();
      #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
      let count = summary.sample_count as u64;
      SummaryDataPoint {
        attributes: tags_to_key_value(&sample),
        time_unix_nano: sample.timestamp * 1_000_000_000,
        count,
        sum: summary.sample_sum,
        quantile_values: summary
          .quantiles
          .iter()
          .map(|quantile| ValueAtQuantile {
            quantile: quantile.quantile,
            value: quantile.value,
            ..Default::default()
          })
          .collect(),
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
  let metrics_by_name_and_type: HashMap<(Bytes, MetricType), Vec<ModelMetric>> = samples
    .into_iter()
    .fold(HashMap::new(), |mut metrics, sample| {
      let key = (
        sample.get_id().name().clone(),
        sample.get_id().mtype().unwrap_or(MetricType::Gauge),
      );
      metrics.entry(key).or_default().push(sample);
      metrics
    });

  let metrics = metrics_by_name_and_type
    .into_iter()
    .filter_map(|((name, mtype), samples)| match mtype {
      MetricType::Gauge | MetricType::DirectGauge | MetricType::Counter(_) => {
        make_simple_metric(samples, name, mtype)
      },
      MetricType::Histogram => make_histogram_metric(samples, name),
      MetricType::Summary => make_summary_metric(samples, name),
      MetricType::DeltaGauge | MetricType::Timer | MetricType::BulkTimer => {
        log::warn!("unsupported OTLP metric type: {mtype:?}");
        None
      },
    })
    .collect();

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
