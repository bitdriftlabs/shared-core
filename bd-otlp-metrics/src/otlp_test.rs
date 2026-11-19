// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{
  OtlpCompression,
  OtlpMetric,
  OtlpPoint,
  encode_otlp_metrics,
  encode_otlp_metrics_with_metadata,
  encode_otlp_points,
};
use crate::protos::metrics::exponential_histogram_data_point::Buckets;
use crate::protos::metrics::metric::Data;
use crate::protos::metrics::{
  AggregationTemporality,
  ExponentialHistogramDataPoint,
  number_data_point,
};
use crate::protos::metrics_service::ExportMetricsServiceRequest;
use crate::{
  CounterType,
  Metric,
  MetricId,
  MetricType,
  MetricValue,
  SummaryBucket,
  SummaryData,
  TagValue,
};
use bytes::Bytes;
use protobuf::Message;
use snap::read::FrameDecoder;
use std::io::Read;
use std::slice;

fn metric(kind: MetricType, timestamp: u64, value: MetricValue) -> Metric {
  Metric::new(
    MetricId::new(
      "metric".into(),
      Some(kind),
      vec![TagValue {
        tag: "region".into(),
        value: "west".into(),
      }],
      false,
    )
    .unwrap(),
    None,
    timestamp,
    value,
  )
}

fn summary(count: f64) -> MetricValue {
  MetricValue::Summary(SummaryData {
    sample_count: count,
    sample_sum: if count == 0.0 { 0.0 } else { 12.5 },
    quantiles: vec![
      SummaryBucket {
        quantile: 0.99,
        value: 9.0,
      },
      SummaryBucket {
        quantile: 0.5,
        value: 3.0,
      },
    ],
  })
}

fn decode(samples: Vec<OtlpMetric>) -> ExportMetricsServiceRequest {
  ExportMetricsServiceRequest::parse_from_bytes(
    &encode_otlp_metrics_with_metadata(samples, OtlpCompression::None).unwrap(),
  )
  .unwrap()
}

fn exponential_point() -> ExponentialHistogramDataPoint {
  ExponentialHistogramDataPoint {
    start_time_unix_nano: 120_000_000_000,
    time_unix_nano: 180_000_000_000,
    count: 9_007_199_254_740_993,
    scale: 5,
    zero_count: 1,
    positive: Some(Buckets {
      offset: -1,
      bucket_counts: vec![9_007_199_254_740_992],
      ..Default::default()
    })
    .into(),
    ..Default::default()
  }
}

#[test]
fn exponential_histograms_keep_integer_counts_and_temporality_in_mixed_batches() {
  assert_mixed_histogram_round_trip(&exponential_point());
}

#[test]
fn exponential_histograms_round_trip_scales_outside_aggregation_range() {
  for scale in [-5, 6] {
    let mut point = exponential_point();
    point.scale = scale;
    assert_mixed_histogram_round_trip(&point);
  }
}

#[test]
fn exponential_histograms_round_trip_more_than_256_buckets() {
  for negative in [false, true] {
    let mut point = exponential_point();
    point.scale = 0;
    point.count = 257;
    point.zero_count = 0;
    point.positive = Some(Buckets {
      offset: 0,
      bucket_counts: vec![1; 257],
      ..Default::default()
    })
    .into();
    if negative {
      point.negative = point.positive.take().into();
    }
    assert_mixed_histogram_round_trip(&point);
  }
}

fn assert_mixed_histogram_round_trip(point: &ExponentialHistogramDataPoint) {
  for (compression, temporality) in [
    (OtlpCompression::None, CounterType::Delta),
    (OtlpCompression::Snappy, CounterType::Delta),
    (OtlpCompression::None, CounterType::Absolute),
    (OtlpCompression::Snappy, CounterType::Absolute),
  ] {
    let encoded = encode_otlp_points(
      vec![
        OtlpPoint::ExponentialHistogram {
          name: "histogram".into(),
          point: point.clone(),
          temporality,
        },
        OtlpMetric {
          metric: metric(MetricType::Summary, 180, summary(3.0)),
          start_time_unix_nano: 120_000_000_000,
          is_monotonic: false,
        }
        .into(),
      ],
      compression,
    )
    .unwrap();
    let decoded = super::deserialize_otlp_metrics_request(&encoded, compression);
    assert!(decoded.contains("exponential_histogram"));
    if matches!(compression, OtlpCompression::None) {
      let request = ExportMetricsServiceRequest::parse_from_bytes(&encoded).unwrap();
      let metrics = &request.resource_metrics[0].scope_metrics[0].metrics;
      assert_eq!(metrics.len(), 2);
      let histogram = metrics
        .iter()
        .find_map(|metric| match &metric.data {
          Some(Data::ExponentialHistogram(histogram)) => Some(histogram),
          _ => None,
        })
        .unwrap();
      assert_eq!(
        histogram.aggregation_temporality.enum_value().unwrap(),
        match temporality {
          CounterType::Delta => AggregationTemporality::AGGREGATION_TEMPORALITY_DELTA,
          CounterType::Absolute => AggregationTemporality::AGGREGATION_TEMPORALITY_CUMULATIVE,
        }
      );
      assert_eq!(histogram.data_points.as_slice(), slice::from_ref(point));
    }
  }
}

#[test]
fn exponential_histograms_reject_invalid_counts_spans_and_optional_sums() {
  let mut invalid = Vec::new();
  let mut point = exponential_point();
  point.count += 1;
  invalid.push(point);
  let mut point = exponential_point();
  point.positive.as_mut().unwrap().offset = i32::MAX;
  invalid.push(point);
  let mut point = exponential_point();
  point.count = u64::MAX;
  point.positive.as_mut().unwrap().bucket_counts = vec![u64::MAX];
  invalid.push(point);
  let mut point = exponential_point();
  point.start_time_unix_nano = point.time_unix_nano;
  invalid.push(point);
  let mut point = exponential_point();
  point.zero_threshold = f64::NAN;
  invalid.push(point);
  for sum in [f64::INFINITY, -1.0] {
    let mut point = exponential_point();
    point.sum = Some(sum);
    invalid.push(point);
  }
  let mut point = exponential_point();
  point.negative = point.positive.take().into();
  point.sum = Some(1.0);
  invalid.push(point);
  for point in invalid {
    assert!(
      encode_otlp_points(
        vec![OtlpPoint::ExponentialHistogram {
          name: "histogram".into(),
          point,
          temporality: CounterType::Delta,
        }],
        OtlpCompression::None
      )
      .is_err()
    );
  }
}

#[test]
fn implicit_and_explicit_gauges_can_share_a_metric_name() {
  let points = [None, Some(MetricType::Gauge)]
    .into_iter()
    .map(|kind| {
      OtlpMetric {
        metric: Metric::new(
          MetricId::new("metric".into(), kind, vec![], false).unwrap(),
          None,
          180,
          MetricValue::Simple(1.0),
        ),
        start_time_unix_nano: 120_000_000_000,
        is_monotonic: false,
      }
      .into()
    })
    .collect();
  let encoded = encode_otlp_points(points, OtlpCompression::None).unwrap();
  let request = ExportMetricsServiceRequest::parse_from_bytes(&encoded).unwrap();
  let metrics = &request.resource_metrics[0].scope_metrics[0].metrics;
  assert_eq!(metrics.len(), 1);
  let Some(Data::Gauge(gauge)) = &metrics[0].data else {
    panic!("expected gauge");
  };
  assert_eq!(gauge.data_points.len(), 2);
}

#[test]
fn mixed_wire_types_cannot_share_a_metric_name() {
  assert!(
    encode_otlp_points(
      vec![
        OtlpPoint::ExponentialHistogram {
          name: "metric".into(),
          point: exponential_point(),
          temporality: CounterType::Delta,
        },
        OtlpMetric {
          metric: metric(MetricType::Summary, 180, summary(3.0)),
          start_time_unix_nano: 120_000_000_000,
          is_monotonic: false,
        }
        .into(),
      ],
      OtlpCompression::None
    )
    .is_err()
  );
}

#[test]
fn histogram_temporalities_cannot_share_a_metric_name() {
  assert!(
    encode_otlp_points(
      [CounterType::Delta, CounterType::Absolute]
        .into_iter()
        .map(|temporality| {
          OtlpPoint::ExponentialHistogram {
            name: "histogram".into(),
            point: exponential_point(),
            temporality,
          }
        })
        .collect(),
      OtlpCompression::None,
    )
    .is_err()
  );
}

#[test]
fn counters_preserve_independent_intervals_temporality_and_monotonicity() {
  let request = decode(vec![
    OtlpMetric {
      metric: metric(
        MetricType::Counter(CounterType::Delta),
        180,
        MetricValue::Simple(42.0),
      ),
      start_time_unix_nano: 120_000_000_000,
      is_monotonic: true,
    },
    OtlpMetric {
      metric: metric(
        MetricType::Counter(CounterType::Delta),
        120,
        MetricValue::Simple(7.0),
      ),
      start_time_unix_nano: 60_000_000_000,
      is_monotonic: true,
    },
  ]);
  let metrics = &request.resource_metrics[0].scope_metrics[0].metrics;
  assert_eq!(metrics.len(), 1);
  let Some(Data::Sum(sum)) = &metrics[0].data else {
    panic!("expected sum")
  };
  assert!(sum.is_monotonic);
  assert_eq!(
    sum.aggregation_temporality.enum_value().unwrap(),
    AggregationTemporality::AGGREGATION_TEMPORALITY_DELTA
  );
  assert_eq!(sum.data_points[0].start_time_unix_nano, 120_000_000_000);
  assert_eq!(sum.data_points[0].time_unix_nano, 180_000_000_000);
  assert_eq!(sum.data_points[1].start_time_unix_nano, 60_000_000_000);
  assert_eq!(sum.data_points[1].time_unix_nano, 120_000_000_000);
  assert_eq!(
    sum.data_points[0].value,
    Some(number_data_point::Value::AsDouble(42.0))
  );
  assert_eq!(&*sum.data_points[0].attributes[0].key, "region");
}

#[test]
fn summary_encodes_interval_count_sum_and_sorted_quantiles() {
  let request = decode(vec![OtlpMetric {
    metric: metric(MetricType::Summary, 180, summary(3.0)),
    start_time_unix_nano: 120_000_000_000,
    is_monotonic: false,
  }]);
  let Some(Data::Summary(summary)) = &request.resource_metrics[0].scope_metrics[0].metrics[0].data
  else {
    panic!("expected summary")
  };
  let point = &summary.data_points[0];
  assert_eq!(point.start_time_unix_nano, 120_000_000_000);
  assert_eq!(point.time_unix_nano, 180_000_000_000);
  assert_eq!(point.count, 3);
  assert_eq!(point.sum, 12.5);
  assert_eq!(
    point
      .quantile_values
      .iter()
      .map(|quantile| (quantile.quantile, quantile.value))
      .collect::<Vec<_>>(),
    vec![(0.5, 3.0), (0.99, 9.0)]
  );
}

#[test]
fn legacy_encoder_retains_unknown_starts_and_nonmonotonic_sums() {
  for counter_type in [CounterType::Delta, CounterType::Absolute] {
    let request = ExportMetricsServiceRequest::parse_from_bytes(&encode_otlp_metrics(
      vec![metric(
        MetricType::Counter(counter_type),
        180,
        MetricValue::Simple(42.0),
      )],
      OtlpCompression::None,
    ))
    .unwrap();
    let Some(Data::Sum(sum)) = &request.resource_metrics[0].scope_metrics[0].metrics[0].data else {
      panic!("expected sum")
    };
    assert!(!sum.is_monotonic);
    assert_eq!(sum.data_points[0].start_time_unix_nano, 0);
    assert_eq!(
      sum.aggregation_temporality.enum_value().unwrap(),
      match counter_type {
        CounterType::Delta => AggregationTemporality::AGGREGATION_TEMPORALITY_DELTA,
        CounterType::Absolute => AggregationTemporality::AGGREGATION_TEMPORALITY_CUMULATIVE,
      }
    );
  }
  let request = ExportMetricsServiceRequest::parse_from_bytes(&encode_otlp_metrics(
    vec![metric(MetricType::Summary, 180, summary(3.0))],
    OtlpCompression::None,
  ))
  .unwrap();
  let Some(Data::Summary(summary)) = &request.resource_metrics[0].scope_metrics[0].metrics[0].data
  else {
    panic!("expected summary")
  };
  assert_eq!(summary.data_points[0].start_time_unix_nano, 0);
  assert_eq!(summary.data_points[0].quantile_values[0].quantile, 0.99);
}

#[test]
fn rejects_invalid_intervals_and_numeric_values() {
  for (start, end) in [
    (180_000_000_000, 180),
    (181_000_000_000, 180),
    (0, u64::MAX),
  ] {
    assert!(
      encode_otlp_metrics_with_metadata(
        vec![OtlpMetric {
          metric: metric(
            MetricType::Counter(CounterType::Delta),
            end,
            MetricValue::Simple(1.0)
          ),
          start_time_unix_nano: start,
          is_monotonic: true
        }],
        OtlpCompression::None
      )
      .is_err()
    );
  }
  for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -1.0] {
    assert!(
      encode_otlp_metrics_with_metadata(
        vec![OtlpMetric {
          metric: metric(
            MetricType::Counter(CounterType::Delta),
            180,
            MetricValue::Simple(value)
          ),
          start_time_unix_nano: 120_000_000_000,
          is_monotonic: true
        }],
        OtlpCompression::None
      )
      .is_err()
    );
  }
  for count in [
    f64::NAN,
    f64::INFINITY,
    -1.0,
    1.5,
    18_446_744_073_709_551_616.0,
  ] {
    assert!(
      encode_otlp_metrics_with_metadata(
        vec![OtlpMetric {
          metric: metric(MetricType::Summary, 180, summary(count)),
          start_time_unix_nano: 120_000_000_000,
          is_monotonic: false
        }],
        OtlpCompression::None
      )
      .is_err()
    );
  }
}

#[test]
fn double_precision_is_preserved_without_claiming_integer_precision() {
  for value in [
    9_007_199_254_740_991.0,
    9_007_199_254_740_992.0,
    9_007_199_254_740_994.0,
    f64::MAX,
  ] {
    let request = decode(vec![OtlpMetric {
      metric: metric(
        MetricType::Counter(CounterType::Delta),
        180,
        MetricValue::Simple(value),
      ),
      start_time_unix_nano: 120_000_000_000,
      is_monotonic: true,
    }]);
    let Some(Data::Sum(sum)) = &request.resource_metrics[0].scope_metrics[0].metrics[0].data else {
      panic!("expected sum")
    };
    assert_eq!(
      sum.data_points[0].value,
      Some(number_data_point::Value::AsDouble(value))
    );
  }
}

#[test]
fn summary_count_accepts_zero_and_the_largest_representable_u64_value() {
  for (count, expected) in [
    (0.0, 0),
    (18_446_744_073_709_549_568.0, 18_446_744_073_709_549_568),
  ] {
    let request = decode(vec![OtlpMetric {
      metric: metric(MetricType::Summary, 180, summary(count)),
      start_time_unix_nano: 120_000_000_000,
      is_monotonic: false,
    }]);
    let Some(Data::Summary(summary)) =
      &request.resource_metrics[0].scope_metrics[0].metrics[0].data
    else {
      panic!("expected summary")
    };
    assert_eq!(summary.data_points[0].count, expected);
  }
}

#[test]
fn rejects_non_utf8_metric_names_and_attributes() {
  let valid = Bytes::from_static(b"valid");
  let invalid = Bytes::from_static(b"\xff");
  for (name, key, value) in [
    (invalid.clone(), valid.clone(), valid.clone()),
    (valid.clone(), invalid.clone(), valid.clone()),
    (valid.clone(), valid, invalid),
  ] {
    let metric = Metric::new(
      MetricId::new(
        name,
        Some(MetricType::Counter(CounterType::Delta)),
        vec![TagValue { tag: key, value }],
        false,
      )
      .unwrap(),
      None,
      180,
      MetricValue::Simple(1.0),
    );
    assert!(
      encode_otlp_metrics_with_metadata(
        vec![OtlpMetric {
          metric,
          start_time_unix_nano: 120_000_000_000,
          is_monotonic: true,
        }],
        OtlpCompression::None,
      )
      .is_err()
    );
  }
}

#[test]
fn rejects_invalid_summary_values_and_metric_metadata() {
  for (count, quantiles, sum) in [
    (0.0, vec![], 12.5),
    (0.0, vec![], -1.0),
    (1.0, vec![(0.5, -1.0)], 1.0),
    (1.0, vec![(0.5, 1.0)], f64::INFINITY),
    (1.0, vec![(0.5, f64::NAN)], 1.0),
    (1.0, vec![(f64::NAN, 1.0)], 1.0),
    (1.0, vec![(-0.1, 1.0)], 1.0),
    (1.0, vec![(1.1, 1.0)], 1.0),
    (1.0, vec![(0.5, 1.0), (0.5, 2.0)], 1.0),
    (1.0, vec![(-0.0, 1.0), (0.0, 1.0)], 1.0),
  ] {
    let value = MetricValue::Summary(SummaryData {
      sample_count: count,
      sample_sum: sum,
      quantiles: quantiles
        .into_iter()
        .map(|(quantile, value)| SummaryBucket { quantile, value })
        .collect(),
    });
    assert!(
      encode_otlp_metrics_with_metadata(
        vec![OtlpMetric {
          metric: metric(MetricType::Summary, 180, value),
          start_time_unix_nano: 120_000_000_000,
          is_monotonic: false
        }],
        OtlpCompression::None
      )
      .is_err()
    );
  }
  for (kind, value, is_monotonic) in [
    (MetricType::Gauge, MetricValue::Simple(1.0), true),
    (MetricType::Counter(CounterType::Delta), summary(1.0), true),
    (MetricType::Timer, MetricValue::Simple(1.0), false),
  ] {
    assert!(
      encode_otlp_metrics_with_metadata(
        vec![OtlpMetric {
          metric: metric(kind, 180, value),
          start_time_unix_nano: 120_000_000_000,
          is_monotonic
        }],
        OtlpCompression::None
      )
      .is_err()
    );
  }
}

#[test]
fn monotonicity_is_part_of_batch_grouping_and_snappy_remains_supported() {
  let samples = [false, true]
    .into_iter()
    .map(|is_monotonic| OtlpMetric {
      metric: metric(
        MetricType::Counter(CounterType::Absolute),
        180,
        MetricValue::Simple(42.0),
      ),
      start_time_unix_nano: 120_000_000_000,
      is_monotonic,
    })
    .collect();
  let bytes = encode_otlp_metrics_with_metadata(samples, OtlpCompression::Snappy).unwrap();
  let mut decoded = Vec::new();
  FrameDecoder::new(bytes.as_ref())
    .read_to_end(&mut decoded)
    .unwrap();
  let request = ExportMetricsServiceRequest::parse_from_bytes(&decoded).unwrap();
  let metrics = &request.resource_metrics[0].scope_metrics[0].metrics;
  assert_eq!(metrics.len(), 2);
  let mut monotonicities = Vec::new();
  for metric in metrics {
    let Some(Data::Sum(sum)) = &metric.data else {
      panic!("expected sum")
    };
    assert_eq!(
      sum.aggregation_temporality.enum_value().unwrap(),
      AggregationTemporality::AGGREGATION_TEMPORALITY_CUMULATIVE
    );
    assert_eq!(sum.data_points[0].start_time_unix_nano, 120_000_000_000);
    monotonicities.push(sum.is_monotonic);
  }
  monotonicities.sort_unstable();
  assert_eq!(monotonicities, vec![false, true]);
}
