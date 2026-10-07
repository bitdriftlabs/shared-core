use super::{OtlpCompression, OtlpMetric, encode_otlp_metrics, encode_otlp_metrics_with_metadata};
use crate::protos::metrics::metric::Data;
use crate::protos::metrics::{AggregationTemporality, number_data_point};
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
use protobuf::Message;
use snap::read::FrameDecoder;
use std::io::Read;

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
    sample_sum: 12.5,
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
fn rejects_invalid_summary_values_and_metric_metadata() {
  for (quantiles, sum) in [
    (vec![(0.5, 1.0)], f64::INFINITY),
    (vec![(0.5, f64::NAN)], 1.0),
    (vec![(f64::NAN, 1.0)], 1.0),
    (vec![(-0.1, 1.0)], 1.0),
    (vec![(1.1, 1.0)], 1.0),
    (vec![(0.5, 1.0), (0.5, 2.0)], 1.0),
    (vec![(-0.0, 1.0), (0.0, 1.0)], 1.0),
  ] {
    let value = MetricValue::Summary(SummaryData {
      sample_count: 1.0,
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
