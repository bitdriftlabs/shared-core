// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::MetricsCollector;
use crate::config::{ActionEmitMetric, TagValue};
use bd_client_stats::Stats;
use bd_client_stats_store::Collector;
use bd_client_stats_store::test::StatsHelper;
use bd_log_primitives::{FieldsRef, LogRef, LogType, log_level};
use bd_stats_common::{MetricType, NameType, labels};
use std::collections::BTreeMap;

fn make_metrics_collector() -> (MetricsCollector, Collector) {
  let collector = Collector::default();
  let stats = Stats::new(collector.clone());
  (MetricsCollector::new(stats), collector)
}

#[test]
fn metric_increment_value_extraction() {
  let fields = [("f1".into(), "1.1".into()), ("f2".into(), "10".into())].into();

  let matching_only_fields = [("m1".into(), "5".into())].into();

  let (metrics_collector, collector) = make_metrics_collector();

  metrics_collector.emit_metrics(
    &[
      &ActionEmitMetric {
        id: "action_id_1".to_string(),
        tags: BTreeMap::new(),
        increment: crate::config::ValueIncrement::Fixed(1),
        metric_type: MetricType::Counter,
      },
      &ActionEmitMetric {
        id: "action_id_2".to_string(),
        tags: BTreeMap::new(),
        increment: crate::config::ValueIncrement::Extract("f2".to_string()),
        metric_type: MetricType::Counter,
      },
      &ActionEmitMetric {
        id: "action_id_3".to_string(),
        tags: BTreeMap::new(),
        increment: crate::config::ValueIncrement::Extract("f1".to_string()),
        metric_type: MetricType::Counter,
      },
      &ActionEmitMetric {
        id: "action_id_4".to_string(),
        tags: BTreeMap::new(),
        increment: crate::config::ValueIncrement::Extract("does not exist".to_string()),
        metric_type: MetricType::Counter,
      },
      &ActionEmitMetric {
        id: "action_id_5".to_string(),
        tags: BTreeMap::new(),
        increment: crate::config::ValueIncrement::Extract("m1".to_string()),
        metric_type: MetricType::Counter,
      },
      &ActionEmitMetric {
        id: "action_id_6".to_string(),
        tags: BTreeMap::new(),
        increment: crate::config::ValueIncrement::Extract("m1".to_string()),
        metric_type: MetricType::Histogram,
      },
    ]
    .into(),
    &LogRef {
      message: &"message".into(),
      session_id: "session_id",
      occurred_at: time::OffsetDateTime::now_utc(),
      log_level: log_level::DEBUG,
      log_type: LogType::Normal,
      fields: FieldsRef::new(&fields, &matching_only_fields),
      capture_session: None,
    },
  );

  collector.assert_workflow_counter_eq(1, "action_id_1", labels! {});
  collector.assert_workflow_counter_eq(10, "action_id_2", labels! {});
  // The 1.0 is parsed as a float, then converted to an integer.
  collector.assert_workflow_counter_eq(1, "action_id_3", labels! {});

  // No counter emitted for action_id_4 as the field does not exist.
  assert!(
    collector
      .find_counter(
        &NameType::ActionId(MetricType::Counter, "action_id_4".to_string()),
        &labels! {}
      )
      .is_none()
  );

  // Values can be extracted from the matching_only_fields.
  collector.assert_workflow_counter_eq(5, "action_id_5", labels! {});
  // Values can be extracted from the matching_only_fields.
  collector.assert_workflow_histogram_observed(5.0, "action_id_6", labels! {});
}

#[test]
fn counter_label_extraction() {
  let fields = [("f1".into(), "foo".into()), ("f2".into(), "bar".into())].into();

  let matching_only_fields = [("m1".into(), "5".into())].into();

  let (metrics_collector, collector) = make_metrics_collector();

  metrics_collector.emit_metrics(
    &[&ActionEmitMetric {
      id: "action_id_1".to_string(),
      tags: [
        ("tag_1".to_string(), TagValue::Extract("f1".to_string())),
        ("tag_2".to_string(), TagValue::Extract("f2".to_string())),
        ("tag_3".to_string(), TagValue::Fixed("fixed".to_string())),
        ("tag_4".to_string(), TagValue::Extract("m1".to_string())),
        (
          "tag_5".to_string(),
          TagValue::Extract("log_level".to_string()),
        ),
        (
          "tag_6".to_string(),
          TagValue::Extract("log_type".to_string()),
        ),
      ]
      .into(),
      increment: crate::config::ValueIncrement::Fixed(1),
      metric_type: MetricType::Counter,
    }]
    .into(),
    &LogRef {
      message: &"message".into(),
      session_id: "session_id",
      occurred_at: time::OffsetDateTime::now_utc(),
      log_level: log_level::DEBUG,
      log_type: LogType::Normal,
      fields: FieldsRef::new(&fields, &matching_only_fields),
      capture_session: None,
    },
  );

  collector.assert_workflow_counter_eq(
    1,
    "action_id_1",
    labels! {
        "tag_1" => "foo",
        "tag_2" => "bar",
        "tag_3" => "fixed",
        "tag_4" => "5",
        "tag_5" => "1",
        "tag_6" => "0",
    },
  );
}
