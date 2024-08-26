// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::MetricsCollector;
use crate::config::{ActionEmitMetric, InsightsDimensions, MetricType, TagValue};
use bd_client_stats::DynamicStats;
use bd_client_stats_store::test::StatsHelper;
use bd_client_stats_store::{BoundedCollector, Collector};
use bd_log_primitives::{log_level, FieldsRef, LogField, LogRef, LogType};
use bd_runtime::runtime::ConfigLoader;
use bd_stats_common::labels;
use bd_test_helpers::workflow::macros::{insight, insights};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

struct Setup {
  sdk_directory: Arc<tempfile::TempDir>,
  collector: Collector,
}

impl Setup {
  fn new() -> Self {
    Self {
      sdk_directory: Arc::new(tempfile::TempDir::with_prefix("bd-metrics_collector").unwrap()),
      collector: Collector::default(),
    }
  }

  fn make_metrics_collector(&self) -> (MetricsCollector, BoundedCollector) {
    let dynamic_stats = DynamicStats::new(
      &self.collector.scope(""),
      &ConfigLoader::new(self.sdk_directory.path()),
    );
    let collector = dynamic_stats.collector_for_test().clone();

    let dynamic_stats = Arc::new(dynamic_stats);
    (MetricsCollector::new(dynamic_stats), collector)
  }
}


#[test]
fn metric_increment_value_extraction() {
  let fields = vec![
    LogField {
      key: "f1".to_string(),
      value: "1.1".into(),
    },
    LogField {
      key: "f2".to_string(),
      value: "10".into(),
    },
  ];

  let matching_only_fields = vec![LogField {
    key: "m1".to_string(),
    value: "5".into(),
  }];

  let setup = Setup::new();
  let (metrics_collector, dynamic_stats_collector) = setup.make_metrics_collector();

  metrics_collector.emit_metric_actions(
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
    &BTreeMap::new(),
    &LogRef {
      message: &"message".into(),
      session_id: "session_id",
      occurred_at: time::OffsetDateTime::now_utc(),
      log_level: log_level::DEBUG,
      log_type: LogType::Normal,
      fields: &FieldsRef::new(&fields, &matching_only_fields),
    },
  );

  dynamic_stats_collector.assert_counter_eq(
    1,
    "workflows_dyn:action",
    labels! {
    "_id" => "action_id_1",
    },
  );

  dynamic_stats_collector.assert_counter_eq(
    10,
    "workflows_dyn:action",
    labels! {
    "_id" => "action_id_2",
    },
  );
  // The 1.0 is parsed as a float, then converted to an integer.
  dynamic_stats_collector.assert_counter_eq(
    1,
    "workflows_dyn:action",
    labels! {
    "_id" => "action_id_3",
    },
  );

  // No counter emitted for action_id_4 as the field does not exist.
  assert!(dynamic_stats_collector
    .find_counter(
      "workflows_dyn:action",
      labels! {
      "_id" => "action_id_4",
      },
    )
    .is_none());

  // Values can be extracted from the matching_only_fields.
  dynamic_stats_collector.assert_counter_eq(
    5,
    "workflows_dyn:action",
    labels! {
    "_id" => "action_id_5",
    },
  );
  // Values can be extracted from the matching_only_fields.
  dynamic_stats_collector.assert_histogram_observed(
    5.0,
    "workflows_dyn:histogram",
    labels! {
    "_id" => "action_id_6",
    },
  );
}

#[test]
fn counter_label_extraction() {
  let fields = vec![
    LogField {
      key: "f1".to_string(),
      value: "foo".into(),
    },
    LogField {
      key: "f2".to_string(),
      value: "bar".into(),
    },
  ];

  let matching_only_fields = vec![LogField {
    key: "m1".to_string(),
    value: "5".into(),
  }];

  let setup = Setup::new();
  let (metrics_collector, dynamic_stats_collector) = setup.make_metrics_collector();

  metrics_collector.emit_metric_actions(
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
    &BTreeMap::new(),
    &LogRef {
      message: &"message".into(),
      session_id: "session_id",
      occurred_at: time::OffsetDateTime::now_utc(),
      log_level: log_level::DEBUG,
      log_type: LogType::Normal,
      fields: &FieldsRef::new(&fields, &matching_only_fields),
    },
  );

  dynamic_stats_collector.assert_counter_eq(
    1,
    "workflows_dyn:action",
    labels! {
        "_id" => "action_id_1",
        "tag_1" => "foo",
        "tag_2" => "bar",
        "tag_3" => "fixed",
        "tag_4" => "5",
        "tag_5" => "1",
        "tag_6" => "0",
    },
  );
}

#[test]
fn collecting_insights_for_synthetic_metrics() {
  let fields = &vec![
    LogField {
      key: "insight_1".to_string(),
      value: "value_1".into(),
    },
    LogField {
      key: "insight_2".to_string(),
      value: "value_2".into(),
    },
  ];

  let setup = Setup::new();
  let (mut metrics_collector, dynamic_stats_collector) = setup.make_metrics_collector();

  metrics_collector.emit_metrics(
    &BTreeSet::from([&ActionEmitMetric {
      id: "action_id_1".to_string(),
      tags: BTreeMap::new(),
      increment: crate::config::ValueIncrement::Fixed(1),
      metric_type: MetricType::Counter,
    }]),
    &LogRef {
      message: &"message".into(),
      session_id: "session_id",
      occurred_at: time::OffsetDateTime::now_utc(),
      log_level: log_level::DEBUG,
      log_type: LogType::Normal,
      fields: &FieldsRef::new(fields, fields),
    },
  );

  // No insights collected as no insights dimensions.
  assert!(dynamic_stats_collector
    .find_counter(
      "workflows_dyn:action",
      labels! {
        "_id" => "action_id_1",
        "_insights" => "true",
        "insight_1" => "value_1",
        "insight_2" => "value_2",
        "key_1" => "value_1",
      }
    )
    .is_none());

  // Provide insight dimensions.
  metrics_collector.update(Some(InsightsDimensions::new(&insights!(
    insight!("insight_1"),
    insight!("insight_2"),
    // This insight should be ignored as it doesn't exist on emitted logs.
    insight!("insight_3")
  ))));

  // No insights collected as insights disabled.
  metrics_collector.emit_metrics(
    &BTreeSet::from([&ActionEmitMetric {
      id: "action_id_1".to_string(),
      tags: BTreeMap::new(),
      increment: crate::config::ValueIncrement::Fixed(1),
      metric_type: MetricType::Counter,
    }]),
    &LogRef {
      message: &"message".into(),
      session_id: "session_id",
      occurred_at: time::OffsetDateTime::now_utc(),
      log_level: log_level::DEBUG,
      log_type: LogType::Normal,
      fields: &FieldsRef::new(fields, fields),
    },
  );

  metrics_collector.emit_metrics(
    &BTreeSet::from([&ActionEmitMetric {
      id: "action_id_1".to_string(),
      tags: BTreeMap::from([("key_1".to_string(), TagValue::Fixed("value_1".to_string()))]),
      increment: crate::config::ValueIncrement::Fixed(1),
      metric_type: MetricType::Counter,
    }]),
    &LogRef {
      message: &"message".into(),
      session_id: "session_id",
      occurred_at: time::OffsetDateTime::now_utc(),
      log_level: log_level::DEBUG,
      log_type: LogType::Normal,
      fields: &FieldsRef::new(fields, fields),
    },
  );

  dynamic_stats_collector.assert_counter_eq(
    1,
    "workflows_dyn:action",
    labels! {
      "_id" => "action_id_1",
      "_insights" => "true",
      "insight_1" => "value_1",
      "insight_2" => "value_2",
      "key_1" => "value_1",
    },
  );
}
