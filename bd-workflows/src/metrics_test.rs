// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/licenses/strict/1.0.0

#![allow(clippy::mutable_key_type)]

use super::MetricsCollector;
use crate::config::{
  ActionEmitMetric,
  JsonFieldExtraction,
  MetricMultiTag,
  TagValue,
  ValueIncrement,
};
use crate::engine::EmitMetricActionCount;
use crate::workflow::{WorkflowCommandCompletionToken, WorkflowEvent};
use bd_client_stats::Stats;
use bd_client_stats_store::test::StatsHelper;
use bd_client_stats_store::{Collector, Counter, Histogram};
use bd_log_matcher::matcher::JsonPathToken;
use bd_log_primitives::{FieldsRef, Log, LogFields, log_level};
use bd_proto::protos::logging::payload::LogType;
use bd_proto::protos::workflow::workflow::MultiTag as MultiTagProto;
use bd_state::Scope;
use bd_stats_common::{MetricType, NameType, labels};
use std::collections::{BTreeMap, HashMap};
use time::OffsetDateTime;

fn make_metrics_collector() -> (MetricsCollector<Counter, Histogram>, Collector) {
  let collector = Collector::default();
  let stats = Stats::new(collector.clone());
  (MetricsCollector::new(stats), collector)
}

#[test]
fn labels_are_payload_owned_for_log_session_command_and_state_metrics() {
  for environment in [None, Some("{}"), Some(r#"{"channel":"previous"}"#)] {
    let mut fields: LogFields = environment
      .map(|value| [("__bd_labels".into(), value.into())].into())
      .unwrap_or_default();
    fields.insert("app_id".into(), "previous-app".into());
    fields.insert("platform".into(), "apple".into());
    fields.insert("app_version".into(), "01.2".into());
    let log = Log {
      message: "message".into(),
      session_id: "session".into(),
      occurred_at: OffsetDateTime::UNIX_EPOCH,
      log_level: log_level::DEBUG,
      log_type: LogType::NORMAL,
      fields,
      matching_fields: LogFields::default(),
      command_attachment: None,
      capture_session: None,
    };
    let change = bd_state::StateChange::inserted(
      Scope::GlobalState,
      "key",
      bd_state::string_value("value"),
      OffsetDateTime::UNIX_EPOCH,
    );
    let token = WorkflowCommandCompletionToken("token".to_string());
    for event in [
      WorkflowEvent::Log(&log),
      WorkflowEvent::SessionStart(&log),
      WorkflowEvent::CommandStart(&log),
      WorkflowEvent::CommandCancelled(&log),
      WorkflowEvent::CommandLog(&log),
      WorkflowEvent::CommandCompletion {
        log: &log,
        token: &token,
      },
      WorkflowEvent::StateChange(
        &change,
        FieldsRef::new(&log.fields, &log.matching_fields),
        "session",
      ),
    ] {
      for metric_type in [MetricType::Counter, MetricType::Histogram] {
        let collector = Collector::default().with_product_metadata(HashMap::from([
          (
            "__bd_labels".to_string(),
            r#"{"channel":"current"}"#.to_string(),
          ),
          ("app_id".to_string(), "current-app".to_string()),
          ("platform".to_string(), "android".to_string()),
        ]));
        let metrics_collector = MetricsCollector::new(Stats::new(collector.clone()));
        let action = ActionEmitMetric {
          id: "action".to_string(),
          // Chart configuration cannot replace SDK identity, even when the payload is unset.
          tags: BTreeMap::from([
            (
              "__bd_labels".to_string(),
              TagValue::Fixed("configured".to_string()),
            ),
            (
              "app_id".to_string(),
              TagValue::Fixed("configured-app".to_string()),
            ),
            (
              "platform".to_string(),
              TagValue::Fixed("configured-platform".to_string()),
            ),
          ]),
          multi_tag: None,
          increment: ValueIncrement::Fixed(1),
          metric_type,
        };
        let actions = BTreeMap::from([(
          &action,
          EmitMetricActionCount {
            emission_count: 1,
            is_parallel: false,
            parallel_source_workflow_index: None,
          },
        )]);
        metrics_collector.emit_metrics(&actions, event, &bd_state::InMemoryStateReader::default());
        let mut tags = environment
          .map(|value| labels! { "__bd_labels" => value })
          .unwrap_or_default();
        tags.insert("app_id".to_string(), "previous-app".to_string());
        tags.insert("platform".to_string(), "apple".to_string());
        match metric_type {
          MetricType::Counter => collector.assert_workflow_counter_eq(1, "action", tags),
          MetricType::Histogram => {
            collector.assert_workflow_histogram_observed(1.0, "action", tags);
          },
        }
      }
    }
  }
}

#[test]
fn identity_tags_do_not_require_chart_configuration_and_support_legacy_platform_fields() {
  for (os, platform) in [("iOS", "apple"), ("Android", "android")] {
    let (metrics_collector, collector) = make_metrics_collector();
    let log = Log {
      message: "message".into(),
      session_id: "session".into(),
      occurred_at: OffsetDateTime::UNIX_EPOCH,
      log_level: log_level::DEBUG,
      log_type: LogType::NORMAL,
      fields: [
        ("__bd_labels".into(), r#"{"channel":"previous"}"#.into()),
        ("app_id".into(), "previous-app".into()),
        ("app_version".into(), "01.2".into()),
        ("os".into(), os.into()),
      ]
      .into(),
      matching_fields: LogFields::default(),
      command_attachment: None,
      capture_session: None,
    };
    let action = ActionEmitMetric {
      id: "action".to_string(),
      tags: BTreeMap::from([(
        "app_version".to_string(),
        TagValue::FieldExtract("app_version".to_string()),
      )]),
      multi_tag: None,
      increment: ValueIncrement::Fixed(1),
      metric_type: MetricType::Counter,
    };
    metrics_collector.emit_metrics(
      &BTreeMap::from([(
        &action,
        EmitMetricActionCount {
          emission_count: 1,
          is_parallel: false,
          parallel_source_workflow_index: None,
        },
      )]),
      WorkflowEvent::Log(&log),
      &bd_state::InMemoryStateReader::default(),
    );
    collector.assert_workflow_counter_eq(
      1,
      "action",
      labels! {
        "__bd_labels" => r#"{"channel":"previous"}"#,
        "app_id" => "previous-app",
        "app_version" => "01.2",
        "platform" => platform,
      },
    );
  }
}

#[test]
fn metric_increment_value_extraction() {
  let fields = [("f1".into(), "1.1".into()), ("f2".into(), "10".into())].into();

  let matching_only_fields = [("m1".into(), "5".into())].into();

  let (metrics_collector, collector) = make_metrics_collector();

  let log = Log {
    message: "message".into(),
    session_id: "session_id".into(),
    occurred_at: time::OffsetDateTime::now_utc(),
    log_level: log_level::DEBUG,
    log_type: LogType::NORMAL,
    fields,
    matching_fields: matching_only_fields,
    command_attachment: None,
    capture_session: None,
  };

  let actions = [
    ActionEmitMetric {
      id: "action_id_1".to_string(),
      tags: BTreeMap::new(),
      multi_tag: None,
      increment: crate::config::ValueIncrement::Fixed(1),
      metric_type: MetricType::Counter,
    },
    ActionEmitMetric {
      id: "action_id_2".to_string(),
      tags: BTreeMap::new(),
      multi_tag: None,
      increment: crate::config::ValueIncrement::Extract("f2".to_string()),
      metric_type: MetricType::Counter,
    },
    ActionEmitMetric {
      id: "action_id_3".to_string(),
      tags: BTreeMap::new(),
      multi_tag: None,
      increment: crate::config::ValueIncrement::Extract("f1".to_string()),
      metric_type: MetricType::Counter,
    },
    ActionEmitMetric {
      id: "action_id_4".to_string(),
      tags: BTreeMap::new(),
      multi_tag: None,
      increment: crate::config::ValueIncrement::Extract("does not exist".to_string()),
      metric_type: MetricType::Counter,
    },
    ActionEmitMetric {
      id: "action_id_5".to_string(),
      tags: BTreeMap::new(),
      multi_tag: None,
      increment: crate::config::ValueIncrement::Extract("m1".to_string()),
      metric_type: MetricType::Counter,
    },
    ActionEmitMetric {
      id: "action_id_6".to_string(),
      tags: BTreeMap::new(),
      multi_tag: None,
      increment: crate::config::ValueIncrement::Extract("m1".to_string()),
      metric_type: MetricType::Histogram,
    },
  ];
  let action_counts: BTreeMap<&ActionEmitMetric, EmitMetricActionCount> = actions
    .iter()
    .map(|action| {
      (
        action,
        EmitMetricActionCount {
          emission_count: 1,
          is_parallel: false,
          parallel_source_workflow_index: None,
        },
      )
    })
    .collect();

  metrics_collector.emit_metrics(
    &action_counts,
    WorkflowEvent::Log(&log),
    &bd_state::InMemoryStateReader::default(),
  );

  collector.assert_workflow_counter_eq(1, "action_id_1", labels! {});
  collector.assert_workflow_counter_eq(10, "action_id_2", labels! {});
  // The 1.0 is parsed as a float, then converted to an integer.
  collector.assert_workflow_counter_eq(1, "action_id_3", labels! {});

  // No counter emitted for action_id_4 as the field does not exist.
  assert!(
    collector
      .find_counter(&NameType::ActionId("action_id_4".to_string()), &labels! {})
      .is_none()
  );

  // Values can be extracted from the matching_only_fields.
  collector.assert_workflow_counter_eq(5, "action_id_5", labels! {});
  // Values can be extracted from the matching_only_fields.
  collector.assert_workflow_histogram_observed(5.0, "action_id_6", labels! {});
}

#[test]
fn session_start_metrics_use_triggering_log_payload() {
  let (metrics_collector, collector) = make_metrics_collector();

  let log = Log {
    message: "message".into(),
    session_id: "session_id".into(),
    occurred_at: OffsetDateTime::now_utc(),
    log_level: log_level::DEBUG,
    log_type: LogType::NORMAL,
    fields: [("device_id".into(), "ios".into())].into(),
    matching_fields: LogFields::default(),
    command_attachment: None,
    capture_session: None,
  };

  let action = ActionEmitMetric {
    id: "action_id".to_string(),
    tags: BTreeMap::from([
      ("fixed".to_string(), TagValue::Fixed("always".to_string())),
      (
        "device_id".to_string(),
        TagValue::FieldExtract("device_id".to_string()),
      ),
      ("message".to_string(), TagValue::LogBodyExtract),
      (
        "feature_flag".to_string(),
        TagValue::StateExtract(Scope::FeatureFlagExposure, "flag_a".to_string()),
      ),
    ]),
    multi_tag: None,
    increment: crate::config::ValueIncrement::Fixed(1),
    metric_type: MetricType::Counter,
  };
  let action_counts = BTreeMap::from([(
    &action,
    EmitMetricActionCount {
      emission_count: 1,
      is_parallel: false,
      parallel_source_workflow_index: None,
    },
  )]);

  let mut state_reader = bd_state::InMemoryStateReader::default();
  state_reader.insert(
    Scope::FeatureFlagExposure,
    "flag_a",
    bd_state::string_value("enabled"),
  );

  metrics_collector.emit_metrics(
    &action_counts,
    WorkflowEvent::SessionStart(&log),
    &state_reader,
  );

  collector.assert_workflow_counter_eq(
    1,
    "action_id",
    labels! {
      "device_id" => "ios",
      "fixed" => "always",
      "feature_flag" => "enabled",
      "message" => "message",
    },
  );
}

#[test]
fn counter_label_extraction() {
  let fields = [("f1".into(), "foo".into()), ("f2".into(), "bar".into())].into();

  let matching_only_fields = [("m1".into(), "5".into())].into();

  let (metrics_collector, collector) = make_metrics_collector();

  let mut state_reader = bd_state::InMemoryStateReader::default();
  state_reader.insert(
    bd_state::Scope::FeatureFlagExposure,
    "enabled_flag",
    bd_state::string_value("variant_a"),
  );

  let log = Log {
    message: "message".into(),
    session_id: "session_id".into(),
    occurred_at: time::OffsetDateTime::now_utc(),
    log_level: log_level::DEBUG,
    log_type: LogType::NORMAL,
    fields,
    matching_fields: matching_only_fields,
    command_attachment: None,
    capture_session: None,
  };

  let action = ActionEmitMetric {
    id: "action_id_1".to_string(),
    tags: [
      (
        "tag_1".to_string(),
        TagValue::FieldExtract("f1".to_string()),
      ),
      (
        "tag_2".to_string(),
        TagValue::FieldExtract("f2".to_string()),
      ),
      ("tag_3".to_string(), TagValue::Fixed("fixed".to_string())),
      (
        "tag_4".to_string(),
        TagValue::FieldExtract("m1".to_string()),
      ),
      (
        "tag_5".to_string(),
        TagValue::FieldExtract("log_level".to_string()),
      ),
      (
        "tag_6".to_string(),
        TagValue::FieldExtract("log_type".to_string()),
      ),
      ("tag_7".to_string(), TagValue::LogBodyExtract),
      (
        "tag_8".to_string(),
        TagValue::StateExtract(
          bd_state::Scope::FeatureFlagExposure,
          "enabled_flag".to_string(),
        ),
      ),
      (
        "tag_9".to_string(),
        TagValue::StateExtract(
          bd_state::Scope::FeatureFlagExposure,
          "missing_flag".to_string(),
        ),
      ),
      (
        "tag_10".to_string(),
        TagValue::StateExtract(
          bd_state::Scope::FeatureFlagExposure,
          "no variant flag".to_string(),
        ),
      ),
    ]
    .into(),
    multi_tag: None,
    increment: crate::config::ValueIncrement::Fixed(1),
    metric_type: MetricType::Counter,
  };
  let action_counts = BTreeMap::from([(
    &action,
    EmitMetricActionCount {
      emission_count: 1,
      is_parallel: false,
      parallel_source_workflow_index: None,
    },
  )]);

  metrics_collector.emit_metrics(&action_counts, WorkflowEvent::Log(&log), &state_reader);

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
        "tag_7" => "message",
        "tag_8" => "variant_a",
    },
  );
}

#[test]
fn metric_multi_tag_fans_out_over_matching_state_entries() {
  let (metrics_collector, collector) = make_metrics_collector();

  let log = Log {
    message: "message".into(),
    session_id: "session_id".into(),
    occurred_at: time::OffsetDateTime::now_utc(),
    log_level: log_level::DEBUG,
    log_type: LogType::NORMAL,
    fields: [("payload".into(), r#"{"tag":"nested"}"#.into())].into(),
    matching_fields: LogFields::default(),
    command_attachment: None,
    capture_session: None,
  };

  let mut state_reader = bd_state::InMemoryStateReader::default();
  state_reader.insert(
    bd_state::Scope::FeatureFlagExposure,
    "experiment_checkout",
    bd_state::string_value("variant_a"),
  );
  state_reader.insert(
    bd_state::Scope::FeatureFlagExposure,
    "experiment_search",
    bd_state::string_value("control"),
  );
  state_reader.insert(
    bd_state::Scope::FeatureFlagExposure,
    "not_an_experiment",
    bd_state::string_value("ignored"),
  );

  let action = ActionEmitMetric {
    id: "action_id_multi".to_string(),
    tags: [
      ("static".to_string(), TagValue::Fixed("tag".to_string())),
      ("json".into(), TagValue::JsonExtract(json_extraction("tag"))),
    ]
    .into(),
    multi_tag: Some(
      MetricMultiTag::new(MultiTagProto {
        scope: bd_proto::protos::state::scope::StateScope::FEATURE_FLAG.into(),
        key_tag_name: "experiment".to_string(),
        value_tag_name: "variant".to_string(),
        key_regex: Some("^experiment_".to_string()),
        value_regex: None,
        ..Default::default()
      })
      .unwrap(),
    ),
    increment: crate::config::ValueIncrement::Fixed(1),
    metric_type: MetricType::Counter,
  };
  let action_counts = BTreeMap::from([(
    &action,
    EmitMetricActionCount {
      emission_count: 1,
      is_parallel: false,
      parallel_source_workflow_index: None,
    },
  )]);

  metrics_collector.emit_metrics(&action_counts, WorkflowEvent::Log(&log), &state_reader);

  collector.assert_workflow_counter_eq(
    1,
    "action_id_multi",
    labels! {
      "static" => "tag",
      "json" => "nested",
      "experiment" => "experiment_checkout",
      "variant" => "variant_a",
    },
  );
  collector.assert_workflow_counter_eq(
    1,
    "action_id_multi",
    labels! {
      "static" => "tag",
      "json" => "nested",
      "experiment" => "experiment_search",
      "variant" => "control",
    },
  );
}

#[test]
fn metric_multi_tag_with_no_matches_emits_nothing() {
  let (metrics_collector, collector) = make_metrics_collector();

  let log = Log {
    message: "message".into(),
    session_id: "session_id".into(),
    occurred_at: time::OffsetDateTime::now_utc(),
    log_level: log_level::DEBUG,
    log_type: LogType::NORMAL,
    fields: LogFields::default(),
    matching_fields: LogFields::default(),
    command_attachment: None,
    capture_session: None,
  };

  let mut state_reader = bd_state::InMemoryStateReader::default();
  state_reader.insert(
    bd_state::Scope::FeatureFlagExposure,
    "non_matching_flag",
    bd_state::string_value("ignored"),
  );

  let action = ActionEmitMetric {
    id: "action_id_no_match".to_string(),
    tags: [("static".to_string(), TagValue::Fixed("tag".to_string()))].into(),
    multi_tag: Some(
      MetricMultiTag::new(MultiTagProto {
        scope: bd_proto::protos::state::scope::StateScope::FEATURE_FLAG.into(),
        key_tag_name: "experiment".to_string(),
        value_tag_name: "variant".to_string(),
        key_regex: Some("^experiment_".to_string()),
        value_regex: None,
        ..Default::default()
      })
      .unwrap(),
    ),
    increment: crate::config::ValueIncrement::Fixed(1),
    metric_type: MetricType::Counter,
  };
  let action_counts = BTreeMap::from([(
    &action,
    EmitMetricActionCount {
      emission_count: 1,
      is_parallel: false,
      parallel_source_workflow_index: None,
    },
  )]);

  metrics_collector.emit_metrics(&action_counts, WorkflowEvent::Log(&log), &state_reader);

  assert!(
    collector
      .find_counter(
        &NameType::ActionId("action_id_no_match".to_string()),
        &labels! { "static" => "tag" },
      )
      .is_none()
  );
}

#[test]
fn multi_tag_cannot_replace_payload_labels() {
  let (metrics_collector, collector) = make_metrics_collector();
  let log = Log {
    message: "message".into(),
    session_id: "session".into(),
    occurred_at: OffsetDateTime::UNIX_EPOCH,
    log_level: log_level::DEBUG,
    log_type: LogType::NORMAL,
    fields: [
      ("__bd_labels".into(), r#"{"channel":"previous"}"#.into()),
      ("app_id".into(), "previous-app".into()),
      ("platform".into(), "apple".into()),
    ]
    .into(),
    matching_fields: LogFields::default(),
    command_attachment: None,
    capture_session: None,
  };
  let mut state = bd_state::InMemoryStateReader::default();
  state.insert(
    Scope::FeatureFlagExposure,
    "flag",
    bd_state::string_value("variant"),
  );
  for (key_tag, value_tag) in [
    ("__bd_labels", "variant"),
    ("flag", "__bd_labels"),
    ("app_id", "variant"),
    ("flag", "app_id"),
    ("platform", "variant"),
    ("flag", "platform"),
  ] {
    let id = format!("{key_tag}-{value_tag}");
    let action = ActionEmitMetric {
      id: id.clone(),
      tags: BTreeMap::new(),
      multi_tag: Some(
        MetricMultiTag::new(MultiTagProto {
          scope: bd_proto::protos::state::scope::StateScope::FEATURE_FLAG.into(),
          key_tag_name: key_tag.to_string(),
          value_tag_name: value_tag.to_string(),
          ..Default::default()
        })
        .unwrap(),
      ),
      increment: ValueIncrement::Fixed(1),
      metric_type: MetricType::Counter,
    };
    metrics_collector.emit_metrics(
      &BTreeMap::from([(
        &action,
        EmitMetricActionCount {
          emission_count: 1,
          is_parallel: false,
          parallel_source_workflow_index: None,
        },
      )]),
      WorkflowEvent::Log(&log),
      &state,
    );
    let extra_tag = if key_tag == "flag" {
      ("flag", "flag")
    } else {
      ("variant", "variant")
    };
    collector.assert_workflow_counter_eq(
      1,
      &id,
      labels! {
        "__bd_labels" => r#"{"channel":"previous"}"#,
        "app_id" => "previous-app",
        "platform" => "apple",
        extra_tag.0 => extra_tag.1,
      },
    );
  }
}

fn json_extraction(key: &str) -> JsonFieldExtraction {
  JsonFieldExtraction {
    field_name: "payload".into(),
    path: vec![JsonPathToken::Key(key.into())],
  }
}

#[test]
fn json_metric_values_and_tags() {
  let (metrics, collector) = make_metrics_collector();
  let log = Log {
    message: "message".into(),
    session_id: "session".into(),
    occurred_at: OffsetDateTime::now_utc(),
    log_level: log_level::DEBUG,
    log_type: LogType::NORMAL,
    fields: [
      (
        "payload".into(),
        r#"{"value":2.5,"tag":"hello\nworld","bool":true,"null":null,"object":{}}"#.into(),
      ),
      ("payload.tag".into(), "literal".into()),
    ]
    .into(),
    matching_fields: LogFields::default(),
    command_attachment: None,
    capture_session: None,
  };
  let tags = BTreeMap::from([
    ("json".into(), TagValue::JsonExtract(json_extraction("tag"))),
    (
      "bool".into(),
      TagValue::JsonExtract(json_extraction("bool")),
    ),
    (
      "literal".into(),
      TagValue::FieldExtract("payload.tag".into()),
    ),
    (
      "missing".into(),
      TagValue::JsonExtract(json_extraction("missing")),
    ),
    (
      "null".into(),
      TagValue::JsonExtract(json_extraction("null")),
    ),
    (
      "object".into(),
      TagValue::JsonExtract(json_extraction("object")),
    ),
  ]);
  let state = bd_state::InMemoryStateReader::default();
  for (id, metric_type) in [
    ("counter", MetricType::Counter),
    ("histogram", MetricType::Histogram),
  ] {
    let action = ActionEmitMetric {
      id: id.into(),
      tags: tags.clone(),
      multi_tag: None,
      increment: ValueIncrement::JsonExtract(json_extraction("value")),
      metric_type,
    };
    metrics.emit_metrics(
      &BTreeMap::from([(
        &action,
        EmitMetricActionCount {
          emission_count: 2,
          is_parallel: false,
          parallel_source_workflow_index: None,
        },
      )]),
      WorkflowEvent::Log(&log),
      &state,
    );
  }
  let labels = labels! { "json" => "hello\nworld", "bool" => "true", "literal" => "literal" };
  collector.assert_workflow_counter_eq(4, "counter", labels.clone());
  collector.assert_workflow_histogram_observed(2.5, "histogram", labels);

  for key in ["missing", "null", "object", "tag", "bool"] {
    let action = ActionEmitMetric {
      id: key.into(),
      tags: BTreeMap::new(),
      multi_tag: None,
      increment: ValueIncrement::JsonExtract(json_extraction(key)),
      metric_type: MetricType::Counter,
    };
    metrics.emit_metrics(
      &BTreeMap::from([(
        &action,
        EmitMetricActionCount {
          emission_count: 1,
          is_parallel: false,
          parallel_source_workflow_index: None,
        },
      )]),
      WorkflowEvent::Log(&log),
      &state,
    );
    assert!(
      collector
        .find_counter(&NameType::ActionId(key.into()), &labels! {})
        .is_none()
    );
  }
}
