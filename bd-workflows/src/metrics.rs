// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./metrics_test.rs"]
mod metrics_test;

use crate::config::{ActionEmitMetric, InsightsDimensions};
use bd_client_stats::DynamicStats;
use bd_log_primitives::LogRef;
use bd_matcher::FieldProvider;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

//
// MetricsCollector
//

// Responsible for emitting statistics related to workflow action metrics and gathering insights
// for all types of actions. This encompasses insights into triggers for flushing buffer actions
// (recording sessions).
#[derive(Debug)]
pub(crate) struct MetricsCollector {
  dynamic_stats: Arc<DynamicStats>,
  // `None` means insights disabled.
  insights_dimensions: Option<InsightsDimensions>,
}

impl MetricsCollector {
  pub(crate) const fn new(dynamic_stats: Arc<DynamicStats>) -> Self {
    Self {
      dynamic_stats,
      insights_dimensions: None,
    }
  }

  pub(crate) fn update(&mut self, insights_dimensions: Option<InsightsDimensions>) {
    match &insights_dimensions {
      Some(insights_dimensions) => {
        log::debug!("insights enabled, configuration: {:?}", insights_dimensions);
      },
      None => {
        log::debug!("insights disabled");
      },
    }

    self.insights_dimensions = insights_dimensions;
  }

  pub(crate) fn emit_metrics(
    &self,
    emit_metric_actions: &BTreeSet<&ActionEmitMetric>,
    log: &LogRef<'_>,
  ) {
    let tags = self.get_insights_tags(log.fields);
    self.emit_metric_actions(emit_metric_actions, &tags, log);
  }

  fn emit_metric_actions(
    &self,
    actions: &BTreeSet<&ActionEmitMetric>,
    insights_tags: &BTreeMap<String, String>,
    log: &LogRef<'_>,
  ) {
    if actions.is_empty() {
      return;
    }

    // TODO(Augustyniak): We dedupe stats in here too only when both their tags and the value of
    // If `counter_increment` values are identical, consider deduping metrics even if their
    // `counter_increment` fields have different values.
    for action in actions {
      let mut tags = insights_tags.clone();

      for (key, value) in &action.tags {
        if let Some(extracted_value) = match value {
          crate::config::TagValue::Extract(extract) => Self::resolve_field_name(extract, log),
          crate::config::TagValue::Fixed(value) => Some(value.as_str().into()),
        } {
          tags.insert(key.to_string(), extracted_value.into_owned());
        }
      }
      tags.insert("_id".to_string(), action.id.clone());

      #[allow(clippy::cast_precision_loss)]
      let maybe_value: anyhow::Result<f64> = match &action.increment {
        crate::config::ValueIncrement::Fixed(value) => Ok(*value as f64),
        crate::config::ValueIncrement::Extract(extract) => Self::resolve_field_name(extract, log)
          .ok_or_else(|| anyhow::anyhow!("field {:?} not found", extract))
          .and_then(|value| value.parse::<f64>().map_err(Into::into)),
      };

      let value = match maybe_value {
        Ok(value) => value,
        Err(e) => {
          log::debug!(
            "failed to extract counter increment for action {:?}: {}",
            action.id,
            e
          );
          continue;
        },
      };

      match action.metric_type {
        crate::config::MetricType::Counter => {
          #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
          self
            .dynamic_stats
            .record_dynamic_counter("workflows_dyn:action", tags, value as u64);
        },
        crate::config::MetricType::Histogram => {
          log::debug!("recording histogram value: {}", value);
          self
            .dynamic_stats
            .record_dynamic_histogram("workflows_dyn:histogram", tags, value);
        },
      }
    }
  }

  fn resolve_field_name<'a>(key: &str, log: &'a LogRef<'a>) -> Option<Cow<'a, str>> {
    match key {
      "log_level" => Some(log.log_level.to_string().into()),
      "log_type" => Some(log.log_type.0.to_string().into()),
      key => log.fields.field_value(key).map(Into::into),
    }
  }

  fn get_insights_tags(&self, fields: &dyn FieldProvider) -> BTreeMap<String, String> {
    let Some(insights_dimensions) = &self.insights_dimensions else {
      return BTreeMap::new();
    };

    if insights_dimensions.is_empty() {
      return BTreeMap::new();
    }

    let mut tags = BTreeMap::new();

    for insight_dimension in insights_dimensions.iter() {
      let Some(value) = fields.field_value(insight_dimension) else {
        continue;
      };


      tags.insert(insight_dimension.to_string(), value.to_string());
    }

    tags.insert("_insights".to_string(), "true".to_string());

    tags
  }
}
