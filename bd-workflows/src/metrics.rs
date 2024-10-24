// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./metrics_test.rs"]
mod metrics_test;

use crate::config::ActionEmitMetric;
use crate::workflow::TriggeredActionEmitSankey;
use bd_client_stats::DynamicStats;
use bd_log_primitives::LogRef;
use bd_matcher::FieldProvider;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

//
// MetricsCollector
//

// Responsible for emitting statistics related to workflow action metrics.
#[derive(Debug)]
pub(crate) struct MetricsCollector {
  dynamic_stats: Arc<DynamicStats>,
}

impl MetricsCollector {
  pub(crate) const fn new(dynamic_stats: Arc<DynamicStats>) -> Self {
    Self { dynamic_stats }
  }

  pub(crate) fn emit_metrics(&self, actions: &BTreeSet<&ActionEmitMetric>, log: &LogRef<'_>) {
    if actions.is_empty() {
      return;
    }

    // TODO(Augustyniak): We dedupe stats in here too only when both their tags and the value of
    // If `counter_increment` values are identical, consider deduping metrics even if their
    // `counter_increment` fields have different values.
    for action in actions {
      let mut tags = BTreeMap::new();

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

  pub(crate) fn emit_sankeys(
    &self,
    actions: &BTreeSet<TriggeredActionEmitSankey<'_>>,
    _log: &LogRef<'_>,
  ) {
    // TODO(Augustyniak): extract appropriate field values.
    for action in actions {
      let mut tags = BTreeMap::new();
      tags.insert("_id".to_string(), action.action.id().to_string());
      tags.insert("_path_id".to_string(), action.path.path_id.to_string());

      self
        .dynamic_stats
        .record_dynamic_counter("workflows_dyn:action", tags, 1);
    }
  }

  fn resolve_field_name<'a>(key: &str, log: &'a LogRef<'a>) -> Option<Cow<'a, str>> {
    match key {
      "log_level" => Some(log.log_level.to_string().into()),
      "log_type" => Some(log.log_type.0.to_string().into()),
      key => log.fields.field_value(key).map(Into::into),
    }
  }
}
