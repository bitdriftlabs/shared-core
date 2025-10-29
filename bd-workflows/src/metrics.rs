// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./metrics_test.rs"]
mod metrics_test;

use crate::config::{ActionEmitMetric, TagValue};
use crate::workflow::TriggeredActionEmitSankey;
use bd_client_stats::Stats;
use bd_log_primitives::Log;
use bd_stats_common::MetricType;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

//
// MetricsCollector
//

// Responsible for emitting statistics related to workflow action metrics.
pub(crate) struct MetricsCollector {
  pub(crate) stats: Arc<Stats>,
}

impl MetricsCollector {
  pub(crate) const fn new(stats: Arc<Stats>) -> Self {
    Self { stats }
  }

  pub(crate) fn emit_metrics(&self, actions: &BTreeSet<&ActionEmitMetric>, log: &Log) {
    // TODO(Augustyniak): We dedupe stats in here too only when both their tags and the value of
    // If `counter_increment` values are identical, consider deduping metrics even if their
    // `counter_increment` fields have different values.
    for action in actions {
      let tags = Self::extract_tags(log, &action.tags);

      #[allow(clippy::cast_precision_loss)]
      let maybe_value: anyhow::Result<f64> = match &action.increment {
        crate::config::ValueIncrement::Fixed(value) => Ok(*value as f64),
        crate::config::ValueIncrement::Extract(extract) => Self::resolve_field_name(extract, log)
          .ok_or_else(|| anyhow::anyhow!("field {extract:?} not found"))
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
        MetricType::Counter => {
          #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
          self
            .stats
            .record_dynamic_counter(tags, &action.id, value as u64);
        },
        MetricType::Histogram => {
          log::debug!("recording histogram value: {value}");
          self.stats.record_dynamic_histogram(tags, &action.id, value);
        },
      }
    }
  }

  pub(crate) fn emit_sankeys(&self, actions: &BTreeSet<TriggeredActionEmitSankey<'_>>, log: &Log) {
    for action in actions {
      let mut tags = Self::extract_tags(log, action.action.tags());
      tags.insert("_path_id".to_string(), action.path.path_id.clone());

      self
        .stats
        .record_dynamic_counter(tags, action.action.id(), 1);
    }
  }

  fn resolve_field_name<'a>(key: &str, log: &'a Log) -> Option<Cow<'a, str>> {
    match key {
      "log_level" => Some(log.log_level.to_string().into()),
      "log_type" => Some((log.log_type as u32).to_string().into()),
      key => log.field_value(key),
    }
  }

  fn extract_tags(log: &Log, tags: &BTreeMap<String, TagValue>) -> BTreeMap<String, String> {
    let mut extracted_tags = BTreeMap::new();

    for (key, value) in tags {
      if let Some(extracted_value) = match value {
        crate::config::TagValue::FieldExtract(extract) => Self::resolve_field_name(extract, log),
        crate::config::TagValue::Fixed(value) => Some(value.as_str().into()),
        crate::config::TagValue::LogBodyExtract => log.message.as_str().map(Cow::Borrowed),
      } {
        extracted_tags.insert(key.clone(), extracted_value.into_owned());
      }
    }

    extracted_tags
  }
}
