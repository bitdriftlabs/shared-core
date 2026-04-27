// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::cli::Options;
use anyhow::anyhow;
use bd_client_stats::observer::{
  ObservedMetric,
  ObservedMetricValue,
  SnapshotObservation,
  StatsObserver,
  UploadAckObservation,
  UploadAttemptObservation,
  set_observer,
};
use parking_lot::Mutex;
use serde::Serialize;
use std::collections::{BTreeMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

pub fn configure(args: &Options, sdk_directory: &Path) -> anyhow::Result<()> {
  let Some(action_id) = args.observe_stats_action_id.clone() else {
    set_observer(None);
    return Ok(());
  };

  let output_path = args.observe_stats_output.clone().unwrap_or_else(|| {
    sdk_directory
      .join("stats-observation")
      .join("metrics.jsonl")
  });

  let observer = JsonlStatsObserver::new(action_id.clone(), &output_path)?;
  log::info!(
    "observing stats action ID '{}' at {}",
    action_id,
    output_path.display()
  );
  set_observer(Some(Arc::new(observer)));
  Ok(())
}

struct JsonlStatsObserver {
  action_id: String,
  file: Mutex<File>,
  pending_uploads: Mutex<HashSet<String>>,
}

impl JsonlStatsObserver {
  fn new(action_id: String, output_path: &Path) -> anyhow::Result<Self> {
    if let Some(parent) = output_path.parent() {
      std::fs::create_dir_all(parent)
        .map_err(|error| anyhow!("creating {}: {error}", parent.display()))?;
    }

    let file = OpenOptions::new()
      .create(true)
      .append(true)
      .open(output_path)
      .map_err(|error| anyhow!("opening {}: {error}", output_path.display()))?;

    Ok(Self {
      action_id,
      file: Mutex::new(file),
      pending_uploads: Mutex::new(HashSet::new()),
    })
  }

  fn matching_metrics(&self, metrics: &[ObservedMetric]) -> Vec<JsonMetric> {
    metrics
      .iter()
      .filter(|metric| metric.action_id == self.action_id)
      .map(JsonMetric::from)
      .collect()
  }

  fn write_event(&self, event: &JsonEvent) {
    let mut file = self.file.lock();
    if let Err(error) = serde_json::to_writer(&mut *file, event) {
      log::error!("failed to serialize observed stats event: {error}");
      return;
    }

    if let Err(error) = writeln!(file) {
      log::error!("failed to terminate observed stats event: {error}");
      return;
    }

    if let Err(error) = file.flush() {
      log::error!("failed to flush observed stats event: {error}");
    }
  }
}

impl StatsObserver for JsonlStatsObserver {
  fn on_snapshot(&self, observation: SnapshotObservation) {
    let metrics = self.matching_metrics(&observation.metrics);
    if metrics.is_empty() {
      return;
    }

    self.write_event(&JsonEvent {
      phase: "snapshot",
      action_id: self.action_id.clone(),
      upload_uuid: None,
      upload_reason: None,
      success: None,
      metrics,
      observed_at_unix_nanos: time::OffsetDateTime::now_utc().unix_timestamp_nanos(),
    });
  }

  fn on_upload_attempt(&self, observation: UploadAttemptObservation) {
    let metrics = self.matching_metrics(&observation.metrics);
    if metrics.is_empty() {
      return;
    }

    self
      .pending_uploads
      .lock()
      .insert(observation.upload_uuid.clone());

    self.write_event(&JsonEvent {
      phase: "upload_attempt",
      action_id: self.action_id.clone(),
      upload_uuid: Some(observation.upload_uuid),
      upload_reason: Some(observation.upload_reason),
      success: None,
      metrics,
      observed_at_unix_nanos: time::OffsetDateTime::now_utc().unix_timestamp_nanos(),
    });
  }

  fn on_upload_ack(&self, observation: UploadAckObservation) {
    if !self.pending_uploads.lock().remove(&observation.upload_uuid) {
      return;
    }

    self.write_event(&JsonEvent {
      phase: "upload_ack",
      action_id: self.action_id.clone(),
      upload_uuid: Some(observation.upload_uuid),
      upload_reason: None,
      success: Some(observation.success),
      metrics: Vec::new(),
      observed_at_unix_nanos: time::OffsetDateTime::now_utc().unix_timestamp_nanos(),
    });
  }
}

#[derive(Serialize)]
struct JsonEvent {
  phase: &'static str,
  action_id: String,
  upload_uuid: Option<String>,
  upload_reason: Option<String>,
  success: Option<bool>,
  metrics: Vec<JsonMetric>,
  observed_at_unix_nanos: i128,
}

#[derive(Serialize)]
struct JsonMetric {
  labels: BTreeMap<String, String>,
  #[serde(flatten)]
  value: JsonMetricValue,
}

impl From<&ObservedMetric> for JsonMetric {
  fn from(metric: &ObservedMetric) -> Self {
    Self {
      labels: metric.labels.clone(),
      value: JsonMetricValue::from(&metric.value),
    }
  }
}

#[derive(Serialize)]
#[serde(tag = "metric_type", rename_all = "snake_case")]
enum JsonMetricValue {
  Counter { value: u64 },
  InlineHistogram { values: Vec<f64> },
  DdSketchHistogram { encoded_len: usize },
}

impl From<&ObservedMetricValue> for JsonMetricValue {
  fn from(value: &ObservedMetricValue) -> Self {
    match value {
      ObservedMetricValue::Counter(value) => Self::Counter { value: *value },
      ObservedMetricValue::InlineHistogram(values) => Self::InlineHistogram {
        values: values.clone(),
      },
      ObservedMetricValue::DdSketchHistogram { encoded_len } => Self::DdSketchHistogram {
        encoded_len: *encoded_len,
      },
    }
  }
}
