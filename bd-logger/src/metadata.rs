// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./metadata_test.rs"]
mod metadata_test;

use crate::global_state::Tracker;
use bd_log::warn_every;
use bd_log_metadata::{AnnotatedLogFields, LogFieldKind, MetadataProvider};
use bd_log_primitives::{LogFieldValue, LogFields};
use bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::LogType;
use itertools::Itertools;
use std::collections::BTreeSet;
use std::sync::{Arc, LazyLock};
use time::ext::NumericalDuration;

static RESERVED_FIELD_NAMES: LazyLock<BTreeSet<String>> = LazyLock::new(|| {
  BTreeSet::from([
    "app_id".to_string(),
    "app_version".to_string(),
    "carrier".to_string(),
    "foreground".to_string(),
    "log_level".to_string(),
    "log_type".to_string(),
    "model".to_string(),
    "network_type".to_string(),
    "os".to_string(),
    "os_version".to_string(),
    "radio_type".to_string(),
  ])
});

//
// LogMetadata
//

// An abstraction for various metadata fields to be included as part of emitted logs.
pub struct LogMetadata {
  // The timestamp to associate with an emitted log.
  pub timestamp: time::OffsetDateTime,
  // A fields to associate with an emitted log.
  pub fields: LogFields,
  pub matching_fields: LogFields,
}

//
// MetadataCollector
//

pub(crate) struct MetadataCollector {
  metadata_provider: Arc<dyn MetadataProvider + Send + Sync>,
  fields: LogFields,
}

impl MetadataCollector {
  pub(crate) fn new(metadata_provider: Arc<dyn MetadataProvider + Send + Sync>) -> Self {
    Self {
      metadata_provider,
      fields: [].into(),
    }
  }
  /// Returns metadata created by combining values acquired by combining the receiver's fields and
  /// passed `fields` argument. It ensures that the `fields` property of the output value does
  /// not have duplicate keys. The combining logic gives precedence to fields coming from the field
  /// provider so in the case of the key conflicts, fields from the field provider override keys
  /// from `fields` argument.
  pub(crate) fn normalized_metadata_with_extra_fields(
    &self,
    // TODO(Augustyniak): Disallow custom fields whose names start with "_".
    fields: AnnotatedLogFields,
    matching_fields: AnnotatedLogFields,
    log_type: LogType,
    global_state_tracker: &mut Tracker,
  ) -> anyhow::Result<LogMetadata> {
    let timestamp = self.metadata_provider.timestamp()?;

    let provider_fields = self.metadata_provider.fields()?;
    let provider_fields = partition_fields(provider_fields);
    global_state_tracker.maybe_update_global_state(&provider_fields.ootb);

    // Attach field provider's fields to session replay, resource logs, and internal SDK logs
    // as matching fields as opposed to 'normal' fields to save on bandwidth usage while still
    // allowing matching on them.
    let (provider_fields, provider_matching_fields) = if log_type == LogType::Replay
      || log_type == LogType::Resource
      || log_type == LogType::InternalSDK
    {
      (PartitionedFields::default(), provider_fields)
    } else {
      (provider_fields, PartitionedFields::default())
    };

    let log_fields = partition_fields(fields);

    // Normalize fields. Process them in the order described below, where fields that are earlier in
    // the list take precedence over fields farther away in the list and cannot be overridden by
    // them.
    let fields = [
      provider_fields.ootb,
      log_fields.ootb,
      log_fields.custom,
      self.fields(),
      provider_fields.custom,
    ]
    .into_iter()
    .flat_map(|f| f)
    .unique_by(|(key, _)| key.clone())
    .collect();

    let matching_fields = partition_fields(matching_fields);

    let matching_fields = [
      provider_matching_fields.ootb,
      matching_fields.ootb,
      matching_fields.custom,
      provider_matching_fields.custom,
    ]
    .into_iter()
    .flat_map(|f| f)
    .unique_by(|(key, _)| key.clone())
    .collect();

    Ok(LogMetadata {
      timestamp,
      fields,
      matching_fields,
    })
  }

  pub(crate) fn add_field(&mut self, key: String, value: LogFieldValue) -> anyhow::Result<()> {
    verify_custom_field_name(&key)?;

    self.fields.insert(key, value);

    Ok(())
  }

  pub(crate) fn remove_field(&mut self, field_key: &str) {
    self.fields.remove(field_key);
  }

  fn fields(&self) -> LogFields {
    self.fields.clone()
  }
}

fn partition_fields(field: AnnotatedLogFields) -> PartitionedFields {
  let mut ootb = LogFields::default();
  let mut custom = LogFields::default();

  for (key, value) in field {
    match value.kind {
      LogFieldKind::Ootb => ootb.insert(key, value.value),
      LogFieldKind::Custom => match verify_custom_field_name(&key) {
        Ok(()) => custom.insert(key, value.value),
        Err(e) => {
          warn_every!(15.seconds(), "failed to process field: {:?}", e);
          continue;
        },
      },
    };
  }

  PartitionedFields { ootb, custom }
}

fn verify_custom_field_name(key: &str) -> anyhow::Result<()> {
  if RESERVED_FIELD_NAMES.contains(key) {
    anyhow::bail!(
      "Custom global field with {:?} name is not allowed as the name is reserved for SDK internal \
       use",
      key
    );
  }

  if key.starts_with('_') {
    anyhow::bail!(
      "Custom global field with {:?} key is not allowed, fields whose key starts with \"_\" are \
       reserved for SDK internal use",
      key
    );
  }

  Ok(())
}

//
// PartitionedFields
//

// A helper to use as a return type for methods that partitions fields into OOTB and custom fields.
#[derive(Default)]
struct PartitionedFields {
  ootb: LogFields,
  custom: LogFields,
}
