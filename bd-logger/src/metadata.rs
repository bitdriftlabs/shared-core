// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./metadata_test.rs"]
mod metadata_test;

use bd_crash_handler::global_state;
use bd_log_metadata::MetadataProvider;
use bd_log_primitives::{
  AnnotatedLogField,
  AnnotatedLogFields,
  LogFieldKey,
  LogFieldKind,
  LogFieldValue,
  LogFields,
};
use bd_log_util::warn_every;
use bd_proto::protos::logging::payload::LogType;
use std::collections::BTreeSet;
use std::collections::hash_map::Entry;
use std::sync::{Arc, LazyLock};
use time::ext::NumericalDuration;

static RESERVED_FIELD_NAMES: LazyLock<BTreeSet<String>> = LazyLock::new(|| {
  BTreeSet::from([
    "_manufacturer".to_string(),
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

pub struct MetadataCollector {
  metadata_provider: Arc<dyn MetadataProvider + Send + Sync>,
  // This single map holds immutable OOTB fields collected at startup as well as fields added
  // through the mutable logger API. Retaining the field kind preserves their different
  // precedence rules without requiring a second cache.
  fields: AnnotatedLogFields,
}

impl MetadataCollector {
  pub(crate) fn new(
    metadata_provider: Arc<dyn MetadataProvider + Send + Sync>,
    initial_ootb_fields: LogFields,
  ) -> Self {
    Self {
      metadata_provider,
      fields: initial_ootb_fields
        .into_iter()
        .map(|(key, value)| (key, AnnotatedLogField::new_ootb(value)))
        .collect(),
    }
  }

  /// Returns log metadata using the last active global state at the end of the last process run.
  /// Log fields take precedence over persisted global state fields to allow the caller to
  /// override values in global state, e.g. when the crash handler knows that the event happened
  /// in the background.
  /// Does *not* invoke the field providers as these would incorrectly reflect the state of the
  /// current process.
  pub(crate) fn metadata_from_fields_with_previous_global_state(
    &self,
    fields: AnnotatedLogFields,
    matching_fields: AnnotatedLogFields,
    global_state_reader: &global_state::Reader,
  ) -> anyhow::Result<LogMetadata> {
    let timestamp = self.metadata_provider.timestamp()?;

    let fields = if let Some(previous_global_state_fields) =
      global_state_reader.previous_global_state_fields()
    {
      previous_global_state_fields
        .clone()
        .into_iter()
        .chain(fields.into_iter().map(|(k, v)| (k, v.value)))
        .collect()
    } else {
      fields.into_iter().map(|(k, v)| (k, v.value)).collect()
    };

    Ok(LogMetadata {
      timestamp,
      fields,
      matching_fields: matching_fields
        .into_iter()
        .map(|(k, v)| (k.clone(), v.value))
        .collect(),
    })
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
    global_state_tracker: &mut global_state::Tracker,
  ) -> anyhow::Result<LogMetadata> {
    let timestamp = self.metadata_provider.timestamp()?;

    let (custom_fields, ootb_fields) = self.metadata_provider.fields()?;

    let provider_fields = PartitionedFields {
      ootb: ootb_fields,
      custom: custom_fields
        .into_iter()
        .filter(|field| match verify_custom_field_name(&field.0) {
          Ok(()) => true,
          Err(e) => {
            warn_every!(15.seconds(), "failed to process field: {e:?}");
            false
          },
        })
        .collect(),
    };
    let persistent_fields = partition_fields(self.fields.clone());

    // For the purpose of tracking global fields we only consider fields from field providers as
    // well as ones set via setField. Use the same precedence rules as when constructing the final
    // fields for consistency and
    let global_state_fields = [
      provider_fields.ootb.clone(),
      persistent_fields.custom.clone(),
      provider_fields.custom.clone(),
      persistent_fields.ootb.clone(),
    ]
    .into_iter()
    .flatten()
    .collect();

    global_state_tracker.maybe_update_global_state(&global_state_fields);

    // Attach field provider's fields to session replay, resource logs, and internal SDK logs
    // as matching fields as opposed to 'normal' fields to save on bandwidth usage while still
    // allowing matching on them.
    let (provider_fields, provider_matching_fields) = if log_type == LogType::REPLAY
      || log_type == LogType::RESOURCE
      || log_type == LogType::INTERNAL_SDK
    {
      (PartitionedFields::default(), provider_fields)
    } else {
      (provider_fields, PartitionedFields::default())
    };

    let log_fields = partition_fields(fields);

    // Normalize fields from lowest to highest precedence so later fields override earlier ones.
    let fields = [
      provider_fields.custom,
      persistent_fields.custom,
      log_fields.custom,
      log_fields.ootb,
      provider_fields.ootb,
      persistent_fields.ootb,
    ]
    .into_iter()
    .flatten()
    .collect();

    let matching_fields = partition_fields(matching_fields);

    let matching_fields = [
      provider_matching_fields.ootb,
      matching_fields.ootb,
      matching_fields.custom,
      provider_matching_fields.custom,
    ]
    .into_iter()
    .rev()
    .flatten()
    .collect();

    Ok(LogMetadata {
      timestamp,
      fields,
      matching_fields,
    })
  }

  pub(crate) fn add_field(&mut self, key: LogFieldKey, value: LogFieldValue) -> anyhow::Result<()> {
    verify_custom_field_name(&key)?;

    match self.fields.entry(key) {
      Entry::Occupied(mut entry) if entry.get().kind != LogFieldKind::Ootb => {
        entry.insert(AnnotatedLogField::new_custom(value));
      },
      Entry::Vacant(entry) => {
        entry.insert(AnnotatedLogField::new_custom(value));
      },
      Entry::Occupied(_) => {},
    }

    Ok(())
  }

  pub(crate) fn update_ootb_field(&mut self, key: LogFieldKey, value: LogFieldValue) {
    self.fields.insert(key, AnnotatedLogField::new_ootb(value));
  }

  pub(crate) fn remove_field(&mut self, field_key: LogFieldKey) {
    if let Entry::Occupied(entry) = self.fields.entry(field_key)
      && entry.get().kind != LogFieldKind::Ootb
    {
      entry.remove();
    }
  }
}

fn partition_fields(field: AnnotatedLogFields) -> PartitionedFields {
  let mut ootb = LogFields::default();
  let mut custom = LogFields::default();

  for (key, value) in field {
    match value.kind {
      LogFieldKind::Ootb => {
        ootb.insert(key, value.value);
      },
      LogFieldKind::Custom => match verify_custom_field_name(&key) {
        Ok(()) => {
          custom.insert(key, value.value);
        },
        Err(e) => {
          warn_every!(15.seconds(), "failed to process field: {e:?}");
        },
      },
    }
  }

  PartitionedFields { ootb, custom }
}

fn verify_custom_field_name(key: &str) -> anyhow::Result<()> {
  if RESERVED_FIELD_NAMES.contains(key) {
    anyhow::bail!(
      "Custom global field with {key:?} name is not allowed as the name is reserved for SDK \
       internal use"
    );
  }

  if key.starts_with('_') {
    anyhow::bail!(
      "Custom global field with {key:?} key is not allowed, fields whose key starts with \"_\" \
       are reserved for SDK internal use"
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
