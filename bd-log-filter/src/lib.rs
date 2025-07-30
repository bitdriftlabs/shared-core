// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./filter_chain_test.rs"]
mod filter_chain_test;

use anyhow::{Context, Result, anyhow};
use bd_log_primitives::{
  FieldsRef,
  LOG_FIELD_NAME_LEVEL,
  LOG_FIELD_NAME_MESSAGE,
  LOG_FIELD_NAME_TYPE,
  Log,
  LogFieldKey,
  LogFieldValue,
  LogFields,
};
use bd_proto::protos::filter::filter::filter::{self};
use bd_proto::protos::filter::filter::{Filter as FilterProto, FiltersConfiguration};
use filter::transform::Transform_type;
use itertools::Itertools;
use regex::Regex;
use std::borrow::Cow;

#[cfg(test)]
#[ctor::ctor]
fn test_global_init() {
  bd_test_helpers::test_global_init();
}

//
// FilterChain
//

// A top-level object that encompasses filters to apply to emitted logs.
pub struct FilterChain {
  filters: Vec<Filter>,
}

impl FilterChain {
  // Returns the creates `FilterChain` instance and the number of filters that could not be created
  // due to config parsing failures.
  pub fn new(configs: FiltersConfiguration) -> (Self, u64) {
    let mut failures_count = 0;
    let filters = configs
      .filters
      .into_iter()
      .filter_map(|f| {
        // Filters whose configuration cannot be understood by the client are ignored, while the
        // rest of the filters remain intact. This approach strikes a balance between
        // consistency (either applying the entire filter or not applying it at all) and
        // maintaining some tolerance for errors.
        // TODO(Augustyniak): Add visibility into the failures from here.
        Filter::new(f)
          .context("invalid filter configuration")
          .inspect_err(|e| {
            failures_count += 1;
            log::debug!("{e}");
          })
          .ok()
      })
      .collect_vec();

    log::debug!(
      "{} filters created, {} failed",
      filters.len(),
      failures_count
    );

    (Self { filters }, failures_count)
  }

  pub fn process(&self, log: &mut Log) {
    for filter in &self.filters {
      let fields_ref = FieldsRef::new(&log.fields, &log.matching_fields);
      if !filter
        .matcher
        .do_match(log.log_level, log.log_type, &log.message, fields_ref, None)
      {
        continue;
      }

      log::trace!(
        "filter matched {:?} log, applying {} transforms",
        log.message,
        filter.transforms.len()
      );

      for transform in &filter.transforms {
        transform.apply(log);
      }
    }
  }
}

//
// Filter
//

pub struct Filter {
  matcher: bd_log_matcher::matcher::Tree,
  transforms: Vec<Transform>,
}

impl Filter {
  pub fn new(config: FilterProto) -> Result<Self> {
    let Some(matcher) = config.matcher.into_option() else {
      anyhow::bail!("no log matcher");
    };

    let matcher = bd_log_matcher::matcher::Tree::new(&matcher)?;
    let transforms = config
      .transforms
      .into_iter()
      .map(Transform::new)
      .collect::<Result<Vec<_>>>()
      .context("invalid transform configuration")?;

    Ok(Self {
      matcher,
      transforms,
    })
  }
}

//
// Transform
//

enum Transform {
  CaptureField(CaptureField),
  SetField(SetField),
  RemoveField(RemoveField),
  RegexMatchAndSubstitute(RegexMatchAndSubstitute),
}

impl Transform {
  fn new(config: filter::Transform) -> Result<Self> {
    let transform_type = config
      .transform_type
      .ok_or_else(|| anyhow!("no transform_type"))?;
    Ok(match transform_type {
      Transform_type::CaptureField(config) => Self::CaptureField(CaptureField::new(config)),
      Transform_type::SetField(config) => {
        Self::SetField(SetField::new(config).context("invalid SetField configuration")?)
      },
      Transform_type::RemoveField(config) => Self::RemoveField(RemoveField::new(config)),
      Transform_type::RegexMatchAndSubstituteField(config) => Self::RegexMatchAndSubstitute(
        RegexMatchAndSubstitute::new(config)
          .context("invalid RegexMatchAndSubstitute configuration")?,
      ),
    })
  }

  fn apply(&self, log: &mut Log) {
    match self {
      Self::CaptureField(capture_field) => capture_field.apply(log),
      Self::SetField(set_field) => set_field.apply(log),
      Self::RemoveField(remove_field) => remove_field.apply(log),
      Self::RegexMatchAndSubstitute(regex_match_and_substitute) => {
        regex_match_and_substitute.apply(log);
      },
    }
  }
}

//
// CaptureField
//

// Captures a given field by changing its type from matching to captured.
// This effectively removes a given fields from the list of matching fields and adds it to
// the list of captured fields.
struct CaptureField {
  field_name: LogFieldKey,
}

impl CaptureField {
  fn new(config: filter::transform::CaptureField) -> Self {
    Self {
      field_name: config.name.into(),
    }
  }

  fn apply(&self, log: &mut Log) {
    let existing_field = log.matching_fields.remove(&self.field_name);

    if let Some(existing_field) = existing_field {
      // Add the field to the list of captured fields.
      set_field(&mut log.fields, self.field_name.clone(), existing_field);
    }
  }
}

//
// FieldType
//

enum FieldType {
  MatchingOnly,
  Captured,
}

//
// SetFieldValue
//

enum SetFieldValue {
  StringValue(String),
  ExistingField(String),
}

impl SetFieldValue {
  fn new(config: filter::transform::set_field::SetFieldValue) -> Result<Self> {
    let Some(value) = config.value else {
      anyhow::bail!("no value field set");
    };

    match value {
      filter::transform::set_field::set_field_value::Value::StringValue(value) => {
        Ok(Self::StringValue(value))
      },
      filter::transform::set_field::set_field_value::Value::ExistingField(config) => {
        Ok(Self::ExistingField(config.name))
      },
    }
  }
}

//
// SetField
//

// Sets a given value for a given field. It overrides existing value in case of a field name
// conflict.
struct SetField {
  field_name: LogFieldKey,
  value: SetFieldValue,
  field_type: FieldType,
  is_override_allowed: bool,
}

impl SetField {
  fn new(config: filter::transform::SetField) -> Result<Self> {
    let field_type = config.field_type.enum_value().unwrap_or_default();

    let Some(set_field_value) = config.value.into_option() else {
      anyhow::bail!("no value field set");
    };

    Ok(Self {
      field_name: config.name.into(),
      value: SetFieldValue::new(set_field_value).context("invalid SetFieldValue configuration")?,
      field_type: match field_type {
        filter::transform::set_field::FieldType::UNKNOWN => anyhow::bail!("unknown field_type"),
        filter::transform::set_field::FieldType::CAPTURED => FieldType::Captured,
        filter::transform::set_field::FieldType::MATCHING_ONLY => FieldType::MatchingOnly,
      },
      is_override_allowed: config.allow_override,
    })
  }

  fn apply(&self, log: &mut Log) {
    if !self.is_override_allowed && extract_value(log, &self.field_name).is_some() {
      // Return if a field with the desired field name already exists and the transform is not
      // allowed to override existing values.
      return;
    }

    // Get the desired new value for the field.
    let value = match &self.value {
      SetFieldValue::StringValue(value) => value.to_string(),
      SetFieldValue::ExistingField(field_name) => {
        let Some(value) = extract_value(log, field_name) else {
          // The field to copy from doesn't exist, the transform is a no-op.
          return;
        };

        value.into_owned()
      },
    };

    match self.field_type {
      FieldType::MatchingOnly => set_field(
        &mut log.matching_fields,
        self.field_name.clone(),
        value.into(),
      ),
      FieldType::Captured => set_field(&mut log.fields, self.field_name.clone(), value.into()),
    }
  }
}

//
// RemoveField
//

// Removes a field from the list of fields (both captured and matching fields).
struct RemoveField {
  field_name: LogFieldKey,
}

impl RemoveField {
  fn new(config: filter::transform::RemoveField) -> Self {
    Self {
      field_name: config.name.into(),
    }
  }

  fn apply(&self, log: &mut Log) {
    log.matching_fields.remove(&self.field_name);
    log.fields.remove(&self.field_name);
  }
}

//
// RegexMatchAndSubstitute
//

struct RegexMatchAndSubstitute {
  field_name: String,
  pattern: Regex,
  substitution: String,
}

impl RegexMatchAndSubstitute {
  fn new(config: filter::transform::RegexMatchAndSubstituteField) -> Result<Self> {
    Ok(Self {
      field_name: config.name,
      pattern: Regex::new(&config.pattern)?,
      substitution: config.substitution,
    })
  }

  fn apply(&self, log: &mut Log) {
    for (key, value) in &mut log.fields {
      if *key != self.field_name {
        continue;
      }
      if let Some(field_string_value) = value.as_str() {
        *value = self
          .produce_new_value(field_string_value)
          .to_string()
          .into();
      }

      // TODO(Augustyniak): This makes an assumption that regex match and substitution applies to
      // the first field with a given name only. This is going to be simplified once we
      // change `LogFields` to be a map instead of a list.
      return;
    }

    for (key, value) in &mut log.matching_fields {
      if *key != self.field_name {
        continue;
      }
      if let Some(field_string_value) = value.as_str() {
        *value = self
          .produce_new_value(field_string_value)
          .to_string()
          .into();
      }

      // TODO(Augustyniak): This makes an assumption that regex match and substitution applies to
      // the first field with a given name only. This is going to be simplified once we
      // change `LogFields`  to be a map instead of a list.
      return;
    }
  }

  fn produce_new_value<'a>(&self, input: &'a str) -> Cow<'a, str> {
    self.pattern.replace_all(input, &self.substitution)
  }
}

fn set_field(fields: &mut LogFields, key: LogFieldKey, value: LogFieldValue) {
  // If a field that's supposed to be captured exists, remove it from the list of captured fields
  // before adding it.
  fields.insert(key, value);
}


/// Extracts a value from the log using the convention that certain names map to the log messsage,
/// type or log level while others map to an arbitrary field.
fn extract_value<'a>(log: &'a Log, key: &str) -> Option<Cow<'a, str>> {
  match key {
    LOG_FIELD_NAME_MESSAGE => log.message.as_str().map(Cow::Borrowed),
    LOG_FIELD_NAME_LEVEL => Some(Cow::Owned(log.log_level.to_string())),
    LOG_FIELD_NAME_TYPE => Some(Cow::Owned(log.log_type.0.to_string())),
    _ => log
      .fields
      .iter()
      .chain(log.matching_fields.iter())
      .find_map(|(k, v)| (k == key).then_some(v))
      .and_then(|f| f.as_str())
      .map(Into::into),
  }
}
