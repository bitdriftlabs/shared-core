// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./filter_chain_test.rs"]
mod filter_chain_test;

use anyhow::{anyhow, Context, Result};
use bd_log_primitives::{FieldsRef, Log, LogField, LogFields};
use bd_proto::protos::filter::filter::filter::transform::capture_fields::fields::Fields_type;
use bd_proto::protos::filter::filter::filter::{self};
use bd_proto::protos::filter::filter::{Filter as FilterProto, FiltersConfiguration};
use filter::transform::Transform_type;

//
// FilterChain
//

// A top-level object that encompasses filters to apply to emitted logs.
pub struct FilterChain {
  filters: Vec<Filter>,
}

impl FilterChain {
  pub fn new(configs: FiltersConfiguration) -> Self {
    let filters = configs
      .filters
      .into_iter()
      .filter_map(|f| {
        // Filters whose configuration cannot be understood by the client are ignored, while the
        // rest of the filters remain intact. This approach strikes a balance between
        // consistency (either applying the entire filter or not applying it at all) and
        // maintaining some tolerance for errors.
        // TODO(Augustyniak): Add visibility into the failures from here.
        Filter::new(f).ok()
      })
      .collect();

    Self { filters }
  }

  pub fn process(&self, log: &mut Log) {
    for filter in &self.filters {
      let fields_ref = FieldsRef::new(&log.fields, &log.matching_fields);
      if !filter
        .matcher
        .do_match(log.log_level, log.log_type, &log.message, &fields_ref)
      {
        continue;
      }

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
      anyhow::bail!("invalid filter configuration: no log matcher");
    };

    let matcher = bd_log_matcher::matcher::Tree::new(&matcher)?;
    let transforms = config
      .transforms
      .into_iter()
      .map(Transform::new)
      .collect::<Result<Vec<_>>>()?;

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
}

impl Transform {
  fn new(config: filter::Transform) -> Result<Self> {
    let transform_type = config
      .transform_type
      .ok_or_else(|| anyhow!("invalid transform configuration: no transform_type"))?;
    Ok(match transform_type {
      Transform_type::CaptureFields(config) => Self::CaptureField(
        CaptureField::new(config).context("invalid CaptureFields configuration")?,
      ),
      Transform_type::SetField(config) => {
        Self::SetField(SetField::new(config).context("invalid SetField configuration")?)
      },
    })
  }

  fn apply(&self, log: &mut Log) {
    match self {
      Self::CaptureField(capture_field) => capture_field.apply(log),
      Self::SetField(set_field) => set_field.apply(log),
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
  field_name: String,
}

impl CaptureField {
  fn new(mut config: filter::transform::CaptureFields) -> Result<Self> {
    let Some(Fields_type::Single(field)) = config.fields.take().unwrap_or_default().fields_type
    else {
      anyhow::bail!("unknown fields_type")
    };

    Ok(Self {
      field_name: field.name,
    })
  }

  fn apply(&self, log: &mut Log) {
    // Look for a field that's supposed to be captured.
    let Some(field_position) = log
      .matching_fields
      .iter()
      .position(|field| field.key == self.field_name)
    else {
      // Matching field with a given key doesn't exist so there is nothing to capture.
      return;
    };

    let field = log.matching_fields.remove(field_position);
    set_field(&mut log.fields, field);
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
// SetField
//

// Sets a given value for a given field. It overrides existing value in case of a field name
// conflict.
struct SetField {
  field_name: String,
  value: String,
  field_type: FieldType,
}

impl SetField {
  fn new(config: filter::transform::SetField) -> Result<Self> {
    let field_type = config.field_type.enum_value().unwrap_or_default();

    Ok(Self {
      field_name: config.name,
      value: config.value,
      field_type: match field_type {
        filter::transform::set_field::FieldType::UNKNOWN => anyhow::bail!("unknown field_type"),
        filter::transform::set_field::FieldType::CAPTURED => FieldType::Captured,
        filter::transform::set_field::FieldType::MATCHING_ONLY => FieldType::MatchingOnly,
      },
    })
  }

  fn apply(&self, log: &mut Log) {
    let field = bd_log_primitives::LogField {
      key: self.field_name.clone(),
      value: self.value.clone().into(),
    };

    match self.field_type {
      FieldType::MatchingOnly => set_field(&mut log.matching_fields, field),
      FieldType::Captured => set_field(&mut log.fields, field),
    }
  }
}

fn set_field(fields: &mut LogFields, field: LogField) {
  // If a field that's supposed to be captured exists, remove it from the list of captured fields
  // before adding it.
  fields.retain(|f| f.key != field.key);
  fields.push(field);
}
