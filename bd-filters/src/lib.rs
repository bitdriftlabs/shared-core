use anyhow::{anyhow, Result};
use bd_log_primitives::owned::LogLine;
use bd_log_primitives::FieldsRef;
use bd_proto::protos::filter::filter::filter::transform::capture_fields::fields::Fields_type;
use bd_proto::protos::filter::filter::filter::{self};
use bd_proto::protos::filter::filter::{Filter as FilterProto, FiltersConfiguration};
use filter::transform::Transform_type;

//
// FiltersChain
//

pub struct FiltersChain {
  filters: Vec<Filter>,
}

impl FiltersChain {
  pub fn new(configs: FiltersConfiguration) -> Self {
    let filters = configs
      .filters
      .into_iter()
      .filter_map(|f| {
        // Filters whose configuration cannot be understood by the client are ignored, while the
        // rest of the filters remain intact. This approach strikes a balance between
        // consistency (either applying the entire filter or not applying it at all) and
        // maintaining some tolerance for errors.
        Filter::new(f).ok()
      })
      .collect();

    Self { filters }
  }

  pub fn process(&self, log: &mut LogLine) {
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
      Transform_type::CaptureFields(config) => {
        Self::CaptureField(CaptureField::new(config).map_err(|e| e.context("CaptureFields"))?)
      },
      Transform_type::SetField(config) => {
        Self::SetField(SetField::new(config).map_err(|e| e.context("SetField"))?)
      },
    })
  }

  fn apply(&self, log: &mut LogLine) {
    match self {
      Self::CaptureField(capture_field) => capture_field.apply(log),
      Self::SetField(set_field) => set_field.apply(log),
    }
  }
}

//
// CaptureField
//

struct CaptureField {
  field_name: String,
}

impl CaptureField {
  fn new(config: filter::transform::CaptureFields) -> Result<Self> {
    let Some(ref fields_type) = config.fields.fields_type else {
      anyhow::bail!("no fields_type")
    };

    let Fields_type::Single(field) = fields_type;

    Ok(Self {
      field_name: field.name.clone(),
    })
  }

  fn apply(&self, log: &mut LogLine) {
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

    // If a field that's supposed to be captured exists, remove it from the list of captured fields
    // before adding it.
    log.fields.retain(|field| field.key != self.field_name);
    log.fields.push(field)
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

struct SetField {
  field_name: String,
  value: String,
  field_type: FieldType,
}

impl SetField {
  fn new(config: filter::transform::SetField) -> Result<Self> {
    let field_type = config
      .field_type
      .enum_value()
      .map_err(|e| anyhow!(format!("invalid enum value {e}")))?;

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

  fn apply(&self, log: &mut LogLine) {
    let field = bd_log_primitives::LogField {
      key: self.field_name.clone(),
      value: self.value.clone().into(),
    };

    match self.field_type {
      FieldType::MatchingOnly => {
        log
          .matching_fields
          .retain(|field| field.key != self.field_name);
        log.matching_fields.push(field)
      },
      FieldType::Captured => {
        log.fields.retain(|field| field.key != self.field_name);
        log.fields.push(field)
      },
    }
  }
}
