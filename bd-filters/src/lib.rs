use anyhow::{anyhow, Result};
use bd_proto::protos::filter::filter::filter::transform::capture_fields::fields::Fields_type;
use bd_proto::protos::filter::filter::filter::{self};
use bd_proto::protos::filter::filter::{Filter as FilterProto, FiltersConfiguration};
use filter::transform::Transform_type;

pub struct FiltersChain {
  filters: Vec<Filter>,
}

impl FiltersChain {
  pub fn new(configs: FiltersConfiguration) -> Result<Self> {
    // let mut filters = Vec::new();
    let filters = configs.filters.into_iter().map(Filter::new).collect::<Result<Vec<_>>>()?;
    // for config in configs.filters {
      // filters.push(Filter::new(config)?);
    // }
    Ok(Self { filters })
  }
}

pub struct Filter {
  matcher: bd_matcher::matcher::Tree,
  transforms: Vec<Transform>,
}

impl Filter {
  pub fn new(config: FilterProto) -> Result<Self> {
    // config.matcher.opt
    let Some(matcher) = config.matcher.into_option() else {
      anyhow::bail!("no matcher");
    };

    let matcher = bd_matcher::matcher::Tree::new(&matcher)?;
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

enum Transform {
  CaptureField(CaptureField),
  SetField(SetField),
}

impl Transform {
  pub fn new(config: filter::Transform) -> Result<Self> {
    let transform_type = config.transform_type.ok_or(anyhow!("asd"))?;
    Ok(match transform_type {
      Transform_type::CaptureFields(config) => Self::CaptureField(CaptureField::new(config)?),
      Transform_type::SetField(config) => Self::SetField(SetField::new(config)?),
    })
  }
}

struct CaptureField {
  field_name: String,
}

impl CaptureField {
  fn new(config: filter::transform::CaptureFields) -> Result<Self> {
    let Some(ref fields_type) = config.fields.fields_type else {
      anyhow::bail!("no field type")
    };

    let Fields_type::Single(field) = fields_type;

    Ok(Self {
      field_name: field.name.clone(),
    })
  }
}

struct SetField {
  field_name: String,
  value: String,
}

impl SetField {
  fn new(config: filter::transform::SetField) -> Result<Self> {
    Ok(Self {
      field_name: config.name,
      value: config.value,
    })
  }
}
