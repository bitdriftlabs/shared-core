use super::{
  CaptureField,
  FieldType,
  Filter,
  FilterChain,
  RegexMatchAndSubstitute,
  RegexMatchAndSubstituteTarget,
  RemoveField,
  SetField,
  SetFieldValue,
  Transform,
};
use anyhow::{Result, anyhow, bail};
use bd_log_matcher::matcher::Tree;
use bd_proto::protos::filter::filter::filter::transform::set_field::FieldType as ProtoFieldType;
use bd_proto_util::serialization::inline::views::filter::{
  self as wire,
  FilterTransformRegexMatchAndSubstituteFieldScrubbingTarget as ScrubKind,
  FilterTransformSetFieldSetFieldValueValue as ValueKind,
  FilterTransformTransformType as TransformKind,
};
use regex_lite::Regex;

impl FilterChain {
  pub fn from_view(config: &wire::FiltersConfiguration<'_>) -> Result<(Self, u64)> {
    let mut failures_count = 0;
    let filters = config
      .filters()?
      .iter()
      .filter_map(|filter| {
        Filter::from_view(filter)
          .inspect_err(|error| {
            failures_count += 1;
            log::debug!("invalid filter configuration: {error}");
          })
          .ok()
      })
      .collect();
    Ok((Self { filters }, failures_count))
  }
}

impl Filter {
  fn from_view(config: &wire::Filter<'_>) -> Result<Self> {
    let matcher = config
      .matcher()?
      .ok_or_else(|| anyhow!("matcher field not set"))?;
    Ok(Self {
      matcher: Tree::from_view(&matcher)?,
      transforms: config
        .transforms()?
        .iter()
        .map(Transform::from_view)
        .collect::<Result<_>>()?,
    })
  }
}

impl Transform {
  fn from_view(config: &wire::FilterTransform<'_>) -> Result<Self> {
    Ok(
      match config
        .transform_type()?
        .ok_or_else(|| anyhow!("transform_type field not set"))?
      {
        TransformKind::CaptureField(config) => Self::CaptureField(CaptureField {
          field_name: config.name()?.to_owned().into(),
        }),
        TransformKind::RemoveField(config) => Self::RemoveField(RemoveField {
          field_name: config.name()?.to_owned().into(),
        }),
        TransformKind::SetField(config) => Self::SetField(SetField::from_view(&config)?),
        TransformKind::RegexMatchAndSubstituteField(config) => {
          Self::RegexMatchAndSubstitute(RegexMatchAndSubstitute::from_view(&config)?)
        },
      },
    )
  }
}

impl SetField {
  fn from_view(config: &wire::FilterTransformSetField<'_>) -> Result<Self> {
    let value = config
      .value()?
      .ok_or_else(|| anyhow!("no value field set"))?;
    let value = match value
      .value()?
      .ok_or_else(|| anyhow!("invalid SetFieldValue configuration: no value field set"))?
    {
      ValueKind::StringValue(value) => SetFieldValue::StringValue(value.to_owned()),
      ValueKind::ExistingField(field) => SetFieldValue::ExistingField(field.name()?.to_owned()),
    };
    let field_type = match config.field_type()?.enum_value_or_default() {
      ProtoFieldType::UNKNOWN => bail!("unknown field_type"),
      ProtoFieldType::CAPTURED => FieldType::Captured,
      ProtoFieldType::MATCHING_ONLY => FieldType::MatchingOnly,
    };
    Ok(Self {
      field_name: config.name()?.to_owned().into(),
      value,
      field_type,
      is_override_allowed: config.allow_override()?,
    })
  }
}

impl RegexMatchAndSubstitute {
  fn from_view(config: &wire::FilterTransformRegexMatchAndSubstituteField<'_>) -> Result<Self> {
    let target = match config
      .scrubbing_target()?
      .ok_or_else(|| anyhow!("no scrubbing_target set"))?
    {
      ScrubKind::Name(name) => RegexMatchAndSubstituteTarget::Field(name.to_owned().into()),
      ScrubKind::MessageBody(true) => RegexMatchAndSubstituteTarget::MessageBody,
      ScrubKind::MessageBody(false) => bail!("message_body scrubbing target must be true"),
      ScrubKind::GlobalScrub(scrub) => RegexMatchAndSubstituteTarget::GlobalScrub {
        ignored_fields: scrub
          .ignored_fields()?
          .into_iter()
          .map(Into::into)
          .collect(),
      },
    };
    log::debug!(
      "creating RegexMatchAndSubstitute transform for target '{target:?}', pattern '{}', \
       substitution '{}'",
      config.pattern()?,
      config.substitution()?
    );
    Ok(Self {
      target,
      pattern: Regex::new(config.pattern()?)?,
      substitution: config.substitution()?.to_owned(),
    })
  }
}
