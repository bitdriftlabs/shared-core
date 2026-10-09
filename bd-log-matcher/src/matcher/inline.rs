#[cfg(test)]
#[path = "./inline_test.rs"]
mod tests;

use super::{InputType, JsonPathToken, LOG_LEVEL_KEY, LOG_TYPE_KEY, Leaf, Tree};
use crate::state_value_matcher::StateValueMatcher;
use crate::value_matcher::{
  DoubleMatch,
  IntMatch,
  NanEqualFloat,
  StringMatch,
  ValueOrSavedFieldId,
};
use crate::version::VersionMatch;
use anyhow::{anyhow, bail};
use bd_proto::protos::state::scope::StateScope;
use bd_proto_util::serialization::inline::views::log_matcher::{
  self as wire,
  LogMatcherBaseLogMatcherMatchType as LeafKind,
  LogMatcherBaseLogMatcherTagMatchValueMatch as TagKind,
  LogMatcherMatcher as MatcherKind,
};
use bd_proto_util::serialization::inline::views::matcher::{
  self as state_wire,
  StateValueMatchValueMatch as StateKind,
};
use bd_proto_util::serialization::inline::views::value_matcher::{
  self as values,
  DoubleValueMatchDoubleValueMatchType as DoubleValue,
  IntValueMatchIntValueMatchType as IntValue,
  JsonPathValueMatchKeyOrIndexKeyOrIndex as PathValue,
  StringValueMatchStringValueMatchType as StringValue,
};
use bd_proto_util::serialization::inline::{Message, ProtoDeserialize};
use bd_state::Scope;

impl Tree {
  pub fn from_proto_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
    Self::from_view(&wire::LogMatcher::from_proto_bytes(bytes)?)
  }

  pub fn from_inline(message: &Message<'_>) -> anyhow::Result<Self> {
    Self::from_view(&wire::LogMatcher::from_inline(message)?)
  }

  pub fn from_view(message: &wire::LogMatcher<'_>) -> anyhow::Result<Self> {
    match message
      .matcher()?
      .ok_or_else(|| anyhow!("missing log matcher"))?
    {
      MatcherKind::BaseMatcher(child) => Ok(Self::Base(Leaf::from_view(&child)?)),
      MatcherKind::OrMatcher(child) => Ok(Self::Or(
        child
          .log_matchers()?
          .iter()
          .map(Self::from_view)
          .collect::<anyhow::Result<_>>()?,
      )),
      MatcherKind::AndMatcher(child) => Ok(Self::And(
        child
          .log_matchers()?
          .iter()
          .map(Self::from_view)
          .collect::<anyhow::Result<_>>()?,
      )),
      MatcherKind::NotMatcher(child) => Ok(Self::Not(Box::new(Self::from_view(&child)?))),
    }
  }
}

impl Leaf {
  fn from_view(message: &wire::LogMatcherBaseLogMatcher<'_>) -> anyhow::Result<Self> {
    match message
      .match_type()?
      .ok_or_else(|| anyhow!("missing log_matcher"))?
    {
      LeafKind::MessageMatch(child) => Ok(Self::StringValue(
        InputType::Message,
        string_match(
          &child
            .string_value_match()?
            .ok_or_else(|| anyhow!("missing protobuf message field 1"))?,
        )?,
      )),
      LeafKind::TagMatch(child) => Self::tag_from_view(&child),
      LeafKind::StateMatch(child) => {
        let scope = match child.scope()?.enum_value_or_default() {
          StateScope::FEATURE_FLAG => Scope::FeatureFlagExposure,
          StateScope::GLOBAL_STATE => Scope::GlobalState,
          StateScope::SYSTEM => Scope::System,
          _ => bail!("Unsupported state scope"),
        };
        let input = InputType::State(scope, child.state_key()?.to_owned());
        Ok(
          match StateValueMatcher::from_view(
            &child
              .state_value_match()?
              .ok_or_else(|| anyhow!("missing protobuf message field 3"))?,
          )? {
            StateValueMatcher::String(matcher) => Self::StringValue(input, matcher),
            StateValueMatcher::Int(matcher) => Self::IntValue(input, matcher),
            StateValueMatcher::Double(matcher) => Self::DoubleValue(input, matcher),
            StateValueMatcher::IsSet => Self::IsSetValue(input),
          },
        )
      },
      LeafKind::SampledMatch(child) => Ok(Self::Sampled(child.sample_rate()?)),
    }
  }

  fn tag_from_view(message: &wire::LogMatcherBaseLogMatcherTagMatch<'_>) -> anyhow::Result<Self> {
    let key = message.tag_key()?.to_owned();
    let input = InputType::Field(key.clone());
    Ok(
      match message
        .value_match()?
        .ok_or_else(|| anyhow!("missing tag_match value_match"))?
      {
        TagKind::StringValueMatch(child) => Self::StringValue(input, string_match(&child)?),
        TagKind::IntValueMatch(child) if key == LOG_LEVEL_KEY => Self::LogLevel(int_match(&child)?),
        TagKind::IntValueMatch(child) if key == LOG_TYPE_KEY => {
          let ValueOrSavedFieldId::Value(value) = int_value(&child)? else {
            bail!("log_type must be a value");
          };
          Self::LogType(value.try_into()?)
        },
        TagKind::IntValueMatch(child) => Self::IntValue(input, int_match(&child)?),
        TagKind::SemVerValueMatch(child) => Self::VersionValue(
          input,
          VersionMatch::new(child.operator()?, child.match_value()?)?,
        ),
        TagKind::IsSetMatch(_) => Self::IsSetValue(input),
        TagKind::DoubleValueMatch(child) => Self::DoubleValue(input, double_match(&child)?),
        TagKind::JsonValueMatch(child) => Self::JsonPathValue {
          field_key: key,
          path: child
            .key_or_index()?
            .iter()
            .map(parse_json_path_view)
            .collect::<anyhow::Result<_>>()?,
          matcher: StringMatch::new(child.operator()?, child.match_value()?.to_owned().into())?,
        },
      },
    )
  }
}

impl StateValueMatcher {
  pub fn from_inline(message: &Message<'_>) -> anyhow::Result<Self> {
    Self::from_view(&state_wire::StateValueMatch::from_inline(message)?)
  }

  pub fn from_view(message: &state_wire::StateValueMatch<'_>) -> anyhow::Result<Self> {
    match message
      .value_match()?
      .ok_or_else(|| anyhow!("missing value_match in StateValueMatch"))?
    {
      StateKind::StringValueMatch(child) => Ok(Self::String(string_match(&child)?)),
      StateKind::IntValueMatch(child) => Ok(Self::Int(int_match(&child)?)),
      StateKind::DoubleValueMatch(child) => Ok(Self::Double(double_match(&child)?)),
      StateKind::IsSetMatch(_) => Ok(Self::IsSet),
    }
  }
}

pub fn parse_json_path_inline(message: &Message<'_>) -> anyhow::Result<JsonPathToken> {
  parse_json_path_view(&values::JsonPathValueMatchKeyOrIndex::from_inline(message)?)
}

pub fn parse_json_path_view(
  message: &values::JsonPathValueMatchKeyOrIndex<'_>,
) -> anyhow::Result<JsonPathToken> {
  match message
    .key_or_index()?
    .ok_or_else(|| anyhow!("missing json path key or index"))?
  {
    PathValue::Key(key) => Ok(JsonPathToken::Key(key.to_owned())),
    PathValue::Index(index) => Ok(JsonPathToken::Index(index)),
  }
}

fn int_value(message: &values::IntValueMatch<'_>) -> anyhow::Result<ValueOrSavedFieldId<i32>> {
  Ok(match message.int_value_match_type()? {
    Some(IntValue::SaveFieldId(id)) => ValueOrSavedFieldId::SaveFieldId(id.to_owned()),
    Some(IntValue::MatchValue(value)) => ValueOrSavedFieldId::Value(value),
    None => ValueOrSavedFieldId::Value(0),
  })
}

fn int_match(message: &values::IntValueMatch<'_>) -> anyhow::Result<IntMatch> {
  IntMatch::new(message.operator()?, int_value(message)?)
}

fn string_match(message: &values::StringValueMatch<'_>) -> anyhow::Result<StringMatch> {
  let value = match message.string_value_match_type()? {
    Some(StringValue::SaveFieldId(id)) => ValueOrSavedFieldId::SaveFieldId(id.to_owned()),
    Some(StringValue::MatchValue(value)) => ValueOrSavedFieldId::Value(value.to_owned()),
    None => ValueOrSavedFieldId::Value(String::new()),
  };
  StringMatch::new(message.operator()?, value)
}

fn double_match(message: &values::DoubleValueMatch<'_>) -> anyhow::Result<DoubleMatch> {
  let value = match message.double_value_match_type()? {
    Some(DoubleValue::SaveFieldId(id)) => ValueOrSavedFieldId::SaveFieldId(id.to_owned()),
    Some(DoubleValue::MatchValue(value)) => ValueOrSavedFieldId::Value(NanEqualFloat(value)),
    None => ValueOrSavedFieldId::Value(NanEqualFloat(0.0)),
  };
  DoubleMatch::new(message.operator()?, value)
}
