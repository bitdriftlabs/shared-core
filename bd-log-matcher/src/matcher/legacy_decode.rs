use super::{InputType, Leaf, Tree};
use crate::value_matcher::{IntMatch, StringMatch, ValueOrSavedFieldId};
use anyhow::{Result, anyhow};
use bd_proto::protos::config::v1::config::log_matcher::base_log_matcher::{
  StringMatchType,
  log_level_match,
};
use bd_proto::protos::value_matcher::value_matcher::Operator;
use bd_proto_util::serialization::inline::views::config::{
  self as wire,
  LogMatcherBaseLogMatcherMatchType as LeafKind,
  LogMatcherMatchType as MatcherKind,
};
use log_level_match::ComparisonOperator;
use regex_lite::escape;

impl Tree {
  pub fn from_legacy_view(config: &wire::LogMatcher<'_>) -> Result<Self> {
    match config
      .match_type()?
      .ok_or_else(|| anyhow!("missing legacy log matcher"))?
    {
      MatcherKind::BaseMatcher(matcher) => Ok(Self::Base(Leaf::from_legacy_view(&matcher)?)),
      MatcherKind::OrMatcher(matcher) => Ok(Self::Or(
        matcher
          .matcher()?
          .iter()
          .map(Self::from_legacy_view)
          .collect::<Result<_>>()?,
      )),
      MatcherKind::AndMatcher(matcher) => Ok(Self::And(
        matcher
          .matcher()?
          .iter()
          .map(Self::from_legacy_view)
          .collect::<Result<_>>()?,
      )),
      MatcherKind::NotMatcher(matcher) => {
        Ok(Self::Not(Box::new(Self::from_legacy_view(&matcher)?)))
      },
    }
  }
}

impl Leaf {
  fn from_legacy_view(config: &wire::LogMatcherBaseLogMatcher<'_>) -> Result<Self> {
    match config
      .match_type()?
      .ok_or_else(|| anyhow!("missing legacy log matcher"))?
    {
      LeafKind::LogLevelMatch(matcher) => {
        let operator = match matcher.operator()?.enum_value_or_default() {
          ComparisonOperator::LESS_THAN => Operator::OPERATOR_LESS_THAN,
          ComparisonOperator::LESS_THAN_OR_EQUAL => Operator::OPERATOR_LESS_THAN_OR_EQUAL,
          ComparisonOperator::EQUALS => Operator::OPERATOR_EQUALS,
          ComparisonOperator::GREATER_THAN => Operator::OPERATOR_GREATER_THAN,
          ComparisonOperator::GREATER_THAN_OR_EQUAL => Operator::OPERATOR_GREATER_THAN_OR_EQUAL,
        };
        Ok(Self::LogLevel(IntMatch::new(
          operator.into(),
          ValueOrSavedFieldId::Value(matcher.log_level()?.value()),
        )?))
      },
      LeafKind::MessageMatch(matcher) => Ok(Self::StringValue(
        InputType::Message,
        map_string_value(
          matcher.match_value()?,
          matcher.match_type()?.enum_value_or_default(),
        )?,
      )),
      LeafKind::TagMatch(matcher) => Ok(Self::StringValue(
        InputType::Field(matcher.tag_key()?.to_owned()),
        map_string_value(
          matcher.match_value()?,
          matcher.match_type()?.enum_value_or_default(),
        )?,
      )),
      LeafKind::TypeMatch(matcher) => Ok(Self::LogType(matcher.type_()?)),
      LeafKind::AnyMatch(_) => Ok(Self::Any),
    }
  }
}

pub(super) fn map_string_value(value: &str, match_type: StringMatchType) -> Result<StringMatch> {
  let (value, operator) = match match_type {
    StringMatchType::EXACT => (value.to_owned(), Operator::OPERATOR_EQUALS),
    StringMatchType::PREFIX => (format!("^{}.*", escape(value)), Operator::OPERATOR_REGEX),
    StringMatchType::REGEX => (value.to_owned(), Operator::OPERATOR_REGEX),
  };
  StringMatch::new(operator.into(), value.into())
}
