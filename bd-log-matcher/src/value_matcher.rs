use anyhow::{Result, anyhow};
use bd_log_primitives::tiny_set::TinyMap;
use bd_proto::protos::value_matcher::value_matcher::double_value_match::Double_value_match_type;
use bd_proto::protos::value_matcher::value_matcher::int_value_match::Int_value_match_type;
use bd_proto::protos::value_matcher::value_matcher::string_value_match::String_value_match_type;
use bd_proto::protos::value_matcher::value_matcher::{
  IntValueMatch as IntValueMatch_type,
  Operator,
  StringValueMatch as StringValueMatch_type,
};
use protobuf::EnumOrUnknown;
use regex::Regex;
use std::ops::Deref;

//
// ValueOrSavedFieldId
//

// This entire dance is so that we can either store a value T, or an ID to a saved field, and then
// resolve it to T later, in either value or reference form, with minimal copying.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValueOrSavedFieldId<T> {
  Value(T),
  SaveFieldId(String),
}

impl ValueOrSavedFieldId<i32> {
  pub fn from_proto(int_value_match: &IntValueMatch_type) -> Self {
    // This used to not be a oneof so supply an equivalent default if the field is not set.
    match int_value_match
      .int_value_match_type
      .as_ref()
      .unwrap_or(&Int_value_match_type::MatchValue(0))
    {
      Int_value_match_type::MatchValue(v) => Self::Value(*v),
      Int_value_match_type::SaveFieldId(s) => Self::SaveFieldId(s.clone()),
    }
  }
}

impl ValueOrSavedFieldId<String> {
  pub fn from_proto(string_value_match: &StringValueMatch_type) -> Self {
    // This used to not be a oneof so supply an equivalent default if the field is not set.
    match string_value_match
      .string_value_match_type
      .as_ref()
      .unwrap_or(&String_value_match_type::MatchValue(String::new()))
    {
      String_value_match_type::MatchValue(s) => Self::Value(s.clone()),
      String_value_match_type::SaveFieldId(s) => Self::SaveFieldId(s.clone()),
    }
  }
}

impl ValueOrSavedFieldId<NanEqualFloat> {
  pub fn from_proto(
    double_value_match: &bd_proto::protos::value_matcher::value_matcher::DoubleValueMatch,
  ) -> Self {
    // This used to not be a oneof so supply an equivalent default if the field is not
    // set.
    match double_value_match
      .double_value_match_type
      .as_ref()
      .unwrap_or(&Double_value_match_type::MatchValue(0.0))
    {
      Double_value_match_type::MatchValue(d) => Self::Value(NanEqualFloat(*d)),
      Double_value_match_type::SaveFieldId(s) => Self::SaveFieldId(s.clone()),
    }
  }
}

pub enum ValueOrRef<'a, T> {
  Value(T),
  Ref(&'a T),
}

//
// NanEqualFloat
//

/// A float which compares as equal when both are NaN.
#[derive(Clone, Debug, PartialOrd)]
pub struct NanEqualFloat(pub f64);

impl PartialEq for NanEqualFloat {
  fn eq(&self, other: &Self) -> bool {
    (self.0.is_nan() && other.0.is_nan()) || ((self.0 - other.0).abs() < f64::EPSILON)
  }
}

impl Eq for NanEqualFloat {}

pub trait MakeValueOrRef<'a, T> {
  #[allow(clippy::ptr_arg)]
  fn make_value_or_ref(value: &'a String) -> Option<ValueOrRef<'a, T>>;
}

impl<T> From<T> for ValueOrSavedFieldId<T> {
  fn from(value: T) -> Self {
    Self::Value(value)
  }
}

impl<'a, T: MakeValueOrRef<'a, T>> ValueOrSavedFieldId<T> {
  pub fn load(
    &'a self,
    extracted_fields: &'a TinyMap<String, String>,
  ) -> Option<ValueOrRef<'a, T>> {
    match self {
      Self::Value(v) => Some(ValueOrRef::Ref(v)),
      Self::SaveFieldId(field_id) => extracted_fields
        .get(field_id)
        .and_then(|v| T::make_value_or_ref(v)),
    }
  }
}

impl<T> Deref for ValueOrRef<'_, T> {
  type Target = T;

  fn deref(&self) -> &Self::Target {
    match self {
      ValueOrRef::Value(v) => v,
      ValueOrRef::Ref(v) => v,
    }
  }
}

impl<'a> MakeValueOrRef<'a, Self> for i32 {
  fn make_value_or_ref(value: &String) -> Option<ValueOrRef<'a, Self>> {
    value.parse::<Self>().ok().map(ValueOrRef::Value)
  }
}

impl<'a> MakeValueOrRef<'a, Self> for NanEqualFloat {
  fn make_value_or_ref(value: &String) -> Option<ValueOrRef<'a, Self>> {
    value
      .parse::<f64>()
      .ok()
      .map(|v| ValueOrRef::Value(Self(v)))
  }
}

impl<'a> MakeValueOrRef<'a, Self> for String {
  fn make_value_or_ref(value: &'a Self) -> Option<ValueOrRef<'a, Self>> {
    Some(ValueOrRef::Ref(value))
  }
}

//
// StringMatch
//

/// Describes a comparison match criteria against a String value
#[derive(Clone, Debug)]
pub struct StringMatch {
  operator: Operator,
  value: ValueOrSavedFieldId<String>,
  regex: Option<Regex>,
}

// `Regex` doesn't implement `PartialEq`.
impl std::cmp::PartialEq for StringMatch {
  fn eq(&self, other: &Self) -> bool {
    self.operator == other.operator
    && self.value == other.value
    // regex's `as_str` returns the underlying regex string.
    && self.regex.as_ref().map_or("", |r| r.as_str()) == other.regex.as_ref().map_or("", |r| r.as_str())
  }
}

impl std::cmp::Eq for StringMatch {}

/// Supports comparison between two Strings
impl StringMatch {
  pub fn from_proto(
    proto: &bd_proto::protos::value_matcher::value_matcher::StringValueMatch,
  ) -> Result<Self> {
    Self::new(
      proto.operator,
      ValueOrSavedFieldId::<String>::from_proto(proto),
    )
  }

  pub fn new(
    operator: EnumOrUnknown<Operator>,
    value: ValueOrSavedFieldId<String>,
  ) -> Result<Self> {
    let operator = operator.enum_value_or_default();

    if operator == Operator::OPERATOR_UNSPECIFIED {
      return Err(anyhow!("UNSPECIFIED operator"));
    }

    // Compile the regex on tree creation to avoid recompiling it on every match.
    let regex = match operator {
      Operator::OPERATOR_REGEX => {
        let ValueOrSavedFieldId::Value(value) = &value else {
          return Err(anyhow!("regex operator requires a value"));
        };

        Some(match Regex::new(value) {
          Ok(regex) => regex,
          Err(e) => return Err(anyhow::Error::new(e).context("invalid regex")),
        })
      },
      _ => None,
    };
    Ok(Self {
      operator,
      value,
      regex,
    })
  }

  pub fn evaluate(&self, candidate: &str, extracted_fields: &TinyMap<String, String>) -> bool {
    let Some(value) = self.value.load(extracted_fields) else {
      return false;
    };

    match self.operator {
      // This should never happen as we check for UNSPECIFIED when we parse
      // workflow config.
      Operator::OPERATOR_UNSPECIFIED => false,
      Operator::OPERATOR_LESS_THAN => *candidate < **value,
      Operator::OPERATOR_LESS_THAN_OR_EQUAL => *candidate <= **value,
      Operator::OPERATOR_EQUALS => candidate == *value,
      Operator::OPERATOR_GREATER_THAN => *candidate > **value,
      Operator::OPERATOR_GREATER_THAN_OR_EQUAL => *candidate >= **value,
      Operator::OPERATOR_NOT_EQUALS => candidate != *value,
      Operator::OPERATOR_REGEX => self
        .regex
        .as_ref()
        .is_some_and(|regex| regex.is_match(candidate)),
    }
  }
}

//
// DoubleMatch
//

/// Describes a comparison match criteria against a f64 value
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DoubleMatch {
  operator: Operator,
  value: ValueOrSavedFieldId<NanEqualFloat>,
}

/// Supports comparison between two integers
impl DoubleMatch {
  pub fn from_proto(
    proto: &bd_proto::protos::value_matcher::value_matcher::DoubleValueMatch,
  ) -> Result<Self> {
    Self::new(
      proto.operator,
      ValueOrSavedFieldId::<NanEqualFloat>::from_proto(proto),
    )
  }

  pub fn new(
    operator: EnumOrUnknown<Operator>,
    value: ValueOrSavedFieldId<NanEqualFloat>,
  ) -> Result<Self> {
    let operator = operator.enum_value_or_default();
    match operator {
      // Regex operator is not valid for f64
      Operator::OPERATOR_REGEX => Err(anyhow!("regex does not support f64")),
      _ => Ok(Self { operator, value }),
    }
  }

  pub fn evaluate(&self, candidate: f64, extracted_fields: &TinyMap<String, String>) -> bool {
    let candidate = NanEqualFloat(candidate);
    let Some(value) = self.value.load(extracted_fields) else {
      return false;
    };

    match self.operator {
      // This should never happen as we check for UNSPECIFIED when we parse
      // workflow config.
      Operator::OPERATOR_UNSPECIFIED => false,
      Operator::OPERATOR_LESS_THAN => candidate < *value,
      Operator::OPERATOR_LESS_THAN_OR_EQUAL => candidate <= *value,
      Operator::OPERATOR_GREATER_THAN => candidate > *value,
      Operator::OPERATOR_GREATER_THAN_OR_EQUAL => candidate >= *value,
      Operator::OPERATOR_NOT_EQUALS => candidate != *value,
      // Real Regex is not supported.
      Operator::OPERATOR_EQUALS | Operator::OPERATOR_REGEX => candidate == *value,
    }
  }
}

/// Describes a comparison match criteria against a int32 value
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IntMatch {
  operator: Operator,
  value: ValueOrSavedFieldId<i32>,
}

/// Supports comparison between two integers
impl IntMatch {
  pub fn from_proto(
    proto: &bd_proto::protos::value_matcher::value_matcher::IntValueMatch,
  ) -> Result<Self> {
    Self::new(
      proto.operator,
      ValueOrSavedFieldId::<i32>::from_proto(proto),
    )
  }

  pub fn new(operator: EnumOrUnknown<Operator>, value: ValueOrSavedFieldId<i32>) -> Result<Self> {
    let operator = operator
      .enum_value()
      .map_err(|_| anyhow!("invalid operator for StringValueMatch"))?;
    match operator {
      // Regex operator is not valid for int32
      Operator::OPERATOR_REGEX => Err(anyhow!("regex does not support int32")),
      _ => Ok(Self { operator, value }),
    }
  }

  pub fn evaluate(&self, candidate: i32, extracted_fields: &TinyMap<String, String>) -> bool {
    let Some(value) = self.value.load(extracted_fields) else {
      return false;
    };

    match self.operator {
      // This should never happen as we check for UNSPECIFIED when we parse
      // workflow config.
      Operator::OPERATOR_UNSPECIFIED => false,
      Operator::OPERATOR_LESS_THAN => candidate < *value,
      Operator::OPERATOR_LESS_THAN_OR_EQUAL => candidate <= *value,
      Operator::OPERATOR_GREATER_THAN => candidate > *value,
      Operator::OPERATOR_GREATER_THAN_OR_EQUAL => candidate >= *value,
      Operator::OPERATOR_NOT_EQUALS => candidate != *value,
      // Real Regex is not supported.
      Operator::OPERATOR_EQUALS | Operator::OPERATOR_REGEX => candidate == *value,
    }
  }
}
