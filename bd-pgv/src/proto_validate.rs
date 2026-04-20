// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./proto_validate_test.rs"]
mod proto_validate_test;

use crate::error;
use crate::generated::protos;
use crate::generated::protos::validate::DurationRules;
use protobuf::reflect::{
  FieldDescriptor,
  MessageDescriptor,
  ReflectFieldRef,
  ReflectValueRef,
  RuntimeFieldType,
  RuntimeType,
};
use protobuf::well_known_types::duration::Duration as ProtoDuration;
use protobuf::well_known_types::timestamp::Timestamp as ProtoTimestamp;
use protos::validate::{
  DoubleRules,
  FieldRules,
  FloatRules,
  Int32Rules,
  Int64Rules,
  MapRules,
  RepeatedRules,
  UInt32Rules,
  UInt64Rules,
};
use std::collections::HashSet;
use std::fmt::Display;
use std::time::Duration;

//
// ValidationOptions
//

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ProtoNameMode {
  #[default]
  FullyQualified,
  PackageRelative,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ValidationOptions {
  pub proto_name_mode: ProtoNameMode,
}

//
// ErrorNameFormatter
//

// Formats descriptor names for user-facing PGV errors. Package-relative names hide the proto
// package while still preserving nested message context.
struct ErrorNameFormatter {
  options: ValidationOptions,
}

impl ErrorNameFormatter {
  const fn new(options: ValidationOptions) -> Self {
    Self { options }
  }

  fn field_name(
    &self,
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
  ) -> String {
    self.name(
      message_descriptor.file_descriptor().package(),
      &field_descriptor.full_name(),
    )
  }

  fn message_name(&self, message_descriptor: &MessageDescriptor) -> String {
    self.name(
      message_descriptor.file_descriptor().package(),
      message_descriptor.full_name(),
    )
  }

  fn oneof_name(&self, message_descriptor: &MessageDescriptor, full_name: &str) -> String {
    self.name(message_descriptor.file_descriptor().package(), full_name)
  }

  fn name(&self, package: &str, full_name: &str) -> String {
    match self.options.proto_name_mode {
      ProtoNameMode::FullyQualified => full_name.to_string(),
      ProtoNameMode::PackageRelative => package_relative_name(package, full_name).to_string(),
    }
  }
}

fn package_relative_name<'a>(package: &str, full_name: &'a str) -> &'a str {
  if package.is_empty() {
    full_name
  } else {
    full_name
      .strip_prefix(package)
      .and_then(|name| name.strip_prefix('.'))
      .unwrap_or(full_name)
  }
}

// Helper for features that are not implemented so we don't silently fail.
fn not_implemented(has_rule: bool, message: &str) -> error::Result<()> {
  if has_rule {
    return Err(error::Error::ProtoValidation(format!(
      "not implemented: {message}",
    )));
  }
  Ok(())
}

// Get a singular field or a default value, depending on whether the field is optional or not.
fn get_singular_or_default<'a>(
  field_descriptor: &FieldDescriptor,
  message: &'a dyn protobuf::MessageDyn,
) -> Option<ReflectValueRef<'a>> {
  if field_descriptor
    .containing_oneof_including_synthetic()
    .is_some()
  {
    // This field is either in a real oneof or in a "synthetic" oneof (marked with the optional
    // keyword) so it's optional, so don't return a default for it if it is not set.
    field_descriptor.get_singular(message)
  } else {
    // This field is required so return a default. is_required() appears to have a bug when last
    // tested.
    Some(field_descriptor.get_singular_field_or_default(message))
  }
}

trait NumericHelper {
  type Item;

  fn rule_name() -> &'static str;

  fn gt(&self) -> Option<Self::Item>;
  fn gte(&self) -> Option<Self::Item>;
  fn lt(&self) -> Option<Self::Item>;
  fn lte(&self) -> Option<Self::Item>;
  fn has_const(&self) -> bool;
  fn in_(&self) -> &[Self::Item];
  fn not_in(&self) -> &[Self::Item];
  fn has_ignore_empty(&self) -> bool;

  fn verify_supported_numeric_rules(&self) -> error::Result<()> {
    not_implemented(
      self.has_const(),
      &format!("{} rules const", Self::rule_name()),
    )?;
    not_implemented(
      !self.in_().is_empty(),
      &format!("{} rules in", Self::rule_name()),
    )?;
    not_implemented(
      !self.not_in().is_empty(),
      &format!("{} rules not_in", Self::rule_name()),
    )?;
    not_implemented(
      self.has_ignore_empty(),
      &format!("{} rules ignore_empty", Self::rule_name()),
    )?;
    Ok(())
  }

  fn validate_all_numeric_rules(
    &self,
    value: Self::Item,
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
    formatter: &ErrorNameFormatter,
  ) -> error::Result<()>
  where
    <Self as NumericHelper>::Item: PartialOrd + Display,
  {
    if self.gt().is_some_and(|gt| value <= gt) {
      return Err(error::Error::ProtoValidation(format!(
        "field '{}' in message '{}' must be > {}",
        formatter.field_name(field_descriptor, message_descriptor),
        formatter.message_name(message_descriptor),
        self.gt().unwrap()
      )));
    }
    if self.lt().is_some_and(|lt| value >= lt) {
      return Err(error::Error::ProtoValidation(format!(
        "field '{}' in message '{}' must be < {}",
        formatter.field_name(field_descriptor, message_descriptor),
        formatter.message_name(message_descriptor),
        self.lt().unwrap()
      )));
    }
    if self.lte().is_some_and(|lte| value > lte) {
      return Err(error::Error::ProtoValidation(format!(
        "field '{}' in message '{}' must be <= {}",
        formatter.field_name(field_descriptor, message_descriptor),
        formatter.message_name(message_descriptor),
        self.lte().unwrap()
      )));
    }
    if self.gte().is_some_and(|gte| value < gte) {
      return Err(error::Error::ProtoValidation(format!(
        "field '{}' in message '{}' must be >= {}",
        formatter.field_name(field_descriptor, message_descriptor),
        formatter.message_name(message_descriptor),
        self.gte().unwrap()
      )));
    }

    self.verify_supported_numeric_rules()?;
    Ok(())
  }
}

macro_rules! impl_numeric_helper {
  ($rule_type:ty, $item_type:ty, $rule_name:literal) => {
    impl NumericHelper for $rule_type {
      type Item = $item_type;

      fn rule_name() -> &'static str {
        $rule_name
      }

      fn gt(&self) -> Option<Self::Item> {
        self.gt
      }
      fn gte(&self) -> Option<Self::Item> {
        self.gte
      }
      fn lt(&self) -> Option<Self::Item> {
        self.lt
      }
      fn lte(&self) -> Option<Self::Item> {
        self.lte
      }
      fn has_const(&self) -> bool {
        self.const_.is_some()
      }
      fn in_(&self) -> &[Self::Item] {
        &self.in_
      }
      fn not_in(&self) -> &[Self::Item] {
        &self.not_in
      }
      fn has_ignore_empty(&self) -> bool {
        self.has_ignore_empty()
      }
    }
  };
}

impl_numeric_helper!(UInt32Rules, u32, "uint32");
impl_numeric_helper!(UInt64Rules, u64, "uint64");
impl_numeric_helper!(Int32Rules, i32, "int32");
impl_numeric_helper!(Int64Rules, i64, "int64");
impl_numeric_helper!(FloatRules, f32, "float");
impl_numeric_helper!(DoubleRules, f64, "double");

fn verify_duration_rules_supported(rules: &DurationRules) -> error::Result<()> {
  not_implemented(rules.has_required(), "duration required")?;
  not_implemented(rules.const_.is_some(), "duration const")?;
  not_implemented(rules.lt.is_some(), "duration lt")?;
  not_implemented(rules.lte.is_some(), "duration lte")?;
  not_implemented(rules.gte.is_some(), "duration gte")?;
  not_implemented(!rules.in_.is_empty(), "duration in")?;
  not_implemented(!rules.not_in.is_empty(), "duration not_in")?;
  Ok(())
}

// Validate google.protobuf.Duration.
fn validate_duration(
  rules: &DurationRules,
  field_descriptor: &FieldDescriptor,
  message_descriptor: &MessageDescriptor,
  duration: &ProtoDuration,
  formatter: &ErrorNameFormatter,
) -> error::Result<()> {
  verify_duration_rules_supported(rules)?;

  let duration: Duration = duration.clone().try_into().map_err(|_| {
    error::Error::ProtoValidation("negative proto duration not supported".to_string())
  })?;
  if let Some(gt) = rules.gt.as_ref() {
    let gt: Duration = gt.clone().try_into().map_err(|_| {
      error::Error::ProtoValidation("negative proto duration not supported".to_string())
    })?;
    if duration <= gt {
      return Err(error::Error::ProtoValidation(format!(
        "duration '{}' in message '{}' requires > {:?}",
        formatter.field_name(field_descriptor, message_descriptor),
        formatter.message_name(message_descriptor),
        gt
      )));
    }
  }

  Ok(())
}

fn verify_timestamp_rules_supported(rules: &FieldRules) -> error::Result<()> {
  let rules = rules.timestamp();
  not_implemented(rules.const_.is_some(), "timestamp const")?;
  not_implemented(rules.lt.is_some(), "timestamp lt")?;
  not_implemented(rules.lte.is_some(), "timestamp lte")?;
  not_implemented(rules.gt.is_some(), "timestamp gt")?;
  not_implemented(rules.gte.is_some(), "timestamp gte")?;
  not_implemented(rules.has_lt_now(), "timestamp lt_now")?;
  not_implemented(rules.has_gt_now(), "timestamp gt_now")?;
  not_implemented(rules.within.is_some(), "timestamp within")?;
  Ok(())
}

// Validate google.protobuf.Timestamp.
fn validate_timestamp(
  rules: &FieldRules,
  field_descriptor: &FieldDescriptor,
  message_descriptor: &MessageDescriptor,
  value: Option<&ProtoTimestamp>,
  formatter: &ErrorNameFormatter,
) -> error::Result<()> {
  let timestamp_rules = rules.timestamp();

  if timestamp_rules.required() && value.is_none() {
    return Err(error::Error::ProtoValidation(format!(
      "field '{}' in message '{}' is required",
      formatter.field_name(field_descriptor, message_descriptor),
      formatter.message_name(message_descriptor)
    )));
  }

  verify_timestamp_rules_supported(rules)?;

  Ok(())
}

fn verify_string_rules_supported(rules: &FieldRules) -> error::Result<()> {
  not_implemented(rules.string().has_const(), "string rules const")?;
  not_implemented(rules.string().has_len(), "string rules len")?;
  not_implemented(rules.string().has_len_bytes(), "string rules len_bytes")?;
  not_implemented(rules.string().has_min_bytes(), "string rules min_bytes")?;
  not_implemented(rules.string().has_max_bytes(), "string rules max_bytes")?;
  not_implemented(rules.string().has_pattern(), "string rules pattern")?;
  not_implemented(rules.string().has_prefix(), "string rules prefix")?;
  not_implemented(rules.string().has_suffix(), "string rules suffix")?;
  not_implemented(rules.string().has_contains(), "string rules contains")?;
  not_implemented(
    rules.string().has_not_contains(),
    "string rules not_contains",
  )?;
  not_implemented(!rules.string().in_.is_empty(), "string rules in")?;
  not_implemented(!rules.string().not_in.is_empty(), "string rules not_in")?;
  not_implemented(rules.string().has_email(), "string rules email")?;
  not_implemented(rules.string().has_hostname(), "string rules hostname")?;
  not_implemented(rules.string().has_ip(), "string rules ip")?;
  not_implemented(rules.string().has_ipv4(), "string rules ipv4")?;
  not_implemented(rules.string().has_ipv6(), "string rules ipv6")?;
  not_implemented(rules.string().has_uri(), "string rules uri")?;
  not_implemented(rules.string().has_uri_ref(), "string rules uri_ref")?;
  not_implemented(rules.string().has_address(), "string rules address")?;
  not_implemented(rules.string().has_uuid(), "string rules uuid")?;
  not_implemented(
    rules.string().has_well_known_regex(),
    "string rules well_known_regex",
  )?;
  Ok(())
}

fn verify_enum_rules_supported(rules: &FieldRules) -> error::Result<()> {
  not_implemented(rules.enum_().has_const(), "enum rules const")?;
  Ok(())
}

// Verify that a reflected field rule set only uses validations we support. The returned bool
// indicates whether descriptor recursion should continue for this field.
fn verify_value_support(
  rules: &FieldRules,
  runtime_type: &RuntimeType,
  formatter: &ErrorNameFormatter,
) -> error::Result<bool> {
  not_implemented(rules.has_any(), "field any")?;

  // The following do not appear to be exposed by the Rust library and are probably not typically
  // used anyway.
  not_implemented(rules.has_fixed32(), "field fixed32")?;
  not_implemented(rules.has_fixed64(), "field fixed64")?;
  not_implemented(rules.has_sfixed32(), "field sfixed32")?;
  not_implemented(rules.has_sfixed64(), "field sfixed64")?;

  match runtime_type {
    RuntimeType::Message(message_descriptor) => {
      if rules
        .message
        .0
        .as_ref()
        .and_then(|message_rules| message_rules.skip)
        .unwrap_or(false)
      {
        return Ok(false);
      }

      if rules.has_duration() {
        if message_descriptor.full_name() != "google.protobuf.Duration" {
          return Err(error::Error::ProtoValidation(format!(
            "not implemented: duration rules on non-Duration field '{}'",
            formatter.message_name(message_descriptor)
          )));
        }

        verify_duration_rules_supported(rules.duration())?;
        return Ok(false);
      }

      if rules.has_timestamp() {
        if message_descriptor.full_name() != "google.protobuf.Timestamp" {
          return Err(error::Error::ProtoValidation(format!(
            "not implemented: timestamp rules on non-Timestamp field '{}'",
            formatter.message_name(message_descriptor)
          )));
        }

        verify_timestamp_rules_supported(rules)?;
        return Ok(false);
      }
    },
    RuntimeType::String => {
      if rules.has_string() {
        verify_string_rules_supported(rules)?;
      }
    },
    RuntimeType::I32 => {
      if rules.has_int32() {
        rules.int32().verify_supported_numeric_rules()?;
      }
    },
    RuntimeType::I64 => {
      if rules.has_int64() {
        rules.int64().verify_supported_numeric_rules()?;
      }
    },
    RuntimeType::U32 => {
      if rules.has_uint32() {
        rules.uint32().verify_supported_numeric_rules()?;
      }
    },
    RuntimeType::U64 => {
      if rules.has_uint64() {
        rules.uint64().verify_supported_numeric_rules()?;
      }
    },
    RuntimeType::F32 => {
      if rules.has_float() {
        rules.float().verify_supported_numeric_rules()?;
      }
    },
    RuntimeType::F64 => {
      if rules.has_double() {
        rules.double().verify_supported_numeric_rules()?;
      }
    },
    RuntimeType::Bool => {},
    RuntimeType::VecU8 => {
      not_implemented(rules.has_bytes(), "bytes validation")?;
    },
    RuntimeType::Enum(_) => {
      if rules.has_enum() {
        verify_enum_rules_supported(rules)?;
      }
    },
  }

  Ok(true)
}

// Validate a reflected value against PGV field rules. The returned bool indicates whether message
// recursion should continue for this value.
fn validate_value(
  rules: &FieldRules,
  runtime_type: &RuntimeType,
  field_descriptor: &FieldDescriptor,
  message_descriptor: &MessageDescriptor,
  value: Option<&ReflectValueRef<'_>>,
  formatter: &ErrorNameFormatter,
) -> error::Result<bool> {
  let recurse = verify_value_support(rules, runtime_type, formatter)?;

  match runtime_type {
    RuntimeType::Message(_) => {
      if rules.has_duration()
        && let Some(ReflectValueRef::Message(duration)) = value
        && duration.descriptor_dyn().full_name() == "google.protobuf.Duration"
      {
        validate_duration(
          rules.duration(),
          field_descriptor,
          message_descriptor,
          duration.downcast_ref().unwrap(),
          formatter,
        )?;
        return Ok(false);
      }

      if rules.has_timestamp()
        && let Some(ReflectValueRef::Message(timestamp)) = value
        && timestamp.descriptor_dyn().full_name() == "google.protobuf.Timestamp"
      {
        validate_timestamp(
          rules,
          field_descriptor,
          message_descriptor,
          Some(timestamp.downcast_ref().unwrap()),
          formatter,
        )?;
        return Ok(false);
      }

      if rules.has_timestamp() && value.is_none() {
        validate_timestamp(rules, field_descriptor, message_descriptor, None, formatter)?;
        return Ok(false);
      }
    },
    RuntimeType::String => {
      if rules.has_string()
        && let Some(ReflectValueRef::String(value)) = value
      {
        if rules.string().has_min_len() && rules.string().min_len() > value.len() as u64 {
          return Err(error::Error::ProtoValidation(format!(
            "field '{}' in message '{}' requires string length >= {}",
            formatter.field_name(field_descriptor, message_descriptor),
            formatter.message_name(message_descriptor),
            rules.string().min_len()
          )));
        }

        if rules.string().has_max_len() && rules.string().max_len() < value.len() as u64 {
          return Err(error::Error::ProtoValidation(format!(
            "field '{}' in message '{}' requires string length <= {}",
            formatter.field_name(field_descriptor, message_descriptor),
            formatter.message_name(message_descriptor),
            rules.string().max_len()
          )));
        }
      }
    },
    RuntimeType::I32 => {
      if rules.has_int32()
        && let Some(ReflectValueRef::I32(value)) = value
      {
        rules.int32().validate_all_numeric_rules(
          *value,
          field_descriptor,
          message_descriptor,
          formatter,
        )?;
      }
    },
    RuntimeType::I64 => {
      if rules.has_int64()
        && let Some(ReflectValueRef::I64(value)) = value
      {
        rules.int64().validate_all_numeric_rules(
          *value,
          field_descriptor,
          message_descriptor,
          formatter,
        )?;
      }
    },
    RuntimeType::U32 => {
      if rules.has_uint32()
        && let Some(ReflectValueRef::U32(value)) = value
      {
        rules.uint32().validate_all_numeric_rules(
          *value,
          field_descriptor,
          message_descriptor,
          formatter,
        )?;
      }
    },
    RuntimeType::U64 => {
      if rules.has_uint64()
        && let Some(ReflectValueRef::U64(value)) = value
      {
        rules.uint64().validate_all_numeric_rules(
          *value,
          field_descriptor,
          message_descriptor,
          formatter,
        )?;
      }
    },
    RuntimeType::F32 => {
      if rules.has_float()
        && let Some(ReflectValueRef::F32(value)) = value
      {
        rules.float().validate_all_numeric_rules(
          *value,
          field_descriptor,
          message_descriptor,
          formatter,
        )?;
      }
    },
    RuntimeType::F64 => {
      if rules.has_double()
        && let Some(ReflectValueRef::F64(value)) = value
      {
        rules.double().validate_all_numeric_rules(
          *value,
          field_descriptor,
          message_descriptor,
          formatter,
        )?;
      }
    },
    RuntimeType::Bool => {
      if rules.has_bool()
        && let Some(ReflectValueRef::Bool(value)) = value
        && rules.bool().has_const()
        && rules.bool().const_() != *value
      {
        return Err(error::Error::ProtoValidation(format!(
          "field '{}' in message '{}' must be constant {}",
          formatter.field_name(field_descriptor, message_descriptor),
          formatter.message_name(message_descriptor),
          rules.bool().const_()
        )));
      }
    },
    RuntimeType::VecU8 => {
      not_implemented(rules.has_bytes(), "bytes validation")?;
    },
    RuntimeType::Enum(enum_descriptor) => {
      if rules.has_enum()
        && let Some(ReflectValueRef::Enum(_, value)) = value
      {
        if rules.enum_().defined_only() && enum_descriptor.value_by_number(*value).is_none() {
          return Err(error::Error::ProtoValidation(format!(
            "field '{}' in message '{}' must be a defined enum. Got {}",
            formatter.field_name(field_descriptor, message_descriptor),
            formatter.message_name(message_descriptor),
            value
          )));
        }

        // Enum reflection exposes the numeric value, so membership rules operate on the raw
        // discriminants declared in the PGV options.
        if !rules.enum_().in_.is_empty() && !rules.enum_().in_.contains(value) {
          return Err(error::Error::ProtoValidation(format!(
            "field '{}' in message '{}' must be one of {:?}",
            formatter.field_name(field_descriptor, message_descriptor),
            formatter.message_name(message_descriptor),
            rules.enum_().in_
          )));
        }

        if rules.enum_().not_in.contains(value) {
          return Err(error::Error::ProtoValidation(format!(
            "field '{}' in message '{}' must not be one of {:?}",
            formatter.field_name(field_descriptor, message_descriptor),
            formatter.message_name(message_descriptor),
            rules.enum_().not_in
          )));
        }
      }
    },
  }

  Ok(recurse)
}

fn verify_repeated_support(
  rules: &RepeatedRules,
  repeated_type: &RuntimeType,
  formatter: &ErrorNameFormatter,
) -> error::Result<bool> {
  let recurse = if let Some(item_rules) = rules.items.as_ref() {
    verify_value_support(item_rules, repeated_type, formatter)?
  } else {
    true
  };

  not_implemented(rules.has_unique(), "repeated unique")?;
  Ok(recurse)
}

// Validate repeated rules.
fn validate_repeated(
  rules: &RepeatedRules,
  repeated_type: &RuntimeType,
  field_descriptor: &FieldDescriptor,
  message_descriptor: &MessageDescriptor,
  message: &dyn protobuf::MessageDyn,
  formatter: &ErrorNameFormatter,
) -> error::Result<bool> {
  let repeated = field_descriptor.get_repeated(message);
  let repeated_len = repeated.len();

  if rules.has_ignore_empty() && rules.ignore_empty() && repeated_len == 0 {
    return Ok(true);
  }

  if rules.has_min_items() && repeated_len < usize::try_from(rules.min_items()).unwrap() {
    return Err(error::Error::ProtoValidation(format!(
      "field '{}' in message '{}' requires repeated items >= {}",
      formatter.field_name(field_descriptor, message_descriptor),
      formatter.message_name(message_descriptor),
      rules.min_items()
    )));
  }

  if rules.has_max_items() && repeated_len > usize::try_from(rules.max_items()).unwrap() {
    return Err(error::Error::ProtoValidation(format!(
      "field '{}' in message '{}' requires repeated items <= {}",
      formatter.field_name(field_descriptor, message_descriptor),
      formatter.message_name(message_descriptor),
      rules.max_items()
    )));
  }

  let mut recurse = verify_repeated_support(rules, repeated_type, formatter)?;
  if let Some(item_rules) = rules.items.as_ref() {
    for value in repeated {
      recurse &= validate_value(
        item_rules,
        repeated_type,
        field_descriptor,
        message_descriptor,
        Some(&value),
        formatter,
      )?;
    }
  }

  Ok(recurse)
}

fn verify_map_support(
  rules: &MapRules,
  value_type: &RuntimeType,
  formatter: &ErrorNameFormatter,
) -> error::Result<bool> {
  let recurse = if let Some(value_rules) = rules.values.as_ref() {
    verify_value_support(value_rules, value_type, formatter)?
  } else {
    true
  };

  not_implemented(rules.has_no_sparse(), "map no_sparse")?;
  not_implemented(rules.keys.is_some(), "map keys")?;
  Ok(recurse)
}

// Validate map rules.
fn validate_map(
  rules: &MapRules,
  value_type: &RuntimeType,
  field_descriptor: &FieldDescriptor,
  message_descriptor: &MessageDescriptor,
  message: &dyn protobuf::MessageDyn,
  formatter: &ErrorNameFormatter,
) -> error::Result<bool> {
  let ReflectFieldRef::Map(map) = field_descriptor.get_reflect(message) else {
    unreachable!("validated map field must reflect as map")
  };
  let map_len = map.len();

  if rules.has_ignore_empty() && rules.ignore_empty() && map_len == 0 {
    return Ok(true);
  }

  if rules.has_min_pairs() && map_len < usize::try_from(rules.min_pairs()).unwrap() {
    return Err(error::Error::ProtoValidation(format!(
      "field '{}' in message '{}' requires map pairs >= {}",
      formatter.field_name(field_descriptor, message_descriptor),
      formatter.message_name(message_descriptor),
      rules.min_pairs()
    )));
  }

  if rules.has_max_pairs() && map_len > usize::try_from(rules.max_pairs()).unwrap() {
    return Err(error::Error::ProtoValidation(format!(
      "field '{}' in message '{}' requires map pairs <= {}",
      formatter.field_name(field_descriptor, message_descriptor),
      formatter.message_name(message_descriptor),
      rules.max_pairs()
    )));
  }

  let mut recurse = verify_map_support(rules, value_type, formatter)?;
  if let Some(value_rules) = rules.values.as_ref() {
    for (_key, value) in &map {
      recurse &= validate_value(
        value_rules,
        value_type,
        field_descriptor,
        message_descriptor,
        Some(&value),
        formatter,
      )?;
    }
  }

  Ok(recurse)
}

// Validate field rules.
fn validate_field(
  field_descriptor: &FieldDescriptor,
  message_descriptor: &MessageDescriptor,
  message: &dyn protobuf::MessageDyn,
  formatter: &ErrorNameFormatter,
) -> error::Result<bool> {
  log::trace!("validating field: {}", field_descriptor.full_name());
  let rules = field_descriptor
    .proto()
    .options
    .as_ref()
    .and_then(|options| protos::validate::exts::rules.get(options));
  if rules.is_none() {
    return Ok(true);
  }
  let rules = rules.unwrap();

  match field_descriptor.runtime_field_type() {
    RuntimeFieldType::Singular(singular) => {
      if matches!(singular, RuntimeType::Message(_))
        && rules
          .message
          .0
          .as_ref()
          .and_then(|message_rules| message_rules.required)
          .unwrap_or(false)
        && field_descriptor.get_singular(message).is_none()
      {
        return Err(error::Error::ProtoValidation(format!(
          "field '{}' in message '{}' is required",
          formatter.field_name(field_descriptor, message_descriptor),
          formatter.message_name(message_descriptor)
        )));
      }

      let value = if matches!(singular, RuntimeType::Message(_)) {
        field_descriptor.get_singular(message)
      } else {
        get_singular_or_default(field_descriptor, message)
      };
      let value = value.as_ref();
      validate_value(
        &rules,
        &singular,
        field_descriptor,
        message_descriptor,
        value,
        formatter,
      )
    },
    RuntimeFieldType::Repeated(repeated) => {
      if rules.has_repeated() {
        validate_repeated(
          rules.repeated(),
          &repeated,
          field_descriptor,
          message_descriptor,
          message,
          formatter,
        )
      } else {
        Ok(true)
      }
    },
    RuntimeFieldType::Map(_key_type, value_type) => {
      if rules.has_map() {
        validate_map(
          rules.map(),
          &value_type,
          field_descriptor,
          message_descriptor,
          message,
          formatter,
        )
      } else {
        Ok(true)
      }
    },
  }
}

fn verify_field_support(
  field_descriptor: &FieldDescriptor,
  formatter: &ErrorNameFormatter,
) -> error::Result<bool> {
  let rules = field_descriptor
    .proto()
    .options
    .as_ref()
    .and_then(|options| protos::validate::exts::rules.get(options));
  if rules.is_none() {
    return Ok(true);
  }
  let rules = rules.unwrap();

  match field_descriptor.runtime_field_type() {
    RuntimeFieldType::Singular(singular) => verify_value_support(&rules, &singular, formatter),
    RuntimeFieldType::Repeated(repeated) => {
      if rules.has_repeated() {
        verify_repeated_support(rules.repeated(), &repeated, formatter)
      } else {
        Ok(true)
      }
    },
    RuntimeFieldType::Map(_key_type, value_type) => {
      if rules.has_map() {
        verify_map_support(rules.map(), &value_type, formatter)
      } else {
        Ok(true)
      }
    },
  }
}

fn verify_descriptor_support_impl(
  message_descriptor: &MessageDescriptor,
  visited: &mut HashSet<String>,
  formatter: &ErrorNameFormatter,
) -> error::Result<()> {
  let descriptor_name = message_descriptor.full_name().to_string();
  if !visited.insert(descriptor_name) {
    return Ok(());
  }

  for field in message_descriptor.fields() {
    if !verify_field_support(&field, formatter)? {
      continue;
    }

    match field.runtime_field_type() {
      RuntimeFieldType::Singular(RuntimeType::Message(message_descriptor))
      | RuntimeFieldType::Repeated(RuntimeType::Message(message_descriptor))
      | RuntimeFieldType::Map(_, RuntimeType::Message(message_descriptor)) => {
        verify_descriptor_support_impl(&message_descriptor, visited, formatter)?;
      },
      RuntimeFieldType::Singular(_) | RuntimeFieldType::Repeated(_) | RuntimeFieldType::Map(..) => {
      },
    }
  }

  Ok(())
}

// Verify that all PGV rules used by a message descriptor are supported by the runtime validator.
pub fn verify_descriptor_support(message_descriptor: &MessageDescriptor) -> error::Result<()> {
  verify_descriptor_support_with_options(message_descriptor, ValidationOptions::default())
}

pub fn verify_descriptor_support_with_options(
  message_descriptor: &MessageDescriptor,
  options: ValidationOptions,
) -> error::Result<()> {
  let formatter = ErrorNameFormatter::new(options);
  verify_descriptor_support_impl(message_descriptor, &mut HashSet::new(), &formatter)
}

// Validate a message using PGV annotations and reflection.
pub fn validate(message: &dyn protobuf::MessageDyn) -> error::Result<()> {
  validate_with_options(message, ValidationOptions::default())
}

pub fn validate_with_options(
  message: &dyn protobuf::MessageDyn,
  options: ValidationOptions,
) -> error::Result<()> {
  let formatter = ErrorNameFormatter::new(options);
  validate_impl(message, &formatter)
}

fn validate_impl(
  message: &dyn protobuf::MessageDyn,
  formatter: &ErrorNameFormatter,
) -> error::Result<()> {
  let message_descriptor = message.descriptor_dyn();
  log::trace!("validating message: {}", message_descriptor.full_name());

  for oneof in message_descriptor.oneofs() {
    // See if the oneof has `option (validate.required) = true` on it.
    if oneof
      .proto()
      .options
      .as_ref()
      .and_then(|oneof_options| protos::validate::exts::required.get(oneof_options))
      .unwrap_or(false)
      && !oneof.fields().any(|field| field.has_field(message))
    {
      return Err(error::Error::ProtoValidation(format!(
        "oneof '{}' in message '{}' is required to be set",
        formatter.oneof_name(&message_descriptor, &oneof.full_name()),
        formatter.message_name(&message_descriptor)
      )));
    }
  }

  for field in message_descriptor.fields() {
    // Check per-field rules.
    if !validate_field(&field, &message_descriptor, message, formatter)? {
      continue;
    }

    // Recursive into sub-messages.
    match field.get_reflect(message) {
      ReflectFieldRef::Optional(optional) => {
        if let Some(ReflectValueRef::Message(message)) = optional.value() {
          validate_impl(&*message, formatter)?;
        }
      },
      ReflectFieldRef::Repeated(repeated) => {
        for value in repeated {
          if let ReflectValueRef::Message(message) = value {
            validate_impl(&*message, formatter)?;
          }
        }
      },
      ReflectFieldRef::Map(map) => {
        for (_, value) in &map {
          if let ReflectValueRef::Message(message) = value {
            validate_impl(&*message, formatter)?;
          }
        }
      },
    }
  }

  Ok(())
}
