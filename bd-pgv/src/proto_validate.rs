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
use protos::validate::{
  BoolRules,
  EnumRules,
  Int32Rules,
  Int64Rules,
  RepeatedRules,
  StringRules,
  UInt32Rules,
  UInt64Rules,
};
use std::any::type_name;
use std::fmt::Display;
use std::time::Duration;

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

// Validate enum rules.
fn validate_enum(
  rules: &EnumRules,
  field_descriptor: &FieldDescriptor,
  message_descriptor: &MessageDescriptor,
  message: &dyn protobuf::MessageDyn,
) -> error::Result<()> {
  if let Some(ReflectValueRef::Enum(descriptor, value)) =
    get_singular_or_default(field_descriptor, message)
  {
    if rules.defined_only() && descriptor.value_by_number(value).is_none() {
      return Err(error::Error::ProtoValidation(format!(
        "field '{}' in message '{}' must be a defined enum. Got {}",
        field_descriptor.full_name(),
        message_descriptor.full_name(),
        value
      )));
    }

    not_implemented(rules.has_const(), "enum rules const")?;
    not_implemented(!rules.in_.is_empty(), "enum rules in")?;
    not_implemented(!rules.not_in.is_empty(), "enum rules not_in")?;
  }

  Ok(())
}

// Validate bool rules.
fn validate_bool(
  rules: &BoolRules,
  field_descriptor: &FieldDescriptor,
  message_descriptor: &MessageDescriptor,
  message: &dyn protobuf::MessageDyn,
) -> error::Result<()> {
  if let Some(ReflectValueRef::Bool(value)) = get_singular_or_default(field_descriptor, message) {
    if rules.has_const() && rules.const_() != value {
      return Err(error::Error::ProtoValidation(format!(
        "field '{}' in message '{}' must be constant {}",
        field_descriptor.full_name(),
        message_descriptor.full_name(),
        rules.const_()
      )));
    }
  }

  Ok(())
}

trait IntHelper {
  type Item;

  fn gt(&self) -> Option<Self::Item>;
  fn gte(&self) -> Option<Self::Item>;
  fn lt(&self) -> Option<Self::Item>;
  fn lte(&self) -> Option<Self::Item>;
  fn has_const(&self) -> bool;
  fn in_(&self) -> &[Self::Item];
  fn not_in(&self) -> &[Self::Item];
  fn has_ignore_empty(&self) -> bool;

  fn validate_all_int_rules(
    &self,
    value: Self::Item,
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
  ) -> error::Result<()>
  where
    <Self as IntHelper>::Item: PartialOrd + Display,
  {
    if self.gt().is_some_and(|gt| value <= gt) {
      return Err(error::Error::ProtoValidation(format!(
        "field '{}' in message '{}' must be > {}",
        field_descriptor.full_name(),
        message_descriptor.full_name(),
        self.gt().unwrap()
      )));
    }

    not_implemented(
      self.has_const(),
      &format!("{} rules const", type_name::<Self::Item>()),
    )?;
    not_implemented(
      self.lt().is_some(),
      &format!("{} rules lt", type_name::<Self::Item>()),
    )?;
    not_implemented(
      self.lte().is_some(),
      &format!("{} rules lte", type_name::<Self::Item>()),
    )?;
    not_implemented(
      self.gte().is_some(),
      &format!("{} rules gte", type_name::<Self::Item>()),
    )?;
    not_implemented(
      !self.in_().is_empty(),
      &format!("{} rules in", type_name::<Self::Item>()),
    )?;
    not_implemented(
      !self.not_in().is_empty(),
      &format!("{} rules not_in", type_name::<Self::Item>()),
    )?;
    not_implemented(
      self.has_ignore_empty(),
      &format!("{} rules ignore_empty", type_name::<Self::Item>()),
    )?;
    Ok(())
  }
}

macro_rules! impl_int_helper {
  ($rule_type:ty, $item_type:ty) => {
    impl IntHelper for $rule_type {
      type Item = $item_type;

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

impl_int_helper!(UInt32Rules, u32);
impl_int_helper!(UInt64Rules, u64);
impl_int_helper!(Int32Rules, i32);
impl_int_helper!(Int64Rules, i64);

// Validate string rules.
fn validate_string(
  rules: &StringRules,
  field_descriptor: &FieldDescriptor,
  message_descriptor: &MessageDescriptor,
  message: &dyn protobuf::MessageDyn,
) -> error::Result<()> {
  if let Some(ReflectValueRef::String(value)) = get_singular_or_default(field_descriptor, message) {
    if rules.has_min_len() && rules.min_len() > value.len().try_into().unwrap() {
      return Err(error::Error::ProtoValidation(format!(
        "field '{}' in message '{}' requires string length >= {}",
        field_descriptor.full_name(),
        message_descriptor.full_name(),
        rules.min_len()
      )));
    }

    if rules.has_max_len() && rules.max_len() < value.len().try_into().unwrap() {
      return Err(error::Error::ProtoValidation(format!(
        "field '{}' in message '{}' requires string length <= {}",
        field_descriptor.full_name(),
        message_descriptor.full_name(),
        rules.max_len()
      )));
    }

    not_implemented(rules.has_const(), "string rules const")?;
    not_implemented(rules.has_len(), "string rules len")?;
    not_implemented(rules.has_len_bytes(), "string rules len_bytes")?;
    not_implemented(rules.has_min_bytes(), "string rules min_bytes")?;
    not_implemented(rules.has_max_bytes(), "string rules max_bytes")?;
    not_implemented(rules.has_pattern(), "string rules pattern")?;
    not_implemented(rules.has_prefix(), "string rules prefix")?;
    not_implemented(rules.has_suffix(), "string rules suffix")?;
    not_implemented(rules.has_contains(), "string rules contains")?;
    not_implemented(rules.has_not_contains(), "string rules not_contains")?;
    not_implemented(!rules.in_.is_empty(), "string rules in")?;
    not_implemented(!rules.not_in.is_empty(), "string rules not_in")?;
    not_implemented(rules.has_email(), "string rules email")?;
    not_implemented(rules.has_hostname(), "string rules hostname")?;
    not_implemented(rules.has_ip(), "string rules ip")?;
    not_implemented(rules.has_ipv4(), "string rules ipv4")?;
    not_implemented(rules.has_ipv6(), "string rules ipv6")?;
    not_implemented(rules.has_uri(), "string rules uri")?;
    not_implemented(rules.has_uri_ref(), "string rules uri_ref")?;
    not_implemented(rules.has_address(), "string rules address")?;
    not_implemented(rules.has_uuid(), "string rules uuid")?;
    not_implemented(
      rules.has_well_known_regex(),
      "string rules well_known_regex",
    )?;
  }

  Ok(())
}

// Validate repeated rules.
fn validate_repeated(
  rules: &RepeatedRules,
  field_descriptor: &FieldDescriptor,
  message_descriptor: &MessageDescriptor,
  message: &dyn protobuf::MessageDyn,
) -> error::Result<()> {
  if rules.has_min_items()
    && field_descriptor.get_repeated(message).len() < rules.min_items().try_into().unwrap()
  {
    return Err(error::Error::ProtoValidation(format!(
      "field '{}' in message '{}' requires repeated items >= {}",
      field_descriptor.full_name(),
      message_descriptor.full_name(),
      rules.min_items()
    )));
  }

  not_implemented(rules.has_max_items(), "repeated max_items")?;
  not_implemented(rules.has_unique(), "repeated unique")?;
  not_implemented(rules.items.is_some(), "repeated items")?;
  not_implemented(rules.has_ignore_empty(), "repeated ignore_empty")?;

  Ok(())
}

// Validate google.protobuf.Duration.
fn validate_duration(
  rules: &DurationRules,
  field_descriptor: &FieldDescriptor,
  message_descriptor: &MessageDescriptor,
  duration: &ProtoDuration,
) -> error::Result<()> {
  not_implemented(rules.has_required(), "duration required")?;
  not_implemented(rules.const_.is_some(), "duration const")?;
  not_implemented(rules.lt.is_some(), "duration lt")?;
  not_implemented(rules.lte.is_some(), "duration lte")?;
  not_implemented(rules.gte.is_some(), "duration gte")?;
  not_implemented(!rules.in_.is_empty(), "duration in")?;
  not_implemented(!rules.not_in.is_empty(), "duration not_in")?;

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
        field_descriptor.full_name(),
        message_descriptor.full_name(),
        gt
      )));
    }
  }

  Ok(())
}

// Validate field rules.
fn validate_field(
  field_descriptor: &FieldDescriptor,
  message_descriptor: &MessageDescriptor,
  message: &dyn protobuf::MessageDyn,
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

  not_implemented(rules.has_any(), "field any")?;
  not_implemented(rules.has_timestamp(), "field timestamp")?;

  // The following do not appear to be exposed by the Rust library and are probably not typically
  // used anyway.
  not_implemented(rules.has_fixed32(), "field fixed32")?;
  not_implemented(rules.has_fixed64(), "field fixed64")?;
  not_implemented(rules.has_sfixed32(), "field sfixed32")?;
  not_implemented(rules.has_sfixed64(), "field sfixed64")?;

  match field_descriptor.runtime_field_type() {
    RuntimeFieldType::Singular(singular) => {
      match singular {
        RuntimeType::Message(_) => {
          // See if the field has `(validate.rules).message = {required: true}` on it.
          if rules
            .message
            .0
            .as_ref()
            .and_then(|message_rules| message_rules.required)
            .unwrap_or(false)
            && field_descriptor.get_singular(message).is_none()
          {
            return Err(error::Error::ProtoValidation(format!(
              "field '{}' in message '{}' is required",
              field_descriptor.full_name(),
              message_descriptor.full_name()
            )));
          }

          // See if the field has duration rules, exists, and is the duration type.
          if rules.has_duration() {
            if let Some(ReflectValueRef::Message(duration)) = field_descriptor.get_singular(message)
            {
              if duration.descriptor_dyn().full_name() == "google.protobuf.Duration" {
                validate_duration(
                  rules.duration(),
                  field_descriptor,
                  message_descriptor,
                  duration.downcast_ref().unwrap(),
                )?;
                return Ok(false); // Do not recurse.
              }
            }
          }
        },
        RuntimeType::String => {
          if rules.has_string() {
            validate_string(
              rules.string(),
              field_descriptor,
              message_descriptor,
              message,
            )?;
          }
        },
        RuntimeType::I32 => {
          if rules.has_int32() {
            if let Some(ReflectValueRef::I32(value)) =
              get_singular_or_default(field_descriptor, message)
            {
              rules
                .int32()
                .validate_all_int_rules(value, field_descriptor, message_descriptor)?;
            }
          }
        },
        RuntimeType::I64 => {
          if rules.has_int64() {
            if let Some(ReflectValueRef::I64(value)) =
              get_singular_or_default(field_descriptor, message)
            {
              rules
                .int64()
                .validate_all_int_rules(value, field_descriptor, message_descriptor)?;
            }
          }
        },
        RuntimeType::U32 => {
          if rules.has_uint32() {
            if let Some(ReflectValueRef::U32(value)) =
              get_singular_or_default(field_descriptor, message)
            {
              rules
                .uint32()
                .validate_all_int_rules(value, field_descriptor, message_descriptor)?;
            }
          }
        },
        RuntimeType::U64 => {
          if rules.has_uint64() {
            if let Some(ReflectValueRef::U64(value)) =
              get_singular_or_default(field_descriptor, message)
            {
              rules
                .uint64()
                .validate_all_int_rules(value, field_descriptor, message_descriptor)?;
            }
          }
        },
        RuntimeType::F32 => {
          not_implemented(rules.has_float(), "float validation")?;
        },
        RuntimeType::F64 => {
          not_implemented(rules.has_double(), "double validation")?;
        },
        RuntimeType::Bool => {
          if rules.has_bool() {
            validate_bool(rules.bool(), field_descriptor, message_descriptor, message)?;
          }
        },
        RuntimeType::VecU8 => {
          not_implemented(rules.has_bytes(), "bytes validation")?;
        },
        RuntimeType::Enum(_) => {
          if rules.has_enum() {
            validate_enum(rules.enum_(), field_descriptor, message_descriptor, message)?;
          }
        },
      }
    },
    RuntimeFieldType::Repeated(_) => {
      if rules.has_repeated() {
        validate_repeated(
          rules.repeated(),
          field_descriptor,
          message_descriptor,
          message,
        )?;
      }
    },
    RuntimeFieldType::Map(..) => {
      not_implemented(rules.has_map(), "map validation")?;
    },
  }

  Ok(true)
}

// Validate a message using PGV annotations and reflection.
pub fn validate(message: &dyn protobuf::MessageDyn) -> error::Result<()> {
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
        oneof.full_name(),
        message_descriptor.full_name()
      )));
    }
  }

  for field in message_descriptor.fields() {
    // Check per-field rules.
    if !validate_field(&field, &message_descriptor, message)? {
      continue;
    }

    // Recursive into sub-messages.
    match field.get_reflect(message) {
      ReflectFieldRef::Optional(optional) => {
        if let Some(ReflectValueRef::Message(message)) = optional.value() {
          validate(&*message)?;
        }
      },
      ReflectFieldRef::Repeated(repeated) => {
        for value in repeated {
          if let ReflectValueRef::Message(message) = value {
            validate(&*message)?;
          }
        }
      },
      ReflectFieldRef::Map(map) => {
        for (_, value) in &map {
          if let ReflectValueRef::Message(message) = value {
            validate(&*message)?;
          }
        }
      },
    }
  }

  Ok(())
}
