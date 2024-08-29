// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_proto::protos::filter::filter::filter::{self};
use filter::Transform;

pub mod macros {
  #[macro_export]
  macro_rules! capture_field {
    (single $name:expr) => {
      $crate::filter::make_capture_fields($name);
    };
  }

  #[macro_export]
  macro_rules! set_field {
    (captured($name:expr) = $value:expr, $overrides_allowed:expr) => {
      $crate::filter::make_set_field_transform(
        $name,
        $value,
        bd_proto::protos::filter::filter::filter::transform::set_field::FieldType::CAPTURED,
        $overrides_allowed,
      )
    };
    (matching($name:expr) = $value:expr,overrides_allowed: expr) => {
      $crate::filter::make_set_field_transform(
        $name,
        $value,
        bd_proto::protos::filter::filter::filter::transform::set_field::FieldType::MATCHING_ONLY,
        $overrides_allowed,
      )
    };
    (captured($name:expr) = $value:expr) => {
      $crate::filter::make_set_field_transform(
        $name,
        $value,
        bd_proto::protos::filter::filter::filter::transform::set_field::FieldType::CAPTURED,
        true,
      )
    };
    (matching($name:expr) = $value:expr) => {
      $crate::filter::make_set_field_transform(
        $name,
        $value,
        bd_proto::protos::filter::filter::filter::transform::set_field::FieldType::MATCHING_ONLY,
        true,
      )
    };
  }

  #[macro_export]
  macro_rules! field_value {
    ($value:expr) => {
      $crate::filter::make_string_field_value($value)
    };
    (field $field_name:expr) => {
      $crate::filter::make_existing_field_field_value($field_name)
    };
  }

  pub use {capture_field, field_value, set_field};
}

#[must_use]
pub fn make_capture_fields(
  field_name: &str,
) -> bd_proto::protos::filter::filter::filter::Transform {
  Transform {
    transform_type: Some(filter::transform::Transform_type::CaptureField(
      filter::transform::CaptureField {
        name: field_name.to_string(),
        ..Default::default()
      },
    )),
    ..Default::default()
  }
}

#[must_use]
pub fn make_set_field_transform(
  name: &str,
  value: bd_proto::protos::filter::filter::filter::transform::set_field::SetFieldValue,
  field_type: bd_proto::protos::filter::filter::filter::transform::set_field::FieldType,
  overrides_allowed: bool,
) -> bd_proto::protos::filter::filter::filter::Transform {
  bd_proto::protos::filter::filter::filter::Transform {
    transform_type: Some(
      bd_proto::protos::filter::filter::filter::transform::Transform_type::SetField(
        bd_proto::protos::filter::filter::filter::transform::SetField {
          name: name.to_string(),
          value: Some(value).into(),
          field_type: field_type.into(),
          allow_override: overrides_allowed,
          ..Default::default()
        },
      ),
    ),
    ..Default::default()
  }
}

#[must_use]
pub fn make_string_field_value(
  value: &str,
) -> bd_proto::protos::filter::filter::filter::transform::set_field::SetFieldValue {
  bd_proto::protos::filter::filter::filter::transform::set_field::SetFieldValue {
    value: Some(bd_proto::protos::filter::filter::filter::transform::set_field::set_field_value
      ::Value::StringValue(value.to_string(),
    )),
    ..Default::default()
  }
}

#[must_use]
pub fn make_existing_field_field_value(
  field_name: &str,
) -> bd_proto::protos::filter::filter::filter::transform::set_field::SetFieldValue {
  bd_proto::protos::filter::filter::filter::transform::set_field::SetFieldValue {
    value: Some(bd_proto::protos::filter::filter::filter::transform::set_field::set_field_value::
      Value::ExistingField(
      bd_proto::protos::filter::filter::filter::transform::set_field::set_field_value::ExistingField
      {
        name: field_name.to_string(),
        ..Default::default()
      }
    )),
    ..Default::default()
  }
}
