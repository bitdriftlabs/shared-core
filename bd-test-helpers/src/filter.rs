// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_proto::protos::filter::filter::filter;
use filter::transform::capture_fields::fields::SingleField;
use filter::transform::capture_fields::Fields;
use filter::Transform;

pub mod macros {
  #[macro_export]
  macro_rules! capture_fields {
    (single $name:expr) => {
      $crate::filter::make_capture_fields($name);
    };
  }

  #[macro_export]
  macro_rules! set_field {
    (captured($name:expr) = $value:expr) => {
      $crate::filter::make_set_field_transform(
        $name,
        $value,
        bd_proto::protos::filter::filter::filter::transform::set_field::FieldType::CAPTURED,
      )
    };
    (matching($name:expr) = $value:expr) => {
      $crate::filter::make_set_field_transform(
        $name,
        $value,
        bd_proto::protos::filter::filter::filter::transform::set_field::FieldType::MATCHING_ONLY,
      )
    };
  }

  pub use {capture_fields, set_field};
}

#[must_use]
pub fn make_capture_fields(
  field_name: &str,
) -> bd_proto::protos::filter::filter::filter::Transform {
  Transform {
    transform_type: Some(filter::transform::Transform_type::CaptureFields(
      filter::transform::CaptureFields {
        fields: Some(
          Fields {
            fields_type: Some(
                bd_proto::protos::filter::filter::filter::transform::capture_fields::fields
                ::Fields_type::Single(
                SingleField {
                    name: field_name.to_string(),
                    ..Default::default()
                })),
            ..Default::default()
          },
        )
        .into(),
        ..Default::default()
      },
    )),
    ..Default::default()
  }
}

#[must_use]
pub fn make_set_field_transform(
  name: &str,
  value: &str,
  field_type: bd_proto::protos::filter::filter::filter::transform::set_field::FieldType,
) -> bd_proto::protos::filter::filter::filter::Transform {
  bd_proto::protos::filter::filter::filter::Transform {
    transform_type: Some(
      bd_proto::protos::filter::filter::filter::transform::Transform_type::SetField(
        bd_proto::protos::filter::filter::filter::transform::SetField {
          name: name.to_string(),
          value: value.to_string(),
          field_type: field_type.into(),
          ..Default::default()
        },
      ),
    ),
    ..Default::default()
  }
}
