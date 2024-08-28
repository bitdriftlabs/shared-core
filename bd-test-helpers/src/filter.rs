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

  pub use capture_fields;
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
                bd_proto::protos::filter::filter::filter::transform::capture_fields::fields::Fields_type::Single(
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
