pub mod de;
pub mod decoder;
#[cfg(test)]
pub mod ser;
pub mod type_codes;

mod deserialize_primitives;
mod serialize_primitives;
mod writer;

pub use crate::deserialize_primitives::{
  deserialize_array_start, deserialize_bool, deserialize_container_end,
  deserialize_f16_after_type_code, deserialize_f32, deserialize_f32_after_type_code,
  deserialize_f64, deserialize_f64_after_type_code, deserialize_long_string_after_type_code,
  deserialize_map_start, deserialize_null, deserialize_short_string_after_type_code,
  deserialize_signed, deserialize_signed_after_type_code, deserialize_string,
  deserialize_type_code, deserialize_unsigned, deserialize_unsigned_after_type_code,
  peek_type_code,
};
pub use crate::serialize_primitives::{
  serialize_array_begin, serialize_boolean, serialize_container_end, serialize_f32, serialize_f64,
  serialize_i64, serialize_map_begin, serialize_null, serialize_string, serialize_string_header,
  serialize_u64,
};
pub use crate::writer::Writer;

pub fn add(left: u64, right: u64) -> u64 {
  left + right
}
#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn it_works() {
    let result = add(2, 2);
    assert_eq!(result, 4);
  }
}
