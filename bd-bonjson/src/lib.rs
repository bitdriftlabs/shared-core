pub mod de;
#[cfg(test)]
pub mod ser;
pub mod type_codes;

mod primitives;
mod writer;

pub use crate::writer::Writer;
pub use crate::primitives::{
    serialize_array_begin,
    serialize_map_begin,
    serialize_container_end,
    serialize_u64,
    serialize_i64,
    serialize_f32,
    serialize_f64,
    serialize_string_header,
    serialize_string,
    serialize_boolean,
    serialize_null,
    deserialize_type_code,
    deserialize_null,
    deserialize_array_start,
    deserialize_map_start,
    deserialize_container_end,
    deserialize_bool,
    deserialize_unsigned,
    deserialize_unsigned_after_type_code,
    deserialize_signed,
    deserialize_signed_after_type_code,
    deserialize_f16_after_type_code,
    deserialize_f32_after_type_code,
    deserialize_f64_after_type_code,
    deserialize_f32,
    deserialize_f64,
    deserialize_short_string_after_type_code,
    deserialize_long_string_after_type_code,
    deserialize_string,
    peek_type_code,
};

#[derive(Debug)]
pub enum Error {
  InvalidSerialization,
  InvalidDeserialization,
  Io { offset: u64 },
}

pub type Result<T> = std::result::Result<T, Error>;

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
