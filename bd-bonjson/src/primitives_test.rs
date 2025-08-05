// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::deserialize_primitives::*;
use crate::serialize_primitives::*;
use crate::type_codes::TypeCode;

#[test]
fn test_nothing() {
  println!("Running empty test to ensure module compiles");
}

#[test]
fn test_null_roundtrip() {
  let mut buffer = [0u8; 16];
  let serialize_size = serialize_null(&mut buffer).unwrap();
  let deserialize_size = deserialize_null(&buffer).unwrap();
  assert_eq!(serialize_size, deserialize_size);
  assert_eq!(serialize_size, 1);
}

#[test]
fn test_boolean_roundtrip() {
  let mut buffer = [0u8; 16];

  // Test true
  let serialize_size = serialize_boolean(&mut buffer, true).unwrap();
  let (deserialize_size, value) = deserialize_bool(&buffer).unwrap();
  assert_eq!(serialize_size, deserialize_size);
  assert!(value);

  // Test false
  let serialize_size = serialize_boolean(&mut buffer, false).unwrap();
  let (deserialize_size, value) = deserialize_bool(&buffer).unwrap();
  assert_eq!(serialize_size, deserialize_size);
  assert!(!value);
}

#[test]
fn test_u64_roundtrip() {
  let test_values = [
    0u64,
    1,
    100,
    255,
    256,
    65_535,
    65_536,
    16_777_215,
    16_777_216,
    4_294_967_295,
    4_294_967_296,
    1_099_511_627_775,
    1_099_511_627_776,
    281_474_976_710_655,
    281_474_976_710_656,
    72_057_594_037_927_935,
    72_057_594_037_927_936,
    u64::MAX,
  ];

  let mut buffer = [0u8; 16];
  for &value in &test_values {
    let serialize_size = serialize_u64(&mut buffer, value).unwrap();
    let (deserialize_size, decoded_value) = deserialize_unsigned_integer(&buffer).unwrap();
    assert_eq!(serialize_size, deserialize_size);
    assert_eq!(value, decoded_value, "Failed for value: {value}");
  }
}

#[test]
fn test_i64_roundtrip() {
  let test_values = [
    -100i64,
    -1,
    0,
    1,
    100,
    -128,
    127,
    -32_768,
    32_767,
    -8_388_608,
    8_388_607,
    -2_147_483_648,
    2_147_483_647,
    -549_755_813_888,
    549_755_813_887,
    -140_737_488_355_328,
    140_737_488_355_327,
    -36_028_797_018_963_968,
    36_028_797_018_963_967,
    i64::MIN,
    i64::MAX,
  ];

  let mut buffer = [0u8; 16];
  for &value in &test_values {
    let serialize_size = serialize_i64(&mut buffer, value).unwrap();
    let (deserialize_size, decoded_value) = deserialize_signed_integer(&buffer).unwrap();
    assert_eq!(serialize_size, deserialize_size);
    assert_eq!(value, decoded_value, "Failed for value: {value}");
  }
}

#[test]
#[allow(clippy::float_cmp)]
fn test_f32_roundtrip() {
  let test_values = [
    0.0f32,
    -0.0,
    1.0,
    -1.0,
    std::f32::consts::PI,
    -std::f32::consts::PI,
    f32::MIN,
    f32::MAX,
    f32::EPSILON,
    f32::MIN_POSITIVE,
  ];

  let mut buffer = [0u8; 16];
  for &value in &test_values {
    let serialize_size = serialize_f32(&mut buffer, value).unwrap();
    let (deserialize_size, decoded_value) = deserialize_f32(&buffer).unwrap();
    assert_eq!(serialize_size, deserialize_size);
    assert_eq!(value, decoded_value, "Failed for value: {value}");
  }
}

#[test]
#[allow(clippy::float_cmp)]
fn test_f64_roundtrip() {
  let test_values = [
    0.0f64,
    -0.0,
    1.0,
    -1.0,
    std::f64::consts::PI,
    -std::f64::consts::PI,
    f64::MIN,
    f64::MAX,
    f64::EPSILON,
    f64::MIN_POSITIVE,
  ];

  let mut buffer = [0u8; 16];
  for &value in &test_values {
    let serialize_size = serialize_f64(&mut buffer, value).unwrap();
    let (deserialize_size, decoded_value) = deserialize_f64(&buffer).unwrap();
    assert_eq!(serialize_size, deserialize_size);
    assert_eq!(value, decoded_value, "Failed for value: {value}");
  }
}

#[test]
fn test_short_string_roundtrip() {
  let test_strings = [
    "",
    "a",
    "hello",
    "world",
    "123456789012345", // 15 chars (max short string)
  ];

  let mut buffer = [0u8; 64];
  for test_str in &test_strings {
    let serialize_size = serialize_string(&mut buffer, test_str).unwrap();
    let (deserialize_size, decoded_str) = deserialize_string(&buffer).unwrap();
    assert_eq!(serialize_size, deserialize_size);
    assert_eq!(*test_str, decoded_str, "Failed for string: '{test_str}'");
  }
}

#[test]
fn test_long_string_roundtrip() {
  let test_strings = [
    "1234567890123456", // 16 chars (min long string)
    "This is a longer string that should be encoded as a long string",
    &"A".repeat(100),
    &"B".repeat(1000),
  ];

  let mut buffer = vec![0u8; 2048];
  for test_str in &test_strings {
    let serialize_size = serialize_string(&mut buffer, test_str).unwrap();
    let (deserialize_size, decoded_str) = deserialize_string(&buffer).unwrap();
    assert_eq!(serialize_size, deserialize_size);
    assert_eq!(
      test_str,
      &decoded_str,
      "Failed for string length: {}",
      test_str.len()
    );
  }
}

#[test]
fn test_container_markers_roundtrip() {
  let mut buffer = [0u8; 16];

  // Test array begin
  let serialize_size = serialize_array_begin(&mut buffer).unwrap();
  let deserialize_size = deserialize_array_start(&buffer).unwrap();
  assert_eq!(serialize_size, deserialize_size);

  // Test map begin
  let serialize_size = serialize_map_begin(&mut buffer).unwrap();
  let deserialize_size = deserialize_map_start(&buffer).unwrap();
  assert_eq!(serialize_size, deserialize_size);

  // Test container end
  let serialize_size = serialize_container_end(&mut buffer).unwrap();
  let deserialize_size = deserialize_container_end(&buffer).unwrap();
  assert_eq!(serialize_size, deserialize_size);
}

#[test]
fn test_type_code_detection() {
  let mut buffer = [0u8; 16];

  // Test null
  serialize_null(&mut buffer).unwrap();
  let type_code = peek_type_code(&buffer).unwrap();
  assert_eq!(type_code, TypeCode::Null as u8);

  // Test true
  serialize_boolean(&mut buffer, true).unwrap();
  let type_code = peek_type_code(&buffer).unwrap();
  assert_eq!(type_code, TypeCode::True as u8);

  // Test false
  serialize_boolean(&mut buffer, false).unwrap();
  let type_code = peek_type_code(&buffer).unwrap();
  assert_eq!(type_code, TypeCode::False as u8);

  // Test array start
  serialize_array_begin(&mut buffer).unwrap();
  let type_code = peek_type_code(&buffer).unwrap();
  assert_eq!(type_code, TypeCode::ArrayStart as u8);
}

#[test]
fn test_string_header_serialization() {
  let mut buffer = [0u8; 64];

  // Short string
  let short_str = "hello";
  let header_size = serialize_string_header(&mut buffer, short_str).unwrap();
  assert_eq!(header_size, 1);
  assert_eq!(
    buffer[0],
    TypeCode::String as u8 + u8::try_from(short_str.len()).unwrap()
  );

  // Long string
  let long_str = "This is a very long string that exceeds 15 characters";
  let header_size = serialize_string_header(&mut buffer, long_str).unwrap();
  assert!(header_size > 1);
  assert_eq!(buffer[0], TypeCode::LongString as u8);
}

#[test]
fn test_edge_cases() {
  let mut buffer = [0u8; 16];

  // Test small positive integers (should use direct encoding)
  for i in 0..=100u64 {
    let serialize_size = serialize_u64(&mut buffer, i).unwrap();
    assert_eq!(
      serialize_size, 1,
      "Small int {i} should serialize to 1 byte"
    );
    let (deserialize_size, decoded) = deserialize_unsigned_integer(&buffer).unwrap();
    assert_eq!(serialize_size, deserialize_size);
    assert_eq!(i, decoded);
  }

  // Test small negative integers (should use direct encoding)
  for i in -100..=100i64 {
    let serialize_size = serialize_i64(&mut buffer, i).unwrap();
    if (0..=100).contains(&i) {
      assert_eq!(
        serialize_size, 1,
        "Small int {i} should serialize to 1 byte"
      );
    }
    let (deserialize_size, decoded) = deserialize_signed_integer(&buffer).unwrap();
    assert_eq!(serialize_size, deserialize_size);
    assert_eq!(i, decoded);
  }
}

#[test]
#[allow(clippy::float_cmp)]
fn test_float_optimization() {
  let mut buffer = [0u8; 16];

  // Test f64 that can be represented as f32
  let value_f64 = 8.125f64;
  let serialize_size = serialize_f64(&mut buffer, value_f64).unwrap();
  // Should serialize as f32 (5 bytes) not f64 (9 bytes)
  assert!(
    serialize_size <= 5,
    "f64 that fits in f32 should optimize to f32 encoding"
  );

  let (deserialize_size, decoded) = deserialize_f64(&buffer).unwrap();
  assert_eq!(serialize_size, deserialize_size);
  assert_eq!(value_f64, decoded);
}

#[test]
fn test_buffer_boundary_conditions() {
  // Test with exactly the right buffer size
  let test_str = "exact";
  let mut buffer = vec![0u8; test_str.len() + 1]; // Header + content
  let serialize_size = serialize_string(&mut buffer, test_str).unwrap();
  let (deserialize_size, decoded) = deserialize_string(&buffer).unwrap();
  assert_eq!(serialize_size, deserialize_size);
  assert_eq!(test_str, decoded);

  // Test with buffer too small (should error)
  let mut small_buffer = [0u8; 1];
  let result = serialize_string(&mut small_buffer, "toolong");
  assert!(result.is_err());
}
