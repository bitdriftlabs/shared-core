// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::decoder::Decoder;
use crate::encoder::Encoder;
use crate::Value;
use std::collections::HashMap;

#[test]
fn test_encode_null() {
  let mut encoder = Encoder::new();
  let value = Value::Null;
  
  let result = encoder.encode(&value).expect("Failed to encode null");
  
  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_bool_true() {
  let mut encoder = Encoder::new();
  let value = Value::Bool(true);
  
  let result = encoder.encode(&value).expect("Failed to encode bool");
  
  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_bool_false() {
  let mut encoder = Encoder::new();
  let value = Value::Bool(false);
  
  let result = encoder.encode(&value).expect("Failed to encode bool");
  
  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_float() {
  let mut encoder = Encoder::new();
  let value = Value::Float(3.14159);
  
  let result = encoder.encode(&value).expect("Failed to encode float");
  
  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_signed_positive() {
  let mut encoder = Encoder::new();
  let value = Value::Signed(12345);
  
  let result = encoder.encode(&value).expect("Failed to encode signed");
  
  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_signed_negative() {
  let mut encoder = Encoder::new();
  let value = Value::Signed(-6789);
  
  let result = encoder.encode(&value).expect("Failed to encode signed");
  
  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_unsigned() {
  let mut encoder = Encoder::new();
  let value = Value::Unsigned(98765);
  
  let result = encoder.encode(&value).expect("Failed to encode unsigned");
  
  // Verify we can decode it back
  // Note: The decoder may convert unsigned values that fit in i64 to Signed
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  
  // Check if the value was converted to signed (this is expected behavior)
  match decoded {
    Value::Signed(v) => assert_eq!(v, 98765),
    Value::Unsigned(v) => assert_eq!(v, 98765),
    _ => panic!("Expected integer value, got {:?}", decoded),
  }
}

#[test]
fn test_encode_string() {
  let mut encoder = Encoder::new();
  let value = Value::String("Hello, World!".to_string());
  
  let result = encoder.encode(&value).expect("Failed to encode string");
  
  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_empty_string() {
  let mut encoder = Encoder::new();
  let value = Value::String(String::new());
  
  let result = encoder.encode(&value).expect("Failed to encode empty string");
  
  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_string_with_unicode() {
  let mut encoder = Encoder::new();
  let value = Value::String("🦀 Rust is awesome! 🚀".to_string());
  
  let result = encoder.encode(&value).expect("Failed to encode unicode string");
  
  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_empty_array() {
  let mut encoder = Encoder::new();
  let value = Value::Array(Vec::new());
  
  let result = encoder.encode(&value).expect("Failed to encode empty array");
  
  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_array_with_values() {
  let mut encoder = Encoder::new();
  let value = Value::Array(vec![
    Value::Null,
    Value::Bool(true),
    Value::Signed(42),
    Value::String("test".to_string()),
  ]);
  
  let result = encoder.encode(&value).expect("Failed to encode array");
  
  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_nested_arrays() {
  let mut encoder = Encoder::new();
  let value = Value::Array(vec![
    Value::Array(vec![Value::Signed(1), Value::Signed(2)]),
    Value::Array(vec![Value::String("a".to_string()), Value::String("b".to_string())]),
  ]);
  
  let result = encoder.encode(&value).expect("Failed to encode nested arrays");
  
  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_empty_object() {
  let mut encoder = Encoder::new();
  let value = Value::Object(HashMap::new());
  
  let result = encoder.encode(&value).expect("Failed to encode empty object");
  
  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_object_with_values() {
  let mut encoder = Encoder::new();
  let mut obj = HashMap::new();
  obj.insert("null_key".to_string(), Value::Null);
  obj.insert("bool_key".to_string(), Value::Bool(false));
  obj.insert("number_key".to_string(), Value::Signed(123));
  obj.insert("string_key".to_string(), Value::String("value".to_string()));
  
  let value = Value::Object(obj);
  
  let result = encoder.encode(&value).expect("Failed to encode object");
  
  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_nested_objects() {
  let mut encoder = Encoder::new();
  
  let mut inner_obj = HashMap::new();
  inner_obj.insert("inner_key".to_string(), Value::String("inner_value".to_string()));
  
  let mut outer_obj = HashMap::new();
  outer_obj.insert("outer_key".to_string(), Value::Object(inner_obj));
  outer_obj.insert("simple_key".to_string(), Value::Signed(42));
  
  let value = Value::Object(outer_obj);
  
  let result = encoder.encode(&value).expect("Failed to encode nested objects");
  
  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_mixed_nested_structure() {
  let mut encoder = Encoder::new();
  
  let mut obj = HashMap::new();
  obj.insert("array".to_string(), Value::Array(vec![
    Value::Signed(1),
    Value::Signed(2),
    Value::Array(vec![Value::String("nested".to_string())]),
  ]));
  obj.insert("object".to_string(), {
    let mut inner = HashMap::new();
    inner.insert("key".to_string(), Value::Bool(true));
    Value::Object(inner)
  });
  
  let value = Value::Object(obj);
  
  let result = encoder.encode(&value).expect("Failed to encode mixed structure");
  
  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encoder_reuse() {
  let mut encoder = Encoder::new();
  
  // Encode first value
  let value1 = Value::String("first".to_string());
  let result1 = encoder.encode(&value1).expect("Failed to encode first value");
  
  // Encode second value (should reuse the encoder)
  let value2 = Value::Signed(42);
  let result2 = encoder.encode(&value2).expect("Failed to encode second value");
  
  // Verify both encodings work
  let mut decoder1 = Decoder::new(&result1);
  let decoded1 = decoder1.decode().expect("Failed to decode first");
  assert_eq!(decoded1, value1);
  
  let mut decoder2 = Decoder::new(&result2);
  let decoded2 = decoder2.decode().expect("Failed to decode second");
  assert_eq!(decoded2, value2);
}

#[test]
fn test_encoder_with_capacity() {
  let mut encoder = Encoder::with_capacity(1024);
  let value = Value::String("test".to_string());
  
  let result = encoder.encode(&value).expect("Failed to encode with capacity");
  
  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encoder_clear() {
  let mut encoder = Encoder::new();
  
  // Encode a value
  let value = Value::String("test".to_string());
  encoder.encode(&value).expect("Failed to encode");
  
  // Clear and encode another value
  encoder.clear();
  let value2 = Value::Signed(123);
  let result = encoder.encode(&value2).expect("Failed to encode after clear");
  
  // Verify the second encoding works
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value2);
}

#[test]
fn test_encoder_buffer_access() {
  let mut encoder = Encoder::new();
  let value = Value::String("test".to_string());
  
  encoder.encode(&value).expect("Failed to encode");
  
  // Access buffer without consuming encoder
  let buffer = encoder.buffer();
  assert!(!buffer.is_empty());
  
  // Verify we can still use the encoder
  let value2 = Value::Signed(456);
  encoder.encode(&value2).expect("Failed to encode again");
}

#[test]
fn test_encode_large_numbers() {
  let mut encoder = Encoder::new();
  
  // Test large positive signed
  let value1 = Value::Signed(i64::MAX);
  let result1 = encoder.encode(&value1).expect("Failed to encode large positive");
  let mut decoder1 = Decoder::new(&result1);
  let decoded1 = decoder1.decode().expect("Failed to decode");
  assert_eq!(decoded1, value1);
  
  // Test large negative signed
  let value2 = Value::Signed(i64::MIN);
  let result2 = encoder.encode(&value2).expect("Failed to encode large negative");
  let mut decoder2 = Decoder::new(&result2);
  let decoded2 = decoder2.decode().expect("Failed to decode");
  assert_eq!(decoded2, value2);
  
  // Test large unsigned that requires staying unsigned
  let value3 = Value::Unsigned(u64::MAX);
  let result3 = encoder.encode(&value3).expect("Failed to encode large unsigned");
  let mut decoder3 = Decoder::new(&result3);
  let decoded3 = decoder3.decode().expect("Failed to decode");
  assert_eq!(decoded3, value3); // u64::MAX cannot fit in i64, so should stay unsigned
}

#[test]
fn test_encode_special_floats() {
  let mut encoder = Encoder::new();
  
  // Test zero
  let value1 = Value::Float(0.0);
  let result1 = encoder.encode(&value1).expect("Failed to encode zero");
  let mut decoder1 = Decoder::new(&result1);
  let decoded1 = decoder1.decode().expect("Failed to decode");
  assert_eq!(decoded1, value1);
  
  // Test negative zero
  let value2 = Value::Float(-0.0);
  let result2 = encoder.encode(&value2).expect("Failed to encode negative zero");
  let mut decoder2 = Decoder::new(&result2);
  let decoded2 = decoder2.decode().expect("Failed to decode");
  assert_eq!(decoded2, value2);
  
  // Test infinity
  let value3 = Value::Float(f64::INFINITY);
  let result3 = encoder.encode(&value3).expect("Failed to encode infinity");
  let mut decoder3 = Decoder::new(&result3);
  let decoded3 = decoder3.decode().expect("Failed to decode");
  assert_eq!(decoded3, value3);
  
  // Test negative infinity
  let value4 = Value::Float(f64::NEG_INFINITY);
  let result4 = encoder.encode(&value4).expect("Failed to encode negative infinity");
  let mut decoder4 = Decoder::new(&result4);
  let decoded4 = decoder4.decode().expect("Failed to decode");
  assert_eq!(decoded4, value4);
  
  // Test NaN (Note: NaN != NaN, so we need special handling)
  let value5 = Value::Float(f64::NAN);
  let result5 = encoder.encode(&value5).expect("Failed to encode NaN");
  let mut decoder5 = Decoder::new(&result5);
  let decoded5 = decoder5.decode().expect("Failed to decode");
  if let Value::Float(f) = decoded5 {
    assert!(f.is_nan());
  } else {
    panic!("Expected Float value");
  }
}
