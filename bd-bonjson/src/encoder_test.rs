// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::Value;
use crate::decoder::Decoder;
use crate::encoder::encode;
use std::collections::HashMap;

#[test]
fn test_encode_null() {
  let mut buffer = Vec::new();
  let value = Value::Null;

  let result = encode(&mut buffer, &value).expect("Failed to encode null");

  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_bool_true() {
  let mut buffer = Vec::new();
  let value = Value::Bool(true);

  let result = encode(&mut buffer, &value).expect("Failed to encode bool");

  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_bool_false() {
  let mut buffer = Vec::new();
  let value = Value::Bool(false);

  let result = encode(&mut buffer, &value).expect("Failed to encode bool");

  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_float() {
  let mut buffer = Vec::new();
  let value = Value::Float(3.14159);

  let result = encode(&mut buffer, &value).expect("Failed to encode float");

  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_signed_positive() {
  let mut buffer = Vec::new();
  let value = Value::Signed(12345);

  let result = encode(&mut buffer, &value).expect("Failed to encode signed");

  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_signed_negative() {
  let mut buffer = Vec::new();
  let value = Value::Signed(-6789);

  let result = encode(&mut buffer, &value).expect("Failed to encode signed");

  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_unsigned() {
  let mut buffer = Vec::new();
  let value = Value::Unsigned(98765);

  let result = encode(&mut buffer, &value).expect("Failed to encode unsigned");

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
  let mut buffer = Vec::new();
  let value = Value::String("Hello, World!".to_string());

  let result = encode(&mut buffer, &value).expect("Failed to encode string");

  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_empty_string() {
  let mut buffer = Vec::new();
  let value = Value::String(String::new());

  let result = encode(&mut buffer, &value)
    .expect("Failed to encode");

  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_string_with_unicode() {
  let mut buffer = Vec::new();
  let value = Value::String("🦀 Rust is awesome! 🚀".to_string());

  let result = encode(&mut buffer, &value)
    .expect("Failed to encode");

  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_empty_array() {
  let mut buffer = Vec::new();
  let value = Value::Array(Vec::new());

  let result = encode(&mut buffer, &value)
    .expect("Failed to encode");

  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_array_with_values() {
  let mut buffer = Vec::new();
  let value = Value::Array(vec![
    Value::Null,
    Value::Bool(true),
    Value::Signed(42),
    Value::String("test".to_string()),
  ]);

  let result = encode(&mut buffer, &value).expect("Failed to encode array");

  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_nested_arrays() {
  let mut buffer = Vec::new();
  let value = Value::Array(vec![
    Value::Array(vec![Value::Signed(1), Value::Signed(2)]),
    Value::Array(vec![
      Value::String("a".to_string()),
      Value::String("b".to_string()),
    ]),
  ]);

  let result = encode(&mut buffer, &value)
    .expect("Failed to encode");

  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_empty_object() {
  let mut buffer = Vec::new();
  let value = Value::Object(HashMap::new());

  let result = encode(&mut buffer, &value)
    .expect("Failed to encode");

  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_object_with_values() {
  let mut buffer = Vec::new();
  let mut obj = HashMap::new();
  obj.insert("null_key".to_string(), Value::Null);
  obj.insert("bool_key".to_string(), Value::Bool(false));
  obj.insert("number_key".to_string(), Value::Signed(123));
  obj.insert("string_key".to_string(), Value::String("value".to_string()));

  let value = Value::Object(obj);

  let result = encode(&mut buffer, &value).expect("Failed to encode object");

  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_nested_objects() {
  let mut buffer = Vec::new();

  let mut inner_obj = HashMap::new();
  inner_obj.insert(
    "inner_key".to_string(),
    Value::String("inner_value".to_string()),
  );

  let mut outer_obj = HashMap::new();
  outer_obj.insert("outer_key".to_string(), Value::Object(inner_obj));
  outer_obj.insert("simple_key".to_string(), Value::Signed(42));

  let value = Value::Object(outer_obj);

  let result = encode(&mut buffer, &value)
    .expect("Failed to encode");

  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encode_mixed_nested_structure() {
  let mut buffer = Vec::new();

  let mut obj = HashMap::new();
  obj.insert(
    "array".to_string(),
    Value::Array(vec![
      Value::Signed(1),
      Value::Signed(2),
      Value::Array(vec![Value::String("nested".to_string())]),
    ]),
  );
  obj.insert("object".to_string(), {
    let mut inner = HashMap::new();
    inner.insert("key".to_string(), Value::Bool(true));
    Value::Object(inner)
  });

  let value = Value::Object(obj);

  let result = encode(&mut buffer, &value)
    .expect("Failed to encode");

  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encoder_reuse() {
  let mut buffer = Vec::new();

  // Encode first value
  let value1 = Value::String("first".to_string());
  let result1 = encode(&mut buffer, &value1)
    .expect("Failed to encode");

  // Encode second value (should reuse the encoder)
  let value2 = Value::Signed(42);
  let result2 = encode(&mut buffer, &value2)
    .expect("Failed to encode");

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
  let mut buffer = Vec::with_capacity(1024);
  let value = Value::String("test".to_string());

  let result = encode(&mut buffer, &value)
    .expect("Failed to encode");

  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_encoder_clear() {
  let mut buffer = Vec::new();

  // Encode a value
  let value = Value::String("test".to_string());
  encode(&mut buffer, &value).expect("Failed to encode");

  // Clear and encode another value
  buffer.clear();
  let value2 = Value::Signed(123);
  let result = encode(&mut buffer, &value2).expect("Failed to encode after clear");

  // Verify the second encoding works
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value2);
}

#[test]
fn test_encoder_buffer_access() {
  let mut buffer = Vec::new();
  let value = Value::String("test".to_string());

  encode(&mut buffer, &value).expect("Failed to encode");

  // Access buffer
  assert!(!buffer.is_empty());

  // Verify we can still use the buffer for more encoding
  let value2 = Value::Signed(456);
  let second_result = encode(&mut buffer, &value2).expect("Failed to encode again");
  
  // The buffer now contains the second encoding result
  let mut decoder = Decoder::new(&second_result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value2);
}

#[test]
fn test_encode_large_numbers() {
  let mut buffer = Vec::new();

  // Test large positive signed
  let value1 = Value::Signed(i64::MAX);
  let result1 = encode(&mut buffer, &value1)
    .expect("Failed to encode");
  let mut decoder1 = Decoder::new(&result1);
  let decoded1 = decoder1.decode().expect("Failed to decode");
  assert_eq!(decoded1, value1);

  // Test large negative signed
  let value2 = Value::Signed(i64::MIN);
  let result2 = encode(&mut buffer, &value2)
    .expect("Failed to encode");
  let mut decoder2 = Decoder::new(&result2);
  let decoded2 = decoder2.decode().expect("Failed to decode");
  assert_eq!(decoded2, value2);

  // Test large unsigned that requires staying unsigned
  let value3 = Value::Unsigned(u64::MAX);
  let result3 = encode(&mut buffer, &value3)
    .expect("Failed to encode");
  let mut decoder3 = Decoder::new(&result3);
  let decoded3 = decoder3.decode().expect("Failed to decode");
  assert_eq!(decoded3, value3); // u64::MAX cannot fit in i64, so should stay unsigned
}

#[test]
fn test_encode_special_floats() {
  let mut buffer = Vec::new();

  // Test zero
  let value1 = Value::Float(0.0);
  let result1 = encode(&mut buffer, &value1).expect("Failed to encode zero");
  let mut decoder1 = Decoder::new(&result1);
  let decoded1 = decoder1.decode().expect("Failed to decode");
  assert_eq!(decoded1, value1);

  // Test negative zero
  let value2 = Value::Float(-0.0);
  let result2 = encode(&mut buffer, &value2)
    .expect("Failed to encode");
  let mut decoder2 = Decoder::new(&result2);
  let decoded2 = decoder2.decode().expect("Failed to decode");
  assert_eq!(decoded2, value2);

  // Test infinity
  let value3 = Value::Float(f64::INFINITY);
  let result3 = encode(&mut buffer, &value3).expect("Failed to encode infinity");
  let mut decoder3 = Decoder::new(&result3);
  let decoded3 = decoder3.decode().expect("Failed to decode");
  assert_eq!(decoded3, value3);

  // Test negative infinity
  let value4 = Value::Float(f64::NEG_INFINITY);
  let result4 = encode(&mut buffer, &value4)
    .expect("Failed to encode");
  let mut decoder4 = Decoder::new(&result4);
  let decoded4 = decoder4.decode().expect("Failed to decode");
  assert_eq!(decoded4, value4);

  // Test NaN (Note: NaN != NaN, so we need special handling)
  let value5 = Value::Float(f64::NAN);
  let result5 = encode(&mut buffer, &value5).expect("Failed to encode NaN");
  let mut decoder5 = Decoder::new(&result5);
  let decoded5 = decoder5.decode().expect("Failed to decode");
  if let Value::Float(f) = decoded5 {
    assert!(f.is_nan());
  } else {
    panic!("Expected Float value");
  }
}

#[test]
fn test_encode_deeply_nested_mixed_structures() {
  let mut buffer = Vec::new();

  // Build a complex deeply nested structure with mixed arrays and objects
  let mut root_obj = HashMap::new();

  // Add a deeply nested array structure (5 levels deep)
  let deep_array = Value::Array(vec![
    Value::Array(vec![
      Value::Array(vec![
        Value::Array(vec![
          Value::Array(vec![
            Value::Signed(42),
            Value::String("deep".to_string()),
            Value::Bool(true),
          ]),
          Value::Null,
        ]),
        Value::Float(3.14159),
      ]),
      Value::String("level3".to_string()),
    ]),
    Value::Signed(100),
  ]);
  root_obj.insert("deep_arrays".to_string(), deep_array);

  // Add a mixed object-array-object structure
  let mut level1_obj = HashMap::new();
  level1_obj.insert(
    "data".to_string(),
    Value::Array(vec![
      Value::Signed(1),
      Value::Signed(2),
      {
        let mut nested_obj = HashMap::new();
        nested_obj.insert(
          "inner".to_string(),
          Value::Array(vec![Value::String("nested_string".to_string()), {
            let mut deep_obj = HashMap::new();
            deep_obj.insert("deepest".to_string(), Value::Bool(false));
            deep_obj.insert(
              "numbers".to_string(),
              Value::Array(vec![
                Value::Unsigned(u64::MAX), // Use a value that won't fit in i64
                Value::Float(-123.456),
                Value::Signed(-789),
              ]),
            );
            Value::Object(deep_obj)
          }]),
        );
        nested_obj.insert("sibling".to_string(), Value::Null);
        Value::Object(nested_obj)
      },
      Value::String("after_object".to_string()),
    ]),
  );
  level1_obj.insert("metadata".to_string(), Value::String("info".to_string()));
  root_obj.insert("mixed_structure".to_string(), Value::Object(level1_obj));

  // Add an array of objects with various data types
  let objects_array = Value::Array(vec![
    {
      let mut obj1 = HashMap::new();
      obj1.insert("type".to_string(), Value::String("first".to_string()));
      obj1.insert("value".to_string(), Value::Signed(123));
      obj1.insert("active".to_string(), Value::Bool(true));
      Value::Object(obj1)
    },
    {
      let mut obj2 = HashMap::new();
      obj2.insert("type".to_string(), Value::String("second".to_string()));
      obj2.insert(
        "coordinates".to_string(),
        Value::Array(vec![
          Value::Float(12.34),
          Value::Float(56.78),
          Value::Float(90.12),
        ]),
      );
      obj2.insert(
        "tags".to_string(),
        Value::Array(vec![
          Value::String("tag1".to_string()),
          Value::String("tag2".to_string()),
        ]),
      );
      Value::Object(obj2)
    },
    {
      let mut obj3 = HashMap::new();
      obj3.insert("type".to_string(), Value::String("third".to_string()));
      obj3.insert("empty_array".to_string(), Value::Array(vec![]));
      obj3.insert("empty_object".to_string(), Value::Object(HashMap::new()));
      obj3.insert("null_value".to_string(), Value::Null);
      Value::Object(obj3)
    },
  ]);
  root_obj.insert("object_array".to_string(), objects_array);

  // Add some simple values for completeness
  root_obj.insert(
    "simple_string".to_string(),
    Value::String("simple".to_string()),
  );
  root_obj.insert("simple_number".to_string(), Value::Signed(999));
  root_obj.insert("simple_bool".to_string(), Value::Bool(false));
  root_obj.insert("simple_null".to_string(), Value::Null);

  let complex_value = Value::Object(root_obj);

  // Encode the complex structure
  let result = encode(&mut buffer, &complex_value)
    .expect("Failed to encode deeply nested structure");

  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder
    .decode()
    .expect("Failed to decode deeply nested structure");

  // Since HashMap order is not guaranteed, we'll verify the structure piece by piece
  // instead of doing a direct equality check
  if let Value::Object(root) = &decoded {
    // Verify the structure has the expected keys
    assert_eq!(root.len(), 7); // Should have 7 top-level keys
    assert!(root.contains_key("deep_arrays"));
    assert!(root.contains_key("mixed_structure"));
    assert!(root.contains_key("object_array"));
    assert!(root.contains_key("simple_string"));
    assert!(root.contains_key("simple_number"));
    assert!(root.contains_key("simple_bool"));
    assert!(root.contains_key("simple_null"));

    // Verify the deeply nested array structure
    if let Value::Array(deep_arrays) = root.get("deep_arrays").unwrap() {
      assert_eq!(deep_arrays.len(), 2);
      assert_eq!(deep_arrays[1], Value::Signed(100));

      if let Value::Array(level1) = &deep_arrays[0] {
        assert_eq!(level1.len(), 2);
        assert_eq!(level1[1], Value::String("level3".to_string()));

        if let Value::Array(level2) = &level1[0] {
          assert_eq!(level2.len(), 2);
          assert_eq!(level2[1], Value::Float(3.14159));

          if let Value::Array(level3) = &level2[0] {
            assert_eq!(level3.len(), 2);
            assert_eq!(level3[1], Value::Null);

            if let Value::Array(level4) = &level3[0] {
              assert_eq!(level4.len(), 3); // This array has 3 elements
              assert_eq!(level4[0], Value::Signed(42));
              assert_eq!(level4[1], Value::String("deep".to_string()));
              assert_eq!(level4[2], Value::Bool(true));
            } else {
              panic!("Expected array at level 4");
            }
          } else {
            panic!("Expected array at level 3");
          }
        } else {
          panic!("Expected array at level 2");
        }
      } else {
        panic!("Expected array at level 1");
      }
    } else {
      panic!("Expected array for deep_arrays");
    }

    // Verify the mixed structure
    if let Value::Object(mixed) = root.get("mixed_structure").unwrap() {
      assert_eq!(mixed.len(), 2);
      assert_eq!(
        mixed.get("metadata").unwrap(),
        &Value::String("info".to_string())
      );

      if let Value::Array(data_array) = mixed.get("data").unwrap() {
        assert_eq!(data_array.len(), 4);
        assert_eq!(data_array[0], Value::Signed(1));
        assert_eq!(data_array[1], Value::Signed(2));
        assert_eq!(data_array[3], Value::String("after_object".to_string()));

        if let Value::Object(nested_obj) = &data_array[2] {
          assert_eq!(nested_obj.len(), 2);
          assert_eq!(nested_obj.get("sibling").unwrap(), &Value::Null);

          if let Value::Array(inner_array) = nested_obj.get("inner").unwrap() {
            assert_eq!(inner_array.len(), 2);
            assert_eq!(inner_array[0], Value::String("nested_string".to_string()));

            if let Value::Object(deep_obj) = &inner_array[1] {
              assert_eq!(deep_obj.len(), 2);
              assert_eq!(deep_obj.get("deepest").unwrap(), &Value::Bool(false));

              if let Value::Array(numbers) = deep_obj.get("numbers").unwrap() {
                assert_eq!(numbers.len(), 3);
                assert_eq!(numbers[0], Value::Unsigned(u64::MAX)); // Should stay unsigned
                assert_eq!(numbers[1], Value::Float(-123.456));
                assert_eq!(numbers[2], Value::Signed(-789));
              } else {
                panic!("Expected array for numbers");
              }
            } else {
              panic!("Expected object in inner array");
            }
          } else {
            panic!("Expected array for inner");
          }
        } else {
          panic!("Expected object in data array");
        }
      } else {
        panic!("Expected array for data");
      }
    } else {
      panic!("Expected object for mixed_structure");
    }

    // Verify the array of objects
    if let Value::Array(obj_array) = root.get("object_array").unwrap() {
      assert_eq!(obj_array.len(), 3);

      // Check first object
      if let Value::Object(obj1) = &obj_array[0] {
        assert_eq!(obj1.len(), 3);
        assert_eq!(
          obj1.get("type").unwrap(),
          &Value::String("first".to_string())
        );
        assert_eq!(obj1.get("value").unwrap(), &Value::Signed(123));
        assert_eq!(obj1.get("active").unwrap(), &Value::Bool(true));
      } else {
        panic!("Expected object at index 0");
      }

      // Check second object with nested arrays
      if let Value::Object(obj2) = &obj_array[1] {
        assert_eq!(obj2.len(), 3);
        assert_eq!(
          obj2.get("type").unwrap(),
          &Value::String("second".to_string())
        );

        if let Value::Array(coords) = obj2.get("coordinates").unwrap() {
          assert_eq!(coords.len(), 3);
          assert_eq!(coords[0], Value::Float(12.34));
          assert_eq!(coords[1], Value::Float(56.78));
          assert_eq!(coords[2], Value::Float(90.12));
        } else {
          panic!("Expected array for coordinates");
        }

        if let Value::Array(tags) = obj2.get("tags").unwrap() {
          assert_eq!(tags.len(), 2);
          assert_eq!(tags[0], Value::String("tag1".to_string()));
          assert_eq!(tags[1], Value::String("tag2".to_string()));
        } else {
          panic!("Expected array for tags");
        }
      } else {
        panic!("Expected object at index 1");
      }

      // Check third object with empty containers
      if let Value::Object(obj3) = &obj_array[2] {
        assert_eq!(obj3.len(), 4);
        assert_eq!(
          obj3.get("type").unwrap(),
          &Value::String("third".to_string())
        );
        assert_eq!(obj3.get("null_value").unwrap(), &Value::Null);

        if let Value::Array(empty_arr) = obj3.get("empty_array").unwrap() {
          assert!(empty_arr.is_empty());
        } else {
          panic!("Expected array for empty_array");
        }

        if let Value::Object(empty_obj) = obj3.get("empty_object").unwrap() {
          assert!(empty_obj.is_empty());
        } else {
          panic!("Expected object for empty_object");
        }
      } else {
        panic!("Expected object at index 2");
      }
    } else {
      panic!("Expected array for object_array");
    }

    // Verify simple values are preserved
    assert_eq!(
      root.get("simple_string").unwrap(),
      &Value::String("simple".to_string())
    );
    assert_eq!(root.get("simple_number").unwrap(), &Value::Signed(999));
    assert_eq!(root.get("simple_bool").unwrap(), &Value::Bool(false));
    assert_eq!(root.get("simple_null").unwrap(), &Value::Null);
  } else {
    panic!("Expected root object");
  }
}

// Tests for in-place encoding

use crate::encoder::encode_into;
use crate::serialize_primitives::SerializationError;

#[test]
fn test_in_place_encode_null() {
  let mut buffer = [0u8; 16];
  let value = Value::Null;

  let bytes_written = encode_into(&mut buffer, &value).expect("Failed to encode null");
  assert_eq!(bytes_written, 1);

  // Verify we can decode it back
  let mut decoder = Decoder::new(&buffer[.. bytes_written]);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_in_place_encode_bool() {
  let mut buffer = [0u8; 16];
  let value = Value::Bool(true);

  let bytes_written = encode_into(&mut buffer, &value).expect("Failed to encode bool");
  assert_eq!(bytes_written, 1);

  // Verify we can decode it back
  let mut decoder = Decoder::new(&buffer[.. bytes_written]);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_in_place_encode_string() {
  let mut buffer = [0u8; 32];
  let value = Value::String("hello".to_string());

  let bytes_written = encode_into(&mut buffer, &value).expect("Failed to encode string");

  // Verify we can decode it back
  let mut decoder = Decoder::new(&buffer[.. bytes_written]);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_in_place_encode_array() {
  let mut buffer = [0u8; 64];
  let value = Value::Array(vec![
    Value::Signed(1),
    Value::Signed(2),
    Value::String("test".to_string()),
  ]);

  let bytes_written = encode_into(&mut buffer, &value).expect("Failed to encode array");

  // Verify we can decode it back
  let mut decoder = Decoder::new(&buffer[.. bytes_written]);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_in_place_encode_object() {
  let mut buffer = [0u8; 128];

  let mut obj = HashMap::new();
  obj.insert("name".to_string(), Value::String("test".to_string()));
  obj.insert("age".to_string(), Value::Signed(25));
  let value = Value::Object(obj);

  let bytes_written = encode_into(&mut buffer, &value).expect("Failed to encode object");

  // Verify we can decode it back
  let mut decoder = Decoder::new(&buffer[.. bytes_written]);
  let decoded = decoder.decode().expect("Failed to decode");

  // Since HashMap ordering is not guaranteed, we need to check content differently
  if let Value::Object(decoded_obj) = decoded {
    assert_eq!(decoded_obj.len(), 2);
    assert_eq!(
      decoded_obj.get("name").unwrap(),
      &Value::String("test".to_string())
    );
    assert_eq!(decoded_obj.get("age").unwrap(), &Value::Signed(25));
  } else {
    panic!("Expected object");
  }
}

#[test]
fn test_in_place_encode_buffer_full() {
  let mut buffer = [0u8; 2]; // Very small buffer
  let value = Value::String("this is a long string that won't fit".to_string());

  let result = encode_into(&mut buffer, &value);
  assert!(result.is_err());
  assert_eq!(result.unwrap_err(), SerializationError::BufferFull);
}

#[test]
fn test_in_place_encode_position_tracking() {
  let mut buffer = [0u8; 16];

  let value = Value::Null;
  let bytes_written = encode_into(&mut buffer, &value).expect("Failed to encode null");

  assert_eq!(bytes_written, 1);

  // Second encode should start fresh (functions are stateless)
  let value2 = Value::Bool(true);
  let bytes_written2 = encode_into(&mut buffer, &value2).expect("Failed to encode bool");

  assert_eq!(bytes_written2, 1);
}

#[test]
fn test_in_place_encode_exact_fit() {
  // Create a value that will exactly fit in the buffer
  let mut buffer = [0u8; 8]; // Exactly the size needed for a small signed integer
  let value = Value::Signed(42);

  let bytes_written = encode_into(&mut buffer, &value).expect("Failed to encode");
  assert!(bytes_written <= buffer.len());

  // Verify we can decode it back
  let mut decoder = Decoder::new(&buffer[.. bytes_written]);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);
}

#[test]
fn test_in_place_encode_reuse() {
  let mut buffer = [0u8; 32];

  // First encoding
  let value1 = Value::Signed(123);
  encode_into(&mut buffer, &value1).expect("Failed to encode first value");

  // Second encoding (functions are stateless, so each call starts from position 0)
  let value2 = Value::String("hello".to_string());
  let bytes_written2 = encode_into(&mut buffer, &value2).expect("Failed to encode second value");

  // Verify the second value overwrote the first
  let mut decoder = Decoder::new(&buffer[.. bytes_written2]);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value2);
}

#[test]
fn test_in_place_encode_deeply_nested() {
  let mut buffer = [0u8; 256];

  // Create deeply nested structure
  let nested_value = Value::Array(vec![Value::Array(vec![Value::Array(vec![
    Value::String("deep".to_string()),
    Value::Signed(42),
  ])])]);

  let bytes_written =
    encode_into(&mut buffer, &nested_value).expect("Failed to encode nested structure");

  // Verify we can decode it back
  let mut decoder = Decoder::new(&buffer[.. bytes_written]);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, nested_value);
}

#[test]
fn test_standalone_encode_function() {
  let mut buffer = Vec::new();
  let value = Value::Object({
    let mut map = HashMap::new();
    map.insert("test".to_string(), Value::Signed(42));
    map.insert("flag".to_string(), Value::Bool(true));
    map
  });

  let result = encode(&mut buffer, &value).expect("Failed to encode with standalone function");

  // Verify we can decode it back
  let mut decoder = Decoder::new(&result);
  let decoded = decoder.decode().expect("Failed to decode");
  assert_eq!(decoded, value);

  // Verify buffer was reused correctly
  let second_value = Value::String("second test".to_string());
  let second_result = encode(&mut buffer, &second_value).expect("Failed to encode second value");
  
  let mut second_decoder = Decoder::new(&second_result);
  let second_decoded = second_decoder.decode().expect("Failed to decode second value");
  assert_eq!(second_decoded, second_value);
}
