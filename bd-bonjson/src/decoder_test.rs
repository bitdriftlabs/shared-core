// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::*;
use crate::writer::Writer;
use core::f64;
use std::io::Cursor;
use std::vec;

#[ctor::ctor]
fn test_global_init() {
  bd_test_helpers::test_global_init();
}

#[test]
fn test_decode_null() {
  let mut buf = vec![0u8; 10];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };
  writer.write_null().unwrap();
  let pos = usize::try_from(cursor.position()).unwrap();
  let data_slice = &buf[.. pos];
  let value = from_slice(data_slice).unwrap();
  assert_eq!(value, Value::Null);
}

#[test]
fn test_decode_booleans() {
  // Test true
  let mut buf = vec![0u8; 10];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };
  writer.write_boolean(true).unwrap();
  let pos = usize::try_from(cursor.position()).unwrap();
  let data_slice = &buf[.. pos];
  let value = from_slice(data_slice).unwrap();
  assert_eq!(value, Value::Bool(true));

  // Test false
  let mut buf = vec![0u8; 10];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };
  writer.write_boolean(false).unwrap();
  let pos = usize::try_from(cursor.position()).unwrap();
  let data_slice = &buf[.. pos];
  let value = from_slice(data_slice).unwrap();
  assert_eq!(value, Value::Bool(false));
}

#[test]
fn test_decode_integers() {
  // Small positive integer
  let mut buf = vec![0u8; 10];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };
  writer.write_signed(42).unwrap();
  let pos = usize::try_from(cursor.position()).unwrap();
  let data_slice = &buf[.. pos];
  let value = from_slice(data_slice).unwrap();
  assert_eq!(value, Value::Signed(42));

  // Small negative integer
  let mut buf = vec![0u8; 10];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };
  writer.write_signed(-42).unwrap();
  let pos = usize::try_from(cursor.position()).unwrap();
  let data_slice = &buf[.. pos];
  let value = from_slice(data_slice).unwrap();
  assert_eq!(value, Value::Signed(-42));

  // Large positive integer
  let mut buf = vec![0u8; 16];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };
  let large_val = 1_000_000_000;
  writer.write_signed(large_val).unwrap();
  let pos = usize::try_from(cursor.position()).unwrap();
  let data_slice = &buf[.. pos];
  let value = from_slice(data_slice).unwrap();
  assert_eq!(value, Value::Signed(large_val));
}

#[test]
#[allow(clippy::cast_possible_wrap)]
fn test_decode_unsigned() {
  // Small unsigned
  let mut buf = vec![0u8; 10];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };
  writer.write_unsigned(42).unwrap();
  let pos = usize::try_from(cursor.position()).unwrap();
  let data_slice = &buf[.. pos];
  let value = from_slice(data_slice).unwrap();
  assert_eq!(value, Value::Signed(42));

  // Large unsigned
  let mut buf = vec![0u8; 16];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };
  let large_val = 4_000_000_000;
  writer.write_unsigned(large_val).unwrap();
  let pos = usize::try_from(cursor.position()).unwrap();
  let data_slice = &buf[.. pos];
  let value = from_slice(data_slice).unwrap();
  assert_eq!(value, Value::Signed(large_val as i64));

  // Huge unsigned
  let mut buf = vec![0u8; 16];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };
  let huge_val = i64::MAX as u64 + 1;
  writer.write_unsigned(huge_val).unwrap();
  let pos = usize::try_from(cursor.position()).unwrap();
  let data_slice = &buf[.. pos];
  let value = from_slice(data_slice).unwrap();
  assert_eq!(value, Value::Unsigned(huge_val));
}

#[test]
fn test_decode_floats() {
  // Positive float
  let mut buf = vec![0u8; 16];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };
  writer.write_float(f64::consts::PI).unwrap();
  let pos = usize::try_from(cursor.position()).unwrap();
  let data_slice = &buf[.. pos];
  let value = from_slice(data_slice).unwrap();
  assert_eq!(value, Value::Float(f64::consts::PI));

  // Negative float
  let mut buf = vec![0u8; 16];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };
  writer.write_float(-f64::consts::E).unwrap();
  let pos = usize::try_from(cursor.position()).unwrap();
  let data_slice = &buf[.. pos];
  let value = from_slice(data_slice).unwrap();
  assert_eq!(value, Value::Float(-f64::consts::E));

  // Float with conversion from f32
  let mut buf = vec![0u8; 16];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };
  writer.write_f32(1.5f32).unwrap();
  let pos = usize::try_from(cursor.position()).unwrap();
  let data_slice = &buf[.. pos];
  let value = from_slice(data_slice).unwrap();
  assert_eq!(value, Value::Float(1.5));
}

#[test]
fn test_decode_strings() {
  // Short string
  let mut buf = vec![0u8; 32];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };
  writer.write_str("hello").unwrap();
  let pos = usize::try_from(cursor.position()).unwrap();
  let data_slice = &buf[.. pos];
  let value = from_slice(data_slice).unwrap();
  assert_eq!(value, Value::String("hello".to_string()));

  // Empty string
  let mut buf = vec![0u8; 10];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };
  writer.write_str("").unwrap();
  let pos = usize::try_from(cursor.position()).unwrap();
  let data_slice = &buf[.. pos];
  let value = from_slice(data_slice).unwrap();
  assert_eq!(value, Value::String(String::new()));

  // Long string
  let long_str =
    "This is a much longer string that should definitely use the long string encoding mechanism";
  let mut buf = vec![0u8; 128];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };
  writer.write_str(long_str).unwrap();
  let pos = usize::try_from(cursor.position()).unwrap();
  let data_slice = &buf[.. pos];
  let value = from_slice(data_slice).unwrap();
  assert_eq!(value, Value::String(long_str.to_string()));
}

#[test]
fn test_decode_array() {
  // Simple array
  let mut buf = vec![0u8; 64];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };

  writer.write_array_begin().unwrap();
  writer.write_null().unwrap();
  writer.write_boolean(true).unwrap();
  writer.write_signed(42).unwrap();
  writer.write_str("hello").unwrap();
  writer.write_container_end().unwrap();

  let pos = usize::try_from(cursor.position()).unwrap();
  let data_slice = &buf[.. pos];
  let value = from_slice(data_slice).unwrap();

  if let Value::Array(items) = value {
    assert_eq!(items.len(), 4);
    assert_eq!(items[0], Value::Null);
    assert_eq!(items[1], Value::Bool(true));
    assert_eq!(items[2], Value::Signed(42));
    assert_eq!(items[3], Value::String("hello".to_string()));
  } else {
    panic!("Expected array, got {value:?}");
  }

  // Empty array
  let mut buf = vec![0u8; 10];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };

  writer.write_array_begin().unwrap();
  writer.write_container_end().unwrap();

  let pos = usize::try_from(cursor.position()).unwrap();
  let data_slice = &buf[.. pos];
  let value = from_slice(data_slice).unwrap();

  if let Value::Array(items) = value {
    assert_eq!(items.len(), 0);
  } else {
    panic!("Expected empty array, got {value:?}");
  }
}

#[test]
fn test_decode_object() {
  // Simple object
  let mut buf = vec![0u8; 128];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };

  writer.write_map_begin().unwrap();
  writer.write_str("name").unwrap();
  writer.write_str("John").unwrap();
  writer.write_str("age").unwrap();
  writer.write_signed(30).unwrap();
  writer.write_str("active").unwrap();
  writer.write_boolean(true).unwrap();
  writer.write_container_end().unwrap();

  let pos = usize::try_from(cursor.position()).unwrap();
  let data_slice = &buf[.. pos];
  let value = from_slice(data_slice).unwrap();

  if let Value::Object(map) = value {
    assert_eq!(map.len(), 3);
    assert_eq!(map.get("name").unwrap(), &Value::String("John".to_string()));
    assert_eq!(map.get("age").unwrap(), &Value::Signed(30));
    assert_eq!(map.get("active").unwrap(), &Value::Bool(true));
  } else {
    panic!("Expected object, got {value:?}");
  }

  // Empty object
  let mut buf = vec![0u8; 10];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };

  writer.write_map_begin().unwrap();
  writer.write_container_end().unwrap();

  let pos = usize::try_from(cursor.position()).unwrap();
  let data_slice = &buf[.. pos];
  let value = from_slice(data_slice).unwrap();

  if let Value::Object(map) = value {
    assert_eq!(map.len(), 0);
  } else {
    panic!("Expected empty object, got {value:?}");
  }
}

#[test]
fn test_nested_structures() {
  let mut buf = vec![0u8; 512];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };

  // {"users": [{"name": "Alice", "id": 1}, {"name": "Bob", "id": 2}]}
  writer.write_map_begin().unwrap();
  writer.write_str("users").unwrap();

  writer.write_array_begin().unwrap();

  // First user
  writer.write_map_begin().unwrap();
  writer.write_str("name").unwrap();
  writer.write_str("Alice").unwrap();
  writer.write_str("id").unwrap();
  writer.write_signed(1).unwrap();
  writer.write_container_end().unwrap();

  // Second user
  writer.write_map_begin().unwrap();
  writer.write_str("name").unwrap();
  writer.write_str("Bob").unwrap();
  writer.write_str("id").unwrap();
  writer.write_signed(2).unwrap();
  writer.write_container_end().unwrap();

  writer.write_container_end().unwrap(); // End array
  writer.write_container_end().unwrap(); // End object

  let pos = usize::try_from(cursor.position()).unwrap();
  let data_slice = &buf[.. pos];
  let value = from_slice(data_slice).unwrap();

  // Verify structure
  if let Value::Object(map) = &value {
    if let Some(Value::Array(users)) = map.get("users") {
      assert_eq!(users.len(), 2);

      if let Value::Object(alice) = &users[0] {
        assert_eq!(
          alice.get("name").unwrap(),
          &Value::String("Alice".to_string())
        );
        assert_eq!(alice.get("id").unwrap(), &Value::Signed(1));
      } else {
        panic!("Expected object for first user");
      }

      if let Value::Object(bob) = &users[1] {
        assert_eq!(bob.get("name").unwrap(), &Value::String("Bob".to_string()));
        assert_eq!(bob.get("id").unwrap(), &Value::Signed(2));
      } else {
        panic!("Expected object for second user");
      }
    } else {
      panic!("Expected array for users");
    }
  } else {
    panic!("Expected object at root");
  }
}

#[test]
fn test_error_handling() {
  // Incomplete data (map with no end)
  let mut buf = vec![0u8; 64];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };

  writer.write_map_begin().unwrap();
  writer.write_str("name").unwrap();
  writer.write_str("John").unwrap();
  // Missing container end

  let pos = usize::try_from(cursor.position()).unwrap();
  let data_slice = &buf[.. pos];
  let result = from_slice(data_slice);
  assert!(result.is_err());

  // Invalid map key (not a string)
  let mut buf = vec![0u8; 64];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };

  writer.write_map_begin().unwrap();
  writer.write_signed(123).unwrap(); // Keys must be strings
  writer.write_str("value").unwrap();
  writer.write_container_end().unwrap();

  let pos = usize::try_from(cursor.position()).unwrap();
  let data_slice = &buf[.. pos];
  let result = from_slice(data_slice);
  assert!(result.is_err());
}

#[test]
#[allow(clippy::cast_possible_wrap)]
#[allow(clippy::cast_sign_loss)]
fn test_boundary_values() {
  // Test min/max integers
  let values = [
    0i64,
    1,
    -1,
    100,
    -100,
    i64::from(i8::MIN),
    i64::from(i8::MAX),
    i64::from(i16::MIN),
    i64::from(i16::MAX),
    i64::from(i32::MIN),
    i64::from(i32::MAX),
    i64::MIN,
    i64::MAX,
  ];

  for value in values {
    let mut buf = vec![0u8; 16];
    let mut cursor = Cursor::new(&mut buf);
    let mut writer = Writer {
      writer: &mut cursor,
    };
    writer.write_signed(value).unwrap();
    let pos = usize::try_from(cursor.position()).unwrap();

    let data_slice = &buf[.. pos];
    let decoded = from_slice(data_slice).unwrap();

    if u64::try_from(value).is_ok() {
      // Unsigned conversion may happen
      match decoded {
        Value::Signed(v) => assert_eq!(v, value),
        Value::Unsigned(v) => assert_eq!(v as i64, value),
        _ => panic!("Expected integer or unsigned, got {decoded:?}"),
      }
    } else {
      assert_eq!(decoded, Value::Signed(value));
    }
  }

  // Test min/max unsigned
  let values = [
    0u64,
    1,
    100,
    255,
    256,
    65535,
    65536,
    u64::from(u8::MAX),
    u64::from(u16::MAX),
    u64::from(u32::MAX),
    u64::MAX,
  ];

  for value in values {
    let mut buf = vec![0u8; 16];
    let mut cursor = Cursor::new(&mut buf);
    let mut writer = Writer {
      writer: &mut cursor,
    };
    writer.write_unsigned(value).unwrap();
    let pos = usize::try_from(cursor.position()).unwrap();

    let data_slice = &buf[.. pos];
    let decoded = from_slice(data_slice).unwrap();

    if i64::try_from(value).is_ok() {
      // Integer conversion may happen
      match decoded {
        Value::Signed(v) => assert_eq!(v as u64, value),
        Value::Unsigned(v) => assert_eq!(v, value),
        _ => panic!("Expected integer or unsigned, got {decoded:?}"),
      }
    } else {
      assert_eq!(decoded, Value::Unsigned(value));
    }
  }
}

#[test]
#[allow(clippy::float_cmp)]
fn test_accessor_methods() {
  // Create test values
  let null_val = Value::Null;
  let bool_val = Value::Bool(true);
  let int_val = Value::Signed(42);
  let float_val = Value::Float(3.123);
  let str_val = Value::String("hello".to_string());

  let arr = vec![Value::Signed(1), Value::Signed(2)];
  let array_val = Value::Array(arr);

  let mut map = HashMap::new();
  map.insert("key".to_string(), Value::String("value".to_string()));
  let obj_val = Value::Object(map);

  // Test is_ methods
  assert!(null_val.is_null());
  assert!(bool_val.is_bool());
  assert!(int_val.is_integer());
  assert!(float_val.is_float());
  assert!(str_val.is_string());
  assert!(array_val.is_array());
  assert!(obj_val.is_object());

  // Test as_ methods
  assert!(null_val.as_null().is_ok());
  assert!(bool_val.as_bool().unwrap());
  assert_eq!(int_val.as_integer().unwrap(), 42);
  assert_eq!(float_val.as_float().unwrap(), 3.123);
  assert_eq!(str_val.as_string().unwrap(), "hello");
  assert_eq!(array_val.as_array().unwrap().len(), 2);
  assert_eq!(obj_val.as_object().unwrap().len(), 1);

  // Test failed as_ methods
  assert!(null_val.as_bool().is_err());
  assert!(bool_val.as_integer().is_err());
  assert!(int_val.as_string().is_err());

  // Test getters
  assert_eq!(array_val.get_index(0).unwrap(), &Value::Signed(1));
  assert_eq!(array_val.get_index(1).unwrap(), &Value::Signed(2));
  assert_eq!(
    obj_val.get("key").unwrap(),
    &Value::String("value".to_string())
  );
}

#[test]
fn test_partial_decode_unterminated_array() {
  let mut buf = vec![0u8; 64];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };

  // Write an array beginning but don't terminate it
  writer.write_array_begin().unwrap();
  writer.write_null().unwrap();
  writer.write_boolean(true).unwrap();
  writer.write_signed(42).unwrap();
  // Missing container end

  let pos = usize::try_from(cursor.position()).unwrap();

  // Try to decode - should fail with partial result
  let result = from_slice(&buf[.. pos]);
  assert!(result.is_err());

  let err = result.err().unwrap();
  assert_eq!(
    *err.error(),
    DeserializationErrorWithOffset::Error(DeserializationError::PrematureEnd, pos)
  );

  // Check partial result contains valid elements
  match err.partial_value() {
    Some(Value::Array(elements)) => {
      assert_eq!(elements.len(), 3);
      assert_eq!(elements[0], Value::Null);
      assert_eq!(elements[1], Value::Bool(true));
      assert_eq!(elements[2], Value::Signed(42));
    },
    _ => panic!("Expected partial array, got {:?}", err.partial_value()),
  }
}

#[test]
fn test_partial_decode_unterminated_object() {
  let mut buf = vec![0u8; 64];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };

  // Write an object beginning but don't terminate it
  writer.write_map_begin().unwrap();
  writer.write_str("key1").unwrap();
  writer.write_str("value1").unwrap();
  writer.write_str("key2").unwrap();
  writer.write_signed(42).unwrap();
  // Missing container end

  let pos = usize::try_from(cursor.position()).unwrap();

  // Try to decode - should fail with partial result
  let result = from_slice(&buf[.. pos]);
  assert!(result.is_err());

  let err = result.err().unwrap();
  assert_eq!(
    *err.error(),
    DeserializationErrorWithOffset::Error(DeserializationError::PrematureEnd, pos)
  );

  // Check partial result contains valid key-value pairs
  match err.partial_value() {
    Some(Value::Object(obj)) => {
      assert_eq!(obj.len(), 2);
      assert_eq!(
        obj.get("key1").unwrap(),
        &Value::String("value1".to_string())
      );
      assert_eq!(obj.get("key2").unwrap(), &Value::Signed(42));
    },
    _ => panic!("Expected partial object, got {:?}", err.partial_value()),
  }
}

#[test]
fn test_partial_decode_nested_containers() {
  let mut buf = vec![0u8; 128];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };

  // Create a nested structure that's unterminated
  writer.write_map_begin().unwrap();
  writer.write_str("name").unwrap();
  writer.write_str("test").unwrap();
  writer.write_str("data").unwrap();
  writer.write_array_begin().unwrap();
  writer.write_signed(1).unwrap();
  writer.write_signed(2).unwrap();
  writer.write_signed(3).unwrap();
  // Missing end for array and object

  let pos = usize::try_from(cursor.position()).unwrap();

  // Try to decode - should fail with partial result
  let result = from_slice(&buf[.. pos]);
  assert!(result.is_err());

  let err = result.err().unwrap();

  // Check partial result structure
  match err.partial_value() {
    Some(Value::Object(obj)) => {
      assert_eq!(obj.len(), 2);
      assert_eq!(obj.get("name").unwrap(), &Value::String("test".to_string()));

      match obj.get("data").unwrap() {
        Value::Array(arr) => {
          assert_eq!(arr.len(), 3);
          assert_eq!(arr[0], Value::Signed(1));
          assert_eq!(arr[1], Value::Signed(2));
          assert_eq!(arr[2], Value::Signed(3));
        },
        _ => panic!("Expected array for 'data' field"),
      }
    },
    _ => panic!("Expected partial object, got {:?}", err.partial_value()),
  }
}

#[test]
fn test_partial_decode_truncated_string() {
  let buf = [0x84, b'a', b'b', b'c'];

  // Try to decode - should fail with error but no partial result
  let result = from_slice(&buf);
  assert!(result.is_err());

  let err = result.err().unwrap();
  // String types don't produce partial results, so we expect just a fatal error
  assert!(err.is_fatal());
  assert_eq!(err.partial_value(), None);
}

#[test]
fn test_partial_decode_with_invalid_type_code() {
  // Create a buffer with valid data followed by an invalid type code
  let mut buf = vec![0u8; 64];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };

  writer.write_array_begin().unwrap();
  writer.write_null().unwrap();
  writer.write_boolean(true).unwrap();

  let pos = usize::try_from(cursor.position()).unwrap();

  // Add an invalid type code
  buf[pos] = 0x65; // Invalid type code

  // Try to decode
  let result = from_slice(&buf[..= pos]);
  assert!(result.is_err());

  let err = result.err().unwrap();

  // Should have a partial array with the valid elements
  match err.partial_value() {
    Some(Value::Array(elements)) => {
      assert_eq!(elements.len(), 2);
      assert_eq!(elements[0], Value::Null);
      assert_eq!(elements[1], Value::Bool(true));
    },
    _ => panic!("Expected partial array, got {:?}", err.partial_value()),
  }

  // And the error should indicate unexpected type code
  assert!(matches!(
    *err.error(),
    DeserializationErrorWithOffset::Error(DeserializationError::UnexpectedTypeCode, _)
  ));
}

#[test]
fn test_partial_decode_object_with_invalid_key() {
  let mut buf = vec![0u8; 64];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };

  // Create an object with valid data
  writer.write_map_begin().unwrap();
  writer.write_str("key1").unwrap();
  writer.write_str("value1").unwrap();

  // Then add a non-string key (invalid)
  writer.write_signed(123).unwrap();

  let pos = usize::try_from(cursor.position()).unwrap();

  // Try to decode
  let result = from_slice(&buf[.. pos]);
  assert!(result.is_err());

  let err = result.err().unwrap();

  // Should have a partial object with the valid key-value pair
  match err.partial_value() {
    Some(Value::Object(obj)) => {
      assert_eq!(obj.len(), 1);
      assert_eq!(
        obj.get("key1").unwrap(),
        &Value::String("value1".to_string())
      );
    },
    _ => panic!("Expected partial object, got {:?}", err.partial_value()),
  }

  // And the error should indicate non-string key
  assert!(matches!(
    *err.error(),
    DeserializationErrorWithOffset::Error(DeserializationError::NonStringKeyInMap, _)
  ));
}

#[test]
fn test_decode_special_strings() {
  // Test empty string
  let mut buf = vec![0u8; 16];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };
  writer.write_str("").unwrap();
  let pos = usize::try_from(cursor.position()).unwrap();

  let data_slice = &buf[.. pos];
  let value = from_slice(data_slice).unwrap();
  assert_eq!(value, Value::String(String::new()));

  // Test emoji and multi-byte characters
  let special_str = "こんにちは世界 🌍 emoji test";
  let mut buf = vec![0u8; 128];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };
  writer.write_str(special_str).unwrap();
  let pos = usize::try_from(cursor.position()).unwrap();

  let data_slice = &buf[.. pos];
  let value = from_slice(data_slice).unwrap();
  assert_eq!(value, Value::String(special_str.to_string()));

  // Test string with control characters
  let control_str = "Line 1\nLine 2\tTabbed\rCarriage Return";
  let mut buf = vec![0u8; 64];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };
  writer.write_str(control_str).unwrap();
  let pos = usize::try_from(cursor.position()).unwrap();

  let data_slice = &buf[.. pos];
  let value = from_slice(data_slice).unwrap();
  assert_eq!(value, Value::String(control_str.to_string()));
}

#[test]
fn test_decode_empty_input() {
  // Test with completely empty input
  let buf = [];
  let data_slice = &buf;
  let result = from_slice(data_slice);
  assert!(result.is_err());
}

#[test]
fn test_deeply_nested_structures() {
  // Create a deeply nested array structure
  let mut buf = vec![0u8; 1024];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };

  // Start array level 1
  writer.write_array_begin().unwrap();

  // Level 2
  writer.write_array_begin().unwrap();

  // Level 3
  writer.write_array_begin().unwrap();

  // Level 4
  writer.write_array_begin().unwrap();

  // Level 5
  writer.write_array_begin().unwrap();
  writer.write_signed(42).unwrap();
  writer.write_container_end().unwrap(); // End level 5

  writer.write_container_end().unwrap(); // End level 4
  writer.write_container_end().unwrap(); // End level 3
  writer.write_container_end().unwrap(); // End level 2
  writer.write_container_end().unwrap(); // End level 1

  let pos = usize::try_from(cursor.position()).unwrap();

  // Test decoding the deeply nested structure
  let data_slice = &buf[.. pos];
  let value = from_slice(data_slice).unwrap();

  // Verify the structure at each level
  if let Value::Array(level1) = value {
    assert_eq!(level1.len(), 1);

    if let Value::Array(level2) = &level1[0] {
      assert_eq!(level2.len(), 1);

      if let Value::Array(level3) = &level2[0] {
        assert_eq!(level3.len(), 1);

        if let Value::Array(level4) = &level3[0] {
          assert_eq!(level4.len(), 1);

          if let Value::Array(level5) = &level4[0] {
            assert_eq!(level5.len(), 1);
            assert_eq!(level5[0], Value::Signed(42));
          } else {
            panic!("Expected array at level 5");
          }
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
}

#[test]
fn test_mixed_object_and_array_nesting() {
  // Create a structure with mixed object and array nesting
  let mut buf = vec![0u8; 512];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };

  // Root object
  writer.write_map_begin().unwrap();

  writer.write_str("array").unwrap();
  // Nested array in object
  writer.write_array_begin().unwrap();

  // Object in array
  writer.write_map_begin().unwrap();
  writer.write_str("nested").unwrap();

  // Array in object in array
  writer.write_array_begin().unwrap();
  writer.write_signed(1).unwrap();
  writer.write_signed(2).unwrap();
  writer.write_container_end().unwrap();

  writer.write_container_end().unwrap(); // End inner object
  writer.write_container_end().unwrap(); // End array

  writer.write_str("value").unwrap();
  writer.write_str("test").unwrap();

  writer.write_container_end().unwrap(); // End root object

  let pos = usize::try_from(cursor.position()).unwrap();

  // Test decoding the mixed nested structure
  let data_slice = &buf[.. pos];
  let value = from_slice(data_slice).unwrap();

  // Verify the structure
  if let Value::Object(root) = &value {
    assert_eq!(root.len(), 2);
    assert_eq!(
      root.get("value").unwrap(),
      &Value::String("test".to_string())
    );

    if let Value::Array(array) = root.get("array").unwrap() {
      assert_eq!(array.len(), 1);

      if let Value::Object(inner_obj) = &array[0] {
        assert_eq!(inner_obj.len(), 1);

        if let Value::Array(inner_array) = inner_obj.get("nested").unwrap() {
          assert_eq!(inner_array.len(), 2);
          assert_eq!(inner_array[0], Value::Signed(1));
          assert_eq!(inner_array[1], Value::Signed(2));
        } else {
          panic!("Expected array for 'nested' field");
        }
      } else {
        panic!("Expected object inside array");
      }
    } else {
      panic!("Expected array for 'array' field");
    }
  } else {
    panic!("Expected object at root");
  }
}

#[test]
fn test_value_none_removed_successful_decode() {
  // Test that valid data decodes successfully without Value::None
  let valid_data = [0x85, 0x48, 0x65, 0x6c, 0x6c, 0x6f]; // "Hello"
  match from_slice(&valid_data) {
    Ok(Value::String(s)) => assert_eq!(s, "Hello"),
    Ok(other) => panic!("Expected string, got: {other:?}"),
    Err(e) => panic!("Unexpected error: {e:?}"),
  }
}

#[test]
fn test_value_none_removed_fatal_errors() {
  // Test that invalid data returns fatal errors (no partial value)
  let invalid_data = [0x65]; // Invalid type code
  match from_slice(&invalid_data) {
    Ok(value) => panic!("Expected error, got: {value:?}"),
    Err(err) => {
      assert!(err.is_fatal(), "Expected fatal error, got: {err:?}");
      assert!(err.partial_value().is_none(), "Expected no partial value");
    },
  }
}

#[test]
fn test_value_none_removed_partial_errors() {
  // Test that truncated data returns partial results
  let truncated_array = [0x9a, 0x6d, 0x85, 0x48, 0x65, 0x6c, 0x6c, 0x6f]; // Start of array with null and "Hello", but no end
  match from_slice(&truncated_array) {
    Ok(value) => panic!("Expected error, got: {value:?}"),
    Err(err) => {
      assert!(
        !err.is_fatal(),
        "Expected partial error, got fatal: {err:?}"
      );
      assert!(err.partial_value().is_some(), "Expected partial value");
    },
  }
}

#[test]
fn test_all_variants() {
  use std::collections::HashMap;

  let all_value_types = vec![
    Value::Null,
    Value::Bool(true),
    Value::Float(std::f64::consts::PI),
    Value::Signed(-42),
    Value::Unsigned(42),
    Value::String("test".to_string()),
    Value::Array(vec![]),
    Value::Object(HashMap::new()),
  ];

  assert_eq!(all_value_types.len(), 8);

  assert!(matches!(all_value_types[0], Value::Null));
  assert!(matches!(all_value_types[1], Value::Bool(true)));
  assert!(matches!(all_value_types[2], Value::Float(_)));
  assert!(matches!(all_value_types[3], Value::Signed(-42)));
  assert!(matches!(all_value_types[4], Value::Unsigned(42)));
  assert!(matches!(all_value_types[5], Value::String(_)));
  assert!(matches!(all_value_types[6], Value::Array(_)));
  assert!(matches!(all_value_types[7], Value::Object(_)));
}

#[test]
fn test_decode_error_convenience_methods() {
  use crate::deserialize_primitives::DeserializationError;

  let fatal_error = DecodeError::Fatal(DeserializationErrorWithOffset::Error(
    DeserializationError::UnexpectedTypeCode,
    0,
  ));

  assert!(fatal_error.is_fatal());
  assert!(fatal_error.partial_value().is_none());

  let partial_error = DecodeError::Partial {
    partial_value: Value::Null,
    error: DeserializationErrorWithOffset::Error(DeserializationError::UnexpectedTypeCode, 0),
  };

  assert!(!partial_error.is_fatal());
  assert!(partial_error.partial_value().is_some());
  matches!(partial_error.partial_value(), Some(Value::Null));
}

#[test]
fn test_decode_buf() {
  use bytes::Bytes;

  // Test decoding various values using decode_buf with Bytes (which implements Buf)
  let mut buf = vec![0u8; 10];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };
  
  // Write null value
  writer.write_null().unwrap();
  let pos = usize::try_from(cursor.position()).unwrap();
  let data_slice = &buf[.. pos];
  
  // Test with Bytes
  let bytes = Bytes::copy_from_slice(data_slice);
  let value = from_buf(bytes).unwrap();
  assert_eq!(value, Value::Null);
  
  // Test with string value
  let mut buf = vec![0u8; 20];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };
  writer.write_str("hello").unwrap();
  let pos = usize::try_from(cursor.position()).unwrap();
  let data_slice = &buf[.. pos];
  
  let bytes = Bytes::copy_from_slice(data_slice);
  let value = from_buf(bytes).unwrap();
  assert_eq!(value, Value::String("hello".to_string()));
  
  // Test with integer value
  let mut buf = vec![0u8; 20];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };
  writer.write_signed(42).unwrap();
  let pos = usize::try_from(cursor.position()).unwrap();
  let data_slice = &buf[.. pos];
  
  let bytes = Bytes::copy_from_slice(data_slice);
  let value = from_buf(bytes).unwrap();
  assert_eq!(value, Value::Signed(42));
  
  // Test with a slice directly (also implements Buf)
  let mut buf = vec![0u8; 20];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };
  writer.write_boolean(true).unwrap();
  let pos = usize::try_from(cursor.position()).unwrap();
  let data_slice = &buf[.. pos];
  
  // Test with a slice directly (which also implements Buf)
  let value = from_buf(data_slice).unwrap();
  assert_eq!(value, Value::Bool(true));
}
