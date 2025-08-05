// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::*;
use crate::writer::Writer;
use core::f64;
use std::{io::Cursor, vec};

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

  let mut decoder = Decoder::new(&buf[..pos]);
  let value = decoder.decode().unwrap();
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

  let mut decoder = Decoder::new(&buf[..pos]);
  let value = decoder.decode().unwrap();
  assert_eq!(value, Value::Bool(true));

  // Test false
  let mut buf = vec![0u8; 10];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };
  writer.write_boolean(false).unwrap();
  let pos = usize::try_from(cursor.position()).unwrap();

  let mut decoder = Decoder::new(&buf[..pos]);
  let value = decoder.decode().unwrap();
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

  let mut decoder = Decoder::new(&buf[..pos]);
  let value = decoder.decode().unwrap();
  assert_eq!(value, Value::Signed(42));

  // Small negative integer
  let mut buf = vec![0u8; 10];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };
  writer.write_signed(-42).unwrap();
  let pos = usize::try_from(cursor.position()).unwrap();

  let mut decoder = Decoder::new(&buf[..pos]);
  let value = decoder.decode().unwrap();
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

  let mut decoder = Decoder::new(&buf[..pos]);
  let value = decoder.decode().unwrap();
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

  let mut decoder = Decoder::new(&buf[..pos]);
  let value = decoder.decode().unwrap();
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

  let mut decoder = Decoder::new(&buf[..pos]);
  let value = decoder.decode().unwrap();
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

  let mut decoder = Decoder::new(&buf[..pos]);
  let value = decoder.decode().unwrap();
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

  let mut decoder = Decoder::new(&buf[..pos]);
  let value = decoder.decode().unwrap();
  assert_eq!(value, Value::Float(f64::consts::PI));

  // Negative float
  let mut buf = vec![0u8; 16];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };
  writer.write_float(-f64::consts::E).unwrap();
  let pos = usize::try_from(cursor.position()).unwrap();

  let mut decoder = Decoder::new(&buf[..pos]);
  let value = decoder.decode().unwrap();
  assert_eq!(value, Value::Float(-f64::consts::E));

  // Float with conversion from f32
  let mut buf = vec![0u8; 16];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };
  writer.write_f32(1.5f32).unwrap();
  let pos = usize::try_from(cursor.position()).unwrap();

  let mut decoder = Decoder::new(&buf[..pos]);
  let value = decoder.decode().unwrap();
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

  let mut decoder = Decoder::new(&buf[..pos]);
  let value = decoder.decode().unwrap();
  assert_eq!(value, Value::String("hello".to_string()));

  // Empty string
  let mut buf = vec![0u8; 10];
  let mut cursor = Cursor::new(&mut buf);
  let mut writer = Writer {
    writer: &mut cursor,
  };
  writer.write_str("").unwrap();
  let pos = usize::try_from(cursor.position()).unwrap();

  let mut decoder = Decoder::new(&buf[..pos]);
  let value = decoder.decode().unwrap();
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

  let mut decoder = Decoder::new(&buf[..pos]);
  let value = decoder.decode().unwrap();
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
  let mut decoder = Decoder::new(&buf[..pos]);
  let value = decoder.decode().unwrap();

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
  let mut decoder = Decoder::new(&buf[..pos]);
  let value = decoder.decode().unwrap();

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
  let mut decoder = Decoder::new(&buf[..pos]);
  let value = decoder.decode().unwrap();

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
  let mut decoder = Decoder::new(&buf[..pos]);
  let value = decoder.decode().unwrap();

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
  let mut decoder = Decoder::new(&buf[..pos]);
  let value = decoder.decode().unwrap();

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
  let mut decoder = Decoder::new(&buf[..pos]);
  let result = decoder.decode();
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
  let mut decoder = Decoder::new(&buf[..pos]);
  let result = decoder.decode();
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

    let mut decoder = Decoder::new(&buf[..pos]);
    let decoded = decoder.decode().unwrap();

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

    let mut decoder = Decoder::new(&buf[..pos]);
    let decoded = decoder.decode().unwrap();

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
  let result = decode_value(&buf[..pos]);
  assert!(result.is_err());

  let err = result.err().unwrap();
  assert_eq!(
    err.error,
    DeserializationErrorWithOffset::Error(DeserializationError::PrematureEnd, pos)
  );

  // Check partial result contains valid elements
  match err.value {
    Value::Array(elements) => {
      assert_eq!(elements.len(), 3);
      assert_eq!(elements[0], Value::Null);
      assert_eq!(elements[1], Value::Bool(true));
      assert_eq!(elements[2], Value::Signed(42));
    },
    _ => panic!("Expected partial array, got {:?}", err.value),
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
  let result = decode_value(&buf[..pos]);
  assert!(result.is_err());

  let err = result.err().unwrap();
  assert_eq!(
    err.error,
    DeserializationErrorWithOffset::Error(DeserializationError::PrematureEnd, pos)
  );

  // Check partial result contains valid key-value pairs
  match err.value {
    Value::Object(obj) => {
      assert_eq!(obj.len(), 2);
      assert_eq!(
        obj.get("key1").unwrap(),
        &Value::String("value1".to_string())
      );
      assert_eq!(obj.get("key2").unwrap(), &Value::Signed(42));
    },
    _ => panic!("Expected partial object, got {:?}", err.value),
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
  let result = decode_value(&buf[..pos]);
  assert!(result.is_err());

  let err = result.err().unwrap();

  // Check partial result structure
  match err.value {
    Value::Object(obj) => {
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
    _ => panic!("Expected partial object, got {:?}", err.value),
  }
}

#[test]
fn test_partial_decode_truncated_string() {
  let buf = [0x84, b'a', b'b', b'c'];

  // Try to decode - should fail with error but no partial result
  let result = decode_value(&buf);
  assert!(result.is_err());

  let err = result.err().unwrap();
  // String types don't produce partial results, so we expect just an error
  assert_eq!(err.value, Value::None);
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
  let result = decode_value(&buf[..pos + 1]);
  assert!(result.is_err());

  let err = result.err().unwrap();

  // Should have a partial array with the valid elements
  match err.value {
    Value::Array(elements) => {
      assert_eq!(elements.len(), 2);
      assert_eq!(elements[0], Value::Null);
      assert_eq!(elements[1], Value::Bool(true));
    },
    _ => panic!("Expected partial array, got {:?}", err.value),
  }

  // And the error should indicate unexpected type code
  assert!(matches!(
    err.error,
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
  let result = decode_value(&buf[..pos]);
  assert!(result.is_err());

  let err = result.err().unwrap();

  // Should have a partial object with the valid key-value pair
  match err.value {
    Value::Object(obj) => {
      assert_eq!(obj.len(), 1);
      assert_eq!(
        obj.get("key1").unwrap(),
        &Value::String("value1".to_string())
      );
    },
    _ => panic!("Expected partial object, got {:?}", err.value),
  }

  // And the error should indicate non-string key
  assert!(matches!(
    err.error,
    DeserializationErrorWithOffset::Error(DeserializationError::NonStringKeyInMap, _)
  ));
}
