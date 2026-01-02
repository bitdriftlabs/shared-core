// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// We need to alias crate to bd_proto_util for the macro to work within the crate itself
use crate::serialization::runtime::Tag;
use crate::{self as bd_proto_util};
use anyhow::Result;
use bd_macros::proto_serializable;
use bd_proto_util::serialization::{ProtoFieldDeserialize, ProtoFieldSerialize, ProtoMessage};
use protobuf::{CodedInputStream, CodedOutputStream, Message};

#[test]
fn test_simple_struct() -> Result<()> {
  #[proto_serializable]
  #[derive(Debug, PartialEq, Default)]
  struct Foo {
    #[field(id = 1)]
    a: u32,
    #[field(id = 2)]
    b: String,
  }

  let foo = Foo {
    a: 42,
    b: "hello".to_string(),
  };

  let mut buf = Vec::new();
  let mut os = CodedOutputStream::vec(&mut buf);

  // We simulate being a field in a parent message, field number 10
  foo.serialize(10, &mut os)?;
  os.flush()?;
  drop(os);

  let mut is = CodedInputStream::from_bytes(&buf);

  // Read the tag (10)
  let raw_tag = is.read_raw_varint32()?;
  let tag = Tag::new(raw_tag)?;

  assert_eq!(tag.field_number, 10);
  assert_eq!(tag.wire_type, protobuf::rt::WireType::LengthDelimited);

  // Deserialize
  let foo2 = Foo::deserialize(&mut is)?;

  assert_eq!(foo, foo2);
  Ok(())
}

#[test]
fn test_struct_with_map() -> Result<()> {
  use ahash::AHashMap;

  #[proto_serializable]
  #[derive(Debug, PartialEq, Default)]
  struct Foo {
    #[field(id = 1)]
    name: String,
    #[field(id = 2)]
    #[field(repeated)]
    tags: AHashMap<String, String>,
  }

  let mut tags = AHashMap::new();
  tags.insert("key1".to_string(), "value1".to_string());
  tags.insert("key2".to_string(), "value2".to_string());
  tags.insert("key3".to_string(), "value3".to_string());

  let foo = Foo {
    name: "test".to_string(),
    tags,
  };

  let mut buf = Vec::new();
  let mut os = CodedOutputStream::vec(&mut buf);
  foo.serialize(10, &mut os)?;
  os.flush()?;
  drop(os);

  let mut is = CodedInputStream::from_bytes(&buf);
  let raw_tag = is.read_raw_varint32()?;
  let field_num = raw_tag >> 3;
  assert_eq!(field_num, 10);

  let foo2 = Foo::deserialize(&mut is)?;
  assert_eq!(foo.name, foo2.name);
  assert_eq!(foo.tags.len(), foo2.tags.len());
  for (k, v) in &foo.tags {
    assert_eq!(Some(v), foo2.tags.get(k));
  }

  Ok(())
}

#[test]
fn test_nested_struct() -> Result<()> {
  #[proto_serializable]
  #[derive(Debug, PartialEq, Default, Clone)]
  struct Bar {
    #[field(id = 1)]
    x: i64,
  }

  #[proto_serializable]
  #[derive(Debug, PartialEq, Default)]
  struct Foo {
    #[field(id = 1)]
    bar: Bar,
    #[field(id = 2)]
    val: Option<u32>,
  }

  let foo = Foo {
    bar: Bar { x: -123 },
    val: Some(999),
  };

  let mut buf = Vec::new();
  let mut os = CodedOutputStream::vec(&mut buf);
  foo.serialize(5, &mut os)?;
  os.flush()?;
  drop(os);

  let mut is = CodedInputStream::from_bytes(&buf);
  let raw_tag = is.read_raw_varint32()?;
  let field_num = raw_tag >> 3;
  assert_eq!(field_num, 5);

  let foo2 = Foo::deserialize(&mut is)?;
  assert_eq!(foo, foo2);

  Ok(())
}

#[test]
fn test_explicit_field_numbering() -> Result<()> {
  #[proto_serializable]
  #[derive(Debug, PartialEq, Default)]
  struct ExplicitFields {
    #[field(id = 1)]
    name: String,
    #[field(id = 2)]
    age: u32,
    #[field(id = 3)]
    email: String,
  }

  let obj = ExplicitFields {
    name: "Alice".to_string(),
    age: 30,
    email: "alice@example.com".to_string(),
  };

  let mut buf = Vec::new();
  let mut os = CodedOutputStream::vec(&mut buf);
  obj.serialize(1, &mut os)?;
  os.flush()?;
  drop(os);

  let mut is = CodedInputStream::from_bytes(&buf);
  let raw_tag = is.read_raw_varint32()?;
  let field_num = raw_tag >> 3;
  assert_eq!(field_num, 1);

  let obj2 = ExplicitFields::deserialize(&mut is)?;
  assert_eq!(obj, obj2);

  Ok(())
}

#[test]
fn test_explicit_with_skip() -> Result<()> {
  #[proto_serializable]
  #[derive(Debug, PartialEq)]
  struct WithSkip {
    #[field(id = 1)]
    name: String,
    #[field(skip)]
    #[field(default = r#""default_value".to_string()"#)]
    skipped: String,
    #[field(id = 2)]
    age: u32,
  }

  let obj = WithSkip {
    name: "Bob".to_string(),
    skipped: "this is ignored".to_string(),
    age: 25,
  };

  let mut buf = Vec::new();
  let mut os = CodedOutputStream::vec(&mut buf);
  obj.serialize(1, &mut os)?;
  os.flush()?;
  drop(os);

  let mut is = CodedInputStream::from_bytes(&buf);
  let raw_tag = is.read_raw_varint32()?;
  let field_num = raw_tag >> 3;
  assert_eq!(field_num, 1);

  let obj2 = WithSkip::deserialize(&mut is)?;

  // The deserialized object should have default value for skipped field
  assert_eq!(obj2.name, "Bob");
  assert_eq!(obj2.age, 25);
  assert_eq!(obj2.skipped, "default_value");

  Ok(())
}

#[test]
fn test_explicit_override_all_fields() -> Result<()> {
  #[proto_serializable]
  #[derive(Debug, PartialEq, Default)]
  struct ExplicitOverride {
    #[field(id = 3)]
    name: String,
    #[field(id = 1)]
    age: u32,
    #[field(id = 2)]
    email: String,
  }

  let obj = ExplicitOverride {
    name: "Charlie".to_string(),
    age: 40,
    email: "charlie@example.com".to_string(),
  };

  let mut buf = Vec::new();
  let mut os = CodedOutputStream::vec(&mut buf);
  obj.serialize(1, &mut os)?;
  os.flush()?;
  drop(os);

  let mut is = CodedInputStream::from_bytes(&buf);
  let raw_tag = is.read_raw_varint32()?;
  let field_num = raw_tag >> 3;
  assert_eq!(field_num, 1);

  let obj2 = ExplicitOverride::deserialize(&mut is)?;
  assert_eq!(obj, obj2);

  Ok(())
}

#[test]
fn test_enum_struct_variant_explicit() -> Result<()> {
  #[proto_serializable]
  #[derive(Debug, PartialEq, Default)]
  enum MyEnum {
    #[field(id = 1)]
    #[field(deserialize)]
    StructVariant {
      #[field(id = 1)]
      name: String,
      #[field(id = 2)]
      count: u32,
    },
    #[field(id = 2)]
    #[default]
    Unit,
  }

  let obj = MyEnum::StructVariant {
    name: "test".to_string(),
    count: 42,
  };

  let mut buf = Vec::new();
  let mut os = CodedOutputStream::vec(&mut buf);
  obj.serialize(1, &mut os)?;
  os.flush()?;
  drop(os);

  let mut is = CodedInputStream::from_bytes(&buf);
  let raw_tag = is.read_raw_varint32()?;
  let field_num = raw_tag >> 3;
  assert_eq!(field_num, 1);

  let obj2 = MyEnum::deserialize(&mut is)?;
  assert_eq!(obj, obj2);

  Ok(())
}

#[test]
fn test_roundtrip_proto_serializable() -> Result<()> {
  #[proto_serializable]
  #[derive(Debug, PartialEq, Default)]
  struct Data {
    #[field(id = 1)]
    text: String,
    #[field(id = 2)]
    number: i32,
    #[field(id = 3)]
    flag: bool,
  }

  let original = Data {
    text: "Hello, World!".to_string(),
    number: -42,
    flag: true,
  };

  // Serialize
  let mut buf = Vec::new();
  let mut os = CodedOutputStream::vec(&mut buf);
  original.serialize(1, &mut os)?;
  os.flush()?;
  drop(os);

  // Deserialize
  let mut is = CodedInputStream::from_bytes(&buf);
  let _tag = is.read_raw_varint32()?;
  let roundtripped = Data::deserialize(&mut is)?;

  assert_eq!(original, roundtripped);
  Ok(())
}

#[test]
fn test_roundtrip_with_rust_protobuf() -> Result<()> {
  use protobuf::well_known_types::wrappers::StringValue;

  // Create our custom struct with same wire format
  #[proto_serializable]
  #[derive(Debug, Default, PartialEq)]
  struct CustomStringValue {
    #[field(id = 1)]
    value: String,
  }

  // Create a StringValue using rust-protobuf
  let mut string_val = StringValue::new();
  string_val.value = "test_value".to_string();

  // Serialize using rust-protobuf
  let pb_bytes = string_val.write_to_bytes()?;

  // Deserialize rust-protobuf bytes into our custom struct (without field wrapper)
  let custom = CustomStringValue::deserialize_message_from_bytes(&pb_bytes)?;

  // Verify fields match
  assert_eq!(custom.value, "test_value");

  // Serialize our custom struct (without field wrapper)
  let buf = custom.serialize_message_to_bytes()?;

  // Deserialize back into rust-protobuf
  let string_val2 = StringValue::parse_from_bytes(&buf)?;

  // Verify fields match original
  assert_eq!(string_val.value, string_val2.value);

  Ok(())
}

#[test]
fn test_roundtrip_nested_with_protobuf() -> Result<()> {
  use protobuf::well_known_types::any::Any;
  use protobuf::well_known_types::wrappers::StringValue;

  // Our custom struct that should be compatible
  #[proto_serializable]
  #[derive(Debug, Default)]
  struct CustomAny {
    #[field(id = 1)]
    type_url: String,
    #[field(id = 2)]
    value: Vec<u8>,
  }


  // Create a StringValue using rust-protobuf
  let mut string_val = StringValue::new();
  string_val.value = "nested_test".to_string();
  let string_bytes = string_val.write_to_bytes()?;

  // Wrap it in Any
  let mut any = Any::new();
  any.type_url = "type.googleapis.com/google.protobuf.StringValue".to_string();
  any.value = string_bytes.clone();
  let any_bytes = any.write_to_bytes()?;

  // Deserialize Any into our custom struct (without field wrapper)
  let custom_any = CustomAny::deserialize_message_from_bytes(&any_bytes)?;

  assert_eq!(
    custom_any.type_url,
    "type.googleapis.com/google.protobuf.StringValue"
  );
  assert_eq!(custom_any.value, string_bytes);

  // Serialize back (without field wrapper)
  let buf = custom_any.serialize_message_to_bytes()?;

  // Deserialize back into rust-protobuf Any
  let any2 = Any::parse_from_bytes(&buf)?;

  assert_eq!(any.type_url, any2.type_url);
  assert_eq!(any.value, any2.value);

  Ok(())
}

#[test]
fn test_enum_roundtrip() -> Result<()> {
  #[proto_serializable]
  #[derive(Debug, PartialEq, Default)]
  enum Status {
    #[field(id = 1)]
    #[field(deserialize)]
    #[default]
    Pending,
    #[field(id = 2)]
    Active {
      #[field(id = 1)]
      user_id: String,
      #[field(id = 2)]
      start_time: i64,
    },
    #[field(id = 3)]
    Complete(i32),
  }

  let test_cases = vec![
    Status::Pending,
    Status::Active {
      user_id: "user123".to_string(),
      start_time: 1_234_567_890,
    },
    Status::Complete(42),
  ];

  for original in test_cases {
    let mut buf = Vec::new();
    let mut os = CodedOutputStream::vec(&mut buf);
    original.serialize(1, &mut os)?;
    os.flush()?;
    drop(os);

    let mut is = CodedInputStream::from_bytes(&buf);
    let _tag = is.read_raw_varint32()?;
    let roundtripped = Status::deserialize(&mut is)?;

    assert_eq!(original, roundtripped);
  }

  Ok(())
}

#[test]
fn test_field_numbering_starts_at_one() -> Result<()> {
  // This test verifies that field numbers start at 1, not 0
  #[proto_serializable]
  #[derive(Debug, PartialEq, Default)]
  struct TestExplicitFieldNumbers {
    #[field(id = 1)]
    first: String,
    #[field(id = 2)]
    second: u32,
  }

  let obj = TestExplicitFieldNumbers {
    first: "test".to_string(),
    second: 42,
  };

  // Serialize and deserialize to verify field numbering
  let bytes = obj.serialize_message_to_bytes()?;
  let deserialized = TestExplicitFieldNumbers::deserialize_message_from_bytes(&bytes)?;
  assert_eq!(obj, deserialized);

  Ok(())
}

#[test]
fn test_serialize_as() -> Result<()> {
  #[proto_serializable]
  #[derive(Debug, PartialEq)]
  struct Example {
    #[field(id = 1, serialize_as = "u64")]
    index: usize,
    #[field(id = 2)]
    name: String,
  }

  let original = Example {
    index: 42_usize,
    name: "test".to_string(),
  };

  // Serialize
  let bytes = original.serialize_message_to_bytes()?;

  // Deserialize
  let mut is = CodedInputStream::from_bytes(&bytes);
  let deserialized = Example::deserialize_message(&mut is)?;

  assert_eq!(original, deserialized);
  assert_eq!(deserialized.index, 42_usize);

  Ok(())
}

#[test]
#[allow(clippy::box_collection)]
fn test_wrapper_types() -> Result<()> {
  use std::sync::Arc;

  #[proto_serializable]
  #[derive(Debug, PartialEq, Default)]
  struct WithWrappers {
    #[field(id = 1)]
    boxed: Box<String>,
    #[field(id = 2)]
    arced: Arc<u32>,
  }

  let original = WithWrappers {
    boxed: Box::new("hello".to_string()),
    arced: Arc::new(42),
  };

  // Serialize
  let bytes = original.serialize_message_to_bytes()?;

  // Deserialize
  let deserialized = WithWrappers::deserialize_message_from_bytes(&bytes)?;

  assert_eq!(*original.boxed, *deserialized.boxed);
  assert_eq!(*original.arced, *deserialized.arced);

  Ok(())
}
