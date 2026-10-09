// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// We need to alias crate to bd_proto_util for the macro to work within the crate itself
use crate::serialization::runtime::Tag;
use crate::{self as bd_proto_util};
use anyhow::Result;
use bd_macros::{proto_deserialize, proto_serializable};
use bd_proto::protos::client::runtime::Runtime;
use bd_proto::protos::logging::payload::Data as LoggingData;
use bd_proto::protos::state::state_payload::StateValue;
use bd_proto_util::serialization::inline::ProtoDeserialize;
use bd_proto_util::serialization::{
  ProtoFieldDeserialize,
  ProtoFieldSerialize,
  ProtoMessageDeserialize,
  ProtoMessageSerialize,
};
use protobuf::{CodedInputStream, CodedOutputStream, Message};
use std::fmt::Debug;

fn retained_parity<T>(bytes: &[u8]) -> Result<()>
where
  T: for<'a> ProtoDeserialize<'a> + Message + PartialEq + Debug,
{
  assert_eq!(T::from_proto_bytes(bytes)?, T::parse_from_bytes(bytes)?);
  Ok(())
}

#[test]
fn response_retained_recursive_values_and_maps_match_generated_decoder() -> Result<()> {
  for bytes in [
    vec![10, 1, b'x'],
    vec![18, 5, 10, 0, 18, 1, 255],
    vec![24, 255, 1],
    vec![48, 1],
    vec![58, 10, 10, 8, 10, 1, b'k', 18, 3, 10, 1, b'v'],
    vec![66, 9, 10, 3, 10, 1, b'v', 10, 2, 24, 9],
  ] {
    retained_parity::<LoggingData>(&bytes)?;
    let mut state = Vec::new();
    {
      let mut output = CodedOutputStream::vec(&mut state);
      output.write_bytes(5, &bytes)?;
      output.flush()?;
    }
    retained_parity::<StateValue>(&state)?;
  }
  let mut numeric = Vec::new();
  {
    let mut output = CodedOutputStream::vec(&mut numeric);
    output.write_double(4, 1.25)?;
    output.flush()?;
  }
  retained_parity::<LoggingData>(&numeric)?;
  numeric.clear();
  {
    let mut output = CodedOutputStream::vec(&mut numeric);
    output.write_int64(5, -8)?;
    output.flush()?;
  }
  retained_parity::<LoggingData>(&numeric)?;
  retained_parity::<Runtime>(&[
    10, 7, 10, 1, b'k', 18, 2, 8, 1, 10, 7, 10, 1, b'k', 18, 2, 16, 42, 10, 8, 10, 1, b's', 18, 3,
    26, 1, b'v',
  ])?;
  Ok(())
}

#[test]
fn decoding_only_converts_final_wire_value() -> Result<()> {
  use bd_proto_util::serialization::inline::ProtoDeserialize;

  struct RuntimeNumber(u32);

  #[proto_deserialize]
  struct Converted {
    #[field(
      id = 1,
      required,
      decode_as = "&str",
      deserialize_with = "parse_number"
    )]
    number: RuntimeNumber,
    #[field(id = 2)]
    label: Option<String>,
  }

  fn parse_number(value: &str) -> Result<RuntimeNumber> {
    Ok(RuntimeNumber(value.parse::<u32>()?))
  }

  let decoded = Converted::from_proto_bytes(&[10, 1, b'x', 10, 2, b'4', b'2'])?;
  assert_eq!(decoded.number.0, 42);
  assert_eq!(decoded.label, None);
  assert!(Converted::from_proto_bytes(&[]).is_err());
  assert!(Converted::from_proto_bytes(&[10, 1, b'x']).is_err());
  Ok(())
}

#[test]
fn decoding_only_preserves_defaults_presence_and_collections() -> Result<()> {
  use bd_proto_util::serialization::inline::ProtoDeserialize;

  #[proto_deserialize]
  struct Defaults {
    #[field(id = 1, default = "7")]
    number: u32,
    #[field(id = 2)]
    label: Option<String>,
    #[field(id = 3, repeated)]
    labels: Vec<String>,
    #[field(id = 4)]
    payload: Vec<u8>,
    #[field(id = 5)]
    enabled: bool,
    #[field(id = 6)]
    weight: f64,
    #[field(skip, default = "String::from(\"runtime\")")]
    runtime: String,
  }

  let absent = Defaults::from_proto_bytes(&[])?;
  assert_eq!(absent.number, 7);
  assert!(absent.label.is_none());
  assert_eq!(absent.labels, Vec::<String>::new());
  assert_eq!(absent.payload, Vec::<u8>::new());
  assert!(!absent.enabled);
  assert_eq!(absent.weight, 0.0);
  assert_eq!(absent.runtime, "runtime");

  let mut bytes = Vec::new();
  {
    let mut output = CodedOutputStream::vec(&mut bytes);
    output.write_uint32(1, 0)?;
    output.write_string(2, "")?;
    output.write_string(3, "first")?;
    output.write_string(3, "second")?;
    output.write_bytes(4, b"old")?;
    output.write_bytes(4, b"new")?;
    output.write_bool(5, true)?;
    output.write_double(6, 1.25)?;
    output.write_uint32(99, 123)?;
    output.flush()?;
  }
  let present = Defaults::from_proto_bytes(&bytes)?;
  assert_eq!(present.number, 0);
  assert_eq!(present.label.as_deref(), Some(""));
  assert_eq!(present.labels, ["first", "second"]);
  assert_eq!(present.payload, b"new");
  assert!(present.enabled);
  assert_eq!(present.weight, 1.25);
  assert!(Defaults::from_proto_bytes(&[32, 1, 34, 0]).is_err());
  assert!(Defaults::from_proto_bytes(&[8, 0x80]).is_err());
  Ok(())
}

#[test]
fn decoding_only_passes_merged_nested_views_to_conversion() -> Result<()> {
  use bd_proto_util::serialization::inline::{Message, ProtoDeserialize};

  #[proto_deserialize]
  struct Parent {
    #[field(
      id = 1,
      decode_as = "Option<Message<'_>>",
      deserialize_with = "decode_child"
    )]
    child: Option<(String, String)>,
  }

  fn decode_child(child: Option<Message<'_>>) -> Result<Option<(String, String)>> {
    child
      .map(|child| Ok((child.string(1)?.to_owned(), child.string(2)?.to_owned())))
      .transpose()
  }

  assert_eq!(Parent::from_proto_bytes(&[])?.child, None);
  assert_eq!(
    Parent::from_proto_bytes(&[10, 3, 10, 1, b'a', 10, 3, 18, 1, b'b'])?.child,
    Some(("a".to_owned(), "b".to_owned())),
  );
  Ok(())
}

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
fn decoding_only_dispatches_oneofs_and_nested_messages() -> Result<()> {
  use bd_proto_util::serialization::inline::ProtoDeserialize;

  #[proto_deserialize]
  #[derive(Default)]
  struct Payload {
    #[field(id = 1)]
    name: String,
  }

  #[proto_deserialize]
  enum Response {
    #[field(id = 1)]
    Payload(Payload),
    #[field(id = 2)]
    Empty,
  }

  #[proto_deserialize]
  struct Envelope {
    #[field(oneof)]
    response: Option<Response>,
  }

  assert!(Envelope::from_proto_bytes(&[])?.response.is_none());
  let Some(Response::Payload(payload)) =
    Envelope::from_proto_bytes(&[24, 1, 10, 3, 10, 1, b'a'])?.response
  else {
    panic!("payload expected");
  };
  assert_eq!(payload.name, "a");
  assert!(matches!(
    Envelope::from_proto_bytes(&[18, 0])?.response,
    Some(Response::Empty)
  ));
  Ok(())
}

#[test]
fn decoding_only_constructs_retained_types_and_borrowed_views() -> Result<()> {
  use bd_proto_util::serialization::inline::{Message, ProtoDeserialize};
  use protobuf::well_known_types::duration::Duration;

  #[proto_deserialize(view)]
  struct Borrowed<'a> {
    #[field(id = 1)]
    label: &'a str,
    #[field(id = 2)]
    child: Option<Message<'a>>,
  }

  assert_eq!(Duration::from_proto_bytes(&[8, 7, 16, 1])?.seconds, 7);
  let view = Borrowed::from_proto_bytes(&[10, 1, b'a', 18, 2, 8, 1])?;
  assert_eq!(view.label()?, "a");
  assert_eq!(view.child()?.unwrap().uint(1)?, 1);
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
  #[derive(Debug, PartialEq)]
  enum Status {
    #[field(id = 1)]
    #[field(deserialize)]
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

  impl Default for Status {
    fn default() -> Self {
      Self::Complete(0)
    }
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

#[test]
fn test_enum_tuple_variant_with_default_values() -> Result<()> {
  // This test verifies that enum tuple variants (oneofs) serialize correctly even when their inner
  // values are set to default values (0, "", false, etc.). According to proto3 spec: "If you set a
  // oneof field to the default value (such as setting an int32 oneof field to 0), the 'case' of
  // that oneof field will be set, and the value will be serialized on the wire."
  //
  // Prior to adding serialize_explicit, Normal(0) would serialize to empty bytes and deserialize
  // as Timeout (the default variant), causing data corruption.

  #[proto_serializable]
  #[derive(Debug, PartialEq)]
  enum TransitionType {
    #[field(id = 1, default)]
    Normal(u64),
    #[field(id = 2)]
    Timeout,
    #[field(id = 3)]
    WithString(String),
  }

  impl Default for TransitionType {
    fn default() -> Self {
      Self::Normal(0)
    }
  }

  // Test case 1: Default value for u64 (0) should round-trip correctly
  let test_cases = vec![
    TransitionType::Normal(0),  // Default value - critical test case!
    TransitionType::Normal(42), // Non-default value
    TransitionType::Timeout,    // Unit variant
    TransitionType::WithString(String::new()), // Default value for String
    TransitionType::WithString("hello".to_string()), // Non-default value
  ];

  for (i, original) in test_cases.into_iter().enumerate() {
    let mut buf = Vec::new();
    let mut os = CodedOutputStream::vec(&mut buf);
    original.serialize(1, &mut os)?;
    os.flush()?;
    drop(os);

    // Verify that even default values produce non-empty serialization
    let is_default_value = match &original {
      TransitionType::Normal(0) => true,
      TransitionType::WithString(s) if s.is_empty() => true,
      _ => false,
    };
    if is_default_value {
      assert!(
        !buf.is_empty(),
        "Test case {i}: Default value should serialize to non-empty bytes"
      );
    }

    let mut is = CodedInputStream::from_bytes(&buf);
    let _tag = is.read_raw_varint32()?;
    let roundtripped = TransitionType::deserialize(&mut is)?;

    assert_eq!(
      original, roundtripped,
      "Test case {i}: Round-trip failed for {original:?}"
    );
  }

  Ok(())
}

#[test]
fn test_enum_message_variant_with_empty_message() -> Result<()> {
  #[proto_serializable]
  #[derive(Debug, PartialEq, Default)]
  struct EmptyMessage {}

  #[proto_serializable]
  #[derive(Debug, PartialEq)]
  enum Wrapper {
    #[field(id = 1, default)]
    Empty(EmptyMessage),
    #[field(id = 2)]
    Other(u32),
  }

  impl Default for Wrapper {
    fn default() -> Self {
      Self::Empty(EmptyMessage::default())
    }
  }

  let original = Wrapper::Empty(EmptyMessage::default());
  let mut buf = Vec::new();
  let mut os = CodedOutputStream::vec(&mut buf);
  original.serialize(1, &mut os)?;
  os.flush()?;
  drop(os);

  assert_ne!(buf, [] as [u8; 0]);

  let mut is = CodedInputStream::from_bytes(&buf);
  let _tag = is.read_raw_varint32()?;
  let roundtripped = Wrapper::deserialize(&mut is)?;

  assert_eq!(original, roundtripped);

  Ok(())
}

#[test]
#[allow(clippy::enum_variant_names)] // Test enum intentionally uses uniform naming
fn test_enum_multiple_tuple_variants_with_defaults() -> Result<()> {
  // This test verifies that multiple tuple variants with different types all handle default values
  // correctly.

  #[proto_serializable]
  #[derive(Debug, PartialEq)]
  enum Value {
    #[field(id = 1, default)]
    IntValue(i32),
    #[field(id = 2)]
    UintValue(u32),
    #[field(id = 3)]
    BoolValue(bool),
    #[field(id = 4)]
    StringValue(String),
  }

  impl Default for Value {
    fn default() -> Self {
      Self::IntValue(0)
    }
  }

  let test_cases = vec![
    Value::IntValue(0),                     // Default i32
    Value::IntValue(-42),                   // Non-default i32
    Value::UintValue(0),                    // Default u32
    Value::UintValue(100),                  // Non-default u32
    Value::BoolValue(false),                // Default bool
    Value::BoolValue(true),                 // Non-default bool
    Value::StringValue(String::new()),      // Default String
    Value::StringValue("test".to_string()), // Non-default String
  ];

  for (i, original) in test_cases.into_iter().enumerate() {
    let mut buf = Vec::new();
    let mut os = CodedOutputStream::vec(&mut buf);
    original.serialize(1, &mut os)?;
    os.flush()?;
    drop(os);

    let mut is = CodedInputStream::from_bytes(&buf);
    let _tag = is.read_raw_varint32()?;
    let roundtripped = Value::deserialize(&mut is)?;

    assert_eq!(
      original, roundtripped,
      "Test case {i}: Round-trip failed for {original:?}"
    );
  }

  Ok(())
}

#[test]
fn test_vec_u32_roundtrip() -> Result<()> {
  #[proto_serializable]
  #[derive(Debug, PartialEq)]
  struct VecHolder {
    #[field(id = 1)]
    values: Vec<u32>,
  }

  let original = VecHolder {
    values: vec![0, 1, 2, 42, 1000],
  };

  let mut buf = Vec::new();
  let mut os = CodedOutputStream::vec(&mut buf);
  original.serialize(1, &mut os)?;
  os.flush()?;
  drop(os);

  let mut is = CodedInputStream::from_bytes(&buf);
  let _tag = is.read_raw_varint32()?;
  let roundtripped = VecHolder::deserialize(&mut is)?;

  assert_eq!(original, roundtripped);

  Ok(())
}

#[test]
fn test_map_u32_roundtrip() -> Result<()> {
  #[proto_serializable]
  #[derive(Debug, PartialEq)]
  struct MapHolder {
    #[field(id = 1)]
    values: std::collections::HashMap<String, u32>,
  }

  let mut values = std::collections::HashMap::new();
  values.insert("alpha".to_string(), 1);
  values.insert("beta".to_string(), 42);
  values.insert("gamma".to_string(), 1000);

  let original = MapHolder { values };

  let mut buf = Vec::new();
  let mut os = CodedOutputStream::vec(&mut buf);
  original.serialize(1, &mut os)?;
  os.flush()?;
  drop(os);

  let mut is = CodedInputStream::from_bytes(&buf);
  let _tag = is.read_raw_varint32()?;
  let roundtripped = MapHolder::deserialize(&mut is)?;

  assert_eq!(original, roundtripped);

  Ok(())
}

/// Test that a struct with `validate_against` generates passing validation tests.
///
/// This struct is designed to match the protobuf definition of
/// `FixedSessionStrategyState` from bd-proto.
#[proto_serializable(
  validate_against = "bd_proto::protos::client::key_value::FixedSessionStrategyState"
)]
#[derive(Debug, PartialEq, Default)]
struct ValidatedFixedSessionState {
  #[field(id = 1)]
  session_id: String,
}

/// Test partial validation - Rust struct has fewer fields than proto.
/// Uses `ActivitySessionStrategyState` which has `session_id` (1) and `last_activity_timestamp`
/// (2). Our Rust struct only has `session_id`.
#[proto_serializable(
  validate_against = "bd_proto::protos::client::key_value::ActivitySessionStrategyState",
  validate_partial
)]
#[derive(Debug, PartialEq, Default)]
struct PartialActivitySessionState {
  #[field(id = 1)]
  session_id: String,
  // Note: we intentionally omit last_activity_timestamp (field 2)
}

/// Test with type aliases (Arc<str> should be treated as String).
#[proto_serializable(
  validate_against = "bd_proto::protos::client::key_value::FixedSessionStrategyState"
)]
#[derive(Debug, Default)]
struct ValidatedWithArcStr {
  #[field(id = 1)]
  session_id: std::sync::Arc<str>,
}

/// Validate against google.protobuf.DoubleValue
#[proto_serializable(validate_against = "protobuf::well_known_types::wrappers::DoubleValue")]
#[derive(Debug, PartialEq, Default)]
struct WktDoubleValue {
  #[field(id = 1)]
  value: f64,
}

/// Validate against google.protobuf.FloatValue
#[proto_serializable(validate_against = "protobuf::well_known_types::wrappers::FloatValue")]
#[derive(Debug, PartialEq, Default)]
struct WktFloatValue {
  #[field(id = 1)]
  value: f32,
}

/// Validate against google.protobuf.Int64Value
#[proto_serializable(validate_against = "protobuf::well_known_types::wrappers::Int64Value")]
#[derive(Debug, PartialEq, Default)]
struct WktInt64Value {
  #[field(id = 1)]
  value: i64,
}

/// Validate against google.protobuf.UInt64Value
#[proto_serializable(validate_against = "protobuf::well_known_types::wrappers::UInt64Value")]
#[derive(Debug, PartialEq, Default)]
struct WktUInt64Value {
  #[field(id = 1)]
  value: u64,
}

/// Validate against google.protobuf.Int32Value
#[proto_serializable(validate_against = "protobuf::well_known_types::wrappers::Int32Value")]
#[derive(Debug, PartialEq, Default)]
struct WktInt32Value {
  #[field(id = 1)]
  value: i32,
}

/// Validate against google.protobuf.UInt32Value
#[proto_serializable(validate_against = "protobuf::well_known_types::wrappers::UInt32Value")]
#[derive(Debug, PartialEq, Default)]
struct WktUInt32Value {
  #[field(id = 1)]
  value: u32,
}

/// Validate against google.protobuf.BoolValue
#[proto_serializable(validate_against = "protobuf::well_known_types::wrappers::BoolValue")]
#[derive(Debug, PartialEq, Default)]
struct WktBoolValue {
  #[field(id = 1)]
  value: bool,
}

/// Validate against google.protobuf.StringValue
#[proto_serializable(validate_against = "protobuf::well_known_types::wrappers::StringValue")]
#[derive(Debug, PartialEq, Default)]
struct WktStringValue {
  #[field(id = 1)]
  value: String,
}

/// Validate against google.protobuf.BytesValue
#[proto_serializable(validate_against = "protobuf::well_known_types::wrappers::BytesValue")]
#[derive(Debug, PartialEq, Default)]
struct WktBytesValue {
  #[field(id = 1)]
  value: Vec<u8>,
}

/// Validate against google.protobuf.Duration (multi-field message)
#[proto_serializable(validate_against = "protobuf::well_known_types::duration::Duration")]
#[derive(Debug, PartialEq, Default)]
struct WktDuration {
  #[field(id = 1)]
  seconds: i64,
  #[field(id = 2)]
  nanos: i32,
}

/// Validate against google.protobuf.Timestamp (multi-field message)
#[proto_serializable(validate_against = "protobuf::well_known_types::timestamp::Timestamp")]
#[derive(Debug, PartialEq, Default)]
struct WktTimestamp {
  #[field(id = 1)]
  seconds: i64,
  #[field(id = 2)]
  nanos: i32,
}
