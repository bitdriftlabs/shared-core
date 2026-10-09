#![allow(clippy::unwrap_used)]

use super::{Field, Fields, Message, Value};

#[test]
fn borrows_length_delimited_fields_and_preserves_occurrences() {
  let bytes = [8, 1, 18, 3, b'a', b'b', b'c', 8, 2];
  let fields = Fields::new(&bytes)
    .collect::<anyhow::Result<Vec<_>>>()
    .unwrap();
  assert_eq!(
    fields,
    vec![
      Field {
        number: 1,
        value: Value::Varint(1)
      },
      Field {
        number: 2,
        value: Value::Bytes(&bytes[4 .. 7])
      },
      Field {
        number: 1,
        value: Value::Varint(2)
      },
    ]
  );
  assert_eq!(
    fields[1].value.bytes().unwrap().as_ptr(),
    bytes[4 ..].as_ptr()
  );
  assert_eq!(fields[1].value.string().unwrap(), "abc");
}

#[test]
fn rejects_truncated_fields_and_invalid_tags() {
  for bytes in [&[18, 3, b'a'][..], &[8, 128], &[0], &[15], &[12]] {
    let mut fields = Fields::new(bytes);
    assert!(fields.next().unwrap().is_err(), "{bytes:?}");
    assert!(fields.next().is_none());
  }
}

#[test]
fn skips_unknown_groups_and_checks_value_types() {
  let fields = Fields::new(&[27, 8, 1, 28, 8, 2])
    .collect::<anyhow::Result<Vec<_>>>()
    .unwrap();
  assert_eq!(fields[0].value, Value::Group);
  assert_eq!(fields[1].value.varint().unwrap(), 2);
  assert!(fields[1].value.bytes().is_err());
  assert!(Value::Bytes(&[255]).string().is_err());
}

#[test]
fn merges_singular_messages_and_resets_switched_oneofs() {
  let bytes = [18, 2, 8, 1, 18, 2, 16, 2, 26, 0, 18, 2, 8, 3];
  let message = Message::new(&bytes).unwrap();
  let merged = message.required_message(2).unwrap();
  assert_eq!(merged.uint(1).unwrap(), 3);
  assert_eq!(merged.uint(2).unwrap(), 2);
  assert_eq!(message.messages(2).unwrap().len(), 3);
  let (number, selected) = message.oneof_message(&[2, 3]).unwrap().unwrap();
  assert_eq!(number, 2);
  assert_eq!(selected.uint(1).unwrap(), 3);
  assert_eq!(selected.uint(2).unwrap(), 0);
}

#[test]
fn clones_single_and_merged_messages_without_copying_payload() {
  let bytes = [18, 2, 8, 1, 18, 2, 16, 2];
  let message = Message::new(&bytes).unwrap();
  let clone = message.clone();
  assert_eq!(
    clone
      .fields()
      .next()
      .unwrap()
      .unwrap()
      .value
      .bytes()
      .unwrap()
      .as_ptr(),
    bytes[2 ..].as_ptr()
  );
  let messages = message.messages(2).unwrap();
  assert_eq!(messages[0].clone().uint(1).unwrap(), 1);
  assert_eq!(messages[1].clone().uint(2).unwrap(), 2);
  let merged = message.required_message(2).unwrap().clone();
  assert_eq!(merged.uint(1).unwrap(), 1);
  assert_eq!(merged.uint(2).unwrap(), 2);
  assert_eq!(merged.to_bytes(), vec![8, 1, 16, 2]);
  assert_eq!(merged.without_fields(&[1]).unwrap(), vec![16, 2]);
  assert_eq!(Message::default().fields().count(), 0);
}

#[test]
fn visits_repeated_messages_and_checks_nested_wire_format() {
  let message = Message::new(&[18, 2, 8, 1, 26, 0, 18, 1, 8]).unwrap();
  let mut values = Vec::new();
  let result = message.visit_messages(2, &mut |message| {
    values.push(message.uint(1)?);
    Ok(())
  });
  assert!(result.is_err());
  assert_eq!(values, vec![1]);
  assert!(message.messages(2).is_err());
}

#[test]
fn stops_visiting_at_callback_failure() {
  let message = Message::new(&[18, 2, 8, 1, 18, 2, 8, 2]).unwrap();
  let mut values = Vec::new();
  let result = message.visit_messages(2, &mut |message| {
    values.push(message.uint(1)?);
    anyhow::bail!("stop visiting");
  });
  assert!(result.is_err());
  assert_eq!(values, vec![1]);
}

#[test]
fn scalar_defaults_and_last_occurrence_match_protobuf() {
  let message = Message::new(&[8, 1, 8, 2, 18, 0]).unwrap();
  assert_eq!(message.uint(1).unwrap(), 2);
  assert_eq!(message.uint(3).unwrap(), 0);
  assert_eq!(message.optional_uint(3).unwrap(), None);
  assert_eq!(message.string(2).unwrap(), "");
  assert!(message.uint(2).is_err());
}
