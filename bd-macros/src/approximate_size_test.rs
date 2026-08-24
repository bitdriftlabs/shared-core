// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::ApproximateSize;
use std::mem::size_of_val;
use std::sync::Arc;

#[derive(crate::ApproximateSize)]
struct BoxedPayload {
  message: String,
}

#[derive(crate::ApproximateSize)]
struct DerivedRecord {
  labels: Vec<String>,
  optional: Option<Box<BoxedPayload>>,
  shared: Arc<String>,
}

#[derive(crate::ApproximateSize)]
enum DerivedEvent {
  Empty,
  Message(String),
}

#[test]
fn counts_string_capacity() {
  let mut value = String::with_capacity(32);
  value.push_str("event");

  assert_eq!(
    value.approximate_size_bytes(),
    size_of_val(&value).saturating_add(value.capacity()),
  );
}

#[test]
fn counts_vector_backing_storage_and_children() {
  let mut child = String::with_capacity(24);
  child.push_str("field");
  let mut values = Vec::with_capacity(3);
  values.push(child);

  let expected = size_of_val(&values)
    .saturating_add(
      values
        .capacity()
        .saturating_mul(std::mem::size_of::<String>()),
    )
    .saturating_add(values[0].capacity());
  assert_eq!(values.approximate_size_bytes(), expected);
}

#[test]
fn counts_boxed_pointees_but_not_shared_arcs() {
  let boxed = Box::new([0_u8; 48]);
  let boxed_expected = size_of_val(&boxed).saturating_add(size_of_val(&*boxed));
  assert_eq!(boxed.approximate_size_bytes(), boxed_expected);

  let shared = Arc::new(String::with_capacity(48));
  assert_eq!(shared.approximate_size_bytes(), size_of_val(&shared));
}

#[test]
fn derives_owned_child_storage() {
  let mut label = String::with_capacity(16);
  label.push_str("tag");
  let mut message = String::with_capacity(32);
  message.push_str("payload");
  let record = DerivedRecord {
    labels: vec![label],
    optional: Some(Box::new(BoxedPayload { message })),
    shared: Arc::new(String::with_capacity(64)),
  };

  let expected = size_of_val(&record)
    .saturating_add(
      record
        .labels
        .capacity()
        .saturating_mul(std::mem::size_of::<String>()),
    )
    .saturating_add(record.labels[0].capacity())
    .saturating_add(size_of_val(record.optional.as_deref().unwrap()))
    .saturating_add(record.optional.as_deref().unwrap().message.capacity());
  assert_eq!(record.approximate_size_bytes(), expected);
}

#[test]
fn derives_enum_variant_storage() {
  let mut message = String::with_capacity(40);
  message.push_str("event");
  let event = DerivedEvent::Message(message);

  assert_eq!(
    event.approximate_size_bytes(),
    size_of_val(&event).saturating_add(match &event {
      DerivedEvent::Message(message) => message.capacity(),
      DerivedEvent::Empty => 0,
    }),
  );
  assert_eq!(
    DerivedEvent::Empty.approximate_size_bytes(),
    size_of_val(&DerivedEvent::Empty)
  );
}
