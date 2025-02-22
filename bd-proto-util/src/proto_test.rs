// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::proto::ProtoHashWrapper;
use itertools::Itertools;
use protobuf::Message;
use protobuf::well_known_types::struct_::{Struct, Value};


#[test]
fn test_map_hashing() {
  let fields = vec![
    ("1", "8"),
    ("2", "9"),
    ("3", "10"),
    ("4", "11"),
    ("5", "12"),
    ("6", "13"),
    ("7", "14"),
  ]
  .into_iter()
  .map(|(key, value)| {
    (
      key.to_string(),
      Value {
        kind: Some(
          protobuf::well_known_types::struct_::value::Kind::StringValue(value.to_string()),
        ),
        ..Default::default()
      },
    )
  })
  .collect_vec();

  let s1 = Struct {
    fields: fields.clone().into_iter().collect(),
    ..Default::default()
  };
  let s2 = Struct {
    fields: fields.into_iter().collect(),
    ..Default::default()
  };

  // Two structs with identical maps end up with a different serialized value.
  assert_ne!(s2.write_to_bytes().unwrap(), s1.write_to_bytes().unwrap());

  // Using the map-aware hasher we are able to compute a consistent version nonce.

  let wrapper1 = ProtoHashWrapper::new(s1);
  let wrapper2 = ProtoHashWrapper::new(s2);

  assert_eq!(wrapper1.message_hash(), wrapper2.message_hash());
}
