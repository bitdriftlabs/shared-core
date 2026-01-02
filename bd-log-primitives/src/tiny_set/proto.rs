// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Protobuf serialization implementations for `TinySet` and `TinyMap`.
//!
//! `TinySet` and `TinyMap` are small, efficient collection types for when you have a small number
//! of items. They serialize as repeated fields in protobuf.

use super::{TinyMap, TinySet};
use bd_proto_util::serialization::{
  ProtoFieldDeserialize,
  ProtoFieldSerialize,
  ProtoType,
  RepeatedFieldDeserialize,
};

bd_proto_util::impl_proto_repeated!(TinySet<T>, T, PartialEq);

// TinyMap uses PartialEq for keys instead of Hash+Eq, so we implement it manually
impl<K, V> ProtoType for TinyMap<K, V> {
  fn wire_type() -> protobuf::rt::WireType {
    protobuf::rt::WireType::LengthDelimited
  }
}

impl<K, V> ProtoFieldSerialize for TinyMap<K, V>
where
  K: ProtoFieldSerialize + PartialEq,
  V: ProtoFieldSerialize,
{
  fn compute_size(&self, field_number: u32) -> u64 {
    bd_proto_util::serialization::compute_map_size(self, field_number)
  }

  fn serialize(
    &self,
    field_number: u32,
    os: &mut protobuf::CodedOutputStream<'_>,
  ) -> anyhow::Result<()> {
    bd_proto_util::serialization::serialize_map(self, field_number, os)
  }

  fn compute_size_explicit(&self, field_number: u32) -> u64 {
    // Maps always have explicit presence, so this is the same as compute_size
    self.compute_size(field_number)
  }

  fn serialize_explicit(
    &self,
    field_number: u32,
    os: &mut protobuf::CodedOutputStream<'_>,
  ) -> anyhow::Result<()> {
    // Maps always have explicit presence, so this is the same as serialize
    self.serialize(field_number, os)
  }
}

impl<K, V> RepeatedFieldDeserialize for TinyMap<K, V>
where
  K: ProtoFieldDeserialize + PartialEq + Default,
  V: ProtoFieldDeserialize + Default,
{
  type Element = (K, V);

  fn deserialize_element(is: &mut protobuf::CodedInputStream<'_>) -> anyhow::Result<Self::Element> {
    bd_proto_util::serialization::deserialize_map_entry(is)
  }

  fn add_element(&mut self, (key, value): Self::Element) {
    self.insert(key, value);
  }
}
