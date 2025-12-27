// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Macros for implementing protobuf serialization traits.

/// Implements `ProtoFieldSerialize` and `RepeatedFieldDeserialize` for repeated field collections.
///
/// This macro reduces boilerplate for collection types like `Vec<T>`, `BTreeSet<T>`, and
/// `TinySet<T>` by generating the standard repeated field serialization logic.
///
/// # How it works
///
/// The macro generates implementations that:
/// - Serialize each element with the same field number (protobuf repeated fields)
/// - Use `RepeatedFieldDeserialize` for efficient incremental deserialization
/// - Elements are added directly to the collection without intermediate allocations
///
/// # Variants
///
/// - `impl_proto_repeated!($collection:ty, $item:ident)` - No additional bounds (e.g., `Vec<T>`)
/// - `impl_proto_repeated!($collection:ty, $item:ident, $bounds:path)` - With bounds (e.g.,
///   `BTreeSet<T>` requires `Ord`)
#[macro_export]
macro_rules! impl_proto_repeated {
  // Version with additional trait bounds (e.g., Ord for BTreeSet)
  ($collection:ty, $item:ident, $bounds:path) => {
    impl<$item: $crate::serialization::ProtoType + $bounds> $crate::serialization::ProtoType
      for $collection
    {
      fn wire_type() -> protobuf::rt::WireType {
        $item::wire_type()
      }
    }

    impl<$item: $crate::serialization::ProtoFieldSerialize + $bounds>
      $crate::serialization::ProtoFieldSerialize for $collection
    {
      fn compute_size(&self, field_number: u32) -> u64 {
        self
          .iter()
          .map(|item| item.compute_size(field_number))
          .sum()
      }

      fn serialize(
        &self,
        field_number: u32,
        os: &mut protobuf::CodedOutputStream<'_>,
      ) -> anyhow::Result<()> {
        for item in self {
          item.serialize(field_number, os)?;
        }
        Ok(())
      }
    }

    impl<$item: $crate::serialization::ProtoFieldDeserialize + $bounds>
      $crate::serialization::RepeatedFieldDeserialize for $collection
    where
      $collection: Default + Extend<$item>,
    {
      type Element = $item;

      fn deserialize_element(
        is: &mut protobuf::CodedInputStream<'_>,
      ) -> anyhow::Result<Self::Element> {
        $item::deserialize(is)
      }

      fn add_element(&mut self, element: Self::Element) {
        self.extend(std::iter::once(element));
      }
    }
  };

  // Version without additional trait bounds (for Vec)
  ($collection:ty, $item:ident) => {
    impl<$item: $crate::serialization::ProtoType> $crate::serialization::ProtoType for $collection {
      fn wire_type() -> protobuf::rt::WireType {
        $item::wire_type()
      }
    }

    impl<$item: $crate::serialization::ProtoFieldSerialize>
      $crate::serialization::ProtoFieldSerialize for $collection
    {
      fn compute_size(&self, field_number: u32) -> u64 {
        self
          .iter()
          .map(|item| item.compute_size(field_number))
          .sum()
      }

      fn serialize(
        &self,
        field_number: u32,
        os: &mut protobuf::CodedOutputStream<'_>,
      ) -> anyhow::Result<()> {
        for item in self {
          item.serialize(field_number, os)?;
        }
        Ok(())
      }
    }

    impl<$item: $crate::serialization::ProtoFieldDeserialize>
      $crate::serialization::RepeatedFieldDeserialize for $collection
    where
      $collection: Default + Extend<$item>,
    {
      type Element = $item;

      fn deserialize_element(
        is: &mut protobuf::CodedInputStream<'_>,
      ) -> anyhow::Result<Self::Element> {
        $item::deserialize(is)
      }

      fn add_element(&mut self, element: Self::Element) {
        self.extend(std::iter::once(element));
      }
    }
  };
}
