// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Macros for implementing protobuf serialization traits.

/// Implements protobuf serialization traits for primitive types.
///
/// This macro generates complete `ProtoType`, `ProtoFieldSerialize`, and `ProtoFieldDeserialize`
/// implementations for primitive types like `String`, integers, `bool`, and `f64`.
///
/// # Proto3 Behavior
///
/// The generated implementations automatically skip serializing default values (proto3 behavior):
/// - `0` for numeric types
/// - `false` for bool
/// - `""` for strings
/// - Empty `Vec<u8>` for bytes
///
/// # Parameters
///
/// - `$t` - The type to implement traits for (e.g., `String`, `u32`)
/// - `$wire_type` - The protobuf wire type (e.g., `WireType::LengthDelimited`, `WireType::Varint`)
/// - `$canonical` - The canonical type for validation (e.g., `CanonicalType::String`)
/// - `$write_fn` - The `CodedOutputStream` method to write this type (e.g., `write_string`,
///   `write_uint32`)
/// - `$read_fn` - The `CodedInputStream` method to read this type (e.g., `read_string`,
///   `read_uint32`)
/// - `$v` - Variable name for capturing the value in closures
/// - `$deref` - Expression to convert `&self` to the format expected by `$write_fn`
/// - `$field` - Variable name for the field number in size computation
/// - `$size_expr` - Expression to compute the serialized size (usually calls
///   `protobuf::rt::*_size`)
///
/// # Wire Format
///
/// The wire format depends on the `$wire_type` parameter:
/// - `Varint` - Variable-length integer encoding (most integers)
/// - `LengthDelimited` - Length-prefixed encoding (strings, bytes, messages)
/// - `Fixed64` - Fixed 8-byte encoding (f64, fixed64)
/// - `Fixed32` - Fixed 4-byte encoding (f32, fixed32)
#[macro_export]
macro_rules! impl_proto_for_primitive {
  (
    $t:ty,
    $wire_type:expr,
    $canonical:expr,
    $write_fn:ident,
    $read_fn:ident, |
    $v:ident |
    $deref:expr,size: |
    $field:ident |
    $size_expr:expr
  ) => {
    impl ProtoType for $t {
      fn wire_type() -> WireType {
        $wire_type
      }

      fn canonical_type() -> $crate::serialization::CanonicalType {
        $canonical
      }
    }

    impl ProtoFieldSerialize for $t {
      #[allow(unused_variables, clippy::float_cmp)]
      fn compute_size(&self, $field: u32) -> u64 {
        // Skip serializing if this is the default value (proto3 behavior)
        if self == &<$t as Default>::default() {
          return 0;
        }
        let $v = self;
        $size_expr
      }

      #[allow(clippy::float_cmp)]
      fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> Result<()> {
        // Skip serializing if this is the default value (proto3 behavior)
        if self == &<$t as Default>::default() {
          return Ok(());
        }
        let $v = self;
        os.$write_fn(field_number, $deref)?;
        Ok(())
      }
    }

    impl ProtoFieldDeserialize for $t {
      fn deserialize(is: &mut CodedInputStream<'_>) -> Result<Self> {
        Ok(is.$read_fn()?.into())
      }
    }
  };
}

/// Convenience macro for implementing protobuf traits for varint primitive types.
///
/// This is a specialized wrapper around `impl_proto_for_primitive!` for types that use
/// varint wire encoding (most integer types). It automatically fills in the wire type
/// and standard size/deref expressions.
///
/// # Varint Encoding
///
/// Varint is a variable-length encoding where smaller numbers use fewer bytes:
/// - Values 0-127: 1 byte
/// - Values 128-16383: 2 bytes
/// - And so on...
///
/// This is efficient for small integers but can use up to 10 bytes for large values.
///
/// # Parameters
///
/// - `$t` - The integer type (e.g., `u32`, `i64`, `u64`, `i32`)
/// - `$canonical` - The canonical type for validation (e.g., `CanonicalType::U32`)
/// - `$size_fn` - The `protobuf::rt` function to compute size (e.g., `uint32_size`, `int64_size`)
/// - `$write_fn` - The `CodedOutputStream` write method (e.g., `write_uint32`, `write_int64`)
/// - `$read_fn` - The `CodedInputStream` read method (e.g., `read_uint32`, `read_int64`)
///
/// # Generated Code
///
/// This macro expands to a call to `impl_proto_for_primitive!` with:
/// - Wire type: `WireType::Varint`
/// - Deref expression: `|v| *v` (simple dereference)
/// - Size expression: `protobuf::rt::$size_fn(field_number, *v)`
#[macro_export]
macro_rules! impl_proto_for_varint_primitive {
  ($t:ty, $canonical:expr, $size_fn:ident, $write_fn:ident, $read_fn:ident) => {
    impl_proto_for_primitive!(
      $t,
      WireType::Varint,
      $canonical,
      $write_fn,
      $read_fn,
      |v| *v,
      size: |field_number| protobuf::rt::$size_fn(field_number, *v)
    );
  };
}

/// Implements protobuf serialization traits for transparent wrapper types.
///
/// This macro reduces boilerplate for types that wrap an inner value and should serialize
/// identically to that inner value (e.g., `Box<T>`, `Arc<T>`).
///
/// # How it works
///
/// The macro generates implementations that:
/// - Delegate wire type to the inner type
/// - Delegate canonical type to the inner type
/// - Serialize by dereferencing to the inner value
/// - Deserialize by wrapping the inner value
///
/// # Parameters
///
/// - `$wrapper_type` - The wrapper type path with generic parameter (e.g., `Box<T>`)
/// - `$inner_ident` - The identifier for the generic type parameter (e.g., `T`)
/// - `$construct` - Expression to construct the wrapper from inner value (e.g., `Box::new`)
/// ```
#[macro_export]
macro_rules! impl_proto_for_type_wrapper {
  ($wrapper_type:ident < $inner_ident:ident > , $inner_ident_repeat:ident, $construct:expr) => {
    impl<$inner_ident: $crate::serialization::ProtoType> $crate::serialization::ProtoType
      for $wrapper_type<$inner_ident>
    {
      fn wire_type() -> protobuf::rt::WireType {
        $inner_ident::wire_type()
      }

      fn canonical_type() -> $crate::serialization::CanonicalType {
        $inner_ident::canonical_type()
      }
    }

    impl<$inner_ident: $crate::serialization::ProtoFieldSerialize>
      $crate::serialization::ProtoFieldSerialize for $wrapper_type<$inner_ident>
    {
      fn compute_size(&self, field_number: u32) -> u64 {
        (**self).compute_size(field_number)
      }

      fn serialize(
        &self,
        field_number: u32,
        os: &mut protobuf::CodedOutputStream<'_>,
      ) -> anyhow::Result<()> {
        (**self).serialize(field_number, os)
      }
    }

    impl<$inner_ident: $crate::serialization::ProtoFieldDeserialize>
      $crate::serialization::ProtoFieldDeserialize for $wrapper_type<$inner_ident>
    {
      fn deserialize(is: &mut protobuf::CodedInputStream<'_>) -> anyhow::Result<Self> {
        Ok($construct($inner_ident::deserialize(is)?))
      }
    }
  };
}

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

      fn canonical_type() -> $crate::serialization::CanonicalType {
        $crate::serialization::CanonicalType::Repeated(Box::new($item::canonical_type()))
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

      fn canonical_type() -> $crate::serialization::CanonicalType {
        $crate::serialization::CanonicalType::Repeated(Box::new($item::canonical_type()))
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

/// Implements `ProtoFieldSerialize` and `RepeatedFieldDeserialize` for map-like collections.
///
/// This macro reduces boilerplate for map types like `HashMap<K, V>`, `AHashMap<K, V>`, and
/// `TinyMap<K, V>` by generating the standard map field serialization logic using the
/// `compute_map_size`, `serialize_map`, and `deserialize_map_entry` helpers.
///
/// # How it works
///
/// The macro generates implementations that:
/// - Serialize each key-value pair as a nested message (protobuf map encoding)
/// - Use `RepeatedFieldDeserialize` for efficient incremental deserialization
/// - Delegate to helper functions in `bd_proto_util::serialization::map`
/// ```
#[macro_export]
macro_rules! impl_proto_map {
  // Version with additional type parameters and where clause
  ($map_type:ty, $key:ident, $value:ident, where $($bounds:tt)*) => {
    impl<
        $key: $crate::serialization::ProtoType,
        $value: $crate::serialization::ProtoType,
        $($bounds)*
      > $crate::serialization::ProtoType for $map_type
    {
      fn wire_type() -> protobuf::rt::WireType {
        protobuf::rt::WireType::LengthDelimited
      }

      fn canonical_type() -> $crate::serialization::CanonicalType {
        $crate::serialization::CanonicalType::Map(
          Box::new($key::canonical_type()),
          Box::new($value::canonical_type()),
        )
      }
    }

    impl<$key, $value, $($bounds)*> $crate::serialization::ProtoFieldSerialize for $map_type
    where
      $key: $crate::serialization::ProtoFieldSerialize,
      $value: $crate::serialization::ProtoFieldSerialize,
    {
      fn compute_size(&self, field_number: u32) -> u64 {
        $crate::serialization::compute_map_size(self, field_number)
      }

      fn serialize(
        &self,
        field_number: u32,
        os: &mut protobuf::CodedOutputStream<'_>,
      ) -> anyhow::Result<()> {
        $crate::serialization::serialize_map(self, field_number, os)
      }
    }

    impl<$key, $value, $($bounds)*> $crate::serialization::RepeatedFieldDeserialize for $map_type
    where
      $key: $crate::serialization::ProtoFieldDeserialize + Eq + std::hash::Hash + Default,
      $value: $crate::serialization::ProtoFieldDeserialize + Default,
    {
      type Element = ($key, $value);

      fn deserialize_element(
        is: &mut protobuf::CodedInputStream<'_>,
      ) -> anyhow::Result<Self::Element> {
        $crate::serialization::deserialize_map_entry(is)
      }

      fn add_element(&mut self, (key, value): Self::Element) {
        self.insert(key, value);
      }
    }
  };

  // Version without additional type parameters (for types like AHashMap)
  ($map_type:ty, $key:ident, $value:ident) => {
    impl<
        $key: $crate::serialization::ProtoType,
        $value: $crate::serialization::ProtoType,
      > $crate::serialization::ProtoType for $map_type
    {
      fn wire_type() -> protobuf::rt::WireType {
        protobuf::rt::WireType::LengthDelimited
      }

      fn canonical_type() -> $crate::serialization::CanonicalType {
        $crate::serialization::CanonicalType::Map(
          Box::new($key::canonical_type()),
          Box::new($value::canonical_type()),
        )
      }
    }

    impl<$key, $value> $crate::serialization::ProtoFieldSerialize for $map_type
    where
      $key: $crate::serialization::ProtoFieldSerialize,
      $value: $crate::serialization::ProtoFieldSerialize,
    {
      fn compute_size(&self, field_number: u32) -> u64 {
        $crate::serialization::compute_map_size(self, field_number)
      }

      fn serialize(
        &self,
        field_number: u32,
        os: &mut protobuf::CodedOutputStream<'_>,
      ) -> anyhow::Result<()> {
        $crate::serialization::serialize_map(self, field_number, os)
      }
    }

    impl<$key, $value> $crate::serialization::RepeatedFieldDeserialize for $map_type
    where
      $key: $crate::serialization::ProtoFieldDeserialize + Eq + std::hash::Hash + Default,
      $value: $crate::serialization::ProtoFieldDeserialize + Default,
    {
      type Element = ($key, $value);

      fn deserialize_element(
        is: &mut protobuf::CodedInputStream<'_>,
      ) -> anyhow::Result<Self::Element> {
        $crate::serialization::deserialize_map_entry(is)
      }

      fn add_element(&mut self, (key, value): Self::Element) {
        self.insert(key, value);
      }
    }
  };
}
