// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Macros for implementing protobuf serialization traits.

/// Implements `ProtoType`, `ProtoFieldSerialize`, and `ProtoFieldDeserialize` for a scalar type
/// by delegating to a [`super::wire::WireFormat`] implementation.
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
/// - `$t` - The Rust type to implement traits for (e.g., `u32`, `String`)
/// - `$wire` - The wire format type (e.g., `wire::U32`, `wire::StringOwned`)
///
/// The wire format type provides the wire type, canonical type, and serialization functions.
#[macro_export]
macro_rules! impl_proto_scalar {
  ($t:ty, $wire:ty) => {
    impl $crate::serialization::ProtoType for $t {
      fn wire_type() -> protobuf::rt::WireType {
        <$wire as $crate::serialization::WireFormat>::WIRE_TYPE
      }

      fn canonical_type() -> $crate::serialization::CanonicalType {
        <$wire as $crate::serialization::WireFormat>::CANONICAL
      }
    }

    impl $crate::serialization::ProtoFieldSerialize for $t {
      #[allow(clippy::float_cmp)]
      fn compute_size(&self, field_number: u32) -> u64 {
        // Skip serializing if this is the default value (proto3 behavior)
        if self == &<$t as Default>::default() {
          return 0;
        }
        <$wire as $crate::serialization::WireFormat>::compute_size(self, field_number)
      }

      #[allow(clippy::float_cmp)]
      fn serialize(
        &self,
        field_number: u32,
        os: &mut protobuf::CodedOutputStream<'_>,
      ) -> anyhow::Result<()> {
        // Skip serializing if this is the default value (proto3 behavior)
        if self == &<$t as Default>::default() {
          return Ok(());
        }
        <$wire as $crate::serialization::WireFormat>::write(self, field_number, os)
      }
    }

    impl $crate::serialization::ProtoFieldDeserialize for $t {
      fn deserialize(is: &mut protobuf::CodedInputStream<'_>) -> anyhow::Result<Self> {
        <$wire as $crate::serialization::WireFormat>::read(is)
      }
    }
  };
}

/// Implements `ProtoType`, `ProtoFieldSerialize`, and `ProtoFieldDeserialize` for a string-like
/// type that implements `AsRef<str>` and can be constructed from `String`.
///
/// This is useful for types like `Arc<str>` and `Cow<str>` that serialize as strings but need
/// conversion.
///
/// # Parameters
///
/// - `$t` - The Rust type to implement traits for (must implement `AsRef<str>`)
/// - `$from_owned` - Expression to convert `String` to `$t` (e.g., `|s: String| s.into()`)
#[macro_export]
macro_rules! impl_proto_string_like {
  ($t:ty, $from_owned:expr) => {
    impl $crate::serialization::ProtoType for $t {
      fn wire_type() -> protobuf::rt::WireType {
        protobuf::rt::WireType::LengthDelimited
      }

      fn canonical_type() -> $crate::serialization::CanonicalType {
        $crate::serialization::CanonicalType::String
      }
    }

    impl $crate::serialization::ProtoFieldSerialize for $t {
      fn compute_size(&self, field_number: u32) -> u64 {
        let value_ref: &str = self.as_ref();
        // Skip serializing if empty (proto3 behavior for strings/bytes)
        if value_ref.is_empty() {
          return 0;
        }
        protobuf::rt::string_size(field_number, value_ref)
      }

      fn serialize(
        &self,
        field_number: u32,
        os: &mut protobuf::CodedOutputStream<'_>,
      ) -> anyhow::Result<()> {
        let value_ref: &str = self.as_ref();
        // Skip serializing if empty (proto3 behavior for strings/bytes)
        if value_ref.is_empty() {
          return Ok(());
        }
        os.write_string(field_number, value_ref)?;
        Ok(())
      }
    }

    impl $crate::serialization::ProtoFieldDeserialize for $t {
      fn deserialize(is: &mut protobuf::CodedInputStream<'_>) -> anyhow::Result<Self> {
        let owned = is.read_string()?;
        let from_owned: fn(String) -> $t = $from_owned;
        Ok(from_owned(owned))
      }
    }
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
