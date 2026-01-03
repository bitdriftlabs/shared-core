// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Protobuf serialization traits and implementations.
//!
//! This module provides a trait-based system for manual protobuf serialization that works
//! alongside the `#[proto_serializable]` proc macro.
//!
//! # Module organization
//!
//! - [`map`] - Helper functions for implementing map serialization
//! - [`runtime`] - Runtime helpers used by proc-macro-generated code
//! - [`types`] - Trait implementations for standard types
//! - [`macros`] - Declarative macros for reducing boilerplate
//! - [`wire`] - Wire format types that encapsulate serialization logic
//! - [`validation`] - Runtime validation against protobuf descriptors

use protobuf::rt::WireType;
use protobuf::{CodedInputStream, CodedOutputStream};

// Submodules
#[macro_use]
pub mod macros;
pub mod map;
pub mod runtime;
pub mod types;
pub mod validation;
pub mod wire;

// Re-export commonly used items
pub use map::{compute_map_size, deserialize_map_entry, serialize_map};
pub use runtime::read_nested;
pub use types::TimestampMicros;
pub use validation::{CanonicalType, ValidationResult, validate_field_type};
pub use wire::WireFormat;

/// Trait defining the wire format characteristics of a type.
///
/// Types implement this trait to specify how they map to protobuf wire format.
/// For scalar types, this delegates to a [`wire::WireFormat`] implementation.
pub trait ProtoType {
  /// The wire type of this field. Used for skipping unknown fields or verification.
  fn wire_type() -> WireType;

  /// Returns the canonical type representation for this type.
  ///
  /// This is used for validation against protobuf descriptors. The canonical type maps
  /// Rust types (like `Arc<str>`, `Box<String>`, etc.) to their protobuf equivalents.
  fn canonical_type() -> CanonicalType;
}

/// Trait for types that can be serialized manually to Protobuf streams.
pub trait ProtoFieldSerialize: ProtoType {
  /// Computes the size of this field when serialized.
  /// This should include the tag size (key + wire type).
  fn compute_size(&self, field_number: u32) -> u64;

  /// Serializes this field to the output stream.
  /// This should write the tag first, then the value.
  fn serialize(&self, field_number: u32, os: &mut CodedOutputStream<'_>) -> anyhow::Result<()>;
}

/// Trait for types that can be deserialized manually from Protobuf streams.
pub trait ProtoFieldDeserialize: ProtoType + Sized {
  /// Deserializes the value from the input stream.
  /// This is called *after* the tag for this field has already been read.
  /// The stream is positioned at the start of the value.
  fn deserialize(is: &mut CodedInputStream<'_>) -> anyhow::Result<Self>;
}

/// Optimized trait for repeated field types (maps, vectors, etc.) that allows direct
/// incremental deserialization instead of creating intermediate single-element containers.
///
/// This trait provides a more efficient deserialization path for collections by eliminating
/// creating intermediate single-element containers and merging them.
pub trait RepeatedFieldDeserialize: ProtoType + Sized + Default {
  /// The type of a single repeated element.
  /// For `HashMap<K, V>`, this is (K, V).
  /// For `Vec<T>`, this is T.
  type Element;

  /// Deserialize a single element from the input stream.
  /// This is called *after* the tag has been read.
  fn deserialize_element(is: &mut CodedInputStream<'_>) -> anyhow::Result<Self::Element>;

  /// Add an element to this container.
  fn add_element(&mut self, element: Self::Element);
}

/// Trait for message types that can be serialized/deserialized as top-level messages.
/// This is only implemented for struct types (messages), not primitives or collections.
/// Top-level serialization writes the message fields directly without an outer tag+length wrapper.
pub trait ProtoMessage: ProtoFieldSerialize + ProtoFieldDeserialize {
  /// Serializes this message as a top-level message (without outer tag+length wrapper).
  /// Use this when saving messages to files or other contexts where there's no enclosing message.
  fn serialize_message(&self, os: &mut CodedOutputStream<'_>) -> anyhow::Result<()>;

  // Serializes this message to a byte vector as a top-level message.
  fn serialize_message_to_bytes(&self) -> anyhow::Result<Vec<u8>> {
    let size = self.compute_size(0);
    let capacity = usize::try_from(size)
      .map_err(|_| anyhow::anyhow!("Message size {size} exceeds usize::MAX"))?;
    let mut buf = Vec::with_capacity(capacity);
    {
      let mut os = CodedOutputStream::vec(&mut buf);
      self.serialize_message(&mut os)?;
      os.flush()?;
    }
    Ok(buf)
  }

  /// Deserializes this message as a top-level message (without expecting outer tag+length wrapper).
  /// Use this when loading messages from files or other contexts where there's no enclosing
  /// message.
  fn deserialize_message(is: &mut CodedInputStream<'_>) -> anyhow::Result<Self>;

  /// Deserializes this message from a byte slice as a top-level message.
  fn deserialize_message_from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
    let mut is = CodedInputStream::from_bytes(bytes);
    let msg = Self::deserialize_message(&mut is)?;
    is.check_eof()?;
    Ok(msg)
  }
}

#[cfg(test)]
mod tests {
  mod macro_test;
}
