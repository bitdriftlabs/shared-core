// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

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

use protobuf::rt::WireType;
use protobuf::{CodedInputStream, CodedOutputStream};

// Submodules
#[macro_use]
pub mod macros;
pub mod map;
pub mod runtime;
pub mod types;

// Re-export commonly used items
pub use map::{compute_map_size, deserialize_map_entry, serialize_map};
pub use runtime::{merge_repeated, read_nested};
pub use types::TimestampMicros;

/// Trait defining the wire type of a value.
pub trait ProtoType {
  /// The wire type of this field. Used for skipping unknown fields or verification.
  fn wire_type() -> WireType;
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
///
/// # Example
///
/// For a protobuf map field, instead of:
/// 1. Deserialize entry → create `HashMap` with 1 entry
/// 2. Deserialize entry → create `HashMap` with 1 entry
/// 3. Merge all `HashMaps` together
///
/// We can:
/// 1. Create empty `HashMap`
/// 2. Deserialize entry → insert directly
/// 3. Deserialize entry → insert directly
///
/// # Implementation
///
/// Types implementing this trait should:
/// - Define `Element` as the type of a single repeated item (e.g., `(K, V)` for maps)
/// - Implement `deserialize_element` to read one element from the stream
/// - Implement `add_element` to add an element to the container
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

  /// Deserializes this message as a top-level message (without expecting outer tag+length wrapper).
  /// Use this when loading messages from files or other contexts where there's no enclosing
  /// message.
  fn deserialize_message(is: &mut CodedInputStream<'_>) -> anyhow::Result<Self>;
}

#[cfg(test)]
mod tests {
  mod macro_test;
}
