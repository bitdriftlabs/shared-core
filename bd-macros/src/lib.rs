// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Shared runtime traits and procedural macros.
//!
//! `bd-macros` is an ordinary library so it can expose [`ApproximateSize`] alongside the derives
//! that implement it. The procedural code lives in a private implementation crate because Rust
//! restricts `proc-macro` crates to exporting macros only.
//!
//! # Protobuf Serialization
//!
//! The [`proto_serializable`] attribute macro generates efficient protobuf serialization and
//! deserialization code for Rust structs and enums.
//!
//! ## Overview
//!
//! The macro generates implementations of four key traits:
//! - `ProtoType` defines the wire type for the field.
//! - `ProtoFieldSerialize` serializes the value with a field number.
//! - `ProtoFieldDeserialize` deserializes from a protobuf stream.
//! - `ProtoMessage` provides top-level message serialization for structs.
//!
//! ## Supported Types
//!
//! - Named field structs become protobuf messages.
//! - Enums become protobuf oneofs with unit, tuple, and struct variants.
//!
//! ## Validation
//!
//! The macro can validate against protobuf descriptors:
//!
//! ```ignore
//! #[proto_serializable(validate_against = "bd_proto::proto::MyMessage")]
//! struct MyStruct { ... }
//! ```
//!
//! This generates tests that validate field IDs and types and check bidirectional round-trip
//! serialization. Use `validate_partial` to allow the Rust struct to have fewer fields than the
//! proto:
//!
//! ```ignore
//! #[proto_serializable(validate_against = "...", validate_partial)]
//! struct MyPartialStruct { ... }
//! ```

#[cfg(test)]
extern crate self as bd_macros;

mod approximate_size;

pub use approximate_size::ApproximateSize;
/// Derives [`ApproximateSize`] by recursively summing a struct or enum variant's field
/// allocations.
///
/// The derive adds an [`ApproximateSize`] bound for every field type. It does not offer field
/// overrides: a queue's admission estimate should use the same ownership rules everywhere.
/// `String`, `Vec`, `Box`, `Arc`, `Option`, arrays, and pairs/three-tuples have built-in
/// behavior; application types can derive the trait in turn.
pub use bd_macros_impl::ApproximateSize;
/// Generates protobuf serialization and deserialization implementations for structs and enums.
///
/// # Attributes
///
/// - `#[proto_serializable]` generates serialization and deserialization.
/// - `#[proto_serializable(serialize_only)]` generates serialization only.
/// - `#[proto_serializable(validate_against = "path::to::ProtoType")]` generates descriptor
///   compatibility tests.
/// - `#[proto_serializable(validate_against = "...", validate_partial)]` allows the Rust type
///   to have fewer fields than its protobuf type.
pub use bd_macros_impl::proto_serializable;

#[cfg(test)]
#[path = "./approximate_size_test.rs"]
mod approximate_size_test;
