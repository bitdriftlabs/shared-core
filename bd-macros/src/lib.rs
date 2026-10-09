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
/// The derive adds an [`ApproximateSize`] bound for every non-skipped field type. Use
/// `#[approximate_size(skip)]` for a field whose child allocation is not owned by the queued
/// value, such as an opaque completion handle. The containing value's inline storage is always
/// included. `String`, `Vec`, `Box`, `Arc`, `Option`, arrays, and pairs/three-tuples have
/// built-in behavior; application types can derive the trait in turn.
pub use bd_macros_impl::ApproximateSize;
/// Generates decoding into final structs and oneof enums using borrowed protobuf views.
///
/// Fields use `#[field(id = N)]`, with optional `required`, `default = "expr"`, and `skip`.
/// `decode_as = "Type"` selects the wire representation; `deserialize_with = "path"` applies
/// a fallible conversion after field reads. Scalar duplicates are resolved before conversion.
/// Supported wire types include strings, integer varints, bool, f64, optional values,
/// bytes, repeated strings/messages, maps, protobuf enums, and nested decoder types.
/// No encoding implementation is generated or required.
///
/// ```ignore
/// #[proto_deserialize]
/// struct Rule {
///   #[field(id = 1)]
///   name: String,
///   #[field(id = 2, required, decode_as = "String", deserialize_with = "compile_regex")]
///   pattern: Regex,
/// }
/// ```
///
/// `#[proto_deserialize(view)]` creates lazy borrowed getters instead of an owned struct.
/// `#[proto_deserialize(target = "ForeignType")]` constructs an existing type directly without
/// retaining the declaration as an intermediate model. Target implementations must obey Rust's
/// orphan rules. Enum variants use `#[field(id = N)]`; a struct's `Option<Enum>` uses
/// `#[field(oneof)]`. A whole-message hook uses `#[field(message, deserialize_with =
/// "path")]`.
/// Public oneof enums provide `is_variant_name(message)` predicates without payload decoding.
///
/// Scalar duplicates are last-wins; singular messages merge, and a oneof switch discards prior
/// alternative fragments. Unknown fields are not retained, and lazy views do not validate
/// unused subtrees. Packed scalars and zigzag/fixed-width integer encodings are not supported.
/// Conversion functions return `anyhow::Result`, and nested views share the reader's recursion
/// budget.
pub use bd_macros_impl::proto_deserialize;
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
