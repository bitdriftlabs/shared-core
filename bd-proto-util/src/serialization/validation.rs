// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Runtime validation for protobuf descriptor compatibility.
//!
//! This module provides types and functions for validating at runtime that a Rust type's
//! serialization is compatible with a protobuf descriptor. This is used by the
//! `#[proto_serializable(validate_against = "...")]` attribute to generate compile-time tests.

#[cfg(test)]
#[path = "./validation_test.rs"]
mod tests;

use protobuf::reflect::{RuntimeFieldType, RuntimeType};

/// Canonical protobuf-compatible type representation.
///
/// This enum represents the "canonical" form of a Rust type as it maps to protobuf types.
/// For example, `Arc<str>`, `Rc<String>`, `Box<str>`, and `String` all canonicalize to
/// `CanonicalType::String`.
///
/// This type is used both at compile-time (in the proc macro for type canonicalization) and
/// at runtime (for validation against protobuf descriptors).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CanonicalType {
  /// String types: String, Arc<str>, Rc<String>, Box<str>, &str, etc.
  String,
  /// Bytes types: Vec<u8>, Bytes, etc.
  Bytes,
  /// Boolean type
  Bool,
  /// Signed 32-bit integer
  I32,
  /// Signed 64-bit integer
  I64,
  /// Unsigned 32-bit integer
  U32,
  /// Unsigned 64-bit integer
  U64,
  /// 32-bit floating point
  F32,
  /// 64-bit floating point
  F64,
  /// Nested message type
  Message,
  /// Repeated field (Vec<T> where T is not u8)
  Repeated(Box<Self>),
  /// Map field (`HashMap<K, V>`, etc.)
  Map(Box<Self>, Box<Self>),
  /// Optional field (Option<T>)
  Optional(Box<Self>),
  /// Protobuf enum type (uses i32 on the wire)
  Enum,
}

impl CanonicalType {
  /// Returns a human-readable name for this type (used in error messages).
  #[must_use]
  pub const fn type_name(&self) -> &'static str {
    match self {
      Self::String => "String",
      Self::Bytes => "Bytes (Vec<u8>)",
      Self::Bool => "Bool",
      Self::I32 => "I32",
      Self::I64 => "I64",
      Self::U32 => "U32",
      Self::U64 => "U64",
      Self::F32 => "F32",
      Self::F64 => "F64",
      Self::Enum => "Enum",
      Self::Message => "Message",
      Self::Repeated(_) => "Repeated",
      Self::Map(..) => "Map",
      Self::Optional(inner) => inner.type_name(),
    }
  }

  /// Unwraps any `Optional` wrapper to get the underlying type.
  #[must_use]
  pub fn unwrap_optional(&self) -> &Self {
    match self {
      Self::Optional(inner) => inner.unwrap_optional(),
      other => other,
    }
  }
}

/// Result of validating a Rust type against a protobuf field descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationResult {
  /// The types are compatible
  Ok,
  /// Type mismatch
  TypeMismatch {
    expected: &'static str,
    actual: String,
  },
}

impl ValidationResult {
  /// Returns true if the validation passed.
  #[must_use]
  pub const fn is_ok(&self) -> bool {
    matches!(self, Self::Ok)
  }

  /// Converts the result to a Result type for use with `?` operator.
  ///
  /// # Errors
  /// Returns an error if the validation failed.
  pub fn into_result(self, field_name: &str) -> Result<(), String> {
    match self {
      Self::Ok => Ok(()),
      Self::TypeMismatch { expected, actual } => Err(format!(
        "Field '{field_name}' type mismatch: expected {expected}, got {actual}"
      )),
    }
  }
}

/// Validates that a canonical Rust type is compatible with a protobuf runtime field type.
#[must_use]
pub fn validate_field_type(
  canonical: &CanonicalType,
  proto_type: &RuntimeFieldType,
) -> ValidationResult {
  // Unwrap optional wrapper - protobuf singular fields can be optional in proto3
  let canonical = canonical.unwrap_optional();

  match (canonical, proto_type) {
    // Singular scalar types
    (CanonicalType::String, RuntimeFieldType::Singular(RuntimeType::String))
    | (CanonicalType::Bytes, RuntimeFieldType::Singular(RuntimeType::VecU8))
    | (CanonicalType::Bool, RuntimeFieldType::Singular(RuntimeType::Bool))
    | (CanonicalType::I32, RuntimeFieldType::Singular(RuntimeType::I32))
    | (CanonicalType::I64, RuntimeFieldType::Singular(RuntimeType::I64))
    | (CanonicalType::U32, RuntimeFieldType::Singular(RuntimeType::U32))
    | (CanonicalType::U64, RuntimeFieldType::Singular(RuntimeType::U64))
    | (CanonicalType::F32, RuntimeFieldType::Singular(RuntimeType::F32))
    | (CanonicalType::F64, RuntimeFieldType::Singular(RuntimeType::F64))
    | (CanonicalType::Enum, RuntimeFieldType::Singular(RuntimeType::Enum(_)))
    | (CanonicalType::Message, RuntimeFieldType::Singular(RuntimeType::Message(_))) => {
      ValidationResult::Ok
    },

    // Repeated types
    (CanonicalType::Repeated(inner), RuntimeFieldType::Repeated(elem_type)) => {
      validate_repeated_element(inner, elem_type)
    },

    // Map types
    (CanonicalType::Map(..), RuntimeFieldType::Map(..)) => {
      // For now, we just check that both are maps. We could add key/value type checking later.
      ValidationResult::Ok
    },

    // Mismatch
    (canonical, proto_type) => ValidationResult::TypeMismatch {
      expected: canonical.type_name(),
      actual: format_proto_type(proto_type),
    },
  }
}

/// Validates that a repeated element type matches.
fn validate_repeated_element(inner: &CanonicalType, proto_type: &RuntimeType) -> ValidationResult {
  match (inner, proto_type) {
    (CanonicalType::String, RuntimeType::String)
    | (CanonicalType::Bytes, RuntimeType::VecU8)
    | (CanonicalType::Bool, RuntimeType::Bool)
    | (CanonicalType::I32, RuntimeType::I32)
    | (CanonicalType::I64, RuntimeType::I64)
    | (CanonicalType::U32, RuntimeType::U32)
    | (CanonicalType::U64, RuntimeType::U64)
    | (CanonicalType::F32, RuntimeType::F32)
    | (CanonicalType::F64, RuntimeType::F64)
    | (CanonicalType::Enum, RuntimeType::Enum(_))
    | (CanonicalType::Message, RuntimeType::Message(_)) => ValidationResult::Ok,
    (canonical, proto_type) => ValidationResult::TypeMismatch {
      expected: canonical.type_name(),
      actual: format_runtime_type(proto_type),
    },
  }
}

/// Formats a protobuf `RuntimeFieldType` for error messages.
fn format_proto_type(proto_type: &RuntimeFieldType) -> String {
  match proto_type {
    RuntimeFieldType::Singular(rt) => format!("Singular({})", format_runtime_type(rt)),
    RuntimeFieldType::Repeated(rt) => format!("Repeated({})", format_runtime_type(rt)),
    RuntimeFieldType::Map(k, v) => {
      format!(
        "Map({}, {})",
        format_runtime_type(k),
        format_runtime_type(v)
      )
    },
  }
}

/// Formats a protobuf `RuntimeType` for error messages.
fn format_runtime_type(rt: &RuntimeType) -> String {
  match rt {
    RuntimeType::I32 => "I32".to_string(),
    RuntimeType::I64 => "I64".to_string(),
    RuntimeType::U32 => "U32".to_string(),
    RuntimeType::U64 => "U64".to_string(),
    RuntimeType::F32 => "F32".to_string(),
    RuntimeType::F64 => "F64".to_string(),
    RuntimeType::Bool => "Bool".to_string(),
    RuntimeType::String => "String".to_string(),
    RuntimeType::VecU8 => "Bytes".to_string(),
    RuntimeType::Enum(e) => format!("Enum({})", e.name()),
    RuntimeType::Message(m) => format!("Message({})", m.name()),
  }
}
