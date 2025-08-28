// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![deny(
  clippy::expect_used,
  clippy::panic,
  clippy::todo,
  clippy::unimplemented,
  clippy::unreachable,
  clippy::unwrap_used
)]

pub mod decoder;
pub mod encoder;
pub mod ffi;
pub mod writer;

mod deserialize_primitives;
mod serialize_primitives;
pub mod type_codes;

use deserialize_primitives::DeserializationError;
use std::collections::HashMap;

/// BONJSON has the same value types and structure as JSON.
/// Internally, the "Number" type can be a signed integer, unsigned integer, or float.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
  Null,
  Bool(bool),
  Float(f64),
  Signed(i64),
  Unsigned(u64),
  String(String),
  Array(Vec<Value>),
  Object(HashMap<String, Value>),
}

// Helper methods for Value
impl Value {
  /// Extract null value from this Value.
  ///
  /// # Errors
  /// Returns `DeserializationError::ExpectedNull` if this Value is not null.
  pub fn as_null(&self) -> deserialize_primitives::Result<()> {
    match self {
      Self::Null => Ok(()),
      _ => Err(DeserializationError::ExpectedNull),
    }
  }

  /// Extract boolean value from this Value.
  ///
  /// # Errors
  /// Returns `DeserializationError::ExpectedBoolean` if this Value is not a boolean.
  pub fn as_bool(&self) -> deserialize_primitives::Result<bool> {
    match self {
      Self::Bool(b) => Ok(*b),
      _ => Err(DeserializationError::ExpectedBoolean),
    }
  }

  /// Extract signed integer value from this Value.
  ///
  /// # Errors
  /// Returns `DeserializationError::ExpectedSignedInteger` if this Value is not a signed integer.
  pub fn as_integer(&self) -> deserialize_primitives::Result<i64> {
    match self {
      Self::Signed(n) => Ok(*n),
      _ => Err(DeserializationError::ExpectedSignedInteger),
    }
  }

  /// Extract unsigned integer value from this Value.
  ///
  /// # Errors
  /// Returns `DeserializationError::ExpectedUnsignedInteger` if this Value is not an unsigned
  /// integer.
  pub fn as_unsigned(&self) -> deserialize_primitives::Result<u64> {
    match self {
      Self::Unsigned(n) => Ok(*n),
      _ => Err(DeserializationError::ExpectedUnsignedInteger),
    }
  }

  /// Extract floating point value from this Value.
  ///
  /// # Errors
  /// Returns `DeserializationError::ExpectedFloat` if this Value is not a float.
  pub fn as_float(&self) -> deserialize_primitives::Result<f64> {
    match self {
      Self::Float(n) => Ok(*n),
      _ => Err(DeserializationError::ExpectedFloat),
    }
  }

  /// Extract string value from this Value.
  ///
  /// # Errors
  /// Returns `DeserializationError::ExpectedString` if this Value is not a string.
  pub fn as_string(&self) -> deserialize_primitives::Result<&str> {
    match self {
      Self::String(s) => Ok(s),
      _ => Err(DeserializationError::ExpectedString),
    }
  }

  /// Extract array value from this Value.
  ///
  /// # Errors
  /// Returns `DeserializationError::ExpectedArray` if this Value is not an array.
  pub fn as_array(&self) -> deserialize_primitives::Result<&Vec<Self>> {
    match self {
      Self::Array(arr) => Ok(arr),
      _ => Err(DeserializationError::ExpectedArray),
    }
  }

  /// Extract object value from this Value.
  ///
  /// # Errors
  /// Returns `DeserializationError::ExpectedMap` if this Value is not an object.
  pub fn as_object(&self) -> deserialize_primitives::Result<&HashMap<String, Self>> {
    match self {
      Self::Object(obj) => Ok(obj),
      _ => Err(DeserializationError::ExpectedMap),
    }
  }

  // JSON-like accessor methods
  #[must_use]
  pub fn get(&self, key: &str) -> Option<&Self> {
    match self {
      Self::Object(obj) => obj.get(key),
      _ => None,
    }
  }

  #[must_use]
  pub fn get_index(&self, index: usize) -> Option<&Self> {
    match self {
      Self::Array(arr) => arr.get(index),
      _ => None,
    }
  }

  #[must_use]
  pub fn is_null(&self) -> bool {
    matches!(self, Self::Null)
  }

  #[must_use]
  pub fn is_bool(&self) -> bool {
    matches!(self, Self::Bool(_))
  }

  #[must_use]
  pub fn is_integer(&self) -> bool {
    matches!(self, Self::Signed(_))
  }

  #[must_use]
  pub fn is_unsigned(&self) -> bool {
    matches!(self, Self::Unsigned(_))
  }

  #[must_use]
  pub fn is_float(&self) -> bool {
    matches!(self, Self::Float(_))
  }

  #[must_use]
  pub fn is_string(&self) -> bool {
    matches!(self, Self::String(_))
  }

  #[must_use]
  pub fn is_array(&self) -> bool {
    matches!(self, Self::Array(_))
  }

  #[must_use]
  pub fn is_object(&self) -> bool {
    matches!(self, Self::Object(_))
  }
}
