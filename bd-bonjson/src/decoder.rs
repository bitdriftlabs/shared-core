// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./decoder_test.rs"]
mod decoder_test;

use crate::deserialize_primitives;
use crate::deserialize_primitives::*;
use crate::type_codes::TypeCode;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeserializationErrorWithOffset {
  Error(DeserializationError, usize),
}
pub type Result<T> = std::result::Result<T, PartialDecodeResult>;

#[derive(Debug, Clone, PartialEq)]
pub struct PartialDecodeResult {
  pub value: Value,
  pub error: DeserializationErrorWithOffset,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
  None,
  Null,
  Bool(bool),
  Float(f64),
  Signed(i64),
  Unsigned(u64),
  String(String),
  Array(Vec<Value>),
  Object(HashMap<String, Value>),
}

pub struct Decoder<'a> {
  data: &'a [u8],
  position: usize,
}

impl<'a> Decoder<'a> {
  #[must_use]
  pub fn new(data: &'a [u8]) -> Self {
    Self { data, position: 0 }
  }

  pub fn decode(&mut self) -> Result<Value> {
    self.decode_value()
  }

  fn remaining_data(&self) -> &[u8] {
    &self.data[self.position..]
  }

  fn advance(&mut self, bytes: usize) {
    self.position += bytes;
  }

  fn map_err<T>(&self, result: deserialize_primitives::Result<T>, value: Value) -> Result<T> {
    result.map_err(|e| self.error_here(e, value))
  }

  fn error_here(&self, error: DeserializationError, value: Value) -> PartialDecodeResult {
    PartialDecodeResult {
      value,
      error: DeserializationErrorWithOffset::Error(error, self.position),
    }
  }

  fn propagate_error(&self, error: PartialDecodeResult, value: Value) -> PartialDecodeResult {
    PartialDecodeResult {
      value: value,
      error: error.error,
    }
  }

  #[allow(clippy::cast_possible_wrap)]
  fn decode_value(&mut self) -> Result<Value> {
    let remaining = self.remaining_data();

    let (size, type_code) = self.map_err(deserialize_type_code(remaining), Value::None)?;
    self.advance(size);

    match type_code {
      code if code == TypeCode::Null as u8 => Ok(Value::Null),
      code if code == TypeCode::True as u8 => Ok(Value::Bool(true)),
      code if code == TypeCode::False as u8 => Ok(Value::Bool(false)),
      code if code == TypeCode::ArrayStart as u8 => self.decode_array(),
      code if code == TypeCode::MapStart as u8 => self.decode_object(),
      code if code == TypeCode::LongString as u8 => self.decode_long_string(),
      code if code >= TypeCode::String as u8 && code <= TypeCode::StringEnd as u8 => {
        self.decode_short_string(code)
      },
      code if code == TypeCode::Float16 as u8 => self.decode_f16(),
      code if code == TypeCode::Float32 as u8 => self.decode_f32(),
      code if code == TypeCode::Float64 as u8 => self.decode_f64(),
      code if code <= TypeCode::P100 as u8 => Ok(Value::Signed(i64::from(code))),
      code if code >= TypeCode::N100 as u8 => Ok(Value::Signed(i64::from(code as i8))),
      code if code >= TypeCode::Unsigned as u8 && code <= TypeCode::UnsignedEnd as u8 => {
        self.decode_unsigned_integer(code)
      },
      code if code >= TypeCode::Signed as u8 && code <= TypeCode::SignedEnd as u8 => {
        self.decode_signed_integer(code)
      },
      _ => Err(self.error_here(DeserializationError::UnexpectedTypeCode, Value::None)),
    }
  }

  fn decode_f16(&mut self) -> Result<Value> {
    let remaining = self.remaining_data();
    let (size, value) = self.map_err(deserialize_f16_after_type_code(remaining), Value::None)?;
    self.advance(size);
    Ok(Value::Float(f64::from(value)))
  }

  fn decode_f32(&mut self) -> Result<Value> {
    let remaining = self.remaining_data();
    let (size, value) = self.map_err(deserialize_f32_after_type_code(remaining), Value::None)?;
    self.advance(size);
    Ok(Value::Float(f64::from(value)))
  }

  fn decode_f64(&mut self) -> Result<Value> {
    let remaining = self.remaining_data();
    let (size, value) = self.map_err(deserialize_f64_after_type_code(remaining), Value::None)?;
    self.advance(size);
    Ok(Value::Float(value))
  }

  fn decode_long_string(&mut self) -> Result<Value> {
    let remaining = &self.data[self.position..];
    // let remaining = self.remaining_data();
    let (size, str_slice) = self.map_err(
      deserialize_long_string_after_type_code(remaining),
      Value::None,
    )?;
    self.advance(size);
    Ok(Value::String(str_slice.to_string()))
  }

  fn decode_short_string(&mut self, type_code: u8) -> Result<Value> {
    let remaining = &self.data[self.position..];
    // let remaining = self.remaining_data();
    let (size, str_slice) = self.map_err(
      deserialize_short_string_after_type_code(remaining, type_code),
      Value::None,
    )?;
    self.advance(size);
    Ok(Value::String(str_slice.to_string()))
  }

  #[allow(clippy::cast_possible_wrap)]
  fn decode_unsigned_integer(&mut self, type_code: u8) -> Result<Value> {
    let remaining = self.remaining_data();
    let (size, value) = self.map_err(
      deserialize_unsigned_after_type_code(remaining, type_code),
      Value::None,
    )?;
    self.advance(size);
    if i64::try_from(value).is_ok() {
      Ok(Value::Signed(value as i64))
    } else {
      Ok(Value::Unsigned(value))
    }
  }

  fn decode_signed_integer(&mut self, type_code: u8) -> Result<Value> {
    let remaining = self.remaining_data();
    let (size, value) = self.map_err(
      deserialize_signed_after_type_code(remaining, type_code),
      Value::None,
    )?;
    self.advance(size);
    Ok(Value::Signed(value))
  }

  fn decode_array(&mut self) -> Result<Value> {
    let mut elements = Vec::new();

    loop {
      let remaining = self.remaining_data();
      let type_code = self.map_err(peek_type_code(remaining), Value::Array(elements.clone()))?;

      if type_code == TypeCode::ContainerEnd as u8 {
        self.advance(1);
        break;
      }

      let value = self.decode_value().map_err(|e| {
        if e.value != Value::None {
          elements.push(e.value.clone());
        }
        self.propagate_error(e, Value::Array(elements.clone()))
      })?;

      elements.push(value);
    }

    Ok(Value::Array(elements))
  }

  fn decode_object(&mut self) -> Result<Value> {
    let mut object = HashMap::new();

    loop {
      let remaining = self.remaining_data();
      let type_code = self.map_err(peek_type_code(remaining), Value::Object(object.clone()))?;

      if type_code == TypeCode::ContainerEnd as u8 {
        self.advance(1);
        break;
      }

      // Decode key (must be a string in JSON-like format)
      let key = match self.decode_value() {
        Ok(Value::String(key)) => key,
        Ok(_) => {
          return Err(self.error_here(
            DeserializationError::NonStringKeyInMap,
            Value::Object(object.clone()),
          ));
        },
        Err(e) => return Err(self.propagate_error(e, Value::Object(object.clone()))),
      };

      // Decode value
      let value = self.decode_value().map_err(|e| {
        if e.value != Value::None {
          object.insert(key.clone(), e.value.clone());
        }
        self.propagate_error(e, Value::Object(object.clone()))
      })?;

      object.insert(key, value);
    }

    Ok(Value::Object(object))
  }
}

// Convenience functions
pub fn decode_value(data: &[u8]) -> Result<Value> {
  let mut decoder = Decoder::new(data);
  decoder.decode()
}

// Helper methods for Value
impl Value {
  pub fn as_null(&self) -> deserialize_primitives::Result<()> {
    match self {
      Self::Null => Ok(()),
      _ => Err(DeserializationError::ExpectedNull),
    }
  }

  pub fn as_bool(&self) -> deserialize_primitives::Result<bool> {
    match self {
      Self::Bool(b) => Ok(*b),
      _ => Err(DeserializationError::ExpectedBoolean),
    }
  }

  pub fn as_integer(&self) -> deserialize_primitives::Result<i64> {
    match self {
      Self::Signed(n) => Ok(*n),
      _ => Err(DeserializationError::ExpectedSignedInteger),
    }
  }

  pub fn as_unsigned(&self) -> deserialize_primitives::Result<u64> {
    match self {
      Self::Unsigned(n) => Ok(*n),
      _ => Err(DeserializationError::ExpectedUnsignedInteger),
    }
  }

  pub fn as_float(&self) -> deserialize_primitives::Result<f64> {
    match self {
      Self::Float(n) => Ok(*n),
      _ => Err(DeserializationError::ExpectedFloat),
    }
  }

  pub fn as_string(&self) -> deserialize_primitives::Result<&str> {
    match self {
      Self::String(s) => Ok(s),
      _ => Err(DeserializationError::ExpectedString),
    }
  }

  pub fn as_array(&self) -> deserialize_primitives::Result<&Vec<Self>> {
    match self {
      Self::Array(arr) => Ok(arr),
      _ => Err(DeserializationError::ExpectedArray),
    }
  }

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
