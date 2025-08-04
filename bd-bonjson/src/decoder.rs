#[cfg(test)]
#[path = "./decoder_test.rs"]
mod decoder_test;

use crate::deserialize_primitives;
use crate::deserialize_primitives::*;
use crate::type_codes::TypeCode;
// use crate::{DeserializationError, DeserializationErrorWithOffset, Result};
use std::collections::HashMap;

#[derive(Debug)]
pub enum DeserializationErrorWithOffset {
  Error(DeserializationError, usize),
}
pub type Result<T> = std::result::Result<T, DeserializationErrorWithOffset>;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
  Null,
  Bool(bool),
  Float(f64),
  Integer(i64),
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

  fn decode_value(&mut self) -> Result<Value> {
    let remaining = self.remaining_data();

    let type_code = deserialize_type_code(remaining)
      .and_then(|(size, code)| {
        self.advance(size);
        Ok(code)
      })
      .map_err(|e| DeserializationErrorWithOffset::Error(e, self.position))?;

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
      code if code <= TypeCode::P100 as u8 => {
        self.advance(1);
        Ok(Value::Integer(code as i64))
      },
      code if code >= TypeCode::N100 as u8 => {
        self.advance(1);
        Ok(Value::Integer(code as i8 as i64))
      },
      code if code >= TypeCode::Unsigned as u8 && code <= TypeCode::UnsignedEnd as u8 => {
        self.decode_unsigned(code)
      },
      code if code >= TypeCode::Signed as u8 && code <= TypeCode::SignedEnd as u8 => {
        self.decode_integer(code)
      },
      _ => Err(DeserializationErrorWithOffset::Error(
        DeserializationError::UnexpectedTypeCode,
        self.position,
      )),
    }
  }

  fn decode_f16(&mut self) -> Result<Value> {
    let remaining = self.remaining_data();
    let (size, value) = deserialize_f16_after_type_code(remaining)
      .map_err(|e| DeserializationErrorWithOffset::Error(e, self.position))?;
    self.advance(size);
    Ok(Value::Float(value as f64))
  }

  fn decode_f32(&mut self) -> Result<Value> {
    let remaining = self.remaining_data();
    let (size, value) = deserialize_f32_after_type_code(remaining)
      .map_err(|e| DeserializationErrorWithOffset::Error(e, self.position))?;
    self.advance(size);
    Ok(Value::Float(value as f64))
  }

  fn decode_f64(&mut self) -> Result<Value> {
    let remaining = self.remaining_data();
    let (size, value) = deserialize_f64_after_type_code(remaining)
      .map_err(|e| DeserializationErrorWithOffset::Error(e, self.position))?;
    self.advance(size);
    Ok(Value::Float(value))
  }

  fn decode_long_string(&mut self) -> Result<Value> {
    let remaining = &self.data[self.position..];
    // let remaining = self.remaining_data();
    let (size, str_slice) = deserialize_long_string_after_type_code(remaining)
      .map_err(|e| DeserializationErrorWithOffset::Error(e, self.position))?;
    self.advance(size);
    Ok(Value::String(str_slice.to_string()))
  }

  fn decode_short_string(&mut self, type_code: u8) -> Result<Value> {
    let remaining = &self.data[self.position..];
    // let remaining = self.remaining_data();
    let (size, str_slice) = deserialize_short_string_after_type_code(remaining, type_code)
      .map_err(|e| DeserializationErrorWithOffset::Error(e, self.position))?;
    self.advance(size);
    Ok(Value::String(str_slice.to_string()))
  }

  fn decode_unsigned(&mut self, type_code: u8) -> Result<Value> {
    let remaining = self.remaining_data();
    let (size, value) = deserialize_unsigned_after_type_code(remaining, type_code)
      .map_err(|e| DeserializationErrorWithOffset::Error(e, self.position))?;
    self.advance(size);
    if value <= i64::MAX as u64 {
      Ok(Value::Integer(value as i64))
    } else {
      Ok(Value::Unsigned(value))
    }
  }

  fn decode_integer(&mut self, type_code: u8) -> Result<Value> {
    let remaining = self.remaining_data();
    let (size, value) = deserialize_signed_after_type_code(remaining, type_code)
      .map_err(|e| DeserializationErrorWithOffset::Error(e, self.position))?;
    self.advance(size);
    Ok(Value::Integer(value))
  }

  fn decode_array(&mut self) -> Result<Value> {
    let mut elements = Vec::new();

    // Keep reading values until we hit the container end
    loop {
      let remaining = self.remaining_data();
      if remaining.is_empty() {
        return Err(DeserializationErrorWithOffset::Error(
          DeserializationError::UnterminatedContainer,
          self.position,
        ));
      }

      let type_code = peek_type_code(remaining)
        .map_err(|e| DeserializationErrorWithOffset::Error(e, self.position))?;

      if type_code == TypeCode::ContainerEnd as u8 {
        self.advance(1);
        break;
      }

      elements.push(self.decode_value()?);
    }

    Ok(Value::Array(elements))
  }

  fn decode_object(&mut self) -> Result<Value> {
    let mut object = HashMap::new();

    // Keep reading key-value pairs until we hit the container end
    loop {
      let remaining = self.remaining_data();
      if remaining.is_empty() {
        return Err(DeserializationErrorWithOffset::Error(
          DeserializationError::UnterminatedContainer,
          self.position,
        ));
      }

      let type_code = peek_type_code(remaining)
        .map_err(|e| DeserializationErrorWithOffset::Error(e, self.position))?;

      if type_code == TypeCode::ContainerEnd as u8 {
        self.advance(1);
        break;
      }

      // Decode key (must be a string in JSON-like format)
      let key = match self.decode_value()? {
        Value::String(s) => s,
        _ => {
          return Err(DeserializationErrorWithOffset::Error(
            DeserializationError::NonStringKeyInMap,
            self.position,
          ));
        },
      };

      // Decode value
      let value = self.decode_value()?;

      object.insert(key, value);
    }

    Ok(Value::Object(object))
  }

  pub fn decode_multiple(&mut self) -> Result<Vec<Value>> {
    let mut values = Vec::new();

    while self.position < self.data.len() {
      values.push(self.decode_value()?);
    }

    Ok(values)
  }
}

// Convenience functions
pub fn decode_value(data: &[u8]) -> Result<Value> {
  let mut decoder = Decoder::new(data);
  decoder.decode()
}

pub fn decode_multiple_values(data: &[u8]) -> Result<Vec<Value>> {
  let mut decoder = Decoder::new(data);
  decoder.decode_multiple()
}

// Helper methods for Value
impl Value {
  pub fn as_null(&self) -> deserialize_primitives::Result<()> {
    match self {
      Value::Null => Ok(()),
      _ => Err(DeserializationError::ExpectedNull),
    }
  }

  pub fn as_bool(&self) -> deserialize_primitives::Result<bool> {
    match self {
      Value::Bool(b) => Ok(*b),
      _ => Err(DeserializationError::ExpectedBoolean),
    }
  }

  pub fn as_integer(&self) -> deserialize_primitives::Result<i64> {
    match self {
      Value::Integer(n) => Ok(*n),
      _ => Err(DeserializationError::ExpectedSignedInteger),
    }
  }

  pub fn as_unsigned(&self) -> deserialize_primitives::Result<u64> {
    match self {
      Value::Unsigned(n) => Ok(*n),
      _ => Err(DeserializationError::ExpectedUnsignedInteger),
    }
  }

  pub fn as_float(&self) -> deserialize_primitives::Result<f64> {
    match self {
      Value::Float(n) => Ok(*n),
      _ => Err(DeserializationError::ExpectedFloat),
    }
  }

  pub fn as_string(&self) -> deserialize_primitives::Result<&str> {
    match self {
      Value::String(s) => Ok(s),
      _ => Err(DeserializationError::ExpectedString),
    }
  }

  pub fn as_array(&self) -> deserialize_primitives::Result<&Vec<Value>> {
    match self {
      Value::Array(arr) => Ok(arr),
      _ => Err(DeserializationError::ExpectedArray),
    }
  }

  pub fn as_object(&self) -> deserialize_primitives::Result<&HashMap<String, Value>> {
    match self {
      Value::Object(obj) => Ok(obj),
      _ => Err(DeserializationError::ExpectedMap),
    }
  }

  // JSON-like accessor methods
  pub fn get(&self, key: &str) -> Option<&Value> {
    match self {
      Value::Object(obj) => obj.get(key),
      _ => None,
    }
  }

  pub fn get_index(&self, index: usize) -> Option<&Value> {
    match self {
      Value::Array(arr) => arr.get(index),
      _ => None,
    }
  }

  pub fn is_null(&self) -> bool {
    matches!(self, Value::Null)
  }

  pub fn is_bool(&self) -> bool {
    matches!(self, Value::Bool(_))
  }

  pub fn is_integer(&self) -> bool {
    matches!(self, Value::Integer(_))
  }

  pub fn is_unsigned(&self) -> bool {
    matches!(self, Value::Unsigned(_))
  }

  pub fn is_float(&self) -> bool {
    matches!(self, Value::Float(_))
  }

  pub fn is_string(&self) -> bool {
    matches!(self, Value::String(_))
  }

  pub fn is_array(&self) -> bool {
    matches!(self, Value::Array(_))
  }

  pub fn is_object(&self) -> bool {
    matches!(self, Value::Object(_))
  }
}
