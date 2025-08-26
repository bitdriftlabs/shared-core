// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./decoder_test.rs"]
mod decoder_test;

use crate::deserialize_primitives::{
  DeserializationError,
  deserialize_f16_after_type_code,
  deserialize_f32_after_type_code,
  deserialize_f64_after_type_code,
  deserialize_long_string_after_type_code,
  deserialize_short_string_after_type_code,
  deserialize_signed_after_type_code,
  deserialize_type_code,
  deserialize_unsigned_after_type_code,
  peek_type_code,
};
use crate::type_codes::TypeCode;
use crate::{Value, deserialize_primitives};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeserializationErrorWithOffset {
  // An error that occurred during deserialization, with the byte offset in the data where it
  // occurred.
  Error(DeserializationError, usize),
}

#[derive(Debug, Clone, PartialEq)]
pub enum DecodeError {
  /// Fatal error - no partial value could be decoded
  Fatal(DeserializationErrorWithOffset),
  /// Partial decode - some value was decoded before the error occurred
  Partial {
    partial_value: Value,
    error: DeserializationErrorWithOffset,
  },
}

impl DecodeError {
  /// Check if this is a fatal error (no partial data available)
  #[must_use]
  pub fn is_fatal(&self) -> bool {
    matches!(self, Self::Fatal(_))
  }

  /// Get the error information
  #[must_use]
  pub fn error(&self) -> &DeserializationErrorWithOffset {
    match self {
      Self::Fatal(e) => e,
      Self::Partial { error, .. } => error,
    }
  }

  /// Get the partial value if available
  #[must_use]
  pub fn partial_value(&self) -> Option<&Value> {
    match self {
      Self::Fatal(_) => None,
      Self::Partial { partial_value, .. } => Some(partial_value),
    }
  }
}

pub type Result<T> = std::result::Result<T, DecodeError>;

/// Decoder decodes a buffer containing a BONJSON-encoded data into a `Value`.
pub struct Decoder<'a> {
  data: &'a [u8],
  position: usize,
}

fn propagate_partial_decode(error: &DecodeError, value: Value) -> DecodeError {
  match error {
    DecodeError::Fatal(e) => DecodeError::Partial {
      partial_value: value,
      error: *e,
    },
    DecodeError::Partial { error, .. } => DecodeError::Partial {
      partial_value: value,
      error: *error,
    },
  }
}

impl<'a> Decoder<'a> {
  #[must_use]
  pub fn new(data: &'a [u8]) -> Self {
    Self { data, position: 0 }
  }

  /// Decode the entire buffer and return the resulting value.
  /// On error, it returns the value decoded so far and the error.
  pub fn decode(&mut self) -> Result<Value> {
    self.decode_value()
  }

  fn remaining_data(&self) -> &[u8] {
    &self.data[self.position ..]
  }

  fn advance(&mut self, bytes: usize) {
    self.position += bytes;
  }

  fn map_err<T>(&self, result: deserialize_primitives::Result<T>) -> Result<T> {
    result.map_err(|e| self.fatal_error_here(e))
  }

  fn fatal_error_here(&self, error: DeserializationError) -> DecodeError {
    DecodeError::Fatal(DeserializationErrorWithOffset::Error(error, self.position))
  }

  fn partial_error_here(&self, error: DeserializationError, value: Value) -> DecodeError {
    DecodeError::Partial {
      partial_value: value,
      error: DeserializationErrorWithOffset::Error(error, self.position),
    }
  }

  #[allow(clippy::cast_possible_wrap)]
  fn decode_value(&mut self) -> Result<Value> {
    let remaining = self.remaining_data();

    let (size, type_code) = self.map_err(deserialize_type_code(remaining))?;
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
      code if code == TypeCode::LongNumber as u8 => {
        Err(self.fatal_error_here(DeserializationError::LongNumberNotSupported))
      },
      _ => Err(self.fatal_error_here(DeserializationError::UnexpectedTypeCode)),
    }
  }

  fn decode_f16(&mut self) -> Result<Value> {
    let remaining = self.remaining_data();
    let (size, value) = self.map_err(deserialize_f16_after_type_code(remaining))?;
    self.advance(size);
    Ok(Value::Float(f64::from(value)))
  }

  fn decode_f32(&mut self) -> Result<Value> {
    let remaining = self.remaining_data();
    let (size, value) = self.map_err(deserialize_f32_after_type_code(remaining))?;
    self.advance(size);
    Ok(Value::Float(f64::from(value)))
  }

  fn decode_f64(&mut self) -> Result<Value> {
    let remaining = self.remaining_data();
    let (size, value) = self.map_err(deserialize_f64_after_type_code(remaining))?;
    self.advance(size);
    Ok(Value::Float(value))
  }

  fn decode_long_string(&mut self) -> Result<Value> {
    let remaining = &self.data[self.position ..];
    // let remaining = self.remaining_data();
    let (size, str_slice) = self.map_err(deserialize_long_string_after_type_code(remaining))?;
    self.advance(size);
    Ok(Value::String(str_slice.to_string()))
  }

  fn decode_short_string(&mut self, type_code: u8) -> Result<Value> {
    let remaining = &self.data[self.position ..];
    // let remaining = self.remaining_data();
    let (size, str_slice) = self.map_err(deserialize_short_string_after_type_code(
      remaining, type_code,
    ))?;
    self.advance(size);
    Ok(Value::String(str_slice.to_string()))
  }

  #[allow(clippy::cast_possible_wrap)]
  fn decode_unsigned_integer(&mut self, type_code: u8) -> Result<Value> {
    let remaining = self.remaining_data();
    let (size, value) = self.map_err(deserialize_unsigned_after_type_code(remaining, type_code))?;
    self.advance(size);
    if i64::try_from(value).is_ok() {
      Ok(Value::Signed(value as i64))
    } else {
      Ok(Value::Unsigned(value))
    }
  }

  fn decode_signed_integer(&mut self, type_code: u8) -> Result<Value> {
    let remaining = self.remaining_data();
    let (size, value) = self.map_err(deserialize_signed_after_type_code(remaining, type_code))?;
    self.advance(size);
    Ok(Value::Signed(value))
  }

  fn decode_array(&mut self) -> Result<Value> {
    let mut elements = Vec::new();

    loop {
      let remaining = self.remaining_data();
      let type_code = match peek_type_code(remaining) {
        Ok(code) => code,
        Err(e) => return Err(self.partial_error_here(e, Value::Array(elements))),
      };

      if type_code == TypeCode::ContainerEnd as u8 {
        self.advance(1);
        break;
      }

      let value = self.decode_value().map_err(|e| match e {
        DecodeError::Fatal(_) => propagate_partial_decode(&e, Value::Array(elements.clone())),
        DecodeError::Partial {
          partial_value,
          error,
        } => {
          elements.push(partial_value);
          DecodeError::Partial {
            partial_value: Value::Array(elements.clone()),
            error,
          }
        },
      })?;

      elements.push(value);
    }

    Ok(Value::Array(elements))
  }

  fn decode_object(&mut self) -> Result<Value> {
    let mut object = HashMap::new();

    loop {
      let remaining = self.remaining_data();
      let type_code = match peek_type_code(remaining) {
        Ok(code) => code,
        Err(e) => return Err(self.partial_error_here(e, Value::Object(object))),
      };

      if type_code == TypeCode::ContainerEnd as u8 {
        self.advance(1);
        break;
      }

      // Key must be a string to match JSON rules.
      let key = match self.decode_value() {
        Ok(Value::String(key)) => key,
        Ok(_) => {
          return Err(self.partial_error_here(
            DeserializationError::NonStringKeyInMap,
            Value::Object(object),
          ));
        },
        Err(e) => return Err(propagate_partial_decode(&e, Value::Object(object))),
      };

      let value = self.decode_value().map_err(|e| match e {
        DecodeError::Fatal(_) => propagate_partial_decode(&e, Value::Object(object.clone())),
        DecodeError::Partial {
          partial_value,
          error,
        } => {
          object.insert(key.clone(), partial_value);
          DecodeError::Partial {
            partial_value: Value::Object(object.clone()),
            error,
          }
        },
      })?;

      object.insert(key, value);
    }

    Ok(Value::Object(object))
  }
}

/// Decode a buffer and return the resulting value.
/// On error, it returns the value decoded so far and the error.
pub fn decode_value(data: &[u8]) -> Result<Value> {
  let mut decoder = Decoder::new(data);
  decoder.decode()
}
