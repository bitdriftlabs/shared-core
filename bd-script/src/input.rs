// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::ScriptValue;
use flatbuffers::{Follow, ForwardsUOffset};
use ripsaw::core::Value;
use ripsaw::path::{OwnedSegment, OwnedValuePath};
use ripsaw::prelude::{Collection, NotNan};
use ripsaw::value::Kind;
use std::fmt::Display;

pub enum PathError {
  IndexOutOfRange(isize),
  UnknownKey(String),
  NotAnArray(String),
  NotAnObject(String),
}

/// Base definition for objects passable to `Script::run()`
pub trait Scriptable {
  /// Return a value for a (potentially) nested value or self when empty
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError>;

  /// Define type definitions which will be used as hints within the scripting
  /// engine
  fn schema() -> Kind;
}

impl Display for PathError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "{}",
      match self {
        Self::IndexOutOfRange(size) => format!("index {size} is out of range"),
        Self::UnknownKey(key) => format!("unknown key {key}"),
        Self::NotAnArray(key) => format!("{key} is not an array"),
        Self::NotAnObject(key) => format!("{key} is not an object"),
      }
    )
  }
}

// Conversion impls for commonly used types (timestamps, numbers, strings,
// ...)

impl From<time::OffsetDateTime> for ScriptValue {
  fn from(value: time::OffsetDateTime) -> Self {
    Value::Timestamp(std::time::SystemTime::from(value).into()).into()
  }
}

impl Scriptable for bool {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      Ok(Some(Value::Boolean(*self).into()))
    } else {
      Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ))
    }
  }

  fn schema() -> Kind {
    Kind::boolean()
  }
}

impl From<bool> for ScriptValue {
  fn from(value: bool) -> Self {
    Value::Boolean(value).into()
  }
}

macro_rules! impl_scriptable_for_float {
  ($float_type:ident) => {
    impl Scriptable for $float_type {
      fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
        if path.is_empty() {
          Ok(Some(
            Value::Float(NotNan::new(f64::from(*self)).unwrap_or_default()).into(),
          ))
        } else {
          Err(PathError::UnknownKey(
            OwnedValuePath::from(path.to_vec()).to_string(),
          ))
        }
      }

      fn schema() -> Kind {
        Kind::float()
      }
    }
    impl From<$float_type> for ScriptValue {
      fn from(value: $float_type) -> Self {
        Value::Float(NotNan::new(f64::from(value)).unwrap_or_default()).into()
      }
    }
  };
}

macro_rules! impl_scriptable_for_int {
  ($int_type:ident) => {
    impl Scriptable for $int_type {
      fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
        if path.is_empty() {
          Ok(Some(
            Value::Integer(i64::try_from(*self).unwrap_or_default()).into(),
          ))
        } else {
          Err(PathError::UnknownKey(
            OwnedValuePath::from(path.to_vec()).to_string(),
          ))
        }
      }

      fn schema() -> Kind {
        Kind::integer()
      }
    }

    impl From<$int_type> for ScriptValue {
      fn from(value: $int_type) -> Self {
        Value::Integer(i64::try_from(value).unwrap_or_default()).into()
      }
    }
  };
}

impl_scriptable_for_float!(f32);
impl_scriptable_for_float!(f64);

impl_scriptable_for_int!(u8);
impl_scriptable_for_int!(u16);
impl_scriptable_for_int!(u32);
impl_scriptable_for_int!(u64);
impl_scriptable_for_int!(i8);
impl_scriptable_for_int!(i16);
impl_scriptable_for_int!(i32);
impl_scriptable_for_int!(i64);

impl<T: Scriptable> Scriptable for Option<T>
where
  ScriptValue: From<T>,
{
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    let Some(inner) = self else {
      return Ok(None);
    };
    inner.resolve(path)
  }

  fn schema() -> Kind {
    T::schema()
  }
}

impl<T> From<Option<T>> for ScriptValue
where
  Self: From<T>,
{
  fn from(value: Option<T>) -> Self {
    value.map_or_else(|| Self(Value::Null), Into::<Self>::into)
  }
}

impl<T> Scriptable for &T
where
  T: Scriptable,
{
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    (*self).resolve(path)
  }

  fn schema() -> Kind {
    T::schema()
  }
}

impl Scriptable for ScriptValue {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    let joined_path = OwnedValuePath::from(path.to_vec());
    self.0.get(&joined_path).map_or_else(
      || Err(PathError::UnknownKey(joined_path.to_string())),
      |value| Ok(Some(value.to_owned().into())),
    )
  }

  fn schema() -> Kind {
    todo!()
  }
}

impl Scriptable for String {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      Ok(Some(self.as_str().into()))
    } else {
      Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ))
    }
  }

  fn schema() -> Kind {
    Kind::bytes()
  }
}

impl From<String> for ScriptValue {
  fn from(value: String) -> Self {
    value.as_str().into()
  }
}

impl Scriptable for str {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    self.to_string().resolve(path)
  }

  fn schema() -> Kind {
    Kind::bytes()
  }
}

impl Scriptable for &str {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    self.to_string().resolve(path)
  }

  fn schema() -> Kind {
    Kind::bytes()
  }
}

impl<T: ToOwned + Scriptable> From<Vec<T>> for ScriptValue
where
  <T as ToOwned>::Owned: Scriptable,
  Value: From<<T as ToOwned>::Owned>,
{
  fn from(value: Vec<T>) -> Self {
    Value::Array(value.iter().map(|item| item.to_owned().into()).collect()).into()
  }
}

impl<T: ToOwned + Scriptable> Scriptable for Vec<T>
where
  <T as ToOwned>::Owned: Scriptable,
  ScriptValue: From<<T as ToOwned>::Owned>,
{
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      return Ok(Some(
        Value::Array(
          self
            .iter()
            .map(|item| Into::<ScriptValue>::into(item.to_owned()).0)
            .collect(),
        )
        .into(),
      ));
    }
    let Some(OwnedSegment::Index(index)) = path.first() else {
      return Err(PathError::NotAnObject(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ));
    };
    let Ok(vec_index) = usize::try_from(*index) else {
      return Err(PathError::IndexOutOfRange(*index));
    };
    let Some(value) = self.get(vec_index) else {
      return Err(PathError::IndexOutOfRange(*index));
    };

    value.resolve(&path[1 ..])
  }

  fn schema() -> Kind {
    Kind::bytes()
  }
}

impl<'a, T: Follow<'a> + 'a> Scriptable for flatbuffers::Vector<'a, T>
where
  T::Inner: Scriptable,
  ScriptValue: From<T::Inner>,
{
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      return Ok(Some(
        Value::Array(
          self
            .iter()
            .map(|item| Into::<ScriptValue>::into(item).0)
            .collect(),
        )
        .into(),
      ));
    }

    let Some(OwnedSegment::Index(index)) = path.first() else {
      return Err(PathError::NotAnObject(
        OwnedValuePath::from(path.to_vec()).into(),
      ));
    };
    let index = usize::try_from(*index).map_err(|_| PathError::IndexOutOfRange(*index))?;
    self.get(index).resolve(&path[1 ..])
  }

  fn schema() -> Kind {
    Kind::array(Collection::empty().with_unknown(T::Inner::schema()))
  }
}

impl From<flatbuffers::Vector<'_, u8>> for ScriptValue {
  fn from(value: flatbuffers::Vector<'_, u8>) -> Self {
    Value::Array(
      value
        .iter()
        .map(|item| Into::<Self>::into(item).0)
        .collect::<Vec<_>>(),
    )
    .into()
  }
}

impl<'a, T: Follow<'a> + 'a> From<flatbuffers::Vector<'a, ForwardsUOffset<T>>> for ScriptValue
where
  T::Inner: Scriptable,
  Self: From<<T as Follow<'a>>::Inner>,
{
  fn from(value: flatbuffers::Vector<'a, ForwardsUOffset<T>>) -> Self {
    Value::Array(
      value
        .iter()
        .map(|item| Into::<Self>::into(item).0)
        .collect::<Vec<_>>(),
    )
    .into()
  }
}
