// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{FeatureFlag, ScriptValue};
use flatbuffers::{Follow, ForwardsUOffset};
use std::collections::BTreeMap;
use std::fmt::Display;
use vrl::core::Value;
use vrl::path::{OwnedSegment, OwnedValuePath};
use vrl::prelude::{Collection, NotNan};
use vrl::value::Kind;

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

impl From<&FeatureFlag> for Value {
  fn from(flag: &FeatureFlag) -> Self {
    let mut properties = BTreeMap::from([
      ("name".into(), flag.name.clone().into()),
      ("value".into(), flag.value.clone().into()),
    ]);
    if let Some(last_updated) = flag.last_updated {
      properties.insert(
        "last_updated".into(),
        Into::<ScriptValue>::into(last_updated).0,
      );
    }
    Self::Object(properties)
  }
}

impl Scriptable for FeatureFlag {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      return Ok(Some(ScriptValue(self.into())));
    }
    let joined_path = OwnedValuePath::from(path.to_vec());
    let Some(OwnedSegment::Field(field)) = path.first() else {
      return Err(PathError::NotAnArray(joined_path.to_string()));
    };
    match field.as_str() {
      "name" => Ok(Some(self.name.clone().into())),
      "last_updated" => Ok(Some(self.last_updated.into())),
      "value" => Ok(self.value.clone().map(Into::into)),
      _ => Err(PathError::UnknownKey(joined_path.into())),
    }
  }

  fn schema() -> Kind {
    todo!()
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

impl Scriptable for f32 {
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

impl Scriptable for f64 {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      Ok(Some(
        Value::Float(NotNan::new(*self).unwrap_or_default()).into(),
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

impl Scriptable for u64 {
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

impl Scriptable for u32 {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      Ok(Some(Value::Integer(i64::from(*self)).into()))
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

impl Scriptable for u16 {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      Ok(Some(Value::Integer(i64::from(*self)).into()))
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

impl Scriptable for u8 {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      Ok(Some((*self).into()))
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

impl From<u8> for ScriptValue {
  fn from(value: u8) -> Self {
    Value::Integer(i64::from(value)).into()
  }
}

impl From<u16> for ScriptValue {
  fn from(value: u16) -> Self {
    Value::Integer(i64::from(value)).into()
  }
}

impl From<u32> for ScriptValue {
  fn from(value: u32) -> Self {
    Value::Integer(i64::from(value)).into()
  }
}

impl From<u64> for ScriptValue {
  fn from(value: u64) -> Self {
    Value::Integer(i64::try_from(value).unwrap_or_default()).into()
  }
}

impl From<i8> for ScriptValue {
  fn from(value: i8) -> Self {
    Value::Integer(i64::from(value)).into()
  }
}

impl From<i16> for ScriptValue {
  fn from(value: i16) -> Self {
    Value::Integer(i64::from(value)).into()
  }
}

impl From<i32> for ScriptValue {
  fn from(value: i32) -> Self {
    Value::Integer(i64::from(value)).into()
  }
}

impl From<i64> for ScriptValue {
  fn from(value: i64) -> Self {
    Value::Integer(value).into()
  }
}

impl From<f32> for ScriptValue {
  fn from(value: f32) -> Self {
    Value::Float(NotNan::new(f64::from(value)).unwrap_or_default()).into()
  }
}

impl From<f64> for ScriptValue {
  fn from(value: f64) -> Self {
    Value::Float(NotNan::new(value).unwrap_or_default()).into()
  }
}

impl From<bool> for ScriptValue {
  fn from(value: bool) -> Self {
    Value::Boolean(value).into()
  }
}

impl Scriptable for i8 {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      Ok(Some(Value::Integer(i64::from(*self)).into()))
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

impl Scriptable for i64 {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      Ok(Some(Value::Integer(*self).into()))
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

impl Scriptable for i32 {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      Ok(Some(Value::Integer(i64::from(*self)).into()))
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
{
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
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
