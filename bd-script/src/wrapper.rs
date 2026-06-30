// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::input::PathError;
use crate::{ScriptValue, Scriptable};
use bd_log_primitives::{DataValue, LogFields, LogMapData};
use std::collections::BTreeMap;
use vrl::core::Value;
use vrl::path::{OwnedSegment, OwnedValuePath};
use vrl::prelude::{Collection, NotNan};
use vrl::value::Kind;
use vrl::value::kind::merge::{CollisionStrategy, Strategy};

const FEATURE_FLAGS: &str = "feature_flags";
const FIELDS: &str = "fields";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeatureFlag {
  pub name: String,
  pub value: Option<String>,
  pub last_updated: Option<time::OffsetDateTime>,
}

// Container for overriding feature flag and field values on a nested object
pub struct ScriptableWrapper<'a, T: Scriptable> {
  object: &'a T,
  overrides: BTreeMap<String, ScriptValue>,
}

impl<'a, T: Scriptable> ScriptableWrapper<'a, T> {
  #[must_use]
  pub fn new(object: &'a T, feature_flags: &[FeatureFlag], fields: Option<LogFields>) -> Self {
    let fields_value = Into::<Value>::into(DataValueWrapper(DataValue::Map(LogMapData::new(
      fields
        .unwrap_or_default()
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_owned()))
        .collect(),
    ))));
    Self::new_with_overrides(
      object,
      BTreeMap::from([
        (
          FEATURE_FLAGS.to_string(),
          Value::Array(feature_flags.iter().map(Into::into).collect()).into(),
        ),
        (FIELDS.to_string(), fields_value.into()),
      ]),
    )
  }

  #[must_use]
  pub fn new_with_overrides(object: &'a T, overrides: BTreeMap<String, ScriptValue>) -> Self {
    Self { object, overrides }
  }
}

struct DataValueWrapper(DataValue);

impl From<DataValueWrapper> for Value {
  fn from(value: DataValueWrapper) -> Self {
    match value.0 {
      DataValue::U64(v) => v.into(),
      DataValue::I64(v) => v.into(),
      DataValue::Double(v) => Self::Float(NotNan::new(v.into_inner()).unwrap_or_default()),
      DataValue::Boolean(v) => v.into(),
      DataValue::String(s) => s.into(),
      DataValue::SharedString(s) => s.to_string().into(),
      DataValue::StaticString(s) => s.into(),
      DataValue::Bytes(b) => b.into_payload().into(),
      DataValue::Array(array_data) => array_data
        .items()
        .iter()
        .map(|v| DataValueWrapper(v.to_owned()).into())
        .collect::<Vec<Self>>()
        .into(),
      DataValue::Map(map_data) => map_data
        .entries()
        .iter()
        .map(|(k, v)| (k.to_owned().into(), DataValueWrapper(v.to_owned()).into()))
        .collect::<BTreeMap<_, Self>>()
        .into(),
    }
  }
}

impl<T: Scriptable> Scriptable for ScriptableWrapper<'_, T> {
  fn resolve(
    &self,
    path: &[vrl::path::OwnedSegment],
  ) -> Result<Option<crate::ScriptValue>, crate::input::PathError> {
    if path.is_empty() {
      return self.object.resolve(path);
    }
    let OwnedSegment::Field(name) = &path[0] else {
      let joined_path = OwnedValuePath::from(path.to_vec()).to_string();
      return Err(PathError::NotAnArray(joined_path));
    };
    if let Some(container) = self.overrides.get(name.as_str()) {
      return container.resolve(&path[1 ..]);
    }
    self.object.resolve(path)
  }

  fn schema() -> vrl::prelude::Kind {
    let mut schema = T::schema();
    schema.merge(
      Kind::object(
        Collection::empty()
          .with_known(
            FEATURE_FLAGS,
            Kind::array(Collection::empty().with_unknown(FeatureFlag::schema())),
          )
          .with_known(FIELDS, Kind::any_object()),
      ),
      Strategy {
        collisions: CollisionStrategy::Union,
      },
    );
    schema
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
    Kind::object(
      Collection::empty()
        .with_known("name", Kind::bytes())
        .with_known("value", Kind::bytes().or_null())
        .with_known("last_updated", Kind::timestamp()),
    )
  }
}
