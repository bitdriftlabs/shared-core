// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::input::PathError;
use crate::{ScriptValue, Scriptable};
use std::collections::BTreeMap;
use vrl::core::Value;
use vrl::path::{OwnedSegment, OwnedValuePath};
use vrl::prelude::Collection;
use vrl::value::Kind;
use vrl::value::kind::merge::{CollisionStrategy, Strategy};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeatureFlag {
  pub name: String,
  pub value: Option<String>,
  pub last_updated: Option<time::OffsetDateTime>,
}

// Container for overriding feature flag values on a nested object
pub struct FeatureFlagWrapper<'a, T: Scriptable> {
  object: &'a T,
  feature_flags: ScriptValue,
}

impl<'a, T: Scriptable> FeatureFlagWrapper<'a, T> {
  #[must_use]
  pub fn new(object: &'a T, feature_flags: &[FeatureFlag]) -> Self {
    Self {
      object,
      feature_flags: Value::Array(feature_flags.iter().map(Into::into).collect()).into(),
    }
  }
}

impl<T: Scriptable> Scriptable for FeatureFlagWrapper<'_, T> {
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
    match name.as_str() {
      "feature_flags" => self.feature_flags.resolve(&path[1 ..]),
      _ => self.object.resolve(path),
    }
  }

  fn schema() -> vrl::prelude::Kind {
    let mut schema = T::schema();
    schema.merge(
      Kind::object(Collection::empty().with_known(
        "feature_flags",
        Kind::array(Collection::empty().with_unknown(FeatureFlag::schema())),
      )),
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
