// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::input::Scriptable;
use ripsaw::compiler::{OwnedValueOrRef, SecretTarget, Target};
use ripsaw::core::Value;
use ripsaw::path::{OwnedTargetPath, PathPrefix};

pub struct ScriptableTarget<'a, T: Scriptable> {
  object: &'a T,
}

impl<T: Scriptable> std::fmt::Debug for ScriptableTarget<'_, T> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("ReportTarget").finish()
  }
}

impl<'a, T: Scriptable> ScriptableTarget<'a, T> {
  pub fn new(report: &'a T) -> Self {
    Self { object: report }
  }
}

impl<T: Scriptable> SecretTarget for ScriptableTarget<'_, T> {
  fn get_secret(&self, _key: &str) -> Option<&str> {
    None
  }

  fn insert_secret(&mut self, _key: &str, _value: &str) {}

  fn remove_secret(&mut self, _key: &str) {}
}

impl<T: Scriptable> Target for ScriptableTarget<'_, T> {
  fn target_insert(&mut self, _path: &OwnedTargetPath, _value: Value) -> Result<(), String> {
    Ok(())
  }

  fn target_get(
    &mut self,
    path: &ripsaw::path::OwnedTargetPath,
  ) -> Result<Option<ripsaw::compiler::OwnedValueOrRef<'_>>, String> {
    if path.prefix != PathPrefix::Event {
      return Ok(None);
    }
    match self.object.resolve(&path.path.segments[..]) {
      Ok(value) => Ok(value.map(|v| OwnedValueOrRef::Owned(v.0))),
      Err(path_err) => Err(path_err.to_string()),
    }
  }

  fn target_remove(
    &mut self,
    _path: &ripsaw::path::OwnedTargetPath,
    _compact: bool,
  ) -> Result<Option<ripsaw::prelude::Value>, String> {
    Ok(None)
  }
}
