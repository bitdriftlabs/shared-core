// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./app_version_test.rs"]
mod app_version_test;

use std::sync::Arc;

static APP_VERSIONS_KEY: bd_key_value::Key<AppVersion> =
  bd_key_value::Key::new("app_version.state.1");

//
// AppVersion
//

#[derive(Eq, PartialEq, serde::Serialize, serde::Deserialize, Debug)]
pub(crate) struct AppVersion {
  pub(crate) app_version: String,
  pub(crate) app_version_extra: AppVersionExtra,
}

impl bd_key_value::Storable for AppVersion {}

#[derive(Eq, PartialEq, serde::Serialize, serde::Deserialize, Debug)]
pub enum AppVersionExtra {
  // Comparable using number comparison.
  AppVersionCode(i64),
  // Comparable using semver comparison.
  BuildNumber(String),
}

impl AppVersionExtra {
  pub(crate) const fn name(&self) -> &str {
    match self {
      Self::AppVersionCode(_) => "app_version_code",
      Self::BuildNumber(_) => "build_number",
    }
  }

  pub(crate) fn string_value(&self) -> String {
    match self {
      Self::BuildNumber(value) => value.to_string(),
      Self::AppVersionCode(value) => value.to_string(),
    }
  }
}

//
// Repository
//

pub(crate) struct Repository {
  store: Arc<bd_key_value::Store>,
}

impl Repository {
  pub(crate) fn new(store: Arc<bd_key_value::Store>) -> Self {
    Self { store }
  }

  pub(crate) fn has_changed(&self, app_version: &AppVersion) -> bool {
    let Some(previous_app_version) = self.store.get(&APP_VERSIONS_KEY) else {
      // We do not know what the previous app version was so we assume it has not changed.
      return false;
    };

    app_version != &previous_app_version
  }

  // Sets the current app version and returns the previous app version if it changed.
  pub(crate) fn set(&self, app_version: &AppVersion) -> Option<AppVersion> {
    let Some(previous_app_version) = self.store.get(&APP_VERSIONS_KEY) else {
      self.store.set(&APP_VERSIONS_KEY, app_version);
      return None;
    };

    // Version didn't change.
    if app_version == &previous_app_version {
      return None;
    }

    self.store.set(&APP_VERSIONS_KEY, app_version);

    // Version changed. Return previous app version.
    Some(previous_app_version)
  }
}
