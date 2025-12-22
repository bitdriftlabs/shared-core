// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./app_version_test.rs"]
mod app_version_test;

use bd_proto::protos::client::key_value;
pub use bd_proto::protos::client::key_value::app_version::Extra as AppVersionExtra;
use std::ops::Deref;
use std::sync::Arc;

static APP_VERSION_KEY: bd_key_value::Key<key_value::AppVersion> =
  bd_key_value::Key::new("app_version.state.1");

#[derive(Debug, Clone, PartialEq)]
pub struct AppVersion(key_value::AppVersion);

impl AppVersion {
  pub fn new(version: String, extra: AppVersionExtra) -> Self {
    Self(key_value::AppVersion {
      version,
      extra: Some(extra),
      ..Default::default()
    })
  }

  #[cfg(test)]
  pub fn new_build_number(version: &str, build_number: &str) -> Self {
    Self(key_value::AppVersion {
      version: version.to_string(),
      extra: Some(key_value::app_version::Extra::BuildNumber(
        build_number.to_string(),
      )),
      ..Default::default()
    })
  }
}

impl Deref for AppVersion {
  type Target = key_value::AppVersion;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

//
// Repository
//

pub struct Repository {
  store: Arc<bd_key_value::Store>,
}

impl Repository {
  pub(crate) const fn new(store: Arc<bd_key_value::Store>) -> Self {
    Self { store }
  }

  pub(crate) fn has_changed(&self, app_version: &key_value::AppVersion) -> bool {
    let Some(previous_app_version) = self.store.get(&APP_VERSION_KEY) else {
      // Initialize app version change detection logic by setting the current app version,
      // making it available for comparison after a user updates their app.
      self.store.set(&APP_VERSION_KEY, app_version);

      // We do not know what the previous app version was so we assume it has not changed.
      return false;
    };

    app_version != &previous_app_version
  }

  // Sets the current app version and returns the previous app version if it changed.
  pub(crate) fn set(&self, app_version: &key_value::AppVersion) -> Option<AppVersion> {
    let Some(previous_app_version) = self.store.get(&APP_VERSION_KEY) else {
      // Initialize app version change detection logic by setting the current app version,
      // making it available for comparison after a user updates their app.
      self.store.set(&APP_VERSION_KEY, app_version);
      return None;
    };

    // Version didn't change.
    if app_version == &previous_app_version {
      return None;
    }

    // Version changed. Update the stored app version to effectively mark the current version as
    // "seen".
    self.store.set(&APP_VERSION_KEY, app_version);

    // Version changed. Return previous app version.
    Some(AppVersion(previous_app_version))
  }
}
