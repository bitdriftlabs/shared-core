// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::AppVersion;
use crate::app_version::AppVersionExtra;
use bd_device::Store;
use bd_test_helpers::session::InMemoryStorage;
use pretty_assertions::assert_eq;
use std::sync::Arc;

#[test]
fn app_version_repo() {
  let store = Arc::new(Store::new(Box::<InMemoryStorage>::default()));

  let repo = super::Repository::new(store.clone());
  // Initial check
  assert!(!repo.has_changed(&AppVersion {
    app_version: "1.0.0".to_string(),
    app_version_extra: AppVersionExtra::BuildNumber("1".to_string()),
  }));
  // Consecutive check after app update
  assert!(repo.has_changed(&AppVersion {
    app_version: "1.0.0".to_string(),
    app_version_extra: AppVersionExtra::BuildNumber("2".to_string()),
  }));

  // Initial app version
  assert!(
    repo
      .set(&AppVersion {
        app_version: "1.0.0".to_string(),
        app_version_extra: AppVersionExtra::BuildNumber("1".to_string()),
      })
      .is_none()
  );
  // Follow up check
  assert!(!repo.has_changed(&AppVersion {
    app_version: "1.0.0".to_string(),
    app_version_extra: AppVersionExtra::BuildNumber("1".to_string()),
  }));
  // Follow up update with old app version
  assert!(
    repo
      .set(&AppVersion {
        app_version: "1.0.0".to_string(),
        app_version_extra: AppVersionExtra::BuildNumber("1".to_string()),
      })
      .is_none()
  );
  // Follow up check with new app version
  assert!(repo.has_changed(&AppVersion {
    app_version: "1.0.0".to_string(),
    app_version_extra: AppVersionExtra::BuildNumber("2".to_string()),
  }));

  // Follow up update with new app version
  assert_eq!(
    Some(AppVersion {
      app_version: "1.0.0".to_string(),
      app_version_extra: AppVersionExtra::BuildNumber("1".to_string()),
    }),
    repo.set(&AppVersion {
      app_version: "1.0.0".to_string(),
      app_version_extra: AppVersionExtra::BuildNumber("2".to_string()),
    })
  );

  // Simulate a new SDK configuration without app update.
  let repo = super::Repository::new(store.clone());
  assert!(
    repo
      .set(&AppVersion {
        app_version: "1.0.0".to_string(),
        app_version_extra: AppVersionExtra::BuildNumber("2".to_string()),
      })
      .is_none()
  );

  // Simulate a new SDK configuration with app update.
  let repo = super::Repository::new(store);
  assert_eq!(
    Some(AppVersion {
      app_version: "1.0.0".to_string(),
      app_version_extra: AppVersionExtra::BuildNumber("2".to_string()),
    }),
    repo.set(&AppVersion {
      app_version: "1.0.0".to_string(),
      app_version_extra: AppVersionExtra::BuildNumber("3".to_string()),
    })
  );
}
