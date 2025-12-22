// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::AppVersion;
use bd_test_helpers::session::in_memory_store;
use pretty_assertions::assert_eq;

#[test]
fn app_version_repo() {
  let store = in_memory_store();

  let repo = super::Repository::new(store.clone());
  // Initial check
  assert!(!repo.has_changed(&AppVersion::new_build_number("1.0.0", "1")));
  // Consecutive check after app update
  assert!(repo.has_changed(&AppVersion::new_build_number("1.0.0", "2")));

  // Initial app version
  assert!(
    repo
      .set(&AppVersion::new_build_number("1.0.0", "1"))
      .is_none()
  );
  // Follow up check
  assert!(!repo.has_changed(&AppVersion::new_build_number("1.0.0", "1")));
  // Follow up update with old app version
  assert!(
    repo
      .set(&AppVersion::new_build_number("1.0.0", "1"))
      .is_none()
  );
  // Follow up check with new app version
  assert!(repo.has_changed(&AppVersion::new_build_number("1.0.0", "2")));

  // Follow up update with new app version
  assert_eq!(
    Some(AppVersion::new_build_number("1.0.0", "1")),
    repo.set(&AppVersion::new_build_number("1.0.0", "2"))
  );

  // Simulate a new SDK configuration without app update.
  let repo = super::Repository::new(store.clone());
  assert!(
    repo
      .set(&AppVersion::new_build_number("1.0.0", "2"))
      .is_none()
  );

  // Simulate a new SDK configuration with app update.
  let repo = super::Repository::new(store);
  assert_eq!(
    Some(AppVersion::new_build_number("1.0.0", "2")),
    repo.set(&AppVersion::new_build_number("1.0.0", "3"))
  );
}
