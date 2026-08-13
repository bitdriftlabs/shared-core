// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::default_fixture_paths;

#[test]
fn default_fixtures_are_canonical_and_present() {
  let fixtures = default_fixture_paths().unwrap();

  assert_eq!(fixtures.len(), 2);
  assert_eq!(fixtures.first().unwrap().name, "small");
  assert_eq!(fixtures.last().unwrap().name, "large");
  for fixture in fixtures {
    assert!(fixture.config_path.is_file());
    assert!(fixture.logs_path.is_file());
  }
}
