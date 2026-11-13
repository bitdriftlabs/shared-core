// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::RegexMatcherCorpus;
use crate::default_fixture_paths;

#[test]
fn large_fixture_regex_matchers_load_and_evaluate() {
  let fixture = default_fixture_paths()
    .unwrap()
    .into_iter()
    .find(|fixture| fixture.name == "large")
    .unwrap();
  let all = RegexMatcherCorpus::load(&fixture.config_path, &fixture.logs_path, None).unwrap();
  let typical =
    RegexMatcherCorpus::load(&fixture.config_path, &fixture.logs_path, Some(10 * 1024)).unwrap();

  assert!(
    all
      .patterns()
      .iter()
      .any(|pattern| pattern == "Tubefi ton yaso kodalimuz")
  );
  assert_eq!(all.patterns(), typical.patterns());
  assert!(typical.log_count() < all.log_count());
  assert!(typical.message_bytes() < all.message_bytes());
  assert_eq!(all.evaluate_all(), all.evaluate_all());
}
