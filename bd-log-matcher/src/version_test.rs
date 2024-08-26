// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::Version;
use std::cmp::Ordering;

#[test]
fn to_string() {
  assert_eq!(Version::new("1.2.3").to_string(), "1.2.3");
  assert_eq!(Version::new("1.2").to_string(), "1.2");
  assert_eq!(Version::new("1").to_string(), "1");
  assert_eq!(Version::new("1.2.3-alpha").to_string(), "1.2.3-alpha");
  assert_eq!(Version::new("1.2.3-alpha.1").to_string(), "1.2.3-alpha.1");
  assert_eq!(
    Version::new("1.2.3-alpha.1+build").to_string(),
    "1.2.3-alpha.1+build"
  );
}

#[test]
fn version_comparisons() {
  assert!(cmp("1.2.3", ">", "1.2.1"));
  assert!(cmp("17", "<", "17.1"));
  assert!(cmp("1.2.1", ">", "1.2"));
  assert!(!cmp("1.2", ">=", "1.2.3"));
  assert!(cmp("1.2", "<=", "1.2.3"));
  assert!(!cmp("1.01", ">=", "1.1.1"));
  assert!(cmp("1.01", "<", "1.1.1"));
  assert!(cmp("1.2.1", "<", "1.3"));
  assert!(cmp("1.2.10a", "<", "1.2.2a"));
  assert!(cmp("1.1.1-foo", ">=", "1.1.1"));
  assert!(!cmp("1.1.1-foo", ">", "1.1.1"));
  assert!(!cmp("1.1.1-foo", "<", "1.1.1"));
  assert!(cmp("1.1.1+foo", ">=", "1.1.1"));
  assert!(!cmp("1.1.1+foo", ">", "1.1.1"));
  assert!(!cmp("1.1.1+foo", "<", "1.1.1"));
  assert!(!cmp("1.2.10-alpha", "<", "1.2.2-alpha"));
  assert!(cmp("1.2.10-alpha", ">", "1.2.2-alpha"));
  assert!(cmp("1.2.0439740b", "<", "1.2.1b39740b"));
}

fn lt(a: &str, b: &str) -> bool {
  let a = Version::new(a);
  let b = Version::new(b);

  a.cmp(&b) == Ordering::Less
}

fn lte(a: &str, b: &str) -> bool {
  let a = Version::new(a);
  let b = Version::new(b);

  a.cmp(&b) == Ordering::Less || a.cmp(&b) == Ordering::Equal
}

fn gt(a: &str, b: &str) -> bool {
  let a = Version::new(a);
  let b = Version::new(b);

  a.cmp(&b) == Ordering::Greater
}

fn gte(a: &str, b: &str) -> bool {
  let a = Version::new(a);
  let b = Version::new(b);

  a.cmp(&b) == Ordering::Greater || a.cmp(&b) == Ordering::Equal
}

fn cmp(a: &str, op: &str, b: &str) -> bool {
  match op {
    "<" => lt(a, b),
    "<=" => lte(a, b),
    ">" => gt(a, b),
    ">=" => gte(a, b),
    _ => panic!("Invalid operator"),
  }
}
