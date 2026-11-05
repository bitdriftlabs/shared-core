// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/licenses/strict/1.0.0

use super::{Labels, decode, encode, validate};

#[test]
fn snapshots_are_canonical_and_empty_is_explicit() {
  let labels = Labels::from([("z".into(), String::new()), ("a".into(), "test".into())]);
  assert_eq!(encode(&labels).unwrap(), r#"{"a":"test","z":""}"#);
  assert_eq!(decode(&encode(&labels).unwrap()).unwrap(), labels);
  assert_eq!(encode(&Labels::new()).unwrap(), "{}");
}

#[test]
fn label_limits_use_utf8_bytes_and_reject_reserved_keys() {
  let five = (0 .. 5).map(|key| (key.to_string(), "v".into())).collect();
  assert!(validate(&five).is_ok());
  let mut six = five;
  six.insert("six".into(), "v".into());
  assert!(validate(&six).is_err());
  for (key, value) in [
    (String::new(), String::new()),
    ("__bd_env".into(), "production".into()),
    ("a".repeat(65), String::new()),
    ("x".into(), "x".repeat(257)),
    ("é".repeat(33), String::new()),
  ] {
    assert!(validate(&Labels::from([(key, value)])).is_err());
  }
  assert!(decode(r#"{"x":1}"#).is_err());
  assert!(decode("null").is_err());
}
