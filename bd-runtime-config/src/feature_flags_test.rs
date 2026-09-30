// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use super::*;
use std::cell::Cell;

fn flags(value: u64) -> Arc<dyn FeatureFlags> {
  Arc::new(MemoryFeatureFlags {
    values: HashMap::from([("timeout".to_string(), FeatureFlagValue::Integer(value))]),
  })
}

#[test]
fn watched_values_rebuild_only_when_flags_change() {
  let (sender, receiver) = watch::channel(Some(flags(2)));
  let mut watched = WatchedFeatureFlags::new(Some(receiver), Arc::new(1));
  let calls = Cell::new(0);
  let parse = |flags: &dyn FeatureFlags, _: &u64| {
    calls.set(calls.get() + 1);
    let value = flags.get_integer("timeout", 1);
    if value == 0 {
      Err("invalid timeout")
    } else {
      Ok(value)
    }
  };

  assert_eq!(watched.current(parse), (&2, None));
  assert_eq!(watched.current(parse), (&2, None));
  assert_eq!(calls.get(), 1);

  sender.send(Some(flags(0))).unwrap();
  assert_eq!(watched.current(parse), (&2, Some("invalid timeout")));
  assert_eq!(watched.current(parse), (&2, None));
  assert_eq!(calls.get(), 2);

  sender.send(Some(flags(3))).unwrap();
  assert_eq!(watched.current(parse), (&3, None));
  sender.send(None).unwrap();
  assert_eq!(watched.current(parse), (&1, None));
  assert_eq!(calls.get(), 3);
}

#[test]
fn watched_values_accept_non_clone_values() {
  #[derive(Debug, PartialEq)]
  struct NonClone(u64);

  let (sender, receiver) = watch::channel(Some(flags(2)));
  let mut watched = WatchedFeatureFlags::new(Some(receiver), Arc::new(NonClone(1)));
  assert_eq!(
    watched
      .current(|flags, _| Ok::<_, ()>(NonClone(flags.get_integer("timeout", 1))))
      .0,
    &NonClone(2)
  );
  sender.send(None).unwrap();
  assert_eq!(
    watched.current(|_, _| Ok::<_, ()>(NonClone(0))).0,
    &NonClone(1)
  );
}

#[test]
fn feature_flag_enabled() {
  let mut feature_flags = MemoryFeatureFlags {
    values: HashMap::new(),
  };
  feature_flags
    .values
    .insert("test_not_int".to_string(), FeatureFlagValue::Bool(false));
  feature_flags
    .values
    .insert("test_int".to_string(), FeatureFlagValue::Integer(10));
  assert!(feature_flags.feature_enabled("test_not_int", true, || 9));
  assert!(feature_flags.feature_enabled("test_int", false, || 0));
  assert!(feature_flags.feature_enabled("test_int", false, || 9));
  assert!(!feature_flags.feature_enabled("test_int", true, || 10));
  assert!(!feature_flags.feature_enabled("test_int", true, || 9_999));
  // Wraps around.
  assert!(feature_flags.feature_enabled("test_int", false, || 10_000));
}
