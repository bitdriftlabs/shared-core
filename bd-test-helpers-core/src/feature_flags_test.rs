// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![allow(clippy::unwrap_used)]

use super::{DefaultFeatureFlags, FakeLoader};
use bd_runtime_config::loader::Loader;
use std::sync::Arc;

#[test]
fn loader_publishes_initial_and_updated_feature_flags() {
  let loader = FakeLoader::new(Arc::new(
    DefaultFeatureFlags::default().with_integer_flag("batch_size", 32),
  ));
  let mut snapshot = loader.snapshot_watch();

  assert_eq!(
    snapshot
      .borrow()
      .as_ref()
      .unwrap()
      .get_integer("batch_size", 0),
    32
  );

  loader.update(Arc::new(
    DefaultFeatureFlags::default().with_integer_flag("batch_size", 64),
  ));

  assert!(snapshot.has_changed().unwrap());
  snapshot.borrow_and_update();
  assert_eq!(
    snapshot
      .borrow()
      .as_ref()
      .unwrap()
      .get_integer("batch_size", 0),
    64
  );
}
