// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::loader::{ConfigPtr, Loader, LoaderError, StatsCallbacks, WatchedFileLoader};
use rand::Rng;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::watch;

pub type FeatureFlagsLoader =
  Arc<dyn crate::loader::Loader<dyn crate::feature_flags::FeatureFlags>>;

pub type FeatureFlagsWatch = watch::Receiver<ConfigPtr<dyn crate::feature_flags::FeatureFlags>>;

/// Feature flags with strongly typed defaults.
pub trait FeatureFlags: std::fmt::Debug + Send + Sync {
  /// Determine whether a feature is enabled based on the following algorithm:
  /// 1) If `name` is not of integer type, return default.
  /// 2) If `name` is 0, return false.
  /// 2) If `name` is >= 10,000, return true.
  /// 3) Otherwise if <random> % 10,000 < `name`, return true.
  fn feature_enabled(&self, name: &str, default: bool) -> bool;

  /// Get a boolean by name, returning a default if the name does not exist.
  fn get_bool(&self, name: &str, default: bool) -> bool;

  /// Get an integer by name, returning a default if the name does not exist.
  fn get_integer(&self, name: &str, default: u64) -> u64;

  /// Get a string by name, returning a default if the name does not exist.
  fn get_string(&self, name: &str, default: &Arc<String>) -> Arc<String>;
}

impl FeatureFlags for watch::Receiver<ConfigPtr<dyn crate::feature_flags::FeatureFlags>> {
  fn feature_enabled(&self, name: &str, default: bool) -> bool {
    self
      .borrow()
      .as_ref()
      .unwrap()
      .feature_enabled(name, default)
  }

  fn get_bool(&self, name: &str, default: bool) -> bool {
    self.borrow().as_ref().unwrap().get_bool(name, default)
  }

  fn get_integer(&self, name: &str, default: u64) -> u64 {
    self.borrow().as_ref().unwrap().get_integer(name, default)
  }

  fn get_string(&self, name: &str, default: &Arc<String>) -> Arc<String> {
    self.borrow().as_ref().unwrap().get_string(name, default)
  }
}

// Values can either be booleans, integers, or strings.
#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum FeatureFlagValue {
  Bool(bool),
  Integer(u64),
  String(Arc<String>),
}

// Contains a hashmap of string name to value.
#[derive(Deserialize, Debug)]
struct MemoryFeatureFlags {
  values: HashMap<String, FeatureFlagValue>,
}

impl MemoryFeatureFlags {
  fn feature_enabled(&self, name: &str, default: bool, random: impl Fn() -> u64) -> bool {
    match self.values.get(name) {
      Some(FeatureFlagValue::Integer(i)) => match i {
        0 => false,
        1 ..= 9_999 => random() % 10_000 < *i,
        _ => true,
      },
      _ => default,
    }
  }
}

impl FeatureFlags for MemoryFeatureFlags {
  fn feature_enabled(&self, name: &str, default: bool) -> bool {
    self.feature_enabled(name, default, || rand::thread_rng().gen())
  }

  fn get_bool(&self, name: &str, default: bool) -> bool {
    match self.values.get(name) {
      Some(FeatureFlagValue::Bool(b)) => *b,
      _ => default,
    }
  }

  fn get_integer(&self, name: &str, default: u64) -> u64 {
    match self.values.get(name) {
      Some(FeatureFlagValue::Integer(i)) => *i,
      _ => default,
    }
  }

  fn get_string(&self, name: &str, default: &Arc<String>) -> Arc<String> {
    match self.values.get(name) {
      Some(FeatureFlagValue::String(s)) => s.clone(),
      _ => default.clone(),
    }
  }
}

/// Create a new filesystem based loader for feature flags geared towards use
/// with Kubernetes `ConfigMap` files. `directory_to_watch` will watch the
/// supplied directory for rename/move operations. When such an operation is
/// seen, `file_to_load` will be reloaded. The expected format of the file is
/// YAML with the following structure:
///
/// ```yaml
/// values:
///   key1: 1000
///   key2: hello
///   key3: true
/// ```
///
/// Values are constrained to integers, strings, and booleans, to match the
/// methods in the `FeatureFlags` trait. A failure to deserialize the YAML will
/// result in an empty snapshot such that only default values can be
/// retrieved.
pub fn new_memory_feature_flags_loader(
  directory_to_watch: impl AsRef<std::path::Path>,
  file_to_load: impl AsRef<std::path::Path>,
  stats: impl StatsCallbacks,
) -> Result<Arc<dyn Loader<dyn FeatureFlags>>, LoaderError> {
  WatchedFileLoader::new_loader(
    directory_to_watch,
    file_to_load,
    |feature_flags: Option<MemoryFeatureFlags>| -> ConfigPtr<dyn FeatureFlags> {
      // For feature flags we always return a valid object so callers can unwrap() directly
      // and use defaults.
      Some(std::sync::Arc::new(feature_flags.map_or_else(
        || MemoryFeatureFlags {
          values: HashMap::new(),
        },
        |feature_flags| feature_flags,
      )))
    },
    stats,
  )
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
