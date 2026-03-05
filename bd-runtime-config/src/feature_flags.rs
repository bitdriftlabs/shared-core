// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::loader::{ConfigPtr, Loader, LoaderError, StatsCallbacks, WatchedFileLoader};
use bd_time::{SystemTimeProvider, Ticker, TimeProvider};
use rand::Rng;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::watch;

pub type FeatureFlagsLoader =
  Arc<dyn crate::loader::Loader<dyn crate::feature_flags::FeatureFlags>>;

pub type FeatureFlagsWatch = watch::Receiver<ConfigPtr<dyn crate::feature_flags::FeatureFlags>>;

#[must_use]
pub fn get_integer_from_watch(
  feature_flags: &FeatureFlagsWatch,
  name: &str,
  default: u64,
) -> u64 {
  feature_flags
    .borrow()
    .as_ref()
    .map_or(default, |flags| flags.get_integer(name, default))
}

pub struct RuntimeFlagTicker {
  feature_flags: FeatureFlagsWatch,
  poll_interval_flag: String,
  default_poll_interval_seconds: u64,
  time_provider: Arc<dyn TimeProvider>,
}

impl RuntimeFlagTicker {
  /// Creates a ticker that reloads `poll_interval_flag` before each tick.
  ///
  /// Values are interpreted as seconds. We clamp both the default and runtime value to at least
  /// one second so an accidental `0` cannot create a tight polling loop.
  #[must_use]
  pub fn new(
    feature_flags: FeatureFlagsWatch,
    poll_interval_flag: impl Into<String>,
    default_poll_interval_seconds: u64,
  ) -> Self {
    Self {
      feature_flags,
      poll_interval_flag: poll_interval_flag.into(),
      default_poll_interval_seconds: default_poll_interval_seconds.max(1),
      time_provider: Arc::new(SystemTimeProvider),
    }
  }

  #[must_use]
  pub fn with_time_provider(mut self, time_provider: Arc<dyn TimeProvider>) -> Self {
    self.time_provider = time_provider;
    self
  }
}

#[async_trait::async_trait]
impl Ticker for RuntimeFlagTicker {
  async fn tick(&mut self) {
    let poll_interval_seconds = get_integer_from_watch(
      &self.feature_flags,
      &self.poll_interval_flag,
      self.default_poll_interval_seconds,
    )
    .max(1);
    // Re-read the flag each time so interval updates are applied without restarting the poller.
    self
      .time_provider
      .sleep(time::Duration::seconds(
        i64::try_from(poll_interval_seconds).unwrap_or(i64::MAX),
      ))
      .await;
  }
}

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
    self.feature_enabled(name, default, || rand::rng().random())
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
      Some(std::sync::Arc::new(feature_flags.unwrap_or_else(|| {
        MemoryFeatureFlags {
          values: HashMap::new(),
        }
      })))
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

#[test]
fn get_integer_from_watch_uses_defaults_and_loaded_values() {
  let (_, empty_watch) = watch::channel::<ConfigPtr<dyn FeatureFlags>>(None);
  assert_eq!(get_integer_from_watch(&empty_watch, "poll_interval_s", 60), 60);

  let mut values = HashMap::new();
  values.insert(
    "poll_interval_s".to_string(),
    FeatureFlagValue::Integer(15),
  );
  let (_, loaded_watch) = watch::channel::<ConfigPtr<dyn FeatureFlags>>(Some(Arc::new(
    MemoryFeatureFlags { values },
  )));
  assert_eq!(get_integer_from_watch(&loaded_watch, "poll_interval_s", 60), 15);
}
