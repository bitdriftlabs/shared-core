// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_runtime_config::feature_flags::FeatureFlags;
use bd_runtime_config::loader::{ConfigPtr, Loader};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::watch;

// Fake for external Loader trait.
pub struct FakeLoader<T: FeatureFlags> {
  snapshot_sender: watch::Sender<ConfigPtr<dyn FeatureFlags>>,
  snapshot_receiver: watch::Receiver<ConfigPtr<dyn FeatureFlags>>,
  phantom: PhantomData<T>,
}

impl<T: FeatureFlags + Default + 'static> Default for FakeLoader<T> {
  fn default() -> Self {
    Self::new(Arc::new(T::default()))
  }
}

impl<T: FeatureFlags + 'static> FakeLoader<T> {
  pub fn new(feature_flags: Arc<T>) -> Self {
    let (snapshot_sender, snapshot_receiver) =
      watch::channel(Some(feature_flags as Arc<dyn FeatureFlags>));
    Self {
      snapshot_sender,
      snapshot_receiver,
      phantom: PhantomData,
    }
  }

  pub fn update(&self, feature_flags: Arc<T>) {
    self
      .snapshot_sender
      .send(Some(feature_flags as Arc<dyn FeatureFlags>))
      .unwrap();
  }
}

#[async_trait::async_trait]
impl<T: FeatureFlags + Sized + 'static> Loader<dyn FeatureFlags> for FakeLoader<T> {
  fn snapshot_watch(&self) -> watch::Receiver<ConfigPtr<dyn FeatureFlags>> {
    self.snapshot_receiver.clone()
  }
  async fn shutdown(&self) {}
}

// Mock for external FeatureFlags trait.
mockall::mock! {
  #[derive(Debug)]
  pub FeatureFlags {}
  impl FeatureFlags for FeatureFlags {
    fn feature_enabled(&self, name: &str, default: bool) -> bool;
    fn get_bool(&self, name: &str, default: bool) -> bool;
    fn get_integer(&self, name: &str, default: u64) -> u64;
    fn get_string(&self, name: &str, default: &str) -> &str;
  }
}

// This is an implementation that just returns defaults to ease testing in places that don't care
// about injecting flag values.
#[derive(Debug, Default)]
pub struct DefaultFeatureFlags {
  bool_flags: HashMap<String, bool>,
  integer_flags: HashMap<String, u64>,
}

impl DefaultFeatureFlags {
  #[must_use]
  pub fn with_bool_flag(mut self, name: &str, value: bool) -> Self {
    self.bool_flags.insert(name.to_string(), value);
    self
  }

  #[must_use]
  pub fn with_integer_flag(mut self, name: &str, value: u64) -> Self {
    self.integer_flags.insert(name.to_string(), value);
    self
  }
}

impl FeatureFlags for DefaultFeatureFlags {
  fn feature_enabled(&self, name: &str, default: bool) -> bool {
    *self.bool_flags.get(name).unwrap_or(&default)
  }

  fn get_bool(&self, name: &str, default: bool) -> bool {
    *self.bool_flags.get(name).unwrap_or(&default)
  }

  fn get_integer(&self, name: &str, default: u64) -> u64 {
    *self.integer_flags.get(name).unwrap_or(&default)
  }

  fn get_string<'a>(&'a self, _name: &'a str, default: &'a str) -> &'a str {
    default
  }
}
