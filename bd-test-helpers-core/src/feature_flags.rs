// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#[cfg(test)]
#[path = "./feature_flags_test.rs"]
mod tests;

use bd_runtime_config::feature_flags::FeatureFlags;
use bd_runtime_config::loader::{ConfigPtr, Loader};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::watch;

//
// FakeLoader
//

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

//
// DefaultFeatureFlags
//

#[derive(Debug, Default)]
#[allow(clippy::struct_field_names)]
pub struct DefaultFeatureFlags {
  bool_flags: HashMap<String, bool>,
  integer_flags: HashMap<String, u64>,
  string_flags: HashMap<String, Arc<String>>,
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

  #[must_use]
  pub fn with_string_flag(mut self, name: &str, value: &str) -> Self {
    self
      .string_flags
      .insert(name.to_string(), Arc::new(value.to_string()));
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

  fn get_string(&self, name: &str, default: &Arc<String>) -> Arc<String> {
    self
      .string_flags
      .get(name)
      .cloned()
      .unwrap_or_else(|| default.clone())
  }
}
