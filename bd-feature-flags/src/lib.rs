// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![deny(
  clippy::expect_used,
  clippy::panic,
  clippy::todo,
  clippy::unimplemented,
  clippy::unreachable,
  clippy::unwrap_used
)]

use std::{collections::HashMap, path::Path};
use std::time::{SystemTime, UNIX_EPOCH};

use bd_resilient_kv::KVStore;
use bd_bonjson::Value;

pub struct FeatureFlag {
  pub variant: Option<String>,
  pub timestamp: u64,
}

pub struct FeatureFlags {
  flags_store: KVStore,
  flags_cache: HashMap<String, FeatureFlag>,
}

fn get_current_timestamp() -> u64 {
  u64::try_from(
    SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap_or_default()
      .as_nanos()
  ).unwrap_or(0)
}

impl FeatureFlags {
  pub fn new<P: AsRef<Path>>(
    base_path: P,
    buffer_size: usize,
    high_water_mark_ratio: Option<f32>
  ) -> anyhow::Result<Self> {
    let store = KVStore::new(base_path, buffer_size, high_water_mark_ratio, None)?;

    // Populate the cache from existing store data
    let mut flags_cache = HashMap::new();
    for (key, value) in store.as_hashmap() {
      if let Value::Object(obj) = value {
        // Extract variant and timestamp from the stored object
        let variant = match obj.get("v") {
          Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
          _ => None,
        };
        let timestamp = match obj.get("t") {
          Some(Value::Unsigned(t)) => *t,
          _ => 0, // fallback timestamp if not found
        };

        flags_cache.insert(key.clone(), FeatureFlag { variant, timestamp });
      }
    }

    Ok(Self { flags_store: store, flags_cache })
  }

  #[must_use]
  pub fn get(&self, key: &str) -> Option<&FeatureFlag> {
      self.flags_cache.get(key)
  }

  pub fn set(&mut self, key: String, variant: Option<String>) -> anyhow::Result<()> {
    let timestamp = get_current_timestamp();
    self.flags_cache.insert(key.clone(), FeatureFlag { variant: variant.clone(), timestamp });

    let value = HashMap::from([
      ("v".to_string(), Value::String(variant.unwrap_or_default())),
      ("t".to_string(), Value::Unsigned(timestamp)),
    ]);
    self.flags_store.insert(key, Value::Object(value))?;

    Ok(())
  }

  pub fn clear(&mut self) -> anyhow::Result<()> {
    self.flags_cache.clear();
    self.flags_store.clear()?;
    Ok(())
  }

  #[must_use]
  pub fn as_hashmap(&self) -> &HashMap<String, FeatureFlag> {
    &self.flags_cache
  }
}

#[cfg(test)]
#[path = "./feature_flags_test.rs"]
mod feature_flags_test;
