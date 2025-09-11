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

#[cfg(test)]
#[path = "./feature_flags_test.rs"]
mod feature_flags_test;

// Underlying data structure in the resilient KV store:
// Every feature flag consists of a key-value pair:
// Key: String: the name of the feature flag
// Value: Map: Feature flag entry containing the key-value pairs:
// - "v": String: The variant, or "" if no variant is set
// - "t": Integer: The Unix timestamp in nanoseconds of when the flag was last updated

const VARIANT_KEY: &str = "v";
const TIMESTAMP_KEY: &str = "t";

#[derive(Debug)]
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

fn generate_initial_cache(store: &KVStore) -> HashMap<String, FeatureFlag> {
  let mut flags_cache = HashMap::new();
  for (key, value) in store.as_hashmap() {
    // Note: We discard any malformed entries
    if let Value::Object(obj) = value {
      let variant = match obj.get(VARIANT_KEY) {
        Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
        Some(Value::String(_)) => None,
        _ => continue,
      };

      let timestamp = match obj.get(TIMESTAMP_KEY) {
        Some(Value::Unsigned(t)) => *t,
        #[allow(clippy::cast_sign_loss)]
        Some(Value::Signed(t)) if *t >= 0 => *t as u64,
        _ => continue,
      };

      flags_cache.insert(key.clone(), FeatureFlag { variant, timestamp });
    }
  }
  flags_cache
}

impl FeatureFlags {
  pub fn new<P: AsRef<Path>>(
    base_path: P,
    buffer_size: usize,
    high_water_mark_ratio: Option<f32>
  ) -> anyhow::Result<Self> {
    let flags_store = KVStore::new(base_path, buffer_size, high_water_mark_ratio, None)?;
    let flags_cache = generate_initial_cache(&flags_store);
    Ok(Self { flags_store, flags_cache })
  }

  #[must_use]
  pub fn get(&self, key: &str) -> Option<&FeatureFlag> {
      self.flags_cache.get(key)
  }

  pub fn sync(&self) -> anyhow::Result<()> {
    self.flags_store.sync()
  }

  pub fn set(&mut self, key: String, variant: Option<String>) -> anyhow::Result<()> {
    let timestamp = get_current_timestamp();
    self.flags_cache.insert(key.clone(), FeatureFlag { variant: variant.clone(), timestamp });

    let value = HashMap::from([
      (VARIANT_KEY.to_string(), Value::String(variant.unwrap_or_default())),
      (TIMESTAMP_KEY.to_string(), Value::Unsigned(timestamp)),
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
