// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(
  clippy::expect_used,
  clippy::panic,
  clippy::todo,
  clippy::unimplemented,
  clippy::unreachable,
  clippy::unwrap_used
)]

use super::*;
use tempfile::TempDir;

#[test]
fn test_cache_populated_from_store() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let temp_path = temp_dir.path();

    // Create initial flags and set some values
  {
    let mut flags = FeatureFlags::new(temp_path, 1024, None)?;
    flags.set("flag1".to_string(), Some("variant1".to_string()))?;
    flags.set("flag2".to_string(), None)?; // Test None variant
    flags.sync()?; // Ensure data is written to disk
  } // Drop the instance

  // Create new instance - should load from persistent storage
  let flags = FeatureFlags::new(temp_path, 1024, None)?;

  // Verify flags were loaded into cache
  assert_eq!(flags.as_hashmap().len(), 2);

  let flag1 = flags.get("flag1").expect("flag1 should exist");
  assert_eq!(flag1.variant, Some("variant1".to_string()));

  let flag2 = flags.get("flag2").expect("flag2 should exist");
  assert_eq!(flag2.variant, None);

  Ok(())
}

#[test]
fn test_invalid_entries_discarded_on_load() -> anyhow::Result<()> {
  use bd_bonjson::Value;
  use std::collections::HashMap;

  let temp_dir = TempDir::new()?;
  let temp_path = temp_dir.path();

  // Create a store and insert both valid and invalid entries directly
  {
    let mut flags = FeatureFlags::new(temp_path, 1024, None)?;

    // Add a valid flag through normal API
    flags.set("valid_flag".to_string(), Some("variant1".to_string()))?;

    // Manually insert invalid entries directly into the store
    let store = &mut flags.flags_store;

    // Entry missing VARIANT_KEY
    let missing_variant = HashMap::from([
      ("t".to_string(), Value::Unsigned(12345)),
    ]);
    store.insert("missing_variant".to_string(), Value::Object(missing_variant))?;

    // Entry missing TIMESTAMP_KEY
    let missing_timestamp = HashMap::from([
      ("v".to_string(), Value::String("some_variant".to_string())),
    ]);
    store.insert("missing_timestamp".to_string(), Value::Object(missing_timestamp))?;

    // Entry missing both keys
    let missing_both = HashMap::from([
      ("other_key".to_string(), Value::String("value".to_string())),
    ]);
    store.insert("missing_both".to_string(), Value::Object(missing_both))?;

    flags.sync()?; // Ensure data is written to disk
  } // Drop the instance to ensure data is written

  // Create new instance - should only load valid entries
  let flags = FeatureFlags::new(temp_path, 1024, None)?;

  // Should only have the valid flag
  assert_eq!(flags.as_hashmap().len(), 1);
  assert!(flags.get("valid_flag").is_some());
  assert!(flags.get("missing_variant").is_none());
  assert!(flags.get("missing_timestamp").is_none());
  assert!(flags.get("missing_both").is_none());

  Ok(())
}
