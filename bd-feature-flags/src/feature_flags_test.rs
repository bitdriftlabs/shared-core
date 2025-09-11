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

#[test]
fn test_new_empty_store() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let temp_path = temp_dir.path();

  let flags = FeatureFlags::new(temp_path, 1024, None)?;

  assert_eq!(flags.as_hashmap().len(), 0);
  assert!(flags.get("nonexistent").is_none());

  Ok(())
}

#[test]
fn test_set_and_get_string_variant() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let temp_path = temp_dir.path();

  let mut flags = FeatureFlags::new(temp_path, 1024, None)?;

  flags.set("test_flag".to_string(), Some("test_variant".to_string()))?;

  let flag = flags.get("test_flag").expect("flag should exist");
  assert_eq!(flag.variant, Some("test_variant".to_string()));
  assert!(flag.timestamp > 0);

  Ok(())
}

#[test]
fn test_set_and_get_none_variant() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let temp_path = temp_dir.path();

  let mut flags = FeatureFlags::new(temp_path, 1024, None)?;

  flags.set("test_flag".to_string(), None)?;

  let flag = flags.get("test_flag").expect("flag should exist");
  assert_eq!(flag.variant, None);
  assert!(flag.timestamp > 0);

  Ok(())
}

#[test]
fn test_set_empty_string_variant() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let temp_path = temp_dir.path();

  let mut flags = FeatureFlags::new(temp_path, 1024, None)?;

  flags.set("test_flag".to_string(), Some(String::new()))?;

  let flag = flags.get("test_flag").expect("flag should exist");
  assert_eq!(flag.variant, Some(String::new()));
  assert!(flag.timestamp > 0);

  Ok(())
}

#[test]
fn test_overwrite_existing_flag() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let temp_path = temp_dir.path();

  let mut flags = FeatureFlags::new(temp_path, 1024, None)?;

  // Set initial value
  flags.set("test_flag".to_string(), Some("initial_variant".to_string()))?;
  let initial_flag = flags.get("test_flag").expect("flag should exist");
  let initial_timestamp = initial_flag.timestamp;
  assert_eq!(initial_flag.variant, Some("initial_variant".to_string()));

  // Wait a bit and overwrite with new value
  std::thread::sleep(std::time::Duration::from_millis(1));
  flags.set("test_flag".to_string(), Some("updated_variant".to_string()))?;

  let updated_flag = flags.get("test_flag").expect("flag should exist");
  assert_eq!(updated_flag.variant, Some("updated_variant".to_string()));
  assert!(updated_flag.timestamp > initial_timestamp);

  Ok(())
}

#[test]
fn test_set_multiple_flags() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let temp_path = temp_dir.path();

  let mut flags = FeatureFlags::new(temp_path, 1024, None)?;

  flags.set("flag1".to_string(), Some("variant1".to_string()))?;
  flags.set("flag2".to_string(), None)?;
  flags.set("flag3".to_string(), Some("variant3".to_string()))?;

  assert_eq!(flags.as_hashmap().len(), 3);

  let flag1 = flags.get("flag1").expect("flag1 should exist");
  assert_eq!(flag1.variant, Some("variant1".to_string()));

  let flag2 = flags.get("flag2").expect("flag2 should exist");
  assert_eq!(flag2.variant, None);

  let flag3 = flags.get("flag3").expect("flag3 should exist");
  assert_eq!(flag3.variant, Some("variant3".to_string()));

  Ok(())
}

#[test]
fn test_clear() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let temp_path = temp_dir.path();

  let mut flags = FeatureFlags::new(temp_path, 1024, None)?;

  // Add some flags
  flags.set("flag1".to_string(), Some("variant1".to_string()))?;
  flags.set("flag2".to_string(), None)?;
  assert_eq!(flags.as_hashmap().len(), 2);

  // Clear all flags
  flags.clear()?;
  assert_eq!(flags.as_hashmap().len(), 0);
  assert!(flags.get("flag1").is_none());
  assert!(flags.get("flag2").is_none());

  Ok(())
}

#[test]
fn test_persistence_across_instances() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let temp_path = temp_dir.path();

  // Create first instance and set some flags
  {
    let mut flags = FeatureFlags::new(temp_path, 1024, None)?;
    flags.set("persistent_flag1".to_string(), Some("variant1".to_string()))?;
    flags.set("persistent_flag2".to_string(), None)?;
    flags.sync()?; // Ensure data is written
  } // Drop the instance

  // Create second instance and verify flags are loaded
  let flags = FeatureFlags::new(temp_path, 1024, None)?;
  assert_eq!(flags.as_hashmap().len(), 2);

  let flag1 = flags.get("persistent_flag1").expect("flag1 should exist");
  assert_eq!(flag1.variant, Some("variant1".to_string()));

  let flag2 = flags.get("persistent_flag2").expect("flag2 should exist");
  assert_eq!(flag2.variant, None);

  Ok(())
}

#[test]
fn test_clear_persistence() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let temp_path = temp_dir.path();

  // Create first instance, add flags, then clear
  {
    let mut flags = FeatureFlags::new(temp_path, 1024, None)?;
    flags.set("temp_flag".to_string(), Some("temp_variant".to_string()))?;
    flags.clear()?;
    flags.sync()?; // Ensure clear is written
  } // Drop the instance

  // Create second instance and verify no flags are loaded
  let flags = FeatureFlags::new(temp_path, 1024, None)?;
  assert_eq!(flags.as_hashmap().len(), 0);
  assert!(flags.get("temp_flag").is_none());

  Ok(())
}

#[test]
fn test_get_nonexistent_flag() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let temp_path = temp_dir.path();

  let flags = FeatureFlags::new(temp_path, 1024, None)?;

  assert!(flags.get("nonexistent_flag").is_none());

  Ok(())
}

#[test]
fn test_timestamps_are_different() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let temp_path = temp_dir.path();

  let mut flags = FeatureFlags::new(temp_path, 1024, None)?;

  flags.set("flag1".to_string(), Some("variant1".to_string()))?;
  std::thread::sleep(std::time::Duration::from_millis(1));
  flags.set("flag2".to_string(), Some("variant2".to_string()))?;

  let flag1 = flags.get("flag1").expect("flag1 should exist");
  let flag2 = flags.get("flag2").expect("flag2 should exist");

  assert!(flag2.timestamp > flag1.timestamp);

  Ok(())
}

#[test]
fn test_unicode_flag_names_and_variants() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let temp_path = temp_dir.path();

  let mut flags = FeatureFlags::new(temp_path, 1024, None)?;

  flags.set("日本語フラグ".to_string(), Some("バリアント１".to_string()))?;
  flags.set("🚀flag".to_string(), Some("🎯variant".to_string()))?;

  let jp_flag = flags.get("日本語フラグ").expect("Japanese flag should exist");
  assert_eq!(jp_flag.variant, Some("バリアント１".to_string()));

  let emoji_flag = flags.get("🚀flag").expect("Emoji flag should exist");
  assert_eq!(emoji_flag.variant, Some("🎯variant".to_string()));

  Ok(())
}

#[test]
fn test_very_long_flag_names_and_variants() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let temp_path = temp_dir.path();

  let mut flags = FeatureFlags::new(temp_path, 1024, None)?;

  let long_name = "a".repeat(100);  // Reduced to reasonable size
  let long_variant = "b".repeat(100);  // Reduced to reasonable size

  flags.set(long_name.clone(), Some(long_variant.clone()))?;

  let flag = flags.get(&long_name).expect("long flag should exist");
  assert_eq!(flag.variant, Some(long_variant));

  Ok(())
}

#[test]
fn test_special_characters_in_names_and_variants() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let temp_path = temp_dir.path();

  let mut flags = FeatureFlags::new(temp_path, 1024, None)?;

  let special_name = "flag-with.special_chars@domain.com";
  let special_variant = "variant with spaces\nand\tnewlines";

  flags.set(special_name.to_string(), Some(special_variant.to_string()))?;

  let flag = flags.get(special_name).expect("special flag should exist");
  assert_eq!(flag.variant, Some(special_variant.to_string()));

  Ok(())
}

#[test]
fn test_negative_timestamp_entries_discarded() -> anyhow::Result<()> {
  use bd_bonjson::Value;
  use std::collections::HashMap;

  let temp_dir = TempDir::new()?;
  let temp_path = temp_dir.path();

  // Create a store and manually insert an entry with negative timestamp
  {
    let mut flags = FeatureFlags::new(temp_path, 1024, None)?;

    // Add a valid flag first
    flags.set("valid_flag".to_string(), Some("variant1".to_string()))?;

    // Manually insert invalid entry with negative timestamp
    let store = &mut flags.flags_store;
    let negative_timestamp_entry = HashMap::from([
      ("v".to_string(), Value::String("variant".to_string())),
      ("t".to_string(), Value::Signed(-12345)),
    ]);
    store.insert("negative_timestamp".to_string(), Value::Object(negative_timestamp_entry))?;

    flags.sync()?;
  }

  // Create new instance - should only load valid entries
  let flags = FeatureFlags::new(temp_path, 1024, None)?;

  // Should only have the valid flag, negative timestamp entry should be discarded
  assert_eq!(flags.as_hashmap().len(), 1);
  assert!(flags.get("valid_flag").is_some());
  assert!(flags.get("negative_timestamp").is_none());

  Ok(())
}

#[test]
fn test_malformed_objects_discarded() -> anyhow::Result<()> {
  use bd_bonjson::Value;

  let temp_dir = TempDir::new()?;
  let temp_path = temp_dir.path();

  // Create a store and manually insert malformed entries
  {
    let mut flags = FeatureFlags::new(temp_path, 1024, None)?;

    // Add a valid flag first
    flags.set("valid_flag".to_string(), Some("variant1".to_string()))?;

    // Manually insert malformed entries directly into the store
    let store = &mut flags.flags_store;

    // Non-object value
    store.insert("string_entry".to_string(), Value::String("not_an_object".to_string()))?;

    // Array value
    store.insert("array_entry".to_string(), Value::Array(vec![Value::String("item".to_string())]))?;

    // Integer value
    store.insert("int_entry".to_string(), Value::Unsigned(42))?;

    flags.sync()?;
  }

  // Create new instance - should only load valid entries
  let flags = FeatureFlags::new(temp_path, 1024, None)?;

  // Should only have the valid flag, malformed entries should be discarded
  assert_eq!(flags.as_hashmap().len(), 1);
  assert!(flags.get("valid_flag").is_some());
  assert!(flags.get("string_entry").is_none());
  assert!(flags.get("array_entry").is_none());
  assert!(flags.get("int_entry").is_none());

  Ok(())
}

#[test]
fn test_signed_timestamp_conversion() -> anyhow::Result<()> {
  use bd_bonjson::Value;
  use std::collections::HashMap;

  let temp_dir = TempDir::new()?;
  let temp_path = temp_dir.path();

  // Create a store and manually insert an entry with valid signed timestamp
  {
    let mut flags = FeatureFlags::new(temp_path, 1024, None)?;

    // Manually insert entry with positive signed timestamp
    let store = &mut flags.flags_store;
    let signed_timestamp_entry = HashMap::from([
      ("v".to_string(), Value::String("variant".to_string())),
      ("t".to_string(), Value::Signed(12345)),  // Positive signed value
    ]);
    store.insert("signed_timestamp".to_string(), Value::Object(signed_timestamp_entry))?;

    flags.sync()?;
  }

  // Create new instance - should load the signed timestamp entry
  let flags = FeatureFlags::new(temp_path, 1024, None)?;

  assert_eq!(flags.as_hashmap().len(), 1);
  let flag = flags.get("signed_timestamp").expect("signed timestamp flag should exist");
  assert_eq!(flag.variant, Some("variant".to_string()));
  assert_eq!(flag.timestamp, 12345);

  Ok(())
}

#[test]
fn test_empty_string_variant_loaded_correctly() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let temp_path = temp_dir.path();

  // Test that empty string variant is stored as empty string but loaded as None
  {
    let mut flags = FeatureFlags::new(temp_path, 1024, None)?;
    flags.set("empty_variant_flag".to_string(), Some(String::new()))?;

    // Check that in the current instance, empty string is preserved
    let flag = flags.get("empty_variant_flag").expect("empty variant flag should exist");
    assert_eq!(flag.variant, Some(String::new()));    flags.sync()?;
  }

  // Create new instance and check that empty string variant is loaded as None
  let flags = FeatureFlags::new(temp_path, 1024, None)?;

  let flag = flags.get("empty_variant_flag").expect("empty variant flag should exist");
  // Empty string should be converted to None when loading from storage
  assert_eq!(flag.variant, None);

  Ok(())
}

#[test]
fn test_variant_with_variant_key_conflicts() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let temp_path = temp_dir.path();

  let mut flags = FeatureFlags::new(temp_path, 1024, None)?;

  // Set flags that might conflict with internal keys
  flags.set("v".to_string(), Some("variant_v".to_string()))?;
  flags.set("t".to_string(), Some("variant_t".to_string()))?;

  let flag_v = flags.get("v").expect("flag 'v' should exist");
  assert_eq!(flag_v.variant, Some("variant_v".to_string()));

  let flag_t = flags.get("t").expect("flag 't' should exist");
  assert_eq!(flag_t.variant, Some("variant_t".to_string()));

  Ok(())
}

#[test]
fn test_zero_timestamp_valid() -> anyhow::Result<()> {
  use bd_bonjson::Value;
  use std::collections::HashMap;

  let temp_dir = TempDir::new()?;
  let temp_path = temp_dir.path();

  // Create a store and manually insert an entry with zero timestamp
  {
    let mut flags = FeatureFlags::new(temp_path, 1024, None)?;

    let store = &mut flags.flags_store;
    let zero_timestamp_entry = HashMap::from([
      ("v".to_string(), Value::String("variant".to_string())),
      ("t".to_string(), Value::Unsigned(0)),
    ]);
    store.insert("zero_timestamp".to_string(), Value::Object(zero_timestamp_entry))?;

    flags.sync()?;
  }

  // Create new instance - should load the zero timestamp entry
  let flags = FeatureFlags::new(temp_path, 1024, None)?;

  assert_eq!(flags.as_hashmap().len(), 1);
  let flag = flags.get("zero_timestamp").expect("zero timestamp flag should exist");
  assert_eq!(flag.variant, Some("variant".to_string()));
  assert_eq!(flag.timestamp, 0);

  Ok(())
}

#[test]
fn test_large_buffer_size() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let temp_path = temp_dir.path();

  // Test with larger buffer size
  let mut flags = FeatureFlags::new(temp_path, 10240, None)?;

  // Set multiple flags
  for i in 0..100 {
    flags.set(format!("flag_{i}"), Some(format!("variant_{i}")))?;
  }

  assert_eq!(flags.as_hashmap().len(), 100);

  // Verify a few random flags
  let flag_50 = flags.get("flag_50").expect("flag_50 should exist");
  assert_eq!(flag_50.variant, Some("variant_50".to_string()));

  Ok(())
}

#[test]
fn test_high_water_mark_ratio() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let temp_path = temp_dir.path();

  // Test with custom high water mark ratio
  let mut flags = FeatureFlags::new(temp_path, 1024, Some(0.8))?;

  flags.set("test_flag".to_string(), Some("test_variant".to_string()))?;

  let flag = flags.get("test_flag").expect("flag should exist");
  assert_eq!(flag.variant, Some("test_variant".to_string()));

  Ok(())
}
