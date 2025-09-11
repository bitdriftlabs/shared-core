// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::*;
use tempfile::TempDir;

#[test]
fn test_cache_populated_from_store() -> anyhow::Result<()> {
  let temp_dir = TempDir::new()?;
  let temp_path = temp_dir.path();

  // Create first instance and add some flags
  {
    let mut flags = FeatureFlags::new(temp_path, 1024, None)?;
    flags.set("flag1".to_string(), Some("variant1".to_string()))?;
    flags.set("flag2".to_string(), None)?;
  } // This drops the instance, simulating persistence

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
