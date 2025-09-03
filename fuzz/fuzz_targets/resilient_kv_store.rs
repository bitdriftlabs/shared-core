// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use bd_bonjson::Value;
use bd_resilient_kv::KVStore;
use std::collections::HashMap;
use tempfile::TempDir;

// Wrapper for Value to implement Arbitrary
#[derive(Debug, Clone)]
struct ArbitraryValue(Value);

impl<'a> Arbitrary<'a> for ArbitraryValue {
  fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
    let variant: u8 = u.arbitrary()?;
    Ok(Self(match variant % 8 {
      0 => Value::Null,
      1 => Value::Bool(u.arbitrary()?),
      2 => Value::Float(u.arbitrary()?),
      3 => Value::Signed(u.arbitrary()?),
      4 => Value::Unsigned(u.arbitrary()?),
      5 => Value::String(u.arbitrary()?),
      6 => {
        let len = u.int_in_range(0 ..= 3)?; // Keep arrays small
        let mut arr = Vec::new();
        for _ in 0 .. len {
          arr.push(Self::arbitrary(u)?.0);
        }
        Value::Array(arr)
      },
      7 => {
        let len = u.int_in_range(0 ..= 2)?; // Keep objects small
        let mut obj = HashMap::new();
        for _ in 0 .. len {
          let key: String = u.arbitrary()?;
          let value = Self::arbitrary(u)?.0;
          obj.insert(key, value);
        }
        Value::Object(obj)
      },
      _ => unreachable!(),
    }))
  }
}

#[derive(Arbitrary, Debug)]
struct Operation {
  op_type: OperationType,
  key: String,
  value: ArbitraryValue,
}

#[derive(Arbitrary, Debug)]
enum OperationType {
  Insert,
  Remove,
  Get,
  ContainsKey,
  Len,
  IsEmpty,
  Clear,
  Keys,
  Values,
  BufferUsageRatio,
  Sync,
  Compress,
  Reopen, // Special operation to test persistence
}

libfuzzer_sys::fuzz_target!(|data: Vec<Operation>| {
  // Create a temporary directory for the KVStore files
  let Ok(temp_dir) = TempDir::new() else { return };
  let base_path = temp_dir.path().join("fuzz_store");

  // Try to create a KVStore
  let Ok(mut store) = KVStore::new(&base_path, 8192, Some(0.8), None) else {
    return;
  };

  for operation in data {
    match operation.op_type {
      OperationType::Insert => {
        // Insert operation - ignore result, we're testing for crashes
        let _ = store.insert(operation.key.clone(), operation.value.0.clone());
      },
      OperationType::Remove => {
        // Remove operation - ignore result, we're testing for crashes
        let _ = store.remove(&operation.key);
      },
      OperationType::Get => {
        // Get operation - just ensure it doesn't crash
        let _ = store.get(&operation.key);
      },
      OperationType::ContainsKey => {
        // Contains key operation - should not crash
        let _ = store.contains_key(&operation.key);
      },
      OperationType::Len => {
        // Length operation should always work
        let _ = store.len();
      },
      OperationType::IsEmpty => {
        // Is empty check should always work
        let _ = store.is_empty();
      },
      OperationType::Clear => {
        // Clear operation
        let _ = store.clear();
      },
      OperationType::Keys => {
        // Keys method should not crash
        let _ = store.keys();
      },
      OperationType::Values => {
        // Values method should not crash
        let _ = store.values();
      },
      OperationType::BufferUsageRatio => {
        // Buffer usage ratio should always be valid
        let ratio = store.buffer_usage_ratio();
        // Basic sanity check
        assert!(
          (0.0 ..= 1.0).contains(&ratio),
          "Buffer usage ratio should be between 0 and 1"
        );
      },
      OperationType::Sync => {
        // Sync should not crash
        let _ = store.sync();
      },
      OperationType::Compress => {
        // Compress should not crash
        let _ = store.compress();

        // After compression, basic operations should still work
        let _ = store.len();
        let _ = store.is_empty();
      },
      OperationType::Reopen => {
        // Test persistence by recreating the store
        drop(store);

        // Try to reopen the store from the existing files
        store = match KVStore::new(&base_path, 8192, Some(0.8), None) {
          Ok(s) => s,
          Err(_) => return,
        };

        // After reopening, verify basic operations work
        let _ = store.len();
        let _ = store.is_empty();
      },
    }
  }

  // Final consistency check
  let _ = store.sync();
  let _ = store.len();
});
