// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use bd_bonjson::Value;
use bd_resilient_kv::kv_journal::KVJournal;
use bd_resilient_kv::{DoubleBufferedKVJournal, MemMappedKVJournal};
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
  Set,
  Delete,
  GetHashMap,
  Sync,
  Compress, // Test manual compression/journal switching
}

libfuzzer_sys::fuzz_target!(|data: Vec<Operation>| {
  // Create a temporary directory for the journal files
  let Ok(temp_dir) = TempDir::new() else { return };
  let file_path_a = temp_dir.path().join("fuzz_journal_a.dat");
  let file_path_b = temp_dir.path().join("fuzz_journal_b.dat");

  // Try to create two MemMappedKVJournals for the double buffered journal
  let Ok(journal_a) = MemMappedKVJournal::new(&file_path_a, 8192, Some(0.8), None) else {
    return;
  };

  let Ok(journal_b) = MemMappedKVJournal::new(&file_path_b, 8192, Some(0.8), None) else {
    return;
  };

  // Try to create a DoubleBufferedKVJournal
  let Ok(mut journal) = DoubleBufferedKVJournal::new(journal_a, journal_b) else {
    return;
  };

  for operation in data {
    match operation.op_type {
      OperationType::Set => {
        // Set operation - ignore result, we're testing for crashes
        let _ = journal.set(&operation.key, &operation.value.0);
      },
      OperationType::Delete => {
        // Delete operation - ignore result, we're testing for crashes
        let _ = journal.delete(&operation.key);
      },
      OperationType::GetHashMap => {
        // Get hashmap operation - ensure it doesn't crash
        let _ = journal.as_hashmap();
      },
      OperationType::Sync => {
        // Sync operation should not crash
        let _ = journal.sync();
      },
      OperationType::Compress => {
        // Test compression/journal switching - should not crash
        let _ = journal.compress();

        // After compression, basic operations should still work
        let _ = journal.sync();
        let _ = journal.as_hashmap();
      },
    }
  }

  // Final consistency check
  let _ = journal.sync();
  let _ = journal.as_hashmap();
});
