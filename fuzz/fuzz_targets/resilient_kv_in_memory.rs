// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use bd_bonjson::Value;
use bd_resilient_kv::{InMemoryKVJournal, kvjournal::KVJournal};
use std::collections::HashMap;

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
}

libfuzzer_sys::fuzz_target!(|data: Vec<Operation>| {
  // Create a buffer for the in-memory journal
  let mut buffer = vec![0u8; 8192];

  // Try to create an InMemoryKVJournal
  let Ok(mut journal) = InMemoryKVJournal::new(&mut buffer, Some(0.8), None) else {
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
    }
  }

  // Basic consistency check - journal should be in a valid state
  let _ = journal.sync();
  let _ = journal.as_hashmap();
});
