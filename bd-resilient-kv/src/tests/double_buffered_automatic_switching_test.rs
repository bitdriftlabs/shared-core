// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::kv_journal::{DoubleBufferedKVJournal, KVJournal};
use crate::InMemoryKVJournal;
use bd_bonjson::Value;

#[test]
fn test_double_buffered_automatic_compaction_on_high_water_mark() {
  // Test that compaction works when the same keys are updated repeatedly.
  // The journal will accumulate multiple entries for the same keys (append-only),
  // but compaction will reduce it to just the final values, fitting below high water mark.

  let buffer_a = Box::leak(vec![0u8; 512].into_boxed_slice());
  let buffer_b = Box::leak(vec![0u8; 512].into_boxed_slice());

  // Create journals with 40% high water mark (40% of 512 = ~204 bytes)
  let journal_a = InMemoryKVJournal::new(buffer_a, Some(0.4)).unwrap();
  let journal_b = InMemoryKVJournal::new(buffer_b, Some(0.4)).unwrap();

  let mut journal = DoubleBufferedKVJournal::new(journal_a, journal_b).unwrap();

  let initial_active_a = journal.is_active_journal_a();

  // Repeatedly update the same few keys with different data to fill up the journal
  // This will cause many duplicate entries in the append-only journal
  for round in 0..20 {
    for key_num in 0..3 {
      let key = format!("key{}", key_num);
      let value = format!("data_round_{}_key_{}_with_some_extra_content", round, key_num);
      journal.set(&key, &Value::String(value)).unwrap();
    }
  }

  // This should have triggered high water mark due to many duplicate entries,
  // causing automatic compaction that reduces to just 3 final key-value pairs

  // Verify that journal switching occurred (compaction was triggered)
  let final_active_a = journal.is_active_journal_a();
  assert_ne!(initial_active_a, final_active_a, "Journal should have switched due to high water mark");

  // Verify automatic compaction occurred by checking the journal state
  // After compaction, the high water mark should not be triggered since space was freed

  // Verify data is accessible and only contains the final values
  let hashmap = journal.as_hashmap().unwrap();
  assert_eq!(hashmap.len(), 3, "Should have exactly 3 keys after compaction");

  // Verify the final values are the last ones written (round 19)
  for key_num in 0..3 {
    let key = format!("key{}", key_num);
    let expected_value = format!("data_round_19_key_{}_with_some_extra_content", key_num);
    assert_eq!(hashmap.get(&key), Some(&Value::String(expected_value)),
               "Should have the final value for {}", key);
  }
}

#[test]
fn test_double_buffered_user_callback_when_both_journals_full() {
  // Test that the high water mark flag is set when both journals are full and compaction can't help
  let buffer_a = Box::leak(vec![0u8; 128].into_boxed_slice()); // Very small buffers
  let buffer_b = Box::leak(vec![0u8; 128].into_boxed_slice()); // Very small buffers

  // Create individual journals with very low high water mark to quickly fill both
  let journal_a = InMemoryKVJournal::new(buffer_a, Some(0.3)).unwrap();
  let journal_b = InMemoryKVJournal::new(buffer_b, Some(0.3)).unwrap();

  let mut journal = DoubleBufferedKVJournal::new(journal_a, journal_b).unwrap();

  // Initially the high water mark should not be triggered
  assert!(!journal.is_high_water_mark_triggered(), "High water mark should not be triggered initially");

  // Fill with data until we hit the situation where both journals are full
  let mut successful_writes = 0;
  for i in 0..50 {
    let result = journal.set(&format!("key{}", i), &Value::String(format!("very_long_value_to_quickly_fill_buffers_{}", i)));
    if result.is_ok() {
      successful_writes += 1;
    } else {
      // Once we start getting errors, stop
      break;
    }
  }

  // We should have written some data successfully
  assert!(successful_writes > 0, "Should have written some data successfully");

  // Now try one more write that should trigger the scenario where both journals are full
  let _final_result = journal.set("final_key", &Value::String("final_value_that_will_trigger_compaction_failure".to_string()));

  // Verify the high water mark flag is set when compaction couldn't resolve the issue
  assert!(journal.is_high_water_mark_triggered(), "High water mark should be triggered when both journals are full");
}