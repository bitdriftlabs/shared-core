// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::InMemoryKVJournal;
use crate::kv_journal::{DoubleBufferedKVJournal, KVJournal};
use bd_bonjson::Value;

#[test]
fn test_double_buffered_automatic_switching() {
  // Test that automatic switching occurs when high water mark is reached
  let buffer_a = Box::leak(vec![0u8; 512].into_boxed_slice()); // Larger buffer
  let buffer_b = Box::leak(vec![0u8; 512].into_boxed_slice()); // Larger buffer

  // Create individual journals with 40% high water mark
  let journal_a = InMemoryKVJournal::new(buffer_a, Some(0.4)).unwrap();
  let journal_b = InMemoryKVJournal::new(buffer_b, Some(0.4)).unwrap();

  let mut journal = DoubleBufferedKVJournal::new(journal_a, journal_b).unwrap();

  let initial_active_a = journal.is_active_journal_a();

  // Fill with data that will trigger high water mark through repeated updates to same keys
  // This creates compactable data (many entries for same keys)
  for round in 0 .. 10 {
    for key_num in 0 .. 3 {
      let key = format!("key{}", key_num);
      let value = format!("data_round_{}_key_{}_content", round, key_num);
      journal.set(&key, &Value::String(value)).unwrap();
    }
  }

  // Verify that journal switching occurred due to high water mark
  let final_active_a = journal.is_active_journal_a();
  assert_ne!(
    initial_active_a, final_active_a,
    "Journal should have automatically switched when high water mark was reached"
  );

  // High water mark should not be triggered on the double buffered journal since compaction
  // succeeded
  assert!(
    !journal.is_high_water_mark_triggered(),
    "Double buffered journal's high water mark should not be triggered after successful compaction"
  );
}

#[test]
fn test_double_buffered_set_multiple_forwards_correctly() {
  // Test that set_multiple is properly forwarded to the underlying journals
  let buffer_a = Box::leak(vec![0u8; 1024].into_boxed_slice());
  let buffer_b = Box::leak(vec![0u8; 1024].into_boxed_slice());

  // Create individual journals first
  let journal_a = InMemoryKVJournal::new(buffer_a, Some(0.7)).unwrap();
  let journal_b = InMemoryKVJournal::new(buffer_b, Some(0.7)).unwrap();

  let mut journal = DoubleBufferedKVJournal::new(journal_a, journal_b).unwrap();

  // Create data for batch operation
  let batch_data: Vec<(String, Value)> = (0..20)
    .map(|i| {
      (
        format!("batch_key_{}", i),
        Value::String(format!("batch_value_{}", i)),
      )
    })
    .collect();

  // This should succeed and forward to the underlying journal
  let result = journal.set_multiple(&batch_data);
  assert!(
    result.is_ok(),
    "set_multiple should succeed when forwarded to underlying journal"
  );

  // Verify the data was actually written
  let hashmap = journal.as_hashmap().unwrap();
  assert!(!hashmap.is_empty(), "Journal should contain the batch data");
  assert!(
    hashmap.len() >= batch_data.len(),
    "Journal should contain all batch entries"
  );

  // Verify some of our specific entries are present
  assert!(
    hashmap.contains_key("batch_key_0"),
    "Should contain first batch entry"
  );
  assert!(
    hashmap.contains_key("batch_key_19"),
    "Should contain last batch entry"
  );
}

#[test]
fn test_double_buffered_high_water_mark_detection() {
  // This test verifies that when we exceed high water mark and compaction fails,
  // the double buffered journal's high water mark flag gets set correctly.

  let buffer_a = Box::leak(vec![0u8; 800].into_boxed_slice()); // Larger buffer
  let buffer_b = Box::leak(vec![0u8; 800].into_boxed_slice()); // Larger buffer

  // Create individual journals with 70% high water mark (70% of 800 = 560 bytes)
  let journal_a = InMemoryKVJournal::new(buffer_a, Some(0.7)).unwrap();
  let journal_b = InMemoryKVJournal::new(buffer_b, Some(0.7)).unwrap();

  let mut journal = DoubleBufferedKVJournal::new(journal_a, journal_b).unwrap();

  // Initially, high water mark should not be triggered
  assert!(
    !journal.is_high_water_mark_triggered(),
    "High water mark should not be triggered initially"
  );

  // Step 1: Fill buffer to near capacity with small entries first
  let small_batch: Vec<(String, Value)> = (0..5)
    .map(|i| {
      (
        format!("key{}", i),
        Value::String("small".to_string()),
      )
    })
    .collect();

  let result = journal.set_multiple(&small_batch);
  assert!(result.is_ok(), "Small batch should succeed");

  // Step 2: Add medium-sized entries to approach high water mark
  let medium_batch: Vec<(String, Value)> = (0..10)
    .map(|i| {
      (
        format!("medium_key_{:02}", i),
        Value::String("medium_sized_value_to_fill_buffer".to_string()),
      )
    })
    .collect();

  let result = journal.set_multiple(&medium_batch);
  assert!(result.is_ok(), "Medium batch should succeed");

  // Step 3: Add more data to trigger high water mark
  let trigger_batch: Vec<(String, Value)> = (0..15)
    .map(|i| {
      (
        format!("trigger_key_{:03}", i),
        Value::String("this_value_should_trigger_high_water_mark".to_string()),
      )
    })
    .collect();

  // This operation might succeed or fail, but should trigger high water mark
  let _result = journal.set_multiple(&trigger_batch);

  // The high water mark should be triggered by now due to buffer pressure
  assert!(
    journal.is_high_water_mark_triggered(),
    "High water mark should be triggered when buffer approaches capacity"
  );
}
