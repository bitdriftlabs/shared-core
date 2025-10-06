// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::kv_journal::{DoubleBufferedKVJournal, KVJournal};
use crate::InMemoryKVJournal;
use bd_bonjson::Value;
use std::collections::HashMap;

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
  for round in 0..10 {
    for key_num in 0..3 {
      let key = format!("key{}", key_num);
      let value = format!("data_round_{}_key_{}_content", round, key_num);
      journal.set(&key, &Value::String(value)).unwrap();
    }
  }

  // Verify that journal switching occurred due to high water mark
  let final_active_a = journal.is_active_journal_a();
  assert_ne!(initial_active_a, final_active_a, "Journal should have automatically switched when high water mark was reached");

  // High water mark should not be triggered on the double buffered journal since compaction succeeded
  assert!(!journal.is_high_water_mark_triggered(), "Double buffered journal's high water mark should not be triggered after successful compaction");
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
  let mut batch_data = HashMap::new();
  for i in 0..20 {
    batch_data.insert(
      format!("batch_key_{}", i),
      Value::String(format!("batch_value_{}", i)),
    );
  }

  // This should succeed and forward to the underlying journal
  let result = journal.set_multiple(&batch_data);
  assert!(result.is_ok(), "set_multiple should succeed when forwarded to underlying journal");

  // Verify the data was actually written
  let hashmap = journal.as_hashmap().unwrap();
  assert!(!hashmap.is_empty(), "Journal should contain the batch data");
  assert!(hashmap.len() >= batch_data.len(), "Journal should contain all batch entries");

  // Verify some of our specific entries are present
  assert!(hashmap.contains_key("batch_key_0"), "Should contain first batch entry");
  assert!(hashmap.contains_key("batch_key_19"), "Should contain last batch entry");
}

#[test]
fn test_double_buffered_high_water_mark_detection() {
  // This test verifies that when we exceed high water mark and compaction fails,
  // the double buffered journal's high water mark flag gets set correctly.

  let buffer_a = Box::leak(vec![0u8; 200].into_boxed_slice()); // Small buffer
  let buffer_b = Box::leak(vec![0u8; 200].into_boxed_slice()); // Small buffer

  // Create individual journals with 40% high water mark (40% of 200 = 80 bytes)
  let journal_a = InMemoryKVJournal::new(buffer_a, Some(0.4)).unwrap();
  let journal_b = InMemoryKVJournal::new(buffer_b, Some(0.4)).unwrap();

  let mut journal = DoubleBufferedKVJournal::new(journal_a, journal_b).unwrap();

  // Initially, high water mark should not be triggered
  assert!(!journal.is_high_water_mark_triggered(), "High water mark should not be triggered initially");

  let initial_active_a = journal.is_active_journal_a();

  // Add compactable data via set_multiple that will trigger high water mark but can be compacted
  let mut compactable_batch = HashMap::new();
  for i in 0..50 {
    compactable_batch.insert(
      format!("key{}", i % 3), // Only 3 unique keys, so updates will compact well
      Value::String(format!("compactable_value_data_that_takes_space_{}", i)),
    );
  }

  // This should trigger compaction, switch journals, and succeed without setting high water flag
  let result = journal.set_multiple(&compactable_batch);
  assert!(result.is_ok(), "Compactable batch operation should succeed");

  // At this point, compaction should have occurred and switched journals
  let post_compaction_active_a = journal.is_active_journal_a();
  assert_ne!(initial_active_a, post_compaction_active_a, "Journal should have switched due to compaction");

  // High water mark should be triggered if the current journal's high water mark is triggered
  // This happens regardless of whether the operation succeeded or failed

  // Now add a large amount of data that cannot be compacted successfully
  let mut large_batch = HashMap::new();
  for i in 0..10 {
    large_batch.insert(
      format!("large_key_{}", i),
      Value::String("very_long_value_that_will_fill_up_both_journals_completely_and_cannot_be_compressed".to_string()),
    );
  }

  // Execute the large batch operation
  let _result = journal.set_multiple(&large_batch);

  // The operation may fail if the data is too large, but the high water mark should be triggered
  // because even after compaction, the data is too large
  assert!(journal.is_high_water_mark_triggered(), "High water mark should be triggered when compaction fails or when journal is at capacity");
}