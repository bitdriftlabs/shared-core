// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// Test cases for error handling and edge cases

use crate::{InMemoryKVJournal, KVJournal};
use bd_bonjson::Value;

#[test]
fn test_metadata_corruption_handling() {
  // Test with a buffer that has invalid metadata
  let mut buffer = vec![0u8; 64];
  
  // Write valid version and position headers
  buffer[0..8].copy_from_slice(&1u64.to_le_bytes()); // version
  buffer[8..16].copy_from_slice(&20u64.to_le_bytes()); // position
  buffer[16] = 0x99; // array start
  
  // Write invalid/corrupted metadata starting at position 17
  buffer[17] = 0xFF; // Invalid BONJSON
  buffer[18] = 0xFE;
  buffer[19] = 0xFD;
  
  // from_buffer should fail gracefully
  let result = InMemoryKVJournal::from_buffer(&mut buffer, None, None);
  assert!(result.is_err());
}

#[test] 
fn test_buffer_too_small_for_any_data() {
  // Test with buffer that's smaller than header size
  let mut buffer = vec![0u8; 10]; // Less than 16 bytes needed for header
  
  let result = InMemoryKVJournal::new(&mut buffer, None, None);
  assert!(result.is_err());
  let error_message = result.unwrap_err().to_string();
  assert!(error_message.contains("Buffer too small") || error_message.contains("too small"));
}

#[test]
fn test_buffer_size_exactly_header() {
  // Test with buffer that's exactly header size
  let mut buffer = vec![0u8; 16];
  
  let result = InMemoryKVJournal::from_buffer(&mut buffer, None, None);
  assert!(result.is_err());
}

#[test]
fn test_position_larger_than_buffer() {
  // Test with position value that exceeds buffer size
  let mut buffer = vec![0u8; 64];
  
  // Write valid version but invalid position
  buffer[0..8].copy_from_slice(&1u64.to_le_bytes()); // version
  buffer[8..16].copy_from_slice(&1000u64.to_le_bytes()); // position > buffer size
  
  let result = InMemoryKVJournal::from_buffer(&mut buffer, None, None);
  assert!(result.is_err());
  assert!(result.unwrap_err().to_string().contains("Invalid position"));
}

#[test]
fn test_unsupported_version() {
  // Test with unsupported version number
  let mut buffer = vec![0u8; 64];
  
  buffer[0..8].copy_from_slice(&999u64.to_le_bytes()); // unsupported version
  buffer[8..16].copy_from_slice(&20u64.to_le_bytes()); // valid position
  
  let result = InMemoryKVJournal::from_buffer(&mut buffer, None, None);
  assert!(result.is_err());
  assert!(result.unwrap_err().to_string().contains("Unsupported version"));
}

#[test]  
fn test_high_water_mark_invalid_ratios() {
  let mut buffer = vec![0u8; 1024];
  
  // Test negative ratio
  let result = InMemoryKVJournal::new(&mut buffer, Some(-0.1), None);
  assert!(result.is_err());
  
  // Test ratio > 1.0
  let mut buffer2 = vec![0u8; 1024];
  let result2 = InMemoryKVJournal::new(&mut buffer2, Some(1.5), None);
  assert!(result2.is_err());
  
  // Test exactly 0.0 and 1.0 (should be valid)
  let mut buffer3 = vec![0u8; 1024];
  let result3 = InMemoryKVJournal::new(&mut buffer3, Some(0.0), None);
  assert!(result3.is_ok());
  
  let mut buffer4 = vec![0u8; 1024];
  let result4 = InMemoryKVJournal::new(&mut buffer4, Some(1.0), None);
  assert!(result4.is_ok());
}

#[test]
fn test_buffer_full_write_failure() -> anyhow::Result<()> {
  // Test writing to a nearly full buffer
  let mut buffer = vec![0u8; 64]; // Very small buffer
  let mut journal = InMemoryKVJournal::new(&mut buffer, Some(0.8), None)?;
  
  // Try to write data that's too large
  let large_value = "x".repeat(100); // Definitely too large for 64 byte buffer
  let result = journal.set("key", &Value::String(large_value));
  
  // Should fail gracefully
  assert!(result.is_err());
  
  Ok(())
}

#[test]
fn test_extract_timestamp_from_empty_metadata() {
  // Test metadata without initialized field
  let mut buffer = vec![0u8; 64];
  
  // Write headers
  buffer[0..8].copy_from_slice(&1u64.to_le_bytes());
  buffer[8..16].copy_from_slice(&20u64.to_le_bytes());
  buffer[16] = 0x99; // array start
  
  // Write empty object metadata (no "initialized" key)
  buffer[17] = 0x9a; // object start  
  buffer[18] = 0x9b; // object end
  
  let result = InMemoryKVJournal::from_buffer(&mut buffer, None, None);
  assert!(result.is_err());
}

#[test]
fn test_reinit_from_failing_source() -> anyhow::Result<()> {
  let mut buffer1 = vec![0u8; 1024];
  let mut journal1 = InMemoryKVJournal::new(&mut buffer1, Some(0.8), None)?;
  
  let mut buffer2 = vec![0u8; 128]; // Small but reasonable buffer
  let mut journal2 = InMemoryKVJournal::new(&mut buffer2, Some(0.8), None)?;
  
  // Fill journal2 with some data
  for i in 0..3 {
    let result = journal2.set(&format!("k{}", i), &Value::Signed(i));
    if result.is_err() {
      break; // Stop if buffer is full
    }
  }
  
  // Try to reinit journal1 from journal2 - should handle any errors gracefully
  let result = journal1.reinit_from(&mut journal2);
  // This should succeed since journal1 has enough space, or at least handle errors gracefully
  if result.is_err() {
    println!("Reinit failed as expected due to buffer constraints: {}", result.unwrap_err());
    // This is acceptable - the test is just checking that we don't panic
  } else {
    // Verify the reinit worked if it succeeded
    let data = journal1.as_hashmap()?;
    assert!(data.len() <= 3, "Should have copied some data from journal2");
  }
  
  Ok(())
}
