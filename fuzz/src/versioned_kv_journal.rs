// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use arbitrary::{Arbitrary, Unstructured};
use bd_proto::protos::state::payload::StateValue;
use bd_proto::protos::state::payload::state_value::Value_type;
use bd_resilient_kv::{DataLoss, VersionedKVStore};
use bd_time::{TestTimeProvider, TimeProvider as _};
use std::collections::HashSet;
use std::sync::Arc;
use tempfile::TempDir;
use time::macros::datetime;

// Wrapper for StateValue to implement Arbitrary
#[derive(Debug, Clone)]
struct ArbitraryStateValue(StateValue);

impl<'a> Arbitrary<'a> for ArbitraryStateValue {
  fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
    let variant: u8 = u.arbitrary()?;
    let mut value = StateValue::new();

    match variant % 5 {
      0 => {
        // Null value (no value_type set)
      },
      1 => {
        // String value
        let s: String = u.arbitrary()?;
        value.value_type = Some(Value_type::StringValue(s));
      },
      2 => {
        // Int value
        let i: i64 = u.arbitrary()?;
        value.value_type = Some(Value_type::IntValue(i));
      },
      3 => {
        // Double value
        let d: f64 = u.arbitrary()?;
        value.value_type = Some(Value_type::DoubleValue(d));
      },
      4 => {
        // Bool value
        let b: bool = u.arbitrary()?;
        value.value_type = Some(Value_type::BoolValue(b));
      },
      _ => unreachable!(),
    }

    Ok(Self(value))
  }
}

// Strategy for selecting which key to use
#[derive(Arbitrary, Debug, Clone)]
enum KeyStrategy {
  // Use an existing key from the key pool (by index)
  Existing(u8),
  // Generate a new key
  New(String),
}

// Types of corruption to apply to journal files
#[derive(Arbitrary, Debug, Clone)]
enum CorruptionType {
  // Flip random bytes
  FlipBytes { count: u8 },
  // Truncate file to a random size
  Truncate { size: u32 },
  // Zero out a region
  ZeroRegion { offset: u32, length: u16 },
}

#[derive(Arbitrary, Debug, Clone)]
enum CorruptionTarget {
  Header,
  Random,
}

#[derive(Arbitrary, Debug)]
enum OperationType {
  AdvanceTime(u64), // Advance time by specified microseconds
  Insert {
    key_strategy: KeyStrategy,
    value: ArbitraryStateValue,
  },
  Remove {
    key_strategy: KeyStrategy,
  },
  Get {
    key_strategy: KeyStrategy,
  },
  GetWithTimestamp {
    key_strategy: KeyStrategy,
  },
  ContainsKey {
    key_strategy: KeyStrategy,
  },
  Len,
  IsEmpty,
  AsHashMap,
  Sync,
  Reopen,
  ReopenWithCorruption {
    corruption: CorruptionType,
    target: CorruptionTarget,
  },
  RotateJournal,
}

#[derive(Arbitrary, Debug)]
pub struct VersionedKVJournalFuzzTestCase {
  buffer_size: u32,
  high_water_mark_ratio: Option<f32>,
  operations: Vec<OperationType>,
}

pub struct VersionedKVJournalFuzzTest {
  test_case: VersionedKVJournalFuzzTestCase,
}

impl VersionedKVJournalFuzzTest {
  #[must_use]
  pub fn new(test_case: VersionedKVJournalFuzzTestCase) -> Self {
    Self { test_case }
  }

  pub fn run(self) {
    // Create a temporary directory for the journal files
    let Ok(temp_dir) = TempDir::new() else {
      return;
    };

    // Clamp buffer size to reasonable range (1KB - 1MB)
    let buffer_size = ((self.test_case.buffer_size % 1_048_576) + 1024) as usize;

    // Clamp high water mark ratio to valid range [0.0, 1.0]
    let high_water_mark_ratio = self.test_case.high_water_mark_ratio.and_then(|ratio| {
      let clamped = ratio.clamp(0.0, 1.0);
      if clamped.is_finite() {
        Some(clamped)
      } else {
        None
      }
    });

    let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));

    // Create initial store
    let store_result = tokio::runtime::Runtime::new()
      .unwrap()
      .block_on(VersionedKVStore::new(
        temp_dir.path(),
        "fuzz_journal",
        buffer_size,
        high_water_mark_ratio,
        time_provider.clone(),
      ));

    let Ok((mut store, _data_loss)) = store_result else {
      return;
    };

    // Track expected state for validation
    let mut expected_keys = HashSet::<String>::new();
    // Pool of keys to reuse across operations
    let mut key_pool = Vec::<String>::new();

    // Helper function to select a key based on strategy
    let select_key = |strategy: &KeyStrategy, pool: &mut Vec<String>| -> String {
      match strategy {
        KeyStrategy::Existing(index) => {
          if pool.is_empty() {
            // No existing keys, create a default one
            let key = format!("key_{index}");
            pool.push(key.clone());
            key
          } else {
            // Use modulo to wrap index into valid range
            let idx = (*index as usize) % pool.len();
            pool[idx].clone()
          }
        },
        KeyStrategy::New(key) => {
          // Limit key pool size to avoid unbounded growth
          if pool.len() < 100 {
            pool.push(key.clone());
          }
          key.clone()
        },
      }
    };

    // Helper function to get current timestamp as microseconds
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let current_timestamp_micros =
      || -> u64 { (time_provider.now().unix_timestamp_nanos() / 1_000) as u64 };

    for operation in self.test_case.operations {
      match operation {
        OperationType::AdvanceTime(micros) => {
          // Limit time advance to 1 hour to prevent OffsetDateTime overflow
          const MAX_ADVANCE_MICROS: u64 = 3_600_000_000;
          let clamped_micros = micros.min(MAX_ADVANCE_MICROS);

          // Use try_into to safely convert, or skip if it would overflow
          if let Ok(micros_i64) = i64::try_from(clamped_micros) {
            time_provider.advance(time::Duration::microseconds(micros_i64));
          }
        },
        OperationType::Insert {
          key_strategy,
          value,
        } => {
          let key = select_key(&key_strategy, &mut key_pool);

          let timestamp = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(store.insert(key.clone(), value.0.clone()))
            .unwrap();

          // Since time is frozen unless advanced, the timestamp of the entry should be exactly the
          // current time.
          assert_eq!(timestamp, current_timestamp_micros());

          if value.0.value_type.is_some() {
            expected_keys.insert(key.clone());

            let retrieved = store.get(&key);
            assert!(
              retrieved.is_some(),
              "Value should be retrievable after insert"
            );

            // Verify timestamp is available
            let with_timestamp = store.get_with_timestamp(&key);
            assert!(with_timestamp.is_some());
            assert_eq!(with_timestamp.unwrap().timestamp, timestamp);
          } else {
            expected_keys.remove(&key);
            assert!(store.get(&key).is_none());
          }
        },
        OperationType::Remove { key_strategy } => {
          let key = select_key(&key_strategy, &mut key_pool);

          let timestamp = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(store.remove(&key))
            .unwrap();

          if expected_keys.contains(&key) {
            // Key existed, should get the current timestamp of removal.
            assert_eq!(timestamp, Some(current_timestamp_micros()));

            expected_keys.remove(&key);

            // Verify the value was removed
            assert!(store.get(&key).is_none());
          } else {
            // If key did not exist we'll get None timestamp since no change was made.
            assert_eq!(timestamp, None);
          }
        },
        OperationType::Get { key_strategy } => {
          let key = select_key(&key_strategy, &mut key_pool);
          let value = store.get(&key);

          // Verify consistency with expected state
          if expected_keys.contains(&key) {
            assert!(value.is_some(), "Expected key should exist");
          }
        },
        OperationType::GetWithTimestamp { key_strategy } => {
          let key = select_key(&key_strategy, &mut key_pool);
          let value = store.get_with_timestamp(&key);

          // Verify consistency
          if expected_keys.contains(&key) {
            assert!(value.is_some(), "Expected key should exist");
            assert!(value.unwrap().timestamp > 0, "Timestamp should be positive");
          }
        },
        OperationType::ContainsKey { key_strategy } => {
          let key = select_key(&key_strategy, &mut key_pool);
          let contains = store.contains_key(&key);

          // Verify consistency with expected state
          if expected_keys.contains(&key) {
            assert!(contains, "Expected key should be contained");
          }
        },
        OperationType::Len => {
          let len = store.len();
          // Verify the length matches expected state
          assert_eq!(
            len,
            expected_keys.len(),
            "Store length should match expected"
          );
        },
        OperationType::IsEmpty => {
          let is_empty = store.is_empty();
          assert_eq!(
            is_empty,
            expected_keys.is_empty(),
            "Store empty state should match expected"
          );
        },
        OperationType::AsHashMap => {
          let hashmap = store.as_hashmap();
          // Verify the hashmap size matches
          assert_eq!(
            hashmap.len(),
            expected_keys.len(),
            "HashMap length should match expected"
          );

          // Verify all expected keys are present
          for key in &expected_keys {
            assert!(
              hashmap.contains_key(key),
              "Expected key should be in hashmap"
            );
          }
        },
        OperationType::Sync => {
          // Sync operation should not crash
          let _ = store.sync();
        },
        OperationType::Reopen => {
          // Test persistence by reopening the store
          let name = "fuzz_journal";
          let dir = temp_dir.path();

          // Drop the store to release the file
          // TODO(snowp): ??
          store.sync().unwrap();
          drop(store);

          // Reopen the store
          let store_result =
            tokio::runtime::Runtime::new()
              .unwrap()
              .block_on(VersionedKVStore::open_existing(
                dir,
                name,
                buffer_size,
                high_water_mark_ratio,
                time_provider.clone(),
              ));

          let Ok((reopened_store, data_loss)) = store_result else {
            // If reopening fails, create a new store
            let Ok((new_store, _)) =
              tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(VersionedKVStore::new(
                  dir,
                  name,
                  buffer_size,
                  high_water_mark_ratio,
                  time_provider.clone(),
                ))
            else {
              return;
            };
            store = new_store;
            expected_keys.clear();
            continue;
          };

          store = reopened_store;

          // Verify data consistency after reopen
          if data_loss == DataLoss::Total {
            // Total data loss - clear expected state
            expected_keys.clear();
            key_pool.clear();
          } else {
            // Verify the length is reasonable
            let len = store.len();
            if data_loss == DataLoss::None {
              assert_eq!(
                len,
                expected_keys.len(),
                "Store length should match expected after reopen"
              );
            } else {
              // Partial data loss - update expected state and key pool
              expected_keys.clear();
              key_pool.clear();
              for key in store.as_hashmap().keys() {
                expected_keys.insert(key.clone());
                if key_pool.len() < 100 {
                  key_pool.push(key.clone());
                }
              }
            }
          }

          // Verify that what remains in the store matches expected keys.
          assert_eq!(
            store
              .as_hashmap()
              .keys()
              .cloned()
              .collect::<HashSet<_, _>>(),
            expected_keys,
            "Store keys should match expected after reopen"
          );
        },
        OperationType::ReopenWithCorruption { corruption, target } => {
          // Test recovery from corrupted journal files
          let name = "fuzz_journal";
          let dir = temp_dir.path();

          // Sync to ensure all data is written before corruption
          let _ = store.sync();

          // Drop the store to release the file
          drop(store);

          // Find the active journal file
          let (journal_path, _generation) =
            tokio::runtime::Runtime::new().unwrap().block_on(async {
              let pattern = format!("{name}.jrn.");
              let mut max_gen = 0u64;
              let Ok(mut entries) = tokio::fs::read_dir(dir).await else {
                return (dir.join(format!("{name}.jrn.{max_gen}")), max_gen);
              };

              while let Ok(Some(entry)) = entries.next_entry().await {
                let filename = entry.file_name();
                let filename_str = filename.to_string_lossy();

                if let Some(suffix) = filename_str.strip_prefix(&pattern)
                  && let Some(gen_str) = suffix.split('.').next()
                  && let Ok(generation) = gen_str.parse::<u64>()
                {
                  max_gen = max_gen.max(generation);
                }
              }

              let path = dir.join(format!("{name}.jrn.{max_gen}"));
              (path, max_gen)
            });

          // Apply corruption to the journal file
          if journal_path.exists() {
            let Ok(mut file_data) = std::fs::read(&journal_path) else {
              // If we can't read the file, skip corruption
              let Ok((new_store, _)) =
                tokio::runtime::Runtime::new()
                  .unwrap()
                  .block_on(VersionedKVStore::new(
                    dir,
                    name,
                    buffer_size,
                    high_water_mark_ratio,
                    time_provider.clone(),
                  ))
              else {
                return;
              };
              store = new_store;
              expected_keys.clear();
              key_pool.clear();
              continue;
            };

            // TODO(snowp): Consider targeting specific record types for more focused corruption.
            // This simply makes the fuzzer more likely to hit interesting cases.
            let range_end = match target {
              CorruptionTarget::Header => 17,
              CorruptionTarget::Random => file_data.len(),
            };

            match corruption {
              CorruptionType::FlipBytes { count } => {
                // Flip random bytes
                let flip_count = (count as usize).min(range_end);
                for i in 0 .. flip_count {
                  let idx = (i * 97) % range_end; // Pseudo-random distribution
                  file_data[idx] ^= 0xFF;
                }
              },
              CorruptionType::Truncate { size } => {
                // Truncate to a random size
                let new_size = (size as usize).min(file_data.len());
                file_data.truncate(new_size);
              },
              CorruptionType::ZeroRegion { offset, length } => {
                // Zero out a region
                let start = (offset as usize).min(range_end);
                let end = (start + length as usize).min(range_end);
                for byte in &mut file_data[start .. end] {
                  *byte = 0;
                }
              },
            }

            // Write the corrupted data back
            let _ = std::fs::write(&journal_path, file_data);
          }

          // Try to reopen the store with the corrupted journal
          let store_result =
            tokio::runtime::Runtime::new()
              .unwrap()
              .block_on(VersionedKVStore::open_existing(
                dir,
                name,
                buffer_size,
                high_water_mark_ratio,
                time_provider.clone(),
              ));

          let Ok((reopened_store, data_loss)) = store_result else {
            // If reopening fails due to corruption, create a new store
            let Ok((new_store, _)) =
              tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(VersionedKVStore::new(
                  dir,
                  name,
                  buffer_size,
                  high_water_mark_ratio,
                  time_provider.clone(),
                ))
            else {
              return;
            };
            store = new_store;
            expected_keys.clear();
            key_pool.clear();
            continue;
          };

          store = reopened_store;

          // If the journal claims no data was lost, we should see the same state as before
          // corruption.
          if data_loss == DataLoss::None {
            // No data loss - verify expected state
            assert_eq!(
              store.len(),
              expected_keys.len(),
              "Store length should match expected after corruption with no data loss"
            );

            for key in &expected_keys {
              assert!(
                store.contains_key(key),
                "Key should exist after corruption with no data loss"
              );
            }
          } else {
            // We saw some data loss so we need to update our expected state.
            expected_keys.clear();
            key_pool.clear();
          }

          // In the case of partial data loss, update expected keys based on what was recovered.
          if data_loss == DataLoss::Partial {
            // Update expected state based on what was recovered
            for key in store.as_hashmap().keys() {
              expected_keys.insert(key.clone());
              if key_pool.len() < 100 {
                key_pool.push(key.clone());
              }
            }
          }
        },
        OperationType::RotateJournal => {
          // Manually trigger rotation
          let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(store.rotate_journal());

          if result.is_ok() {
            // Verify the store is still functional after rotation
            let len = store.len();
            assert_eq!(
              len,
              expected_keys.len(),
              "Store length should match expected after rotation"
            );

            // Verify all expected keys are still present
            for key in &expected_keys {
              assert!(store.contains_key(key), "Key should exist after rotation");
            }
          }
        },
      }
    }

    // Final consistency check
    let _ = store.sync();
    let hashmap = store.as_hashmap();
    assert_eq!(
      hashmap.len(),
      expected_keys.len(),
      "Final store length should match expected"
    );
  }
}

#[test]
fn run_all_corpus() {
  crate::run_all_corpus(
    "corpus/versioned_kv_journal",
    |input: VersionedKVJournalFuzzTestCase| {
      VersionedKVJournalFuzzTest::new(input).run();
    },
  );
}
