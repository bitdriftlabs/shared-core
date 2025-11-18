// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use ahash::AHashMap;
use arbitrary::{Arbitrary, Unstructured};
use bd_proto::protos::state::payload::StateValue;
use bd_proto::protos::state::payload::state_value::Value_type;
use bd_resilient_kv::{DataLoss, TimestampedValue, VersionedKVStore};
use bd_time::{TestTimeProvider, TimeProvider as _};
use std::sync::Arc;
use tempfile::TempDir;
use time::macros::datetime;

const JOURNAL_NAME: &str = "fuzz_journal";

// Wrapper for StateValue to implement Arbitrary
#[derive(Debug, Clone)]
struct ArbitraryStateValue(StateValue);

#[derive(Arbitrary, Debug, Clone)]
enum ValueEdgeCase {
  EmptyString,
  LargeString, // ~10KB string
  MinInt,
  MaxInt,
  PositiveInfinity,
  NegativeInfinity,
  NegativeZero,
  NaN,
}

impl ValueEdgeCase {
  fn to_state_value(&self) -> StateValue {
    let mut value = StateValue::new();
    match self {
      Self::EmptyString => {
        value.value_type = Some(Value_type::StringValue(String::new()));
      },
      Self::LargeString => {
        value.value_type = Some(Value_type::StringValue("x".repeat(10_000)));
      },
      Self::MinInt => {
        value.value_type = Some(Value_type::IntValue(i64::MIN));
      },
      Self::MaxInt => {
        value.value_type = Some(Value_type::IntValue(i64::MAX));
      },
      Self::PositiveInfinity => {
        value.value_type = Some(Value_type::DoubleValue(f64::INFINITY));
      },
      Self::NegativeInfinity => {
        value.value_type = Some(Value_type::DoubleValue(f64::NEG_INFINITY));
      },
      Self::NegativeZero => {
        value.value_type = Some(Value_type::DoubleValue(-0.0));
      },
      Self::NaN => {
        value.value_type = Some(Value_type::DoubleValue(f64::NAN));
      },
    }
    value
  }
}

impl<'a> Arbitrary<'a> for ArbitraryStateValue {
  fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
    let variant: u8 = u.arbitrary()?;
    let mut value = StateValue::new();

    // 10% chance of edge case, 90% chance of normal arbitrary value
    if variant.is_multiple_of(10) {
      let edge_case: ValueEdgeCase = u.arbitrary()?;
      return Ok(Self(edge_case.to_state_value()));
    }

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

#[derive(Arbitrary, Debug, Clone)]
enum OperationType {
  AdvanceTime {
    microseconds: u64,
  },
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
  Sync,
  Reopen,
  ReopenWithCorruption {
    corruption: CorruptionType,
    target: CorruptionTarget,
  },
  RotateJournal,
  // Insert multiple entries to stress buffer
  BulkInsert {
    count: u8, // 1-256 entries
    key_prefix: String,
  },
  // Reopen without syncing first (test dirty state handling)
  ReopenWithoutSync,
}

/// Tracks keys created during the test to allow reuse between INSERT/GET/REMOVE operations.
struct KeyPool {
  keys: Vec<String>,
}

impl KeyPool {
  /// Selects a key for writing based on the provided strategy. This either resuses an existing key
  /// or generates a new one, updating the key pool accordingly.
  fn key_for_write(&mut self, strategy: &KeyStrategy) -> String {
    match strategy {
      KeyStrategy::Existing(index) => {
        if self.keys.is_empty() {
          // No existing keys, create a new one.
          let new_key = format!("key_{}", self.keys.len());
          self.keys.push(new_key.clone());
          new_key
        } else {
          // Use modulo to wrap index into valid range
          let idx = (*index as usize) % self.keys.len();
          self.keys[idx].clone()
        }
      },
      KeyStrategy::New(key) => {
        let new_key = format!("{}_{:x}", key, self.keys.len());
        self.keys.push(new_key.clone());
        new_key
      },
    }
  }

  /// Selects a key for reading based on the provided strategy. This either resuses an existing key
  /// or generates a new one without updating the key pool.
  fn key_for_read(&self, strategy: &KeyStrategy) -> String {
    match strategy {
      KeyStrategy::Existing(index) => {
        if self.keys.is_empty() {
          // No existing keys, create a new one. At this point we have not inserted any keys yet so
          // // we can just create a new key.
          format!("key_{}", self.keys.len())
        } else {
          // Use modulo to wrap index into valid range
          let idx = (*index as usize) % self.keys.len();
          self.keys[idx].clone()
        }
      },
      KeyStrategy::New(key) => {
        // Generate a new key that hasn't been used before.
        format!("{}_{:x}", key, self.keys.len())
      },
    }
  }
}

#[derive(Arbitrary, Debug)]
pub struct VersionedKVJournalFuzzTestCase {
  buffer_size: u32,
  high_water_mark_ratio: Option<f32>,
  operations: Vec<OperationType>,
}

pub struct VersionedKVJournalFuzzTest {
  test_case: VersionedKVJournalFuzzTestCase,
  buffer_size: usize,
  high_water_mark_ratio: Option<f32>,
  temp_dir: TempDir,
  time_provider: Arc<TestTimeProvider>,
  state: AHashMap<String, TimestampedValue>,
  keys: KeyPool,
}

impl VersionedKVJournalFuzzTest {
  #[must_use]
  pub fn new(test_case: VersionedKVJournalFuzzTestCase) -> Self {
    // Clamp to a reasonable range (1KB - 1MB)
    let buffer_size = ((test_case.buffer_size % 1_048_576) + 1024) as usize;
    // Clamp high water mark ratio to valid range [0.0, 1.0]
    let high_water_mark_ratio = test_case.high_water_mark_ratio.and_then(|ratio| {
      let clamped = ratio.clamp(0.0, 1.0);
      if clamped.is_finite() {
        Some(clamped)
      } else {
        None
      }
    });

    // Create a temporary directory for the journal files
    let Ok(temp_dir) = TempDir::new() else {
      panic!("Failed to create temporary directory");
    };

    let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));

    Self {
      test_case,
      buffer_size,
      high_water_mark_ratio,
      temp_dir,
      time_provider,
      state: AHashMap::default(),
      keys: KeyPool { keys: Vec::new() },
    }
  }

  async fn new_store(&self) -> anyhow::Result<(VersionedKVStore, DataLoss)> {
    VersionedKVStore::new(
      self.temp_dir.path(),
      JOURNAL_NAME,
      self.buffer_size,
      self.high_water_mark_ratio,
      self.time_provider.clone(),
    )
    .await
  }

  async fn existing_store(&self) -> anyhow::Result<(VersionedKVStore, DataLoss)> {
    VersionedKVStore::open_existing(
      self.temp_dir.path(),
      JOURNAL_NAME,
      self.buffer_size,
      self.high_water_mark_ratio,
      self.time_provider.clone(),
    )
    .await
  }

  pub fn run(self) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(self.run_async());
  }

  async fn run_async(mut self) {
    // Create initial store
    let store_result = self.new_store().await;

    let Ok((mut store, _data_loss)) = store_result else {
      panic!("Failed to create initial VersionedKVStore");
    };

    // Helper function to get current timestamp as microseconds
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let current_timestamp_micros =
      || -> u64 { (self.time_provider.now().unix_timestamp_nanos() / 1_000) as u64 };

    for operation in self.test_case.operations.clone() {
      match operation {
        OperationType::AdvanceTime { microseconds } => {
          // Limit time advance to 1 hour to prevent OffsetDateTime overflow
          const MAX_ADVANCE_MICROS: u64 = 3_600_000_000;
          let clamped_micros = microseconds.min(MAX_ADVANCE_MICROS);

          // Use try_into to safely convert, or skip if it would overflow
          if let Ok(micros_i64) = i64::try_from(clamped_micros) {
            self
              .time_provider
              .advance(time::Duration::microseconds(micros_i64));
          }
        },
        OperationType::Insert {
          key_strategy,
          value,
        } => {
          log::info!("Inserting key with strategy {key_strategy:?} and value {value:?}",);

          let key = self.keys.key_for_write(&key_strategy);

          let timestamp = store.insert(key.clone(), value.0.clone()).await.unwrap();

          // Since time is frozen unless advanced, the timestamp of the entry should be exactly the
          // current time.
          assert_eq!(timestamp, current_timestamp_micros());

          if value.0.value_type.is_some() {
            self.state.insert(
              key.clone(),
              TimestampedValue {
                value: value.0.clone(),
                timestamp,
              },
            );

            // Verify timestamp is available
            let with_timestamp = store.get_with_timestamp(&key);
            assert!(with_timestamp.is_some());
            assert_eq!(with_timestamp.unwrap().timestamp, timestamp);
          } else {
            self.state.remove(&key);
            assert!(store.get(&key).is_none());
          }
        },
        OperationType::Remove { key_strategy } => {
          let key = self.keys.key_for_read(&key_strategy);

          let timestamp = store.remove(&key).await.unwrap();

          if self.state.contains_key(&key) {
            // Key existed, should get the current timestamp of removal.
            assert_eq!(timestamp, Some(current_timestamp_micros()));

            self.state.remove(&key);

            // Verify the value was removed
            assert!(store.get(&key).is_none());
          } else {
            // If key did not exist we'll get None timestamp since no change was made.
            assert_eq!(timestamp, None);
          }
        },
        OperationType::Get { key_strategy } => {
          let key = self.keys.key_for_read(&key_strategy);
          let value = store.get_with_timestamp(&key);

          // Special handling to compare floating point NaN values correctly
          let expected_value = self.state.get(&key).cloned();
          match (&value, &expected_value) {
            (Some(_), None) => panic!("Got value for key that should not exist"),
            (None, Some(_)) => panic!("Did not get value for key that should exist"),
            (Some(value), Some(expected_value)) => assert!(compare_values(value, expected_value)),
            (None, None) => {},
          }
        },
        OperationType::Sync => {
          let _ = store.sync();
        },
        OperationType::Reopen => {
          // Drop the store to release the file
          store.sync().unwrap();
          drop(store);

          // Reopen the store
          let store_result = self.existing_store().await;

          let Ok((reopened_store, data_loss)) = store_result else {
            panic!("Failed to reopen existing VersionedKVStore");
          };

          store = reopened_store;

          assert_eq!(data_loss, DataLoss::None, "Unexpected data loss on reopen");
        },
        OperationType::ReopenWithCorruption { corruption, target } => {
          log::info!("Reopening with corruption: {corruption:?} targeting {target:?}",);

          // Sync to ensure all data is written before corruption
          let _ = store.sync();

          let journal_path = store.journal_path();

          drop(store);

          // Apply corruption to the journal file
          let Ok(mut file_data) = std::fs::read(&journal_path) else {
            panic!("Failed to read journal file for corruption");
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

          // Try to reopen the store with the corrupted journal
          let store_result = self.existing_store().await;

          let Ok((reopened_store, data_loss)) = store_result else {
            // If reopening fails due to corruption, create a new store
            let Ok((new_store, _)) = self.new_store().await else {
              panic!("Failed to reopen or create new VersionedKVStore after corruption");
            };
            store = new_store;
            self.state.clear();
            self.keys.keys.clear();
            continue;
          };

          log::info!("Reopened store with data loss: {data_loss:?}");

          store = reopened_store;

          // If the journal claims no data was lost, we should see the same state as before
          // corruption.
          if data_loss != DataLoss::None {
            // We saw some data loss so we need to update our expected state.
            self.state.clear();
          }

          // In the case of partial data loss, update expected keys based on what was recovered.
          if data_loss == DataLoss::Partial {
            // Update expected state based on what was recovered
            for (key, value) in store.as_hashmap() {
              self.state.insert(key.clone(), value.clone());
            }
          }
        },
        OperationType::RotateJournal => {
          // Manually trigger rotation
          store.rotate_journal().await.unwrap();
        },
        OperationType::BulkInsert { count, key_prefix } => {
          // Insert multiple entries to stress the buffer
          let insert_count = count.max(1) as usize; // At least 1 entry
          for i in 0 .. insert_count {
            let key = format!("{key_prefix}_{i}");
            // Generate a small arbitrary value for bulk inserts
            let mut value = StateValue::new();
            #[allow(clippy::cast_possible_wrap)]
            let int_value = i as i64;
            value.value_type = Some(Value_type::IntValue(int_value));

            let timestamp = store.insert(key.clone(), value.clone()).await.unwrap();

            // Track in our state
            self.state.insert(
              key.clone(),
              TimestampedValue {
                value: value.clone(),
                timestamp,
              },
            );

            // Add to key pool for future operations
            if !self.keys.keys.contains(&key) {
              self.keys.keys.push(key);
            }
          }
        },
        OperationType::ReopenWithoutSync => {
          // Drop the store without syncing first to test dirty state handling
          drop(store);

          // Reopen the store - might see data loss due to unsync'd data
          let store_result = self.existing_store().await;

          let Ok((reopened_store, data_loss)) = store_result else {
            panic!("Failed to reopen existing VersionedKVStore without sync");
          };

          store = reopened_store;

          // Update our expected state based on what was actually persisted
          if data_loss != DataLoss::None {
            self.state.clear();
            if data_loss == DataLoss::Partial {
              for (key, value) in store.as_hashmap() {
                self.state.insert(key.clone(), value.clone());
              }
            }
          }
        },
      }

      // Assert consistency after each operation
      assert!(
        compare_maps(store.as_hashmap(), &self.state),
        "State mismatch, {:?} vs {:?}",
        store.as_hashmap(),
        self.state
      );

      // Verify additional state invariants
      assert_eq!(
        store.len(),
        self.state.len(),
        "Length mismatch after operation"
      );
      assert_eq!(
        store.is_empty(),
        self.state.is_empty(),
        "is_empty mismatch after operation"
      );

      // Verify contains_key consistency for all known keys
      for key in &self.keys.keys {
        assert_eq!(
          store.contains_key(key),
          self.state.contains_key(key),
          "contains_key mismatch for key: {key}"
        );
      }
    }
  }
}

fn compare_maps(
  expected: &AHashMap<String, TimestampedValue>,
  actual: &AHashMap<String, TimestampedValue>,
) -> bool {
  if expected.len() != actual.len() {
    return false;
  }

  for (key, expected_value) in expected {
    match actual.get(key) {
      Some(actual_value) => {
        if !compare_values(expected_value, actual_value) {
          return false;
        }
      },
      None => return false,
    }
  }

  true
}

fn compare_values(expected: &TimestampedValue, actual: &TimestampedValue) -> bool {
  // In the case of both values having identical floating point NaN values, we need to handle that
  // specially since NaN != NaN.
  if let (
    Some(Value_type::DoubleValue(expected_double)),
    Some(Value_type::DoubleValue(actual_double)),
  ) = (&expected.value.value_type, &actual.value.value_type)
    && expected_double.is_nan()
    && actual_double.is_nan()
  {
    return expected.timestamp == actual.timestamp;
  }

  expected == actual
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
