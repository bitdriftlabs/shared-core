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
use bd_resilient_kv::{
  DataLoss,
  PersistentStoreConfig,
  RetentionRegistry,
  Scope,
  TimestampedValue,
  UpdateError,
  VERSIONED_JOURNAL_HEADER_SIZE,
  VersionedKVStore,
};
use bd_time::{TestTimeProvider, TimeProvider as _};
use std::sync::Arc;
use tempfile::TempDir;
use time::macros::datetime;

const JOURNAL_NAME: &str = "fuzz_journal";

// Wrapper for Scope to implement Arbitrary
#[derive(Debug, Clone, Copy)]
struct ArbitraryScope(Scope);

impl<'a> Arbitrary<'a> for ArbitraryScope {
  fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
    let variant: u8 = u.arbitrary()?;
    Ok(Self(match variant % 2 {
      0 => Scope::FeatureFlag,
      1 => Scope::GlobalState,
      _ => unreachable!(),
    }))
  }
}

// Wrapper for StateValue to implement Arbitrary
#[derive(Debug, Clone)]
struct ArbitraryStateValue(StateValue);

#[derive(Arbitrary, Debug, Clone)]
enum ValueEdgeCase {
  EmptyString,
  LargeString,          // ~10KB string
  VeryLargeString,      // ~100KB string
  ExtremelyLargeString, // ~1MB string (may exceed buffer capacity)
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
      Self::VeryLargeString => {
        value.value_type = Some(Value_type::StringValue("y".repeat(100_000)));
      },
      Self::ExtremelyLargeString => {
        value.value_type = Some(Value_type::StringValue("z".repeat(1_000_000)));
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
  // Generate a new key with a scope
  New(ArbitraryScope, String),
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
    scope: ArbitraryScope,
    key_prefix: String,
  },
  // Reopen without syncing first (test dirty state handling)
  ReopenWithoutSync,
  // Extend with arbitrary entries (different from BulkInsert which uses simple int values)
  Extend {
    entries: Vec<(ArbitraryScope, String, ArbitraryStateValue)>,
  },
}

/// Tracks keys created during the test to allow reuse between INSERT/GET/REMOVE operations.
struct KeyPool {
  keys: Vec<(Scope, String)>,
}

impl KeyPool {
  /// Selects a key for writing based on the provided strategy. This either resuses an existing key
  /// or generates a new one, updating the key pool accordingly.
  fn key_for_write(&mut self, strategy: &KeyStrategy) -> (Scope, String) {
    match strategy {
      KeyStrategy::Existing(index) => {
        if self.keys.is_empty() {
          // No existing keys, create a new one.
          let new_key = (Scope::FeatureFlag, format!("key_{}", self.keys.len()));
          self.keys.push(new_key.clone());
          new_key
        } else {
          // Use modulo to wrap index into valid range
          let idx = (*index as usize) % self.keys.len();
          self.keys[idx].clone()
        }
      },
      KeyStrategy::New(scope, key) => {
        let new_key = (scope.0, format!("{}_{:x}", key, self.keys.len()));
        self.keys.push(new_key.clone());
        new_key
      },
    }
  }

  /// Selects a key for reading based on the provided strategy. This either resuses an existing key
  /// or generates a new one without updating the key pool.
  fn key_for_read(&self, strategy: &KeyStrategy) -> (Scope, String) {
    match strategy {
      KeyStrategy::Existing(index) => {
        if self.keys.is_empty() {
          // No existing keys, create a new one. At this point we have not inserted any keys yet so
          // // we can just create a new key.
          (Scope::FeatureFlag, format!("key_{}", self.keys.len()))
        } else {
          // Use modulo to wrap index into valid range
          let idx = (*index as usize) % self.keys.len();
          self.keys[idx].clone()
        }
      },
      KeyStrategy::New(scope, key) => {
        // Generate a new key that hasn't been used before.
        (scope.0, format!("{}_{:x}", key, self.keys.len()))
      },
    }
  }
}

#[derive(Arbitrary, Debug)]
pub struct VersionedKVJournalFuzzTestCase {
  buffer_size: u32,
  high_water_mark_ratio: f32,
  /// Maximum capacity in bytes for dynamic growth (will be clamped to reasonable range)
  max_capacity_bytes: u32,
  operations: Vec<OperationType>,
}

/// Classification of buffer capacity errors
enum CapacityErrorKind {
  /// A single entry is too large for the buffer (> 50% of buffer size)
  OversizedEntry,
  /// Buffer is full due to accumulated entries
  BufferFull,
  /// Not a capacity error
  Other,
}

pub struct VersionedKVJournalFuzzTest {
  test_case: VersionedKVJournalFuzzTestCase,
  buffer_size: usize,
  high_water_mark_ratio: f32,
  max_capacity_bytes: usize,
  temp_dir: TempDir,
  time_provider: Arc<TestTimeProvider>,
  registry: Arc<RetentionRegistry>,
  state: AHashMap<(Scope, String), TimestampedValue>,
  keys: KeyPool,
  /// Track whether the journal is full (capacity exceeded)
  is_full: bool,
}

impl VersionedKVJournalFuzzTest {
  #[must_use]
  pub fn new(test_case: VersionedKVJournalFuzzTestCase) -> Self {
    // Clamp to a reasonable range (4KB - 1MB)
    // Note: 4KB is the minimum required by PersistentStoreConfig
    // Config validation will automatically round to power of 2 if needed
    let buffer_size = ((test_case.buffer_size % 1_048_576) + 4096) as usize;

    // Clamp high water mark ratio to valid range [0.1, 1.0]
    let mut high_water_mark_ratio = test_case.high_water_mark_ratio.clamp(0.1, 1.0);
    if high_water_mark_ratio.is_nan() {
      high_water_mark_ratio = 0.8; // Default value
    }

    // Clamp max capacity to a reasonable range (must be >= buffer_size, <= 1MB)
    // Treat 0 as default (1MB)
    let max_capacity_bytes = if test_case.max_capacity_bytes == 0 {
      1024 * 1024 // Default to 1MB
    } else {
      let max_usize = test_case.max_capacity_bytes as usize;
      // Ensure max >= buffer_size and <= 1MB
      max_usize.max(buffer_size).min(1024 * 1024)
    };

    // Create a temporary directory for the journal files
    let Ok(temp_dir) = TempDir::new() else {
      panic!("Failed to create temporary directory");
    };

    let time_provider = Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00:00 UTC)));
    // Create a registry with no retention handles - this prevents snapshot compression
    // during rotation, significantly speeding up fuzzing
    let registry = Arc::new(RetentionRegistry::new());

    Self {
      test_case,
      buffer_size,
      high_water_mark_ratio,
      max_capacity_bytes,
      temp_dir,
      time_provider,
      registry,
      state: AHashMap::default(),
      keys: KeyPool { keys: Vec::new() },
      is_full: false,
    }
  }

  async fn new_store(&self) -> anyhow::Result<(VersionedKVStore, DataLoss)> {
    let config = PersistentStoreConfig {
      initial_buffer_size: self.buffer_size,
      max_capacity_bytes: self.max_capacity_bytes,
      high_water_mark_ratio: self.high_water_mark_ratio,
    };
    VersionedKVStore::new(
      self.temp_dir.path(),
      JOURNAL_NAME,
      config,
      self.time_provider.clone(),
      self.registry.clone(),
    )
    .await
  }

  async fn existing_store(&self) -> anyhow::Result<(VersionedKVStore, DataLoss)> {
    let config = PersistentStoreConfig {
      initial_buffer_size: self.buffer_size,
      max_capacity_bytes: self.max_capacity_bytes,
      high_water_mark_ratio: self.high_water_mark_ratio,
    };
    VersionedKVStore::new(
      self.temp_dir.path(),
      JOURNAL_NAME,
      config,
      self.time_provider.clone(),
      self.registry.clone(),
    )
    .await
  }

  /// Estimate the size of a single entry in bytes.
  ///
  /// This computes the actual size that would be required to store this key-value pair
  /// in the journal using the Frame encoding.
  fn estimate_entry_size(key: &str, value: &StateValue, timestamp: u64) -> usize {
    use bd_resilient_kv::versioned_kv_journal::framing::Frame;
    // Use a dummy scope and timestamp for size calculation
    // The actual scope doesn't affect size calculation (it's always 1 byte)
    Frame::compute_encoded_size(key, timestamp, value)
  }

  /// Estimate the size of the current state in bytes.
  /// We estimate conservatively to avoid false positives.
  fn estimate_state_size(&self) -> usize {
    let mut total_size = VERSIONED_JOURNAL_HEADER_SIZE;

    for ((_, key_str), timestamped_value) in &self.state {
      // Use our entry size estimator for consistency
      let entry_size = Self::estimate_entry_size(
        key_str,
        &timestamped_value.value,
        timestamped_value.timestamp,
      );
      total_size += entry_size;
    }

    total_size
  }

  /// Verify that when the buffer is flagged as full, the state is actually using
  /// a significant portion of the buffer capacity.
  ///
  /// This validation is lenient for cases with very large individual entries that
  /// may not fit in max capacity, focusing on detecting genuine incorrect full detection.
  fn verify_full_flag(&self) {
    if self.is_full {
      let estimated_size = self.estimate_state_size();

      // Check if we have any entries that individually exceed max capacity.
      // Such entries are genuinely oversized and should legitimately cause CapacityExceeded.
      let has_oversized_entry = self.state.iter().any(|((_, key_str), tv)| {
        Self::estimate_entry_size(
          key_str,
          &tv.value,
          (self
            .time_provider
            .now()
            .unix_timestamp_nanos()
            .checked_div(1_000)
            .unwrap())
          .try_into()
          .unwrap(),
        ) > self.max_capacity_bytes
      });

      if has_oversized_entry {
        // When we have oversized entries (larger than max capacity), we can't reliably
        // validate capacity usage since even a single entry cannot fit. This is an expected
        // failure case.
        log::debug!(
          "Buffer full with oversized entries (> max_capacity) present, skipping capacity \
           validation"
        );
        return;
      }

      // We expect the state to use at least 50% of the buffer size when flagged as full.
      // This threshold is conservative to account for estimation errors and overhead.
      let min_expected_size = self.buffer_size / 2;

      assert!(
        estimated_size >= min_expected_size,
        "Buffer flagged as full but state only uses ~{} bytes out of {} buffer capacity (expected \
         at least {}). This suggests incorrect full detection.",
        estimated_size,
        self.buffer_size,
        min_expected_size
      );
    }
  }

  /// Classify the kind of error that is being returned.
  ///
  /// Returns the kind of capacity error, or `Other` if it's not a capacity error.
  fn classify_error(
    error: &UpdateError,
    entry_size_estimate: Option<usize>,
    _buffer_size: usize,
    max_capacity_bytes: usize,
  ) -> CapacityErrorKind {
    if matches!(error, UpdateError::CapacityExceeded) {
      // If we have an entry size estimate, determine if it's an oversized entry.
      // An entry is considered oversized if it exceeds max_capacity_bytes.
      // Entries smaller than max_capacity should trigger automatic rotation and succeed.
      if let Some(entry_size) = entry_size_estimate
        && entry_size > max_capacity_bytes
      {
        return CapacityErrorKind::OversizedEntry;
      }

      return CapacityErrorKind::BufferFull;
    }

    CapacityErrorKind::Other
  }

  pub fn run(self) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(self.run_async());
  }

  async fn run_async(mut self) {
    // Create initial store
    let store_result = self.new_store().await;

    let Ok((mut store, _data_loss)) = store_result else {
      panic!(
        "Failed to create initial VersionedKVStore: {:?}. Config: buffer_size={}, \
         max_capacity={:?}",
        store_result.err(),
        self.buffer_size,
        self.max_capacity_bytes
      );
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

          let (scope, key_str) = self.keys.key_for_write(&key_strategy);

          let result = store.insert(scope, key_str.clone(), value.0.clone()).await;

          match result {
            Ok(timestamp) => {
              // Reset full flag on successful insert
              self.is_full = false;

              // Since time is frozen unless advanced, the timestamp of the entry should be exactly
              // the current time.
              assert_eq!(timestamp, current_timestamp_micros());

              if value.0.value_type.is_some() {
                self.state.insert(
                  (scope, key_str.clone()),
                  TimestampedValue {
                    value: value.0.clone(),
                    timestamp,
                  },
                );

                // Verify timestamp is available
                let with_timestamp = store.get_with_timestamp(scope, &key_str);
                assert!(with_timestamp.is_some());
                assert_eq!(with_timestamp.unwrap().timestamp, timestamp);
              } else {
                self.state.remove(&(scope, key_str.clone()));
                assert!(store.get(scope, &key_str).is_none());
              }
            },
            Err(e) => {
              // Classify the error
              let entry_size =
                Self::estimate_entry_size(&key_str, &value.0, current_timestamp_micros());
              match Self::classify_error(
                &e,
                Some(entry_size),
                self.buffer_size,
                self.max_capacity_bytes,
              ) {
                CapacityErrorKind::OversizedEntry => {
                  log::info!(
                    "Single entry too large (~{entry_size} bytes) for max capacity ({} bytes), \
                     skipping insert",
                    self.max_capacity_bytes
                  );
                  // Don't set is_full - this is an oversized entry, not accumulated capacity
                  continue;
                },
                CapacityErrorKind::BufferFull => {
                  log::info!(
                    "Journal full (entry size ~{entry_size} bytes), cannot insert more entries"
                  );
                  self.is_full = true;
                  continue;
                },
                CapacityErrorKind::Other => {
                  // Unexpected error, propagate
                  panic!("Unexpected error during insert: {e}");
                },
              }
            },
          }
        },
        OperationType::Remove { key_strategy } => {
          let (scope, key_str) = self.keys.key_for_read(&key_strategy);

          let result = store.remove(scope, &key_str).await;

          match result {
            Ok(timestamp) => {
              // Reset full flag on successful remove
              self.is_full = false;

              let key = (scope, key_str.clone());
              if self.state.contains_key(&key) {
                // Key existed, should get the current timestamp of removal.
                assert_eq!(timestamp, Some(current_timestamp_micros()));

                self.state.remove(&key);

                // Verify the value was removed
                assert!(store.get(scope, &key_str).is_none());
              } else {
                // If key did not exist we'll get None timestamp since no change was made.
                assert_eq!(timestamp, None);
              }
            },
            Err(e) => {
              // Classify the error (remove operations write small deletion markers)
              match Self::classify_error(&e, None, self.buffer_size, self.max_capacity_bytes) {
                CapacityErrorKind::BufferFull | CapacityErrorKind::OversizedEntry => {
                  log::info!("Journal full, cannot remove entries");
                  self.is_full = true;
                  continue;
                },
                CapacityErrorKind::Other => {
                  // Unexpected error, propagate
                  panic!("Unexpected error during remove: {e}");
                },
              }
            },
          }
        },
        OperationType::Get { key_strategy } => {
          let (scope, key_str) = self.keys.key_for_read(&key_strategy);
          let value = store.get_with_timestamp(scope, &key_str);

          // Special handling to compare floating point NaN values correctly
          let key = (scope, key_str);
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

          let Some(journal_path) = store.journal_path() else {
            log::info!("Skipping corruption test for in-memory store");
            continue;
          };

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
            // Reset full flag since we have a fresh store with no data
            self.is_full = false;
            continue;
          };

          log::info!("Reopened store with data loss: {data_loss:?}");

          store = reopened_store;

          // If the journal claims no data was lost, we should see the same state as before
          // corruption.
          if data_loss != DataLoss::None {
            // We saw some data loss so we need to update our expected state.
            self.state.clear();
            // Reset full flag since we lost data
            self.is_full = false;
          }
          // If no data loss, keep is_full flag as-is since we recovered the same state

          // In the case of partial data loss, update expected keys based on what was recovered.
          if data_loss == DataLoss::Partial {
            // Update expected state based on what was recovered
            for (scope, key, value) in store.as_hashmap().iter() {
              self.state.insert((scope, key.clone()), value.clone());
            }
          }
        },
        OperationType::RotateJournal => {
          // Manually trigger rotation
          let result = store.rotate_journal().await;
          match result {
            Ok(_) => {
              // Reset full flag after successful rotation
              self.is_full = false;
            },
            Err(e) => {
              panic!("Unexpected error during rotation: {e}");
            },
          }
        },
        OperationType::BulkInsert {
          count,
          scope,
          key_prefix,
        } => {
          // Insert multiple entries to stress the buffer
          // Limit to 64 entries max for faster fuzzing (still sufficient to test bulk operations)
          let insert_count = (count.max(1) as usize).min(64);
          let scope = scope.0;
          for i in 0 .. insert_count {
            let key_str = format!("{key_prefix}_{i}");
            // Generate a small arbitrary value for bulk inserts
            let mut value = StateValue::new();
            #[allow(clippy::cast_possible_wrap)]
            let int_value = i as i64;
            value.value_type = Some(Value_type::IntValue(int_value));

            let result = store.insert(scope, key_str.clone(), value.clone()).await;

            match result {
              Ok(timestamp) => {
                // Reset full flag on successful insert
                self.is_full = false;

                let key = (scope, key_str.clone());
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
              },
              Err(e) => {
                // Classify the error (bulk inserts use small int values)
                let entry_size =
                  Self::estimate_entry_size(&key_str, &value, current_timestamp_micros());
                match Self::classify_error(
                  &e,
                  Some(entry_size),
                  self.buffer_size,
                  self.max_capacity_bytes,
                ) {
                  CapacityErrorKind::OversizedEntry => {
                    log::info!(
                      "Entry too large (~{entry_size} bytes) during bulk insert at entry {i}, \
                       stopping bulk insert"
                    );
                    break;
                  },
                  CapacityErrorKind::BufferFull => {
                    log::info!(
                      "Journal full during bulk insert at entry {i}, stopping bulk insert"
                    );
                    self.is_full = true;
                    break;
                  },
                  CapacityErrorKind::Other => {
                    // Unexpected error, propagate
                    panic!("Unexpected error during bulk insert: {e}");
                  },
                }
              },
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
          assert_eq!(
            data_loss,
            DataLoss::None,
            "Unexpected data loss on reopen without sync"
          );
        },
        OperationType::Extend { entries } => {
          // Limit batch size to avoid excessive fuzzing time
          let batch_size = entries.len().min(32);
          let entries_to_insert: Vec<_> = entries
            .into_iter()
            .take(batch_size)
            .map(|(scope, key, value)| {
              // Use key pool to track keys
              let tracked_key = format!("batch_{}_{:x}", key, self.keys.keys.len());
              (scope.0, tracked_key, value.0)
            })
            .collect();

          if entries_to_insert.is_empty() {
            // Skip empty batches
            continue;
          }

          log::info!("Extending {} entries", entries_to_insert.len());

          let result = store.extend(entries_to_insert.clone()).await;

          match result {
            Ok(timestamp) => {
              // Reset full flag on successful extend
              self.is_full = false;

              // Process all entries into our state model first (in order)
              // This handles cases where the same key appears multiple times in the batch
              for (scope, key_str, value) in &entries_to_insert {
                let key = (*scope, key_str.clone());
                if value.value_type.is_some() {
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
                } else {
                  // Deletion
                  self.state.remove(&key);
                }
              }

              // Now verify the final state matches the store
              for (scope, key_str, _value) in entries_to_insert {
                let key = (scope, key_str.clone());

                // Check what the final state should be based on our model
                let expected_value = self.state.get(&key);
                let actual_value = store.get_with_timestamp(scope, &key_str);

                match (expected_value, actual_value) {
                  (Some(expected), Some(actual)) => {
                    assert_eq!(
                      actual.timestamp, timestamp,
                      "Timestamp mismatch for key {key:?}"
                    );
                    assert_eq!(
                      &actual.value, &expected.value,
                      "Value mismatch for key {key:?}"
                    );
                  },
                  (None, None) => {
                    // Both agree the key doesn't exist - correct
                  },
                  (Some(_), None) => {
                    panic!("Expected key {key:?} to exist in store but it doesn't");
                  },
                  (None, Some(_)) => {
                    panic!("Expected key {key:?} to not exist in store but it does");
                  },
                }
              }
            },
            Err(e) => {
              // Classify the error
              let total_size: usize = entries_to_insert
                .iter()
                .map(|(_, key_str, value)| {
                  Self::estimate_entry_size(key_str, value, current_timestamp_micros())
                })
                .sum();

              match Self::classify_error(
                &e,
                Some(total_size),
                self.buffer_size,
                self.max_capacity_bytes,
              ) {
                CapacityErrorKind::OversizedEntry | CapacityErrorKind::BufferFull => {
                  log::info!("Extend failed due to capacity (~{total_size} bytes), journal full");
                  self.is_full = true;
                  continue;
                },
                CapacityErrorKind::Other => {
                  // Unexpected error, propagate
                  panic!("Unexpected error during extend: {e}");
                },
              }
            },
          }
        },
      }

      // Verify that if buffer is flagged as full, the state is close to the size of the buffer.
      self.verify_full_flag();

      // Ensure that the store's state matches our expected state
      assert!(
        compare_maps(store.as_hashmap(), &self.state),
        "State mismatch, {:?} vs {:?}",
        store.as_hashmap(),
        self.state
      );
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
      for (scope, key_str) in &self.keys.keys {
        assert_eq!(
          store.contains_key(*scope, key_str),
          self.state.contains_key(&(*scope, key_str.clone())),
          "contains_key mismatch for key: ({scope:?}, {key_str})"
        );
      }
    }
  }
}

fn compare_maps(
  actual_maps: &bd_resilient_kv::ScopedMaps,
  expected: &AHashMap<(Scope, String), TimestampedValue>,
) -> bool {
  if actual_maps.len() != expected.len() {
    return false;
  }

  for (scope, key, actual_value) in actual_maps.iter() {
    match expected.get(&(scope, key.clone())) {
      Some(expected_value) => {
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
