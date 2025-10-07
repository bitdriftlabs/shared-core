// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::KVJournal;
use bd_bonjson::Value;
use std::collections::HashMap;

/// A double-buffered implementation of `KVJournal` that automatically switches between two
/// journal instances when one reaches its high water mark.
///
/// This type holds two journal instances and switches between them when necessary:
/// - Forwards `KVJournal` APIs to the currently active journal
/// - When the active journal passes its high water mark, uses `reinit_from` to initialize the other
///   journal with the compressed journal state of the full one
/// - Once the other journal is initialized, begins forwarding APIs to that journal
#[derive(Debug)]
pub struct DoubleBufferedKVJournal<A: KVJournal, B: KVJournal> {
  journal_a: A,
  journal_b: B,
  journal_a_is_active: bool,
}

impl<A: KVJournal, B: KVJournal> DoubleBufferedKVJournal<A, B> {
  /// Create a new double-buffered KV journal using the provided journal instances.
  /// The journal with the most recent initialization timestamp will be set as the active journal.
  /// If both journals have the same timestamp (or no timestamp), `journal_a` will be the active
  /// journal.
  ///
  /// # Arguments
  /// * `journal_a` - A journal instance
  /// * `journal_b` - Another journal instance
  ///
  /// # Returns
  /// A new `DoubleBufferedKVJournal` instance with the most recently initialized journal active.
  pub fn new(journal_a: A, journal_b: B) -> anyhow::Result<Self> {
    let init_time_a = journal_a.get_init_time();
    let init_time_b = journal_b.get_init_time();
    let has_data_a = !journal_a.as_hashmap()?.is_empty();
    let has_data_b = !journal_b.as_hashmap()?.is_empty();

    // Choose the active journal based on data presence and timestamps
    let active_journal_a = match (has_data_a, has_data_b) {
      (true, false) => true,  // Only A has data
      (false, true) => false, // Only B has data
      (true, true) | (false, false) => init_time_a >= init_time_b, /* Both have data or neither
                                * has data, so use timestamps */
    };

    Ok(Self {
      journal_a,
      journal_b,
      journal_a_is_active: active_journal_a,
    })
  }

  /// Force compression by reinitializing the inactive journal from the active journal and switching
  /// to it. This is useful for manually triggering compression to reduce fragmentation and
  /// optimize storage.
  ///
  /// # Errors
  /// Returns an error if the compression (reinit) operation fails.
  pub fn compress(&mut self) -> anyhow::Result<()> {
    self.switch_journals()
  }

  /// Get which journal is currently active (true = `journal_a`, false = `journal_b`).
  /// This is useful for testing and debugging.
  #[cfg(test)]
  pub fn is_active_journal_a(&self) -> bool {
    self.journal_a_is_active
  }

  /// Switch to the inactive journal by reinitializing it from the active journal.
  fn switch_journals(&mut self) -> anyhow::Result<()> {
    // Reinitialize the inactive journal from the active one
    if self.journal_a_is_active {
      self.journal_b.reinit_from(&self.journal_a)?;
    } else {
      self.journal_a.reinit_from(&self.journal_b)?;
    }

    // Switch active journal
    self.journal_a_is_active = !self.journal_a_is_active;

    Ok(())
  }

  /// Execute an operation with the currently active journal.
  fn with_active_journal_mut<T, F>(&mut self, f: F) -> anyhow::Result<T>
  where
    F: FnOnce(&mut dyn KVJournal) -> anyhow::Result<T>,
  {
    let journal = self.active_journal_mut();
    let hwm_was_already_triggered = journal.is_high_water_mark_triggered();
    let result = f(journal)?;
    let hwm_is_now_triggered = journal.is_high_water_mark_triggered();

    if !hwm_was_already_triggered && hwm_is_now_triggered {
      // Call switch_journals to attempt compaction
      self.switch_journals()?;
    }

    Ok(result)
  }

  /// Get a reference to the currently active journal.
  fn active_journal(&self) -> &dyn KVJournal {
    if self.journal_a_is_active {
      &self.journal_a
    } else {
      &self.journal_b
    }
  }

  /// Get a mutable reference to the currently active journal.
  fn active_journal_mut(&mut self) -> &mut dyn KVJournal {
    if self.journal_a_is_active {
      &mut self.journal_a
    } else {
      &mut self.journal_b
    }
  }
}

impl<A: KVJournal, B: KVJournal> KVJournal for DoubleBufferedKVJournal<A, B> {
  fn high_water_mark(&self) -> usize {
    self.active_journal().high_water_mark()
  }

  fn is_high_water_mark_triggered(&self) -> bool {
    self.active_journal().is_high_water_mark_triggered()
  }

  fn buffer_usage_ratio(&self) -> f32 {
    self.active_journal().buffer_usage_ratio()
  }

  fn get_init_time(&self) -> u64 {
    self.active_journal().get_init_time()
  }

  fn set(&mut self, key: &str, value: &Value) -> anyhow::Result<()> {
    self.with_active_journal_mut(|journal| journal.set(key, value))
  }

  /// Set multiple key-value pairs in this journal.
  ///
  /// This will forward the operation to the currently active journal. If the operation
  /// triggers a high water mark, the journal will automatically switch to the other buffer
  /// and attempt compaction. If compaction succeeds and there's space, a retry will be attempted.
  ///
  /// Note: Setting any value to `Value::Null` will mark that entry for DELETION!
  ///
  /// # Errors
  /// Returns an error if any journal entry cannot be written. If an error occurs,
  /// no data will have been written.
  fn set_multiple(&mut self, entries: &[(String, Value)]) -> anyhow::Result<()> {
    // First attempt using the standard logic
    let result = self.with_active_journal_mut(|journal| journal.set_multiple(entries));

    // If it failed and our high water mark isn't triggered, try again
    // (this handles the case where the underlying journal filled up but compaction freed space)
    if result.is_err() && !self.is_high_water_mark_triggered() {
      self.with_active_journal_mut(|journal| journal.set_multiple(entries))
    } else {
      result
    }
  }

  fn delete(&mut self, key: &str) -> anyhow::Result<()> {
    self.with_active_journal_mut(|journal| journal.delete(key))
  }

  fn clear(&mut self) -> anyhow::Result<()> {
    self.with_active_journal_mut(|journal| journal.clear())
  }

  fn as_hashmap(&self) -> anyhow::Result<HashMap<String, Value>> {
    self.active_journal().as_hashmap()
  }

  fn reinit_from(&mut self, other: &dyn KVJournal) -> anyhow::Result<()> {
    self.with_active_journal_mut(|journal| journal.reinit_from(other))
  }

  fn sync(&self) -> anyhow::Result<()> {
    self.active_journal().sync()
  }
}
