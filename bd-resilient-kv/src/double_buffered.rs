// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::KVJournal;
use bd_bonjson::Value;
use std::collections::HashMap;

/// A double-buffered implementation of KVJournal that automatically switches between two
/// journal instances when one reaches its high water mark.
/// 
/// This type holds two journal instances and switches between them when necessary:
/// - Forwards KVJournal APIs to the currently active journal
/// - When the active journal passes its high water mark, uses `reinit_from` to initialize
///   the other journal with the compressed journal state of the full one
/// - Once the other journal is initialized, begins forwarding APIs to that journal
pub struct DoubleBufferedKVJournal<A: KVJournal, B: KVJournal> {
  /// The primary journal instance
  journal_a: A,
  /// The secondary journal instance
  journal_b: B,
  /// Which journal is currently active (true = journal_a, false = journal_b)
  active_journal_a: bool,
  /// Whether we're in the middle of a journal switch operation
  switching: bool,
}

impl<A: KVJournal, B: KVJournal> DoubleBufferedKVJournal<A, B> {
  /// Create a new double-buffered KV journal using the provided journal instances.
  /// 
  /// # Arguments
  /// * `journal_a` - The primary journal instance
  /// * `journal_b` - The secondary journal instance
  ///
  /// # Returns
  /// A new `DoubleBufferedKVJournal` instance with `journal_a` as the initially active journal.
  pub fn new(journal_a: A, journal_b: B) -> Self {
    Self {
      journal_a,
      journal_b,
      active_journal_a: true,
      switching: false,
    }
  }

  /// Execute an operation with the currently active journal.
  fn with_active_journal<T, F>(&mut self, f: F) -> anyhow::Result<T>
  where
    F: FnOnce(&mut dyn KVJournal) -> anyhow::Result<T>,
  {
    // Avoid borrowing issues by checking switching state first
    if self.switching {
      // If we're switching, just execute on the active journal without switching logic
      let result = if self.active_journal_a {
        f(&mut self.journal_a)
      } else {
        f(&mut self.journal_b)
      };
      return result;
    }

    // Execute operation and check for high water mark
    let (result, high_water_triggered) = if self.active_journal_a {
      let result = f(&mut self.journal_a)?;
      let high_water_triggered = self.journal_a.is_high_water_mark_triggered();
      (result, high_water_triggered)
    } else {
      let result = f(&mut self.journal_b)?;
      let high_water_triggered = self.journal_b.is_high_water_mark_triggered();
      (result, high_water_triggered)
    };

    // Check if we need to switch journals after the operation
    if high_water_triggered {
      self.switch_journals()?;
    }

    Ok(result)
  }

  /// Switch to the inactive journal by reinitializing it from the active journal.
  fn switch_journals(&mut self) -> anyhow::Result<()> {
    // Set switching flag to prevent recursive switching
    self.switching = true;

    // Reinitialize the inactive journal from the active one
    if self.active_journal_a {
      self.journal_b.reinit_from(&mut self.journal_a)?;
    } else {
      self.journal_a.reinit_from(&mut self.journal_b)?;
    }

    // Switch active journal
    self.active_journal_a = !self.active_journal_a;

    // Clear switching flag
    self.switching = false;

    Ok(())
  }

  /// Execute a read-only operation with the currently active journal.
  fn with_active_journal_readonly<T, F>(&self, f: F) -> T
  where
    F: FnOnce(&dyn KVJournal) -> T,
  {
    if self.active_journal_a {
      f(&self.journal_a)
    } else {
      f(&self.journal_b)
    }
  }

  /// Get which journal is currently active (true = journal_a, false = journal_b).
  /// This is useful for testing and debugging.
  pub fn is_active_journal_a(&self) -> bool {
    self.active_journal_a
  }

  /// Get a reference to the currently active journal.
  pub fn active_journal(&self) -> &dyn KVJournal {
    if self.active_journal_a {
      &self.journal_a
    } else {
      &self.journal_b
    }
  }

  /// Get a mutable reference to the currently active journal.
  pub fn active_journal_mut(&mut self) -> &mut dyn KVJournal {
    if self.active_journal_a {
      &mut self.journal_a
    } else {
      &mut self.journal_b
    }
  }

  /// Get a reference to the currently inactive journal.
  pub fn inactive_journal(&self) -> &dyn KVJournal {
    if self.active_journal_a {
      &self.journal_b
    } else {
      &self.journal_a
    }
  }
}

impl<A: KVJournal, B: KVJournal> KVJournal for DoubleBufferedKVJournal<A, B> {
  fn high_water_mark(&self) -> usize {
    self.with_active_journal_readonly(|journal| journal.high_water_mark())
  }

  fn is_high_water_mark_triggered(&self) -> bool {
    self.with_active_journal_readonly(|journal| journal.is_high_water_mark_triggered())
  }

  fn buffer_usage_ratio(&self) -> f32 {
    self.with_active_journal_readonly(|journal| journal.buffer_usage_ratio())
  }

  fn get_init_time(&mut self) -> anyhow::Result<u64> {
    self.with_active_journal(|journal| journal.get_init_time())
  }

  fn set(&mut self, key: &str, value: &Value) -> anyhow::Result<()> {
    self.with_active_journal(|journal| journal.set(key, value))
  }

  fn delete(&mut self, key: &str) -> anyhow::Result<()> {
    self.with_active_journal(|journal| journal.delete(key))
  }

  fn as_hashmap(&mut self) -> anyhow::Result<HashMap<String, Value>> {
    self.with_active_journal(|journal| journal.as_hashmap())
  }

  fn reinit_from(&mut self, other: &mut dyn KVJournal) -> anyhow::Result<()> {
    self.with_active_journal(|journal| journal.reinit_from(other))
  }
}
