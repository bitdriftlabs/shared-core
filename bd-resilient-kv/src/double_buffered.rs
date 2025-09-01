// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{ResilientKv, HighWaterMarkCallback, InMemoryResilientKv};
use bd_bonjson::Value;
use std::collections::HashMap;

/// A double-buffered implementation of ResilientKv that automatically switches between two
/// buffers when one reaches its high water mark.
/// 
/// This type holds two buffers and switches between them when necessary:
/// - Forwards ResilientKv APIs to the currently active buffer
/// - When the active buffer passes its high water mark, uses `from_kv_store` to initialize
///   the other buffer with the compressed kv state of the full one
/// - Once the other buffer is initialized, begins forwarding APIs to that buffer
pub struct DoubleBufferedKv {
  /// The primary buffer storage
  buffer_a: Vec<u8>,
  /// The secondary buffer storage  
  buffer_b: Vec<u8>,
  /// Which buffer is currently active (true = buffer_a, false = buffer_b)
  active_buffer_a: bool,
  /// Whether we're in the middle of a buffer switch operation
  switching: bool,
  /// High water mark ratio configuration
  high_water_mark_ratio: Option<f32>,
  /// Callback for high water mark notifications
  callback: Option<HighWaterMarkCallback>,
}

impl DoubleBufferedKv {
  /// Create a new double-buffered KV store using the provided buffer size.
  /// 
  /// Both internal buffers will be created with the specified size.
  ///
  /// # Arguments
  /// * `buffer_size` - The size of each internal buffer
  /// * `high_water_mark_ratio` - Optional ratio (0.0 to 1.0) for high water mark. Default: 0.8
  /// * `callback` - Optional callback function called when high water mark is exceeded
  ///
  /// # Errors
  /// Returns an error if buffer initialization fails.
  pub fn new(
    buffer_size: usize,
    high_water_mark_ratio: Option<f32>,
    callback: Option<HighWaterMarkCallback>
  ) -> anyhow::Result<Self> {
    let mut buffer_a = vec![0u8; buffer_size];
    let buffer_b = vec![0u8; buffer_size];

    // Initialize the first buffer to establish the initial state
    let _initial_kv = InMemoryResilientKv::new(&mut buffer_a, high_water_mark_ratio, callback)?;

    Ok(Self {
      buffer_a,
      buffer_b,
      active_buffer_a: true,
      switching: false,
      high_water_mark_ratio,
      callback,
    })
  }

  /// Create a new double-buffered KV store with initial data in the first buffer.
  ///
  /// # Arguments
  /// * `initial_data` - The initial buffer data to load
  /// * `secondary_buffer_size` - The size of the secondary buffer
  /// * `high_water_mark_ratio` - Optional ratio (0.0 to 1.0) for high water mark. Default: 0.8
  /// * `callback` - Optional callback function called when high water mark is exceeded
  ///
  /// # Errors
  /// Returns an error if buffer initialization fails.
  pub fn from_data(
    initial_data: Vec<u8>,
    secondary_buffer_size: usize,
    high_water_mark_ratio: Option<f32>,
    callback: Option<HighWaterMarkCallback>
  ) -> anyhow::Result<Self> {
    let buffer_b = vec![0u8; secondary_buffer_size];

    // Validate the initial data by trying to load it
    let mut buffer_a_copy = initial_data.clone();
    let _test_kv = InMemoryResilientKv::from_buffer(&mut buffer_a_copy, high_water_mark_ratio, callback)?;

    Ok(Self {
      buffer_a: initial_data,
      buffer_b,
      active_buffer_a: true,
      switching: false,
      high_water_mark_ratio,
      callback,
    })
  }

  /// Execute an operation with the currently active KV store.
  fn with_active_kv<T, F>(&mut self, f: F) -> anyhow::Result<T>
  where
    F: FnOnce(&mut InMemoryResilientKv<'_>) -> anyhow::Result<T>,
  {
    // Split the borrow to avoid multiple mutable borrows
    let active_buffer_a = self.active_buffer_a;
    let switching = self.switching;
    let high_water_mark_ratio = self.high_water_mark_ratio;
    let callback = self.callback;

    let (active_buffer, inactive_buffer) = if active_buffer_a {
      (&mut self.buffer_a, &mut self.buffer_b)
    } else {
      (&mut self.buffer_b, &mut self.buffer_a)
    };

    // Create KV store from current buffer
    let mut kv = InMemoryResilientKv::from_buffer(active_buffer, high_water_mark_ratio, callback)?;
    
    // Execute the operation
    let result = f(&mut kv)?;
    
    // Check if we need to switch buffers after the operation
    // Only switch if we're not already switching and the high water mark is triggered
    if !switching && kv.is_high_water_mark_triggered() {
      // Set switching flag to prevent recursive switching
      self.switching = true;
      
      // Create new KV store in the inactive buffer from the active one
      let _new_kv = InMemoryResilientKv::from_kv_store(inactive_buffer, &mut kv)?;
      
      // Switch active buffer
      self.active_buffer_a = !active_buffer_a;
      
      // Clear switching flag
      self.switching = false;
    }

    Ok(result)
  }

  /// Execute a read-only operation with the currently active KV store.
  fn with_active_kv_readonly<T, F>(&self, f: F) -> anyhow::Result<T>
  where
    F: FnOnce(&InMemoryResilientKv<'_>) -> anyhow::Result<T>,
  {
    let buffer = if self.active_buffer_a {
      &self.buffer_a
    } else {
      &self.buffer_b
    };

    // Create a copy for read-only access
    let mut buffer_copy = buffer.clone();
    let kv = InMemoryResilientKv::from_buffer(&mut buffer_copy, self.high_water_mark_ratio, None)?;
    
    f(&kv)
  }

  /// Get which buffer is currently active (true = buffer_a, false = buffer_b).
  /// This is useful for testing and debugging.
  pub fn is_active_buffer_a(&self) -> bool {
    self.active_buffer_a
  }

  /// Get the buffer usage ratio of the currently active buffer.
  pub fn active_buffer_usage_ratio(&self) -> anyhow::Result<f32> {
    self.with_active_kv_readonly(|kv| Ok(kv.buffer_usage_ratio()))
  }

  /// Get the size of the currently active buffer.
  pub fn active_buffer_size(&self) -> usize {
    if self.active_buffer_a {
      self.buffer_a.len()
    } else {
      self.buffer_b.len()
    }
  }

  /// Get the size of the currently inactive buffer.
  pub fn inactive_buffer_size(&self) -> usize {
    if self.active_buffer_a {
      self.buffer_b.len()
    } else {
      self.buffer_a.len()
    }
  }
}

impl ResilientKv for DoubleBufferedKv {
  fn high_water_mark(&self) -> usize {
    let ratio = self.high_water_mark_ratio.unwrap_or(0.8);
    let buffer_len = if self.active_buffer_a {
      self.buffer_a.len()
    } else {
      self.buffer_b.len()
    };
    (buffer_len as f32 * ratio) as usize
  }

  fn is_high_water_mark_triggered(&self) -> bool {
    // For a read-only check, we need to be conservative
    // The actual check happens in with_active_kv when modifications occur
    false
  }

  fn buffer_usage_ratio(&self) -> f32 {
    // For a read-only check, we can try to get the ratio but return 0.0 on error
    self.with_active_kv_readonly(|kv| Ok(kv.buffer_usage_ratio())).unwrap_or(0.0)
  }

  fn get_init_time(&mut self) -> anyhow::Result<u64> {
    self.with_active_kv(|kv| kv.get_init_time())
  }

  fn set(&mut self, key: &str, value: &Value) -> anyhow::Result<()> {
    self.with_active_kv(|kv| kv.set(key, value))
  }

  fn delete(&mut self, key: &str) -> anyhow::Result<()> {
    self.with_active_kv(|kv| kv.delete(key))
  }

  fn as_hashmap(&mut self) -> anyhow::Result<HashMap<String, Value>> {
    self.with_active_kv(|kv| kv.as_hashmap())
  }
}
