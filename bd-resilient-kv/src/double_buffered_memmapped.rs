// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{ResilientKv, HighWaterMarkCallback, MemMappedResilientKv};
use bd_bonjson::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs;

/// A double-buffered implementation of ResilientKv that uses memory-mapped files as the backend
/// and automatically switches between two files when one reaches its high water mark.
/// 
/// This type provides persistent storage with automatic buffer management:
/// - Maintains two MemMappedResilientKv instances and switches between them
/// - When the active file passes its high water mark, copies the compressed kv state to the 
///   inactive file, then switches to that file
/// - Both instances remain loaded in memory, avoiding disk I/O on switches
/// - Provides crash resilience through persistent memory-mapped files
pub struct DoubleBufferedMemMappedKv {
  /// Primary memory-mapped KV instance
  kv_a: Option<MemMappedResilientKv>,
  /// Secondary memory-mapped KV instance
  kv_b: Option<MemMappedResilientKv>,
  /// Path to the primary file
  file_path_a: PathBuf,
  /// Path to the secondary file
  file_path_b: PathBuf,
  /// Whether we're currently using kv_a (true) or kv_b (false)
  using_a: bool,
  /// File size for creating new instances
  file_size: usize,
  /// High water mark ratio configuration
  high_water_mark_ratio: Option<f32>,
  /// Callback for high water mark notifications
  callback: Option<HighWaterMarkCallback>,
}

impl DoubleBufferedMemMappedKv {
  /// Create a new double-buffered memory-mapped KV store using the provided file paths.
  /// 
  /// Both files will be created if they don't exist. The first file (file_a) will be used initially.
  ///
  /// # Arguments
  /// * `file_path_a` - Path to the primary file
  /// * `file_path_b` - Path to the secondary file
  /// * `file_size` - Size of each file in bytes
  /// * `high_water_mark_ratio` - Optional ratio (0.0 to 1.0) for high water mark. Default: 0.8
  /// * `callback` - Optional callback function called when high water mark is exceeded
  ///
  /// # Errors
  /// Returns an error if file initialization fails.
  pub fn new<P: AsRef<Path>>(
    file_path_a: P,
    file_path_b: P,
    file_size: usize,
    high_water_mark_ratio: Option<f32>,
    callback: Option<HighWaterMarkCallback>
  ) -> anyhow::Result<Self> {
    let path_a = file_path_a.as_ref().to_path_buf();
    let path_b = file_path_b.as_ref().to_path_buf();

    // Create the primary file and initialize it
    let primary = MemMappedResilientKv::new(&path_a, file_size, high_water_mark_ratio, callback)?;

    // Create the secondary file but don't load it yet (lazy initialization)
    Ok(Self {
      kv_a: Some(primary),
      kv_b: None,
      file_path_a: path_a,
      file_path_b: path_b,
      using_a: true,
      file_size,
      high_water_mark_ratio,
      callback,
    })
  }

  /// Create a new double-buffered memory-mapped KV store with the first file loaded from existing data
  /// and the second file as a backup.
  ///
  /// # Arguments
  /// * `file_path_a` - Path to the primary file (must exist)
  /// * `file_path_b` - Path to the secondary file (will be created as needed)
  /// * `file_size` - Size of the secondary file if it needs to be created
  /// * `high_water_mark_ratio` - Optional ratio (0.0 to 1.0) for high water mark. Default: 0.8
  /// * `callback` - Optional callback function called when high water mark is exceeded
  ///
  /// # Errors
  /// Returns an error if file initialization fails.
  pub fn from_file<P: AsRef<Path>>(
    file_path_a: P,
    file_path_b: P,
    file_size: usize,
    high_water_mark_ratio: Option<f32>,
    callback: Option<HighWaterMarkCallback>
  ) -> anyhow::Result<Self> {
    let path_a = file_path_a.as_ref().to_path_buf();
    let path_b = file_path_b.as_ref().to_path_buf();

    // Load the primary file from existing data
    let primary = MemMappedResilientKv::from_file(&path_a, high_water_mark_ratio, callback)?;

    // Secondary file will be created lazily when needed
    Ok(Self {
      kv_a: Some(primary),
      kv_b: None,
      file_path_a: path_a,
      file_path_b: path_b,
      using_a: true,
      file_size,
      high_water_mark_ratio,
      callback,
    })
  }

  /// Execute a function with the currently active KV store.
  /// 
  /// This function handles the switching logic automatically. If the high water mark is triggered
  /// during the operation, it will switch to the other buffer and copy the data.
  fn with_active_kv<F, R>(&mut self, mut func: F) -> anyhow::Result<R>
  where
    F: FnMut(&mut MemMappedResilientKv) -> anyhow::Result<R>,
  {
    // Get the currently active KV
    let active_kv = if self.using_a {
      self.kv_a.as_mut()
    } else {
      self.kv_b.as_mut()
    };

    let mut active_kv = active_kv.expect("Active KV should always be initialized");

    // Execute the function
    let result = func(&mut active_kv)?;

    // Check if we need to switch buffers
    if active_kv.is_high_water_mark_triggered() {
      self.switch_buffers()?;
    }

    Ok(result)
  }

  /// Switch from the currently active buffer to the inactive one.
  /// 
  /// This copies the current state to the inactive buffer and then switches to it.
  fn switch_buffers(&mut self) -> anyhow::Result<()> {
    // Get the current state from the active KV
    let current_state = if self.using_a {
      self.kv_a.as_mut().unwrap().as_hashmap()?
    } else {
      self.kv_b.as_mut().unwrap().as_hashmap()?
    };

    // Ensure the inactive KV exists
    if self.using_a && self.kv_b.is_none() {
      // Create kv_b
      self.kv_b = Some(MemMappedResilientKv::new(
        &self.file_path_b,
        self.file_size,
        self.high_water_mark_ratio,
        self.callback
      )?);
    } else if !self.using_a && self.kv_a.is_none() {
      // Create kv_a
      self.kv_a = Some(MemMappedResilientKv::new(
        &self.file_path_a,
        self.file_size,
        self.high_water_mark_ratio,
        self.callback
      )?);
    }

    // Get the inactive KV and populate it with the current state
    let inactive_kv = if self.using_a {
      self.kv_b.as_mut().unwrap()
    } else {
      self.kv_a.as_mut().unwrap()
    };

    // Clear the inactive buffer and copy current state
    for (key, value) in current_state {
      inactive_kv.set(&key, &value)?;
    }

    // Switch to the inactive buffer
    self.using_a = !self.using_a;

    // Clean up the old active buffer by recreating it
    if self.using_a {
      // We switched to kv_a, so recreate kv_b
      self.kv_b = None;
    } else {
      // We switched to kv_b, so recreate kv_a  
      self.kv_a = None;
    }

    Ok(())
  }

  /// Get which file is currently active (true = file_a, false = file_b).
  /// This is useful for testing and debugging.
  pub fn is_active_file_a(&self) -> bool {
    self.using_a
  }

  /// Get the path of the currently active file.
  pub fn active_file_path(&self) -> &Path {
    if self.using_a {
      &self.file_path_a
    } else {
      &self.file_path_b
    }
  }

  /// Get the path of the currently inactive file.
  pub fn inactive_file_path(&self) -> &Path {
    if self.using_a {
      &self.file_path_b
    } else {
      &self.file_path_a
    }
  }

  /// Force a sync of the currently active file to disk.
  pub fn sync(&self) -> anyhow::Result<()> {
    if self.using_a {
      if let Some(ref kv) = self.kv_a {
        kv.sync()?;
      }
    } else {
      if let Some(ref kv) = self.kv_b {
        kv.sync()?;
      }
    }
    Ok(())
  }

  /// Get the file size configuration.
  pub fn file_size(&self) -> usize {
    self.file_size
  }

  /// Cleanup old files after successful operation.
  /// This removes the inactive file to save disk space.
  /// 
  /// # Warning
  /// This is irreversible - the inactive file will be permanently deleted.
  pub fn cleanup_inactive_file(&self) -> anyhow::Result<()> {
    let inactive_path = self.inactive_file_path();
    if inactive_path.exists() {
      fs::remove_file(inactive_path)?;
    }
    Ok(())
  }
}

impl ResilientKv for DoubleBufferedMemMappedKv {
  fn high_water_mark(&self) -> usize {
    if self.using_a {
      self.kv_a.as_ref().unwrap().high_water_mark()
    } else {
      self.kv_b.as_ref().unwrap().high_water_mark()
    }
  }

  fn is_high_water_mark_triggered(&self) -> bool {
    if self.using_a {
      self.kv_a.as_ref().unwrap().is_high_water_mark_triggered()
    } else {
      self.kv_b.as_ref().unwrap().is_high_water_mark_triggered()
    }
  }

  fn buffer_usage_ratio(&self) -> f32 {
    if self.using_a {
      self.kv_a.as_ref().unwrap().buffer_usage_ratio()
    } else {
      self.kv_b.as_ref().unwrap().buffer_usage_ratio()
    }
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

  fn get_init_time(&mut self) -> anyhow::Result<u64> {
    self.with_active_kv(|kv| kv.get_init_time())
  }
}
