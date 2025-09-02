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
/// - Maintains two MemMappedResilientKv instances that are always available
/// - When the active file passes its high water mark, copies the compressed kv state to a fresh
///   instance of the inactive file, then switches to that file
/// - Both instances are kept in memory, avoiding disk I/O during switches
/// - Provides crash resilience through persistent memory-mapped files
/// - Both KV instances are initialized at construction time - if either fails, the entire
///   DoubleBufferedMemMappedKv construction fails
pub struct DoubleBufferedMemMappedKv {
  /// Primary memory-mapped KV instance
  kv_a: MemMappedResilientKv,
  /// Secondary memory-mapped KV instance
  kv_b: MemMappedResilientKv,
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
  /// Both files will be created and initialized. Both MemMappedResilientKv instances must
  /// be successfully created or this constructor will fail.
  ///
  /// # Arguments
  /// * `file_path_a` - Path to the primary file
  /// * `file_path_b` - Path to the secondary file
  /// * `file_size` - Size of each file in bytes
  /// * `high_water_mark_ratio` - Optional ratio (0.0 to 1.0) for high water mark. Default: 0.8
  /// * `callback` - Optional callback function called when high water mark is exceeded
  ///
  /// # Errors
  /// Returns an error if either file initialization fails.
  pub fn new<P: AsRef<Path>>(
    file_path_a: P,
    file_path_b: P,
    file_size: usize,
    high_water_mark_ratio: Option<f32>,
    callback: Option<HighWaterMarkCallback>
  ) -> anyhow::Result<Self> {
    let path_a = file_path_a.as_ref().to_path_buf();
    let path_b = file_path_b.as_ref().to_path_buf();

    // Create both files and initialize them
    let primary = MemMappedResilientKv::new(&path_a, file_size, high_water_mark_ratio, callback)?;
    let secondary = MemMappedResilientKv::new(&path_b, file_size, high_water_mark_ratio, callback)?;

    Ok(Self {
      kv_a: primary,
      kv_b: secondary,
      file_path_a: path_a,
      file_path_b: path_b,
      using_a: true,
      file_size,
      high_water_mark_ratio,
      callback,
    })
  }

  /// Create a new double-buffered memory-mapped KV store with the first file loaded from existing data
  /// and the second file initialized.
  ///
  /// # Arguments
  /// * `file_path_a` - Path to the primary file (must exist)
  /// * `file_path_b` - Path to the secondary file (will be created if it doesn't exist)
  /// * `file_size` - Size of the secondary file if it needs to be created
  /// * `high_water_mark_ratio` - Optional ratio (0.0 to 1.0) for high water mark. Default: 0.8
  /// * `callback` - Optional callback function called when high water mark is exceeded
  ///
  /// # Errors
  /// Returns an error if either file initialization fails.
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
    
    // Create the secondary file (it may not exist yet)
    let secondary = if path_b.exists() {
      MemMappedResilientKv::from_file(&path_b, high_water_mark_ratio, callback)?
    } else {
      MemMappedResilientKv::new(&path_b, file_size, high_water_mark_ratio, callback)?
    };

    Ok(Self {
      kv_a: primary,
      kv_b: secondary,
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
      &mut self.kv_a
    } else {
      &mut self.kv_b
    };

    // Execute the function
    let result = func(active_kv)?;

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
      self.kv_a.as_hashmap()?
    } else {
      self.kv_b.as_hashmap()?
    };

    // Recreate the inactive KV with a fresh file
    if self.using_a {
      // Recreate kv_b
      self.kv_b = MemMappedResilientKv::new(
        &self.file_path_b,
        self.file_size,
        self.high_water_mark_ratio,
        self.callback
      )?;
      
      // Copy current state to the fresh kv_b
      for (key, value) in current_state {
        self.kv_b.set(&key, &value)?;
      }
    } else {
      // Recreate kv_a
      self.kv_a = MemMappedResilientKv::new(
        &self.file_path_a,
        self.file_size,
        self.high_water_mark_ratio,
        self.callback
      )?;
      
      // Copy current state to the fresh kv_a
      for (key, value) in current_state {
        self.kv_a.set(&key, &value)?;
      }
    }

    // Switch to the inactive buffer
    self.using_a = !self.using_a;

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
      self.kv_a.sync()?;
    } else {
      self.kv_b.sync()?;
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
      self.kv_a.high_water_mark()
    } else {
      self.kv_b.high_water_mark()
    }
  }

  fn is_high_water_mark_triggered(&self) -> bool {
    if self.using_a {
      self.kv_a.is_high_water_mark_triggered()
    } else {
      self.kv_b.is_high_water_mark_triggered()
    }
  }

  fn buffer_usage_ratio(&self) -> f32 {
    if self.using_a {
      self.kv_a.buffer_usage_ratio()
    } else {
      self.kv_b.buffer_usage_ratio()
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
