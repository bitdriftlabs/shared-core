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
/// - Forwards ResilientKv APIs to the currently active memory-mapped file
/// - When the active file passes its high water mark, uses the compressed kv state to initialize
///   the other file, then switches to that file
/// - Both files persist on disk, providing crash resilience
/// - Automatic cleanup of old files after successful switching
pub struct DoubleBufferedMemMappedKv {
  /// Path to the primary file (file_a)
  file_path_a: PathBuf,
  /// Path to the secondary file (file_b)  
  file_path_b: PathBuf,
  /// The currently active KV store (None if we need to initialize)
  active_kv: Option<MemMappedResilientKv>,
  /// Which file is currently active (true = file_a, false = file_b)
  active_file_a: bool,
  /// Whether we're in the middle of a file switch operation
  switching: bool,
  /// File size configuration
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

    Ok(Self {
      file_path_a: path_a,
      file_path_b: path_b,
      active_kv: Some(primary),
      active_file_a: true,
      switching: false,
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

    Ok(Self {
      file_path_a: path_a,
      file_path_b: path_b,
      active_kv: Some(primary),
      active_file_a: true,
      switching: false,
      file_size,
      high_water_mark_ratio,
      callback,
    })
  }

  /// Get the currently active KV store, creating it if needed.
  fn get_active_kv(&mut self) -> anyhow::Result<&mut MemMappedResilientKv> {
    if self.active_kv.is_none() {
      let active_path = if self.active_file_a {
        &self.file_path_a
      } else {
        &self.file_path_b
      };

      let kv = if active_path.exists() {
        MemMappedResilientKv::from_file(active_path, self.high_water_mark_ratio, self.callback)?
      } else {
        MemMappedResilientKv::new(active_path, self.file_size, self.high_water_mark_ratio, self.callback)?
      };
      
      self.active_kv = Some(kv);
    }

    Ok(self.active_kv.as_mut().unwrap())
  }

  /// Check if we need to switch files and perform the switch if necessary.
  fn check_and_switch_if_needed(&mut self) -> anyhow::Result<()> {
    // Avoid recursive switching
    if self.switching {
      return Ok(());
    }

    let needs_switch = {
      let active_kv = self.get_active_kv()?;
      active_kv.is_high_water_mark_triggered()
    };

    if needs_switch {
      self.switching = true;
      
      // Take the current active KV to use as source
      let mut source_kv = self.active_kv.take().unwrap();
      
      // Determine the target file path
      let target_path = if self.active_file_a {
        &self.file_path_b
      } else {
        &self.file_path_a
      };

      // Create a new memory-mapped file for the target
      // We need to create it fresh first, then copy data from source
      let target_kv = self.create_target_from_source(target_path, &mut source_kv)?;
      
      // Switch to the new file
      self.active_file_a = !self.active_file_a;
      self.active_kv = Some(target_kv);
      
      // Sync the new file to ensure data is persisted
      self.active_kv.as_ref().unwrap().sync()?;
      
      self.switching = false;
    }

    Ok(())
  }

  /// Create a new target file from the source KV store data.
  fn create_target_from_source(
    &self, 
    target_path: &Path, 
    source_kv: &mut MemMappedResilientKv
  ) -> anyhow::Result<MemMappedResilientKv> {
    // Remove the target file if it exists to start fresh
    if target_path.exists() {
      fs::remove_file(target_path)?;
    }

    // Create a new empty memory-mapped file
    let mut target_kv = MemMappedResilientKv::new(
      target_path, 
      self.file_size, 
      self.high_water_mark_ratio, 
      self.callback
    )?;

    // Get the current state from the source as a hashmap
    let data = source_kv.as_hashmap()?;
    
    // Copy the initialization time from the source (for future use)
    let _init_time = source_kv.get_init_time()?;
    
    // Instead of using from_kv_store (which works with buffers), 
    // we'll manually copy the data entry by entry
    for (key, value) in data {
      target_kv.set(&key, &value)?;
    }

    Ok(target_kv)
  }

  /// Get which file is currently active (true = file_a, false = file_b).
  /// This is useful for testing and debugging.
  pub fn is_active_file_a(&self) -> bool {
    self.active_file_a
  }

  /// Get the path of the currently active file.
  pub fn active_file_path(&self) -> &Path {
    if self.active_file_a {
      &self.file_path_a
    } else {
      &self.file_path_b
    }
  }

  /// Get the path of the currently inactive file.
  pub fn inactive_file_path(&self) -> &Path {
    if self.active_file_a {
      &self.file_path_b
    } else {
      &self.file_path_a
    }
  }

  /// Force a sync of the currently active file to disk.
  pub fn sync(&self) -> anyhow::Result<()> {
    if let Some(ref kv) = self.active_kv {
      kv.sync()?;
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
    let ratio = self.high_water_mark_ratio.unwrap_or(0.8);
    (self.file_size as f32 * ratio) as usize
  }

  fn is_high_water_mark_triggered(&self) -> bool {
    if let Some(ref kv) = self.active_kv {
      kv.is_high_water_mark_triggered()
    } else {
      false
    }
  }

  fn buffer_usage_ratio(&self) -> f32 {
    if let Some(ref kv) = self.active_kv {
      kv.buffer_usage_ratio()
    } else {
      0.0
    }
  }

  fn get_init_time(&mut self) -> anyhow::Result<u64> {
    self.get_active_kv()?.get_init_time()
  }

  fn set(&mut self, key: &str, value: &Value) -> anyhow::Result<()> {
    self.get_active_kv()?.set(key, value)?;
    self.check_and_switch_if_needed()?;
    Ok(())
  }

  fn delete(&mut self, key: &str) -> anyhow::Result<()> {
    self.get_active_kv()?.delete(key)?;
    self.check_and_switch_if_needed()?;
    Ok(())
  }

  fn as_hashmap(&mut self) -> anyhow::Result<HashMap<String, Value>> {
    self.get_active_kv()?.as_hashmap()
  }
}
