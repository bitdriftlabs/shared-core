// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use bd_server_stats::stats::Collector;
use std::io::prelude::*;
use tempfile::TempDir;

#[ctor::ctor(unsafe)]
fn test_global_init() {
  bd_test_helpers_core::test_global_init();
}

pub struct Helper {
  temp_dir: TempDir,
  stats: crate::loader::Stats,
}

impl Helper {
  pub fn new() -> Self {
    let collector = Collector::default();
    let scope = collector.scope("loader");
    let stats = crate::loader::Stats::new(&scope);

    Self {
      temp_dir: TempDir::with_prefix("test").unwrap(),
      stats,
    }
  }

  pub fn create_file_and_data_dir(&self, dir_name: &str, file_name: &str, file_contents: &[u8]) {
    let data_dir_path = self.temp_dir.path().join(dir_name);
    log::info!("test data dir={data_dir_path:?}");
    std::fs::create_dir(&data_dir_path).unwrap();

    let data_file_path = data_dir_path.join(file_name);
    log::info!("test data file={data_file_path:?}");
    let mut data_file = std::fs::File::create(&data_file_path).unwrap();
    data_file.write_all(file_contents).unwrap();
  }

  pub const fn temp_dir(&self) -> &TempDir {
    &self.temp_dir
  }

  pub fn stats(&self) -> crate::loader::Stats {
    self.stats.clone()
  }

  pub fn check_stats(&self, deserialize_success: u64, deserialize_failure: u64, io_failure: u64) {
    assert_eq!(deserialize_success, self.stats.deserialize_success.get());
    assert_eq!(deserialize_failure, self.stats.deserialize_failure.get());
    assert_eq!(io_failure, self.stats.io_failure.get());
  }
}

mod feature_flags;
mod loader;
