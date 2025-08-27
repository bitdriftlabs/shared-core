// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// Test code only.
#![allow(clippy::unwrap_used)]

use super::RingBufferStats;
use bd_client_stats_store::{Counter, Scope};
use std::sync::Arc;
use time::ext::NumericalStdDuration;

//
// StatsTestHelper
//

pub struct StatsTestHelper {
  pub stats: Arc<RingBufferStats>,
}

impl StatsTestHelper {
  #[must_use]
  pub fn new(scope: &Scope) -> Self {
    Self {
      stats: Arc::new(RingBufferStats {
        records_written: Some(scope.counter("records_written")),
        records_read: Some(scope.counter("records_read")),
        records_overwritten: Some(scope.counter("records_overwritten")),
        records_refused: Some(scope.counter("records_refused")),
        records_corrupted: Some(scope.counter("records_corrupted")),
        bytes_written: Some(scope.counter("bytes_written")),
        bytes_read: Some(scope.counter("bytes_read")),
        total_bytes_written: Some(scope.counter("total_bytes_written")),
        total_bytes_read: Some(scope.counter("total_bytes_read")),
        bytes_overwritten: Some(scope.counter("bytes_overwritten")),
        bytes_refused: Some(scope.counter("bytes_refused")),
        total_data_loss: Some(scope.counter("total_data_loss")),
      }),
    }
  }

  pub fn wait_for_total_records_written(&self, count: u32) {
    while self.stats.records_written.as_ref().unwrap().get() != u64::from(count) {
      std::thread::sleep(10.std_milliseconds());
    }
  }
}

pub trait OptionalStatGetter {
  fn get_value(&self) -> u64;
}

impl OptionalStatGetter for Option<Counter> {
  fn get_value(&self) -> u64 {
    self.as_ref().unwrap().get()
  }
}
