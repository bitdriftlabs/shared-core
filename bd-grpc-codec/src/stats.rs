// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_stats_common::DynCounter;

//
// DeferredCounter
//

// To support using information received via the stream (e.g. handshake message) to determine the
// labels to use with the stats, this helper first keeps track of the data seen as deferred values
// before being initialized with the counters to use.
#[derive(Debug, Default)]
pub struct DeferredCounter {
  counter: Option<DynCounter>,
  internal_count: u64,
}

impl DeferredCounter {
  #[must_use]
  pub const fn count(&self) -> u64 {
    self.internal_count
  }

  pub fn inc(&mut self) {
    self.inc_by(1);
  }

  pub fn inc_by(&mut self, value: usize) {
    self.internal_count += value as u64;
    if let Some(counter) = &self.counter {
      counter.inc_by(value as u64);
    }
  }

  pub fn initialize(&mut self, counter: DynCounter) {
    debug_assert!(self.counter.is_none());
    counter.inc_by(self.internal_count);
    self.counter = Some(counter);
  }
}
