// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use std::time::Instant;

pub trait Clock {
  fn now(&self) -> Instant;
}

#[derive(Debug, Default, Clone)]
pub struct SystemClock;

impl Clock for SystemClock {
  fn now(&self) -> Instant {
    Instant::now()
  }
}
