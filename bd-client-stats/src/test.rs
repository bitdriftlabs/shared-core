// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::stats::{PeriodicAction, PeriodicSchedule};
use async_trait::async_trait;
use bd_time::Ticker;

//
// TestTickerBackedSchedule
//

pub struct TestTickerBackedSchedule {
  flush_ticker: Box<dyn Ticker>,
  upload_ticker: Box<dyn Ticker>,
}

impl TestTickerBackedSchedule {
  #[must_use]
  pub const fn new(flush_ticker: Box<dyn Ticker>, upload_ticker: Box<dyn Ticker>) -> Self {
    Self {
      flush_ticker,
      upload_ticker,
    }
  }
}

#[async_trait]
impl PeriodicSchedule for TestTickerBackedSchedule {
  async fn next_action(&mut self) -> PeriodicAction {
    tokio::select! {
      () = self.flush_ticker.tick() => PeriodicAction::Flush,
      () = self.upload_ticker.tick() => PeriodicAction::Upload,
    }
  }
}
