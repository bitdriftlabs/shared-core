// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// Test code only.
#![allow(clippy::unwrap_used)]

use crate::Ticker;
use async_trait::async_trait;
use tokio::sync::mpsc;

//
// TestTicker
//

pub struct TestTicker {
  receiver: mpsc::Receiver<()>,
}

#[async_trait]
impl Ticker for TestTicker {
  async fn tick(&mut self) {
    self.receiver.recv().await.unwrap();
  }
}

impl TestTicker {
  #[must_use]
  pub fn new() -> (mpsc::Sender<()>, Self) {
    let (tx, rx) = mpsc::channel(1);
    (tx, Self { receiver: rx })
  }
}
