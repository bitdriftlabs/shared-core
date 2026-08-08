// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

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
