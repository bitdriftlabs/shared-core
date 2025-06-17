// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use std::time::{Duration, Instant};
use tokio::sync::oneshot::error::TryRecvError;

#[derive(Debug)]
pub enum WaitError {
  Timeout,
  ChannelClosed,
}

pub fn blocking_wait_with_timeout(
  receiver: &mut tokio::sync::oneshot::Receiver<()>,
  timeout: Duration,
) -> Result<(), WaitError> {
  let deadline = Instant::now() + timeout;
  loop {
    if Instant::now() > deadline {
      return Err(WaitError::Timeout);
    }
    match receiver.try_recv() {
      Ok(()) => return Ok(()),
      Err(TryRecvError::Closed) => return Err(WaitError::ChannelClosed),
      Err(TryRecvError::Empty) => std::thread::sleep(Duration::from_millis(5)),
    }
  }
}
