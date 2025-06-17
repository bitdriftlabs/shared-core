// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use std::fmt::Debug;
use std::time::{Duration, Instant};

//
// Sender
//

#[derive(Debug)]
pub struct Sender<T: Debug> {
  tx: tokio::sync::oneshot::Sender<T>,
}

impl<T: Debug> Sender<T> {
  #[must_use]
  pub fn new() -> (Self, Receiver<T>) {
    let (tx, rx) = tokio::sync::oneshot::channel();

    (Self { tx }, Receiver { rx })
  }

  pub fn send(self, value: T) {
    if let Err(e) = self.tx.send(value) {
      log::debug!("failed to send completion signal: {e:?}");
    }
  }
}

//
// Receiver
//

#[derive(Debug)]
pub struct Receiver<T: Debug> {
  rx: tokio::sync::oneshot::Receiver<T>,
}

impl<T: Debug> Receiver<T> {
  /// Create a [`bd_completion`] `Receiver<T>` from a raw `tokio::sync::oneshot::Receiver<T>`
  #[must_use]
  pub fn to_bd_completion_rx(rx: tokio::sync::oneshot::Receiver<T>) -> Self {
    Self { rx }
  }

  pub async fn recv(self) -> anyhow::Result<T> {
    match self.rx.await {
      Ok(value) => Ok(value),
      Err(e) => anyhow::bail!("failed to receive completion signal: {e:?}"),
    }
  }

  pub fn blocking_recv(self) -> anyhow::Result<T> {
    Ok(self.rx.blocking_recv()?)
  }

  pub fn blocking_recv_with_timeout(mut self, timeout: Duration) -> Result<T, WaitError> {
    let deadline = Instant::now() + timeout;

    loop {
      if Instant::now() > deadline {
        return Err(WaitError::Timeout);
      }

      match self.rx.try_recv() {
        Ok(value) => return Ok(value),
        Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
          return Err(WaitError::ChannelClosed)
        },
        Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
          std::thread::sleep(Duration::from_millis(5));
        },
      }
    }
  }
}

#[derive(Debug)]
pub enum WaitError {
  Timeout,
  ChannelClosed,
}

impl std::fmt::Display for WaitError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let message = match self {
      Self::Timeout => "timeout duration reached",
      Self::ChannelClosed => "the oneshot channel was closed",
    };

    write!(f, "{message}")
  }
}

impl std::error::Error for WaitError {}
