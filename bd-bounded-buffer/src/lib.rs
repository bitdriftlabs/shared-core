// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![deny(
  clippy::expect_used,
  clippy::panic,
  clippy::todo,
  clippy::unimplemented,
  clippy::unreachable,
  clippy::unwrap_used
)]

#[cfg(test)]
#[path = "./lib_test.rs"]
mod tests;

use bd_client_stats_store::{Counter, Scope};
use bd_log_primitives::size::MemorySized;
use bd_stats_common::labels;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc::error::TryRecvError as TokioTryRecvError;
use tokio::sync::mpsc::{UnboundedReceiver as TokioReceiver, UnboundedSender as TokioSender};

// Like `mpsc::unbounded_channel` but provides a way to specify the maximum amount of
// memory a channel may use. The channel becomes full when items stored within it reach
// the maximum memory capacity.
// The interface for working with the channel is supposed to be a subset of the interface exposed
// by mpsc::unbounded_channel. For improved ergonomics, we keep the interface of our channel to
// be as closed to underlying mpsc::unbounded_channel interface as possible.
#[must_use]
pub fn channel<L: MemorySized>(memory_capacity: usize) -> (Sender<L>, Receiver<L>) {
  let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
  let memory_capacity_usage: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));

  (
    Sender {
      tx,
      memory_capacity_usage: memory_capacity_usage.clone(),
      memory_capacity: memory_capacity as u64,
    },
    Receiver {
      rx,
      memory_capacity_usage,
    },
  )
}

//
// Sender
//

#[derive(Debug)]
pub struct MemoryReservationErrorNoMemory {}

#[derive(Debug, thiserror::Error)]
pub enum TrySendError {
  // Adding a message to the buffer would cause the buffer to exceed it's
  // memory capacity.
  #[error("buffer size overflow")]
  FullSizeOverflow,
  #[error("buffer closed")]
  Closed,
}

#[derive(Debug, thiserror::Error)]
pub enum TryRecvError {
  #[error("channel empty")]
  Empty,
  #[error("channel disconnected")]
  Disconnected,
}

pub struct Sender<T: MemorySized> {
  tx: TokioSender<T>,
  memory_capacity_usage: Arc<AtomicU64>,
  memory_capacity: u64,
}

impl<T: MemorySized> Sender<T> {
  // Try to send a given message to the channel. Fails when the channel is closed or full.
  pub fn try_send(&self, message: T) -> Result<(), TrySendError> {
    let message_size = message.size() as u64;
    let result = self.try_reserve_memory(message_size);

    let Ok(()) = result else {
      return Err(TrySendError::FullSizeOverflow);
    };

    if self.tx.send(message).is_ok() {
      log::trace!("Added {message_size:?} bytes to the channel");
      Ok(())
    } else {
      log::debug!("Failed to add {message_size:?} bytes to the channel: Channel is closed");
      self.void_memory_reservation(message_size);
      Err(TrySendError::Closed)
    }
  }

  fn try_reserve_memory(&self, memory_amount: u64) -> Result<(), MemoryReservationErrorNoMemory> {
    // The reservation of the memory performed by an atomic operation that increases the current
    // memory usage counter. Later we verify whether the reservation did not cause the memory
    // usage to overflow the configured memory capacity limit and void the reservation if it did.
    let pre_add_capacity_usage = self
      .memory_capacity_usage
      .fetch_add(memory_amount, Ordering::SeqCst);
    let post_add_mem_capacity = pre_add_capacity_usage + memory_amount;

    if post_add_mem_capacity > self.memory_capacity {
      log::debug!(
        "Failed to add to the channel: channel reached out {:?} bytes in size and {:?} bytes \
         cannot be added to it without exceeding the configured memory capacity ({:?} bytes)",
        pre_add_capacity_usage,
        memory_amount,
        self.memory_capacity
      );

      // Void the reservation of the memory performed at the beginning of the method as
      // the reservation caused the memory usage to be greater than configured memory capacity
      // limit.
      self.void_memory_reservation(memory_amount);
      return Err(MemoryReservationErrorNoMemory {});
    }

    log::trace!(
      "Reserved {memory_amount:?} bytes in the channel, the new size of the channel is \
       {post_add_mem_capacity:?} bytes"
    );

    // The memory reservation operation completed succesfully. The memory reservation needs to
    // voided once it's not needed anymore. We do it as part of `Receiver::recv()` every time after
    // we consume a struct for which we reserved the memory.
    Ok(())
  }

  fn void_memory_reservation(&self, memory_amount: u64) {
    debug_assert!(self.memory_capacity_usage.load(Ordering::SeqCst) >= memory_amount);
    self
      .memory_capacity_usage
      .fetch_sub(memory_amount, Ordering::SeqCst);
  }
}

impl<T: MemorySized> Clone for Sender<T> {
  fn clone(&self) -> Self {
    Self {
      tx: self.tx.clone(),
      memory_capacity_usage: self.memory_capacity_usage.clone(),
      memory_capacity: self.memory_capacity,
    }
  }
}

//
// Receiver
//

pub struct Receiver<L: MemorySized> {
  rx: TokioReceiver<L>,
  memory_capacity_usage: Arc<AtomicU64>,
}

impl<T: MemorySized> Receiver<T> {
  pub async fn recv(&mut self) -> Option<T> {
    let item = self.rx.recv().await;

    if let Some(unwrapped_item) = &item {
      let size: u64 = unwrapped_item.size() as u64;
      let previous_size = self.memory_capacity_usage.fetch_sub(size, Ordering::SeqCst);

      log::trace!(
        "{size:?} bytes read from the channel, new channel size {:?} bytes",
        previous_size - size,
      );
    }

    item
  }

  pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
    let item = self.rx.try_recv().map_err(|e| match e {
      TokioTryRecvError::Empty => TryRecvError::Empty,
      TokioTryRecvError::Disconnected => TryRecvError::Disconnected,
    })?;

    let size: u64 = item.size() as u64;
    let previous_size = self.memory_capacity_usage.fetch_sub(size, Ordering::SeqCst);

    log::trace!(
      "{size:?} bytes read from the channel, new channel size {:?} bytes",
      previous_size - size,
    );

    Ok(item)
  }

  /// Returns whether the channel is closed (all senders have been dropped).
  #[must_use]
  pub fn is_closed(&self) -> bool {
    self.rx.is_closed()
  }
}

//
// SendCounters
//

#[derive(Debug, Clone)]
pub struct SendCounters {
  ok: Counter,
  err_full_size_overflow: Counter,
  err_closed: Counter,
}

impl SendCounters {
  #[must_use]
  pub fn new(scope: &Scope, operation_name: &str) -> Self {
    Self {
      ok: scope.counter_with_labels(operation_name, labels!("result" => "success")),
      err_full_size_overflow: scope
        .counter_with_labels(operation_name, labels!("result" => "failure_size_overflow")),
      err_closed: scope.counter_with_labels(operation_name, labels!("result" => "failure_closed")),
    }
  }

  pub fn record(&self, result: &std::result::Result<(), TrySendError>) {
    match result {
      Ok(()) => self.ok.inc(),
      Err(TrySendError::FullSizeOverflow) => {
        self.err_full_size_overflow.inc();
      },
      Err(TrySendError::Closed) => self.err_closed.inc(),
    }
  }
}
