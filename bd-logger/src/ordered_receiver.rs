// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./ordered_receiver_test.rs"]
mod tests;

use bd_bounded_buffer::{Receiver, TryRecvError};
use bd_log_primitives::size::MemorySized;

/// Merges two channels (logs and state updates) into a single ordered stream based on sequence
/// numbers. This ensures that single-threaded callers doing `log(); setField(); log();` observe
/// the same ordering in the engine.
#[derive(Debug)]
pub enum OrderedMessage<L, S> {
  Log(L),
  State(S),
}

pub struct OrderedReceiver<L, S>
where
  SequencedMessage<L>: MemorySized,
  SequencedMessage<S>: MemorySized,
{
  log_rx: Receiver<SequencedMessage<L>>,
  state_rx: Receiver<SequencedMessage<S>>,
  // Buffer at most one message from each channel
  buffered_log: Option<SequencedMessage<L>>,
  buffered_state: Option<SequencedMessage<S>>,
  log_closed: bool,
  state_closed: bool,
}

#[derive(Debug)]
pub struct SequencedMessage<T> {
  pub sequence: u64,
  pub message: T,
}

impl<L, S> OrderedReceiver<L, S>
where
  SequencedMessage<L>: MemorySized,
  SequencedMessage<S>: MemorySized,
{
  pub fn new(
    log_rx: Receiver<SequencedMessage<L>>,
    state_rx: Receiver<SequencedMessage<S>>,
  ) -> Self {
    Self {
      log_rx,
      state_rx,
      buffered_log: None,
      buffered_state: None,
      log_closed: false,
      state_closed: false,
    }
  }

  pub async fn recv(&mut self) -> Option<OrderedMessage<L, S>> {
    loop {
      // If we have both buffered, return the one with lower sequence
      if let (Some(log), Some(state)) = (&self.buffered_log, &self.buffered_state) {
        return if log.sequence <= state.sequence {
          let log = self.buffered_log.take()?;
          Some(OrderedMessage::Log(log.message))
        } else {
          let state = self.buffered_state.take()?;
          Some(OrderedMessage::State(state.message))
        };
      }

      // If we have one buffered, try to get the other (non-blocking), then compare or return
      if self.buffered_log.is_some() && self.buffered_state.is_none() {
        if !self.state_closed
          && let Ok(state) = self.state_rx.try_recv()
        {
          self.buffered_state = Some(state);
          continue; // Loop to compare sequences
        }
        // No state available or state channel closed - return the log
        let log = self.buffered_log.take()?;
        return Some(OrderedMessage::Log(log.message));
      }

      if self.buffered_state.is_some() && self.buffered_log.is_none() {
        if !self.log_closed
          && let Ok(log) = self.log_rx.try_recv()
        {
          self.buffered_log = Some(log);
          continue; // Loop to compare sequences
        }
        // No log available or log channel closed - return the state
        let state = self.buffered_state.take()?;
        return Some(OrderedMessage::State(state.message));
      }

      // If both channels are closed and nothing buffered, we're done
      if self.log_closed && self.state_closed {
        return None;
      }

      // Before blocking on select, check if any channels are closed using try_recv
      // This handles the case where both channels are already closed
      if !self.log_closed {
        match self.log_rx.try_recv() {
          Ok(log) => {
            self.buffered_log = Some(log);
            continue; // Loop to potentially get the other message or compare
          },
          Err(TryRecvError::Disconnected) => {
            self.log_closed = true;
          },
          Err(TryRecvError::Empty) => {
            // Channel is open but empty, will use select below
          },
        }
      }

      if !self.state_closed {
        match self.state_rx.try_recv() {
          Ok(state) => {
            self.buffered_state = Some(state);
            continue; // Loop to potentially get the other message or compare
          },
          Err(TryRecvError::Disconnected) => {
            self.state_closed = true;
          },
          Err(TryRecvError::Empty) => {
            // Channel is open but empty, will use select below
          },
        }
      }

      // Check again if both are now closed
      if self.log_closed && self.state_closed {
        return None;
      }

      // Receive from whichever channel has data first (or closes first)
      tokio::select! {
        log = self.log_rx.recv() => {
          if let Some(log) = log {
            self.buffered_log = Some(log);
          } else {
            self.log_closed = true;
            // Immediately check if the other channel is also closed/has data
            match self.state_rx.try_recv() {
              Ok(state) => {
                self.buffered_state = Some(state);
              },
              Err(TryRecvError::Disconnected) => {
                self.state_closed = true;
              },
              Err(TryRecvError::Empty) => {},
            }
          }
        }
        state = self.state_rx.recv() => {
          if let Some(state) = state {
            self.buffered_state = Some(state);
          } else {
            self.state_closed = true;
            // Immediately check if the other channel is also closed/has data
            match self.log_rx.try_recv() {
              Ok(log) => {
                self.buffered_log = Some(log);
              },
              Err(TryRecvError::Disconnected) => {
                self.log_closed = true;
              },
              Err(TryRecvError::Empty) => {},
            }
          }
        }
      }
    }
  }
}
