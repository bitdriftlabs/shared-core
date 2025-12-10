// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./ordered_receiver_test.rs"]
mod tests;

use bd_bounded_buffer::Receiver;

/// Merges two channels (logs and state updates) into a single ordered stream based on sequence
/// numbers. This ensures that single-threaded callers doing `log(); setField(); log();` observe
/// the same ordering in the engine.
pub enum OrderedMessage<L, S> {
  Log(L),
  State(S),
}

pub struct OrderedReceiver<L, S> {
  log_rx: Receiver<SequencedMessage<L>>,
  state_rx: Receiver<SequencedMessage<S>>,
  // Buffer at most one message from each channel
  buffered_log: Option<SequencedMessage<L>>,
  buffered_state: Option<SequencedMessage<S>>,
}

#[derive(Debug)]
pub struct SequencedMessage<T> {
  pub sequence: u64,
  pub message: T,
}

impl<L, S> OrderedReceiver<L, S> {
  pub fn new(
    log_rx: Receiver<SequencedMessage<L>>,
    state_rx: Receiver<SequencedMessage<S>>,
  ) -> Self {
    Self {
      log_rx,
      state_rx,
      buffered_log: None,
      buffered_state: None,
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

      // If we only have one buffered, return it (single-threaded case - both will be queued)
      if let Some(log) = self.buffered_log.take() {
        return Some(OrderedMessage::Log(log.message));
      }
      if let Some(state) = self.buffered_state.take() {
        return Some(OrderedMessage::State(state.message));
      }

      // Need to receive from channels. Use biased select to check log channel first.
      tokio::select! {
        biased;
        log = self.log_rx.recv(), if self.buffered_log.is_none() => {
          match log {
            Some(log) => self.buffered_log = Some(log),
            None if self.buffered_state.is_some() => {
              // Log channel closed but still have state buffered
              let state = self.buffered_state.take()?;
              return Some(OrderedMessage::State(state.message));
            }
            None => return None, // Both channels closed or only log closed with no state
          }
        }
        state = self.state_rx.recv(), if self.buffered_state.is_none() => {
          match state {
            Some(state) => self.buffered_state = Some(state),
            None if self.buffered_log.is_some() => {
              // State channel closed but still have log buffered
              let log = self.buffered_log.take()?;
              return Some(OrderedMessage::Log(log.message));
            }
            None => return None, // Both channels closed or only state closed with no log
          }
        }
      }
    }
  }
}
