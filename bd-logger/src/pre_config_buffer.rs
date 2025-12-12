// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./pre_config_buffer_test.rs"]
mod pre_config_buffer_test;
use bd_client_stats_store::{Counter, Scope};
use bd_log_primitives::size::MemorySized;
use bd_stats_common::labels;
use protobuf::MessageDyn as _;

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum Error {
  #[error("Full size overflow")]
  FullSizeOverflow,
}

//
// PendingStateOperation
//

/// Represents a state update operation that occurred before initialization.
/// These operations are queued and replayed after initialization to ensure
/// proper ordering with logs and proper workflow state transitions.
#[derive(Debug, Clone)]
pub enum PendingStateOperation {
  SetFeatureFlagExposure {
    name: String,
    variant: bd_state::Value,
    session_id: String,
  },
}

//
// PreConfigItem
//

/// An item that can be stored in the pre-config buffer, representing either
/// a log or a state operation that occurred before initialization. This allows
/// both logs and state changes to be replayed in the exact order they arrived.
#[derive(Debug)]
pub enum PreConfigItem {
  Log(bd_log_primitives::Log),
  StateOperation(PendingStateOperation),
}

impl MemorySized for PreConfigItem {
  fn size(&self) -> usize {
    // Don't add extra discriminant overhead - the enum's memory layout already accounts for it.
    // We only need to measure the actual data size of each variant.
    match self {
      Self::Log(log) => log.size(),
      Self::StateOperation(PendingStateOperation::SetFeatureFlagExposure {
        name,
        variant,
        session_id,
      }) => {
        name.len()
          + usize::try_from(variant.compute_size_dyn()).unwrap_or_default()
          + session_id.len()
      },
    }
  }
}

/// An in-memory buffer that is used to buffer events that arrive before an initial logger
/// configuration has been received. This allows us to capture some number of events that can be
/// "replayed" once the configuration has been applied.
#[derive(Debug)]
pub struct PreConfigBuffer<T: MemorySized + std::fmt::Debug> {
  max_size: usize,

  current_size: usize,
  items: Vec<T>,
}

impl<T: MemorySized + std::fmt::Debug> PreConfigBuffer<T> {
  pub const fn new(max_size: usize) -> Self {
    Self {
      max_size,
      current_size: 0,
      items: vec![],
    }
  }

  pub fn push(&mut self, entry: T) -> Result<(), Error> {
    let log_size = entry.size();
    if self.current_size + log_size > self.max_size {
      log::debug!(
        "failed to enqueue log due to items size limit ({}), current size: {}, log size: {}",
        self.max_size,
        self.current_size,
        log_size,
      );
      // Adding a log to the buffer would make it exceed
      // configured bytes size limit.
      return Err(Error::FullSizeOverflow);
    }

    self.current_size += log_size;
    self.items.push(entry);

    Ok(())
  }

  pub fn pop_all(mut self) -> impl Iterator<Item = T> {
    self.current_size = 0;
    self.items.into_iter()
  }

  pub const fn max_size(&self) -> usize {
    self.max_size
  }
}


//
// PushCounters
//

pub struct PushCounters {
  ok: Counter,
  err_full_size_overflow: Counter,
}

impl PushCounters {
  pub(crate) fn new(scope: &Scope) -> Self {
    Self {
      ok: scope.counter_with_labels("log_enqueueing", labels!("result" => "success")),
      err_full_size_overflow: scope.counter_with_labels(
        "log_enqueueing",
        labels!("result" => "failure_size_overflow"),
      ),
    }
  }

  pub(crate) fn record(&self, result: &std::result::Result<(), Error>) {
    match result {
      Ok(()) => self.ok.inc(),
      Err(Error::FullSizeOverflow) => {
        self.err_full_size_overflow.inc();
      },
    }
  }
}
