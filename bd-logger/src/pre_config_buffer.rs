// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./pre_config_buffer_test.rs"]
mod pre_config_buffer_test;
use crate::bounded_buffer::MemorySized;
use bd_client_stats_store::{Counter, Scope};
use bd_stats_common::labels;

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum Error {
  #[error("Full count overflow")]
  FullCountOverflow,
  #[error("Full size overflow")]
  FullSizeOverflow,
}

/// An in-memory log buffer that is used to buffer logs that arrive before an initial logger
/// configuration has been received. This allows us to capture some number of logs that can be
/// "replayed" once the configuration has been applied.
///
/// Note that another way of accomplishing this would be to use a volatile ring buffer to hold the
/// pending logs. While this would have some benefits around being fixed size, it would add
/// implementation complexity as we would have to convert the logs to flatbuffers then back to Rust
/// types in order to do the matching. The current approach allows for a much simpler approach where
/// we accept and return Rust type logs.
#[derive(Debug)]
pub struct PreConfigBuffer<T: MemorySized + std::fmt::Debug> {
  max_count: usize,
  max_size: usize,

  current_size: usize,
  items: Vec<T>,
}

impl<T: MemorySized + std::fmt::Debug> PreConfigBuffer<T> {
  pub const fn new(max_count: usize, max_size: usize) -> Self {
    Self {
      max_count,
      max_size,
      current_size: 0,
      items: vec![],
    }
  }

  pub fn push(&mut self, log: T) -> Result<(), Error> {
    // The buffer is full when it comes to the number of elements it holds.
    if self.items.len() >= self.max_count {
      log::debug!("failed to enqueue log due to items count limit");
      return Err(Error::FullCountOverflow);
    }

    let log_size = log.size();
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
    self.items.push(log);

    Ok(())
  }

  pub fn pop_all(mut self) -> impl Iterator<Item = T> {
    self.current_size = 0;
    self.items.into_iter()
  }

  pub const fn max_count(&self) -> usize {
    self.max_count
  }

  pub const fn max_size(&self) -> usize {
    self.max_size
  }
}


//
// PushCounters
//

pub(crate) struct PushCounters {
  ok: Counter,
  err_full_count_overflow: Counter,
  err_full_size_overflow: Counter,
}

impl PushCounters {
  pub(crate) fn new(scope: &Scope) -> Self {
    Self {
      ok: scope.counter_with_labels("log_enqueueing", labels!("result" => "success")),
      err_full_count_overflow: scope.counter_with_labels(
        "log_enqueueing",
        labels!("result" => "failure_count_overflow"),
      ),
      err_full_size_overflow: scope.counter_with_labels(
        "log_enqueueing",
        labels!("result" => "failure_size_overflow"),
      ),
    }
  }

  pub(crate) fn record(&self, result: &std::result::Result<(), Error>) {
    match result {
      Ok(()) => self.ok.inc(),
      Err(Error::FullCountOverflow) => {
        self.err_full_count_overflow.inc();
      },
      Err(Error::FullSizeOverflow) => {
        self.err_full_size_overflow.inc();
      },
    }
  }
}
