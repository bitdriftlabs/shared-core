// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./init_buffer_test.rs"]
mod init_buffer_test;

use bd_client_stats_store::{Counter, Histogram, Scope};
use bd_log_primitives::size::MemorySized;
use bd_stats_common::{Counter as _, Histogram as _, labels};
use std::pin::Pin;
use time::Duration;
use tokio::time::{Instant, Sleep};

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum Error {
  #[error("Full size overflow")]
  FullSizeOverflow,
}

//
// ReplayReason
//

/// Identifies why buffered initialization work was replayed.
#[derive(Clone, Copy)]
pub enum ReplayReason {
  Scheduled,
  CapacityLog,
  CapacityStateOperation,
}

//
// PendingStateOperation
//

/// Represents a state update that must be replayed in startup order with buffered logs.
#[derive(Debug, Clone)]
pub enum PendingStateOperation {
  SetFeatureFlagExposure {
    name: String,
    variant: Option<String>,
    session_id: String,
  },
}

//
// InitItem
//

/// A log or state update held until initialization replay begins.
#[derive(Debug)]
pub enum InitItem {
  PreviousRunLog(bd_log_primitives::Log),
  Log(bd_log_primitives::Log),
  StateOperation(PendingStateOperation),
}

pub trait Prioritizable {
  fn is_prioritized(&self) -> bool;
}

impl MemorySized for InitItem {
  fn size(&self) -> usize {
    // Don't add extra discriminant overhead - the enum's memory layout already accounts for it.
    // We only need to measure the actual data size of each variant.
    match self {
      Self::PreviousRunLog(log) | Self::Log(log) => log.size(),
      Self::StateOperation(PendingStateOperation::SetFeatureFlagExposure {
        name,
        variant,
        session_id,
      }) => name.len() + variant.as_ref().map_or(0, String::len) + session_id.len(),
    }
  }
}

impl Prioritizable for InitItem {
  fn is_prioritized(&self) -> bool {
    matches!(self, Self::PreviousRunLog(_))
  }
}

//
// InitBuffer
//

/// A FIFO of startup work with a soft memory limit. One item may exceed the limit, and prior-run
/// crash logs can be drained first while preserving their order and the order of all other items.
#[derive(Debug)]
pub struct InitBuffer<T: MemorySized + Prioritizable + std::fmt::Debug> {
  max_size: usize,
  current_size: usize,
  over_limit_item_accepted: bool,
  priority_items: Vec<T>,
  items: Vec<T>,
}

impl<T: MemorySized + Prioritizable + std::fmt::Debug> InitBuffer<T> {
  pub const fn new(max_size: usize) -> Self {
    Self {
      max_size,
      current_size: 0,
      over_limit_item_accepted: false,
      priority_items: vec![],
      items: vec![],
    }
  }

  pub fn can_push(&self, entry: &T) -> bool {
    self.can_push_size(entry.size())
  }

  pub const fn can_push_size(&self, size: usize) -> bool {
    self.current_size + size <= self.max_size || !self.over_limit_item_accepted
  }

  pub fn push(&mut self, entry: T) -> Result<(), Error> {
    let entry_size = entry.size();
    if !self.can_push(&entry) {
      log::debug!(
        "failed to enqueue init item due to size limit ({}), current size: {}, item size: {}",
        self.max_size,
        self.current_size,
        entry_size,
      );
      // Adding an item to the buffer would make it exceed the configured byte limit.
      return Err(Error::FullSizeOverflow);
    }

    if self.current_size + entry_size > self.max_size {
      self.over_limit_item_accepted = true;
      log::debug!(
        "accepting init item beyond size limit ({}), current size: {}, item size: {}",
        self.max_size,
        self.current_size,
        entry_size,
      );
    }
    self.current_size += entry_size;
    if entry.is_prioritized() {
      self.priority_items.push(entry);
    } else {
      self.items.push(entry);
    }
    Ok(())
  }

  pub fn drain(mut self) -> impl Iterator<Item = T> {
    self.current_size = 0;
    self.priority_items.into_iter().chain(self.items)
  }

  pub const fn item_count(&self) -> usize {
    self.priority_items.len() + self.items.len()
  }

  pub const fn max_size(&self) -> usize {
    self.max_size
  }
}

//
// PendingInitBuffer
//

/// Holds startup work after configuration has created a processing pipeline but before that work
/// is replayed. The replay deadline is extended at most once for a pending crash report.
pub struct PendingInitBuffer {
  buffer: InitBuffer<InitItem>,
  stats: InitBufferStats,
  replay_sleep: Option<Pin<Box<Sleep>>>,
  replay_deadline: Option<Instant>,
  crash_pending_delay_applied: bool,
}

impl PendingInitBuffer {
  pub fn new(buffer: InitBuffer<InitItem>, stats: InitBufferStats) -> Self {
    Self {
      buffer,
      stats,
      replay_sleep: None,
      replay_deadline: None,
      crash_pending_delay_applied: false,
    }
  }

  pub const fn buffer(&self) -> &InitBuffer<InitItem> {
    &self.buffer
  }

  pub fn push(&mut self, item: InitItem) -> Result<(), Error> {
    let result = self.buffer.push(item);
    self.stats.record_push(&result);
    result
  }

  pub fn schedule(
    &mut self,
    base_delay: Duration,
    crash_pending: bool,
    crash_delay: Duration,
  ) -> bool {
    let replay_delay = if crash_pending {
      self.crash_pending_delay_applied = true;
      self.stats.record_crash_hint(crash_delay);
      base_delay + crash_delay
    } else {
      base_delay
    };

    if replay_delay.is_zero() {
      return true;
    }

    let deadline = Instant::now() + replay_delay.unsigned_abs();
    self.replay_deadline = Some(deadline);
    self.replay_sleep = Some(Box::pin(tokio::time::sleep_until(deadline)));
    false
  }

  pub fn apply_crash_pending_hint(&mut self, crash_delay: Duration) -> bool {
    if self.crash_pending_delay_applied {
      self.stats.record_crash_hint_already_applied();
      return false;
    }
    self.crash_pending_delay_applied = true;
    self.stats.record_crash_hint(crash_delay);

    if crash_delay.is_zero() {
      return false;
    }

    let deadline = self.replay_deadline.unwrap_or_else(Instant::now) + crash_delay.unsigned_abs();
    self.replay_deadline = Some(deadline);
    if let Some(sleep) = &mut self.replay_sleep {
      sleep.as_mut().reset(deadline);
    } else {
      self.replay_sleep = Some(Box::pin(tokio::time::sleep_until(deadline)));
    }
    true
  }

  pub fn replay_sleep(&mut self) -> &mut Option<Pin<Box<Sleep>>> {
    &mut self.replay_sleep
  }

  pub fn drain(mut self, reason: ReplayReason) -> impl Iterator<Item = InitItem> {
    self.replay_sleep = None;
    self.replay_deadline = None;
    self
      .stats
      .record_replay(reason, self.buffer.item_count(), self.buffer.current_size);
    self.buffer.drain()
  }
}

//
// InitBufferStats
//

/// Metrics for startup buffering. The legacy metric scope is retained for dashboard compatibility.
pub struct InitBufferStats {
  pushes: PushCounters,
  replay_scheduled: Counter,
  replay_capacity_log: Counter,
  replay_capacity_state_operation: Counter,
  replay_item_count: Histogram,
  replay_byte_count: Histogram,
  crash_hint_extension_applied: Counter,
  crash_hint_already_applied: Counter,
  crash_hint_delay_disabled: Counter,
}

impl InitBufferStats {
  pub(crate) fn new(scope: &Scope) -> Self {
    let scope = scope.scope("pre_config_log_buffer");
    Self {
      pushes: PushCounters::new(&scope),
      replay_scheduled: scope
        .counter_with_labels("init_buffer_replay", labels!("reason" => "scheduled")),
      replay_capacity_log: scope
        .counter_with_labels("init_buffer_replay", labels!("reason" => "capacity_log")),
      replay_capacity_state_operation: scope.counter_with_labels(
        "init_buffer_replay",
        labels!("reason" => "capacity_state_operation"),
      ),
      replay_item_count: scope.histogram("init_buffer_replay_item_count"),
      replay_byte_count: scope.histogram("init_buffer_replay_byte_count"),
      crash_hint_extension_applied: scope.counter_with_labels(
        "init_buffer_crash_hint",
        labels!("outcome" => "extension_applied"),
      ),
      crash_hint_already_applied: scope.counter_with_labels(
        "init_buffer_crash_hint",
        labels!("outcome" => "already_applied"),
      ),
      crash_hint_delay_disabled: scope.counter_with_labels(
        "init_buffer_crash_hint",
        labels!("outcome" => "delay_disabled"),
      ),
    }
  }

  pub(crate) fn record_push(&self, result: &std::result::Result<(), Error>) {
    self.pushes.record(result);
  }

  fn record_replay(&self, reason: ReplayReason, item_count: usize, byte_count: usize) {
    match reason {
      ReplayReason::Scheduled => self.replay_scheduled.inc(),
      ReplayReason::CapacityLog => self.replay_capacity_log.inc(),
      ReplayReason::CapacityStateOperation => self.replay_capacity_state_operation.inc(),
    }
    self.replay_item_count.observe(histogram_value(item_count));
    self.replay_byte_count.observe(histogram_value(byte_count));
  }

  fn record_crash_hint(&self, crash_delay: Duration) {
    if crash_delay.is_zero() {
      self.crash_hint_delay_disabled.inc();
    } else {
      self.crash_hint_extension_applied.inc();
    }
  }

  fn record_crash_hint_already_applied(&self) {
    self.crash_hint_already_applied.inc();
  }
}

fn histogram_value(value: usize) -> f64 {
  f64::from(u32::try_from(value).unwrap_or(u32::MAX))
}

//
// PushCounters
//

struct PushCounters {
  ok: Counter,
  err_full_size_overflow: Counter,
}

impl PushCounters {
  fn new(scope: &Scope) -> Self {
    Self {
      ok: scope.counter_with_labels("log_enqueueing", labels!("result" => "success")),
      err_full_size_overflow: scope.counter_with_labels(
        "log_enqueueing",
        labels!("result" => "failure_size_overflow"),
      ),
    }
  }

  fn record(&self, result: &std::result::Result<(), Error>) {
    match result {
      Ok(()) => self.ok.inc(),
      Err(Error::FullSizeOverflow) => self.err_full_size_overflow.inc(),
    }
  }
}
