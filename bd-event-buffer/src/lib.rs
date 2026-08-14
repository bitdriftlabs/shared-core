// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_client_common::PlatformMutex;
use bd_log_primitives::size::MemorySized;
use bd_log_primitives::{DataValue, LogFields, LogLevel, LogLine, log_level};
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::MemoryPressureLevel;
use bd_proto::protos::logging::payload::LogType;
use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use tokio::sync::{Notify, oneshot};

#[cfg(test)]
#[path = "./event_buffer_prop_test.rs"]
mod prop_tests;
#[cfg(test)]
#[path = "./lib_test.rs"]
mod tests;

pub const ENTRY_OVERHEAD_BYTES: usize = 64;

#[derive(Debug)]
pub enum StateUpdateMessage {
  AddLogField(String, DataValue),
  UpdateOotbLogField(String, DataValue),
  RemoveLogField(String),
  SetFeatureFlagExposure(String, Option<String>),
  SetMemoryPressureLevel { level: MemoryPressureLevel },
  SetEntityId(Option<String>),
  FlushState(Option<bd_completion::Sender<()>>),
}

impl MemorySized for StateUpdateMessage {
  fn size(&self) -> usize {
    std::mem::size_of_val(self)
      + match self {
        Self::AddLogField(key, value) | Self::UpdateOotbLogField(key, value) => {
          key.size() + value.size()
        },
        Self::RemoveLogField(key) => key.len(),
        Self::SetFeatureFlagExposure(flag, variant) => {
          flag.len() + variant.as_ref().map_or(0, String::len)
        },
        Self::SetMemoryPressureLevel { .. } => 0,
        Self::SetEntityId(id) => id.as_ref().map_or(0, String::len),
        Self::FlushState(sender) => std::mem::size_of_val(sender),
      }
  }
}

enum CompletionHandle {
  Log(oneshot::Sender<()>),
  State(bd_completion::Sender<()>),
}
impl fmt::Debug for CompletionHandle {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("CompletionHandle")
  }
}
impl CompletionHandle {
  fn complete(self) {
    match self {
      Self::Log(sender) => {
        let _ = sender.send(());
      },
      Self::State(sender) => sender.send(()),
    }
  }
}

#[derive(Debug)]
pub struct CapturedLog {
  pub log: LogLine,
  pub logger_fields: Arc<LogFields>,
  completion: Option<CompletionHandle>,
  pub blocking: bool,
}
impl CapturedLog {
  pub fn new(log: LogLine, blocking: bool, completion: Option<oneshot::Sender<()>>) -> Self {
    Self {
      log,
      logger_fields: Arc::default(),
      completion: completion.map(CompletionHandle::Log),
      blocking,
    }
  }
}

#[derive(Debug)]
pub enum EventBufferEntry {
  Log(CapturedLog),
  State(StateUpdateMessage),
}
impl EventBufferEntry {
  #[must_use]
  pub fn lane(&self) -> RetentionLane {
    match self {
      Self::Log(log) => retention_lane(log.log.log_type, log.log.log_level, log.blocking),
      Self::State(_) => RetentionLane::Protected,
    }
  }
  #[must_use]
  pub fn size(&self) -> usize {
    ENTRY_OVERHEAD_BYTES
      + match self {
        Self::Log(log) => log.log.size(),
        Self::State(state) => state.size(),
      }
  }
  pub fn complete(mut self) {
    if let Some(completion) = self.take_completion() {
      completion.complete();
    }
  }
  fn take_completion(&mut self) -> Option<CompletionHandle> {
    match self {
      Self::Log(log) => log.completion.take(),
      Self::State(StateUpdateMessage::FlushState(completion)) => {
        completion.take().map(CompletionHandle::State)
      },
      Self::State(_) => None,
    }
  }
}

#[must_use]
pub fn retention_lane(log_type: LogType, log_level: LogLevel, blocking: bool) -> RetentionLane {
  if blocking || matches!(log_type, LogType::LIFECYCLE | LogType::DEVICE) {
    RetentionLane::Protected
  } else if log_level <= log_level::DEBUG {
    RetentionLane::Low
  } else {
    RetentionLane::High
  }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LoggerFieldMapLimits {
  pub max_fields: usize,
  pub max_bytes: usize,
}
impl Default for LoggerFieldMapLimits {
  fn default() -> Self {
    Self {
      max_fields: 128,
      max_bytes: 32 * 1024,
    }
  }
}

#[derive(Debug, thiserror::Error)]
pub enum FieldMapError {
  #[error("logger field map has reached its field count limit")]
  TooManyFields,
  #[error("logger field map has reached its byte limit")]
  TooManyBytes,
  #[error(transparent)]
  InvalidFieldName(#[from] anyhow::Error),
}

#[derive(Clone)]
pub struct EventBuffer {
  inner: Arc<EventBufferInner>,
}
struct EventBufferInner {
  state: PlatformMutex<LoggerEventBufferState>,
  notify: Notify,
}
struct LoggerEventBufferState {
  retention: EventBufferState<EventBufferEntry>,
  field_limits: LoggerFieldMapLimits,
  logger_fields: Arc<LogFields>,
}

impl EventBuffer {
  #[must_use]
  pub fn new(limits: EventBufferLimits, field_limits: LoggerFieldMapLimits) -> Self {
    Self {
      inner: Arc::new(EventBufferInner {
        state: PlatformMutex::new(LoggerEventBufferState {
          retention: EventBufferState::new(limits),
          field_limits,
          logger_fields: Arc::default(),
        }),
        notify: Notify::new(),
      }),
    }
  }
  pub fn set_pending_limits(&self, limits: EventBufferLimits) {
    self.inner.state.lock().retention.set_pending_limits(limits);
  }
  pub fn set_field(
    &self,
    key: bd_log_primitives::LogFieldKey,
    value: bd_log_primitives::LogFieldValue,
  ) -> Result<(), FieldMapError> {
    bd_log_primitives::verify_custom_field_name(&key)?;
    let mut state = self.inner.state.lock();
    let existing_bytes = state
      .logger_fields
      .get(&key)
      .map_or(0, |existing| field_size(&key, existing));
    let new_bytes = field_size(&key, &value);
    let field_count =
      state.logger_fields.len() + usize::from(!state.logger_fields.contains_key(&key));
    let byte_count = fields_size(&state.logger_fields) - existing_bytes + new_bytes;
    if field_count > state.field_limits.max_fields {
      return Err(FieldMapError::TooManyFields);
    }
    if byte_count > state.field_limits.max_bytes {
      return Err(FieldMapError::TooManyBytes);
    }
    Arc::make_mut(&mut state.logger_fields).insert(key, value);
    Ok(())
  }
  pub fn remove_field(&self, key: &str) {
    Arc::make_mut(&mut self.inner.state.lock().logger_fields).remove(key);
  }
  #[must_use]
  pub fn admit(&self, mut entry: EventBufferEntry) -> AdmissionOutcome {
    let outcome = {
      let mut state = self.inner.state.lock();
      if let EventBufferEntry::Log(log) = &mut entry {
        log.logger_fields = state.logger_fields.clone();
      }
      state.retention.admit(entry.lane(), entry.size(), entry)
    };
    if outcome == AdmissionOutcome::Admitted {
      self.inner.notify.notify_one();
    }
    outcome
  }
  pub async fn next_batch(&self, max_entries: usize) -> Vec<EventBufferEntry> {
    assert!(max_entries > 0, "next_batch requires a non-zero batch size");
    loop {
      let notified = self.inner.notify.notified();
      let (batch, closed) = {
        let mut state = self.inner.state.lock();
        (
          state.retention.take_batch(max_entries),
          state.retention.is_closed(),
        )
      };
      if !batch.is_empty() || closed {
        return batch;
      }
      notified.await;
    }
  }
  pub fn close(&self) {
    self.inner.state.lock().retention.close();
    self.inner.notify.notify_waiters();
  }
  pub fn reserve_fixture_capacity(&self) {
    self.inner.state.lock().retention.reserve_fixture_capacity();
  }
}
fn field_size(
  key: &bd_log_primitives::LogFieldKey,
  value: &bd_log_primitives::LogFieldValue,
) -> usize {
  ENTRY_OVERHEAD_BYTES + key.len() + value.size()
}
fn fields_size(fields: &LogFields) -> usize {
  fields
    .iter()
    .map(|(key, value)| field_size(key, value))
    .sum()
}

//
// RetentionLane
//

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RetentionLane {
  Low,
  High,
  Protected,
}

impl RetentionLane {
  #[must_use]
  pub const fn is_evictable(self) -> bool {
    !matches!(self, Self::Protected)
  }
}

//
// EventBufferLimits
//

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EventBufferLimits {
  pub log_limit_bytes: usize,
  pub total_limit_bytes: usize,
}

//
// AdmissionOutcome
//

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionOutcome {
  Admitted,
  RejectedFull,
  RejectedOversized,
  Closed,
}

//
// EventBufferState
//

/// A synchronous, priority-aware retention state machine. The owner supplies synchronization and
/// associates its own entry payload with the provided lane and accounting size.
pub struct EventBufferState<T> {
  limits: EventBufferLimits,
  pending_limits: Option<EventBufferLimits>,
  next_admission_id: u64,
  closed: bool,
  protected: VecDeque<QueuedEntry<T>>,
  high: VecDeque<QueuedEntry<T>>,
  low: VecDeque<QueuedEntry<T>>,
  protected_bytes: usize,
  evictable_bytes: usize,
  high_bytes: usize,
  low_bytes: usize,
}

struct QueuedEntry<T> {
  admission_id: u64,
  bytes: usize,
  entry: T,
}

impl<T> EventBufferState<T> {
  #[must_use]
  pub fn new(limits: EventBufferLimits) -> Self {
    Self {
      limits,
      pending_limits: None,
      next_admission_id: 0,
      closed: false,
      protected: VecDeque::new(),
      high: VecDeque::new(),
      low: VecDeque::new(),
      protected_bytes: 0,
      evictable_bytes: 0,
      high_bytes: 0,
      low_bytes: 0,
    }
  }

  pub fn set_pending_limits(&mut self, limits: EventBufferLimits) {
    self.pending_limits = Some(limits);
  }

  /// Applies a prevalidated admission. Rejected entries are dropped by this call.
  pub fn admit(&mut self, lane: RetentionLane, bytes: usize, entry: T) -> AdmissionOutcome {
    self.apply_pending_limits();
    if self.closed {
      return AdmissionOutcome::Closed;
    }
    if bytes > self.limits.total_limit_bytes
      || (lane.is_evictable() && bytes > self.limits.log_limit_bytes)
    {
      return AdmissionOutcome::RejectedOversized;
    }
    if !self.reserve(lane) || !self.make_room(lane, bytes) {
      return AdmissionOutcome::RejectedFull;
    }

    let admission_id = self.next_admission_id;
    self.next_admission_id = self.next_admission_id.wrapping_add(1);
    self.push(
      QueuedEntry {
        admission_id,
        bytes,
        entry,
      },
      lane,
    );
    AdmissionOutcome::Admitted
  }

  #[must_use]
  pub fn take_batch(&mut self, max_entries: usize) -> Vec<T> {
    let mut result = Vec::with_capacity(max_entries);
    while result.len() < max_entries {
      let lane = [
        RetentionLane::Protected,
        RetentionLane::High,
        RetentionLane::Low,
      ]
      .into_iter()
      .filter_map(|lane| {
        self
          .queue(lane)
          .front()
          .map(|entry| (lane, entry.admission_id))
      })
      .min_by_key(|(_, admission_id)| *admission_id)
      .map(|(lane, _)| lane);
      let Some(lane) = lane else { break };
      let Some(entry) = self.queue_mut(lane).pop_front() else {
        break;
      };
      self.remove_bytes(lane, entry.bytes);
      result.push(entry.entry);
    }
    result
  }

  pub fn close(&mut self) {
    if !self.closed {
      self.closed = true;
      self.discard_all();
    }
  }

  #[must_use]
  pub const fn is_closed(&self) -> bool {
    self.closed
  }

  /// Reserves fixture capacity before a benchmark's instrumented region.
  pub fn reserve_fixture_capacity(&mut self) {
    for lane in [
      RetentionLane::Low,
      RetentionLane::High,
      RetentionLane::Protected,
    ] {
      let _ = self.queue_mut(lane).try_reserve(1);
    }
  }

  fn reserve(&mut self, lane: RetentionLane) -> bool {
    self.queue_mut(lane).try_reserve(1).is_ok()
  }

  fn apply_pending_limits(&mut self) {
    let Some(limits) = self.pending_limits.take() else {
      return;
    };
    self.limits = limits;
    self.evict_for_budget_shrink(self.log_bytes_over_limit());
    self.evict_for_budget_shrink(self.total_bytes_over_limit());
  }

  fn make_room(&mut self, lane: RetentionLane, incoming_bytes: usize) -> bool {
    let log_needed = if lane.is_evictable() {
      self
        .evictable_bytes
        .saturating_add(incoming_bytes)
        .saturating_sub(self.limits.log_limit_bytes)
    } else {
      0
    };
    let total_needed = self
      .total_bytes()
      .saturating_add(incoming_bytes)
      .saturating_sub(self.limits.total_limit_bytes);
    let bytes_needed = log_needed.max(total_needed);
    if self.evictable_bytes_available_to(lane) < bytes_needed {
      return false;
    }
    self.evict_for_limit(lane, bytes_needed)
  }

  fn evict_for_limit(&mut self, incoming_lane: RetentionLane, mut bytes_needed: usize) -> bool {
    match incoming_lane {
      RetentionLane::Low => bytes_needed == 0,
      RetentionLane::High => {
        bytes_needed = self.evict_from_lane(RetentionLane::Low, bytes_needed);
        bytes_needed == 0
      },
      RetentionLane::Protected => {
        bytes_needed = self.evict_from_lane(RetentionLane::Low, bytes_needed);
        bytes_needed = self.evict_from_lane(RetentionLane::High, bytes_needed);
        bytes_needed == 0
      },
    }
  }

  fn evict_for_budget_shrink(&mut self, mut bytes_needed: usize) {
    for lane in [RetentionLane::Low, RetentionLane::High] {
      bytes_needed = self.evict_from_lane(lane, bytes_needed);
    }
  }

  fn evict_from_lane(&mut self, lane: RetentionLane, bytes_needed: usize) -> usize {
    if bytes_needed == 0 {
      return 0;
    }
    let removed_bytes = {
      let queue = self.queue_mut(lane);
      let mut start = queue.len();
      let mut freed_bytes = 0;
      while start > 0 && freed_bytes < bytes_needed {
        start -= 1;
        freed_bytes += queue[start].bytes;
      }
      queue.truncate(start);
      freed_bytes
    };
    self.remove_bytes(lane, removed_bytes);
    bytes_needed.saturating_sub(removed_bytes)
  }

  fn push(&mut self, entry: QueuedEntry<T>, lane: RetentionLane) {
    self.add_bytes(lane, entry.bytes);
    self.queue_mut(lane).push_back(entry);
  }

  fn discard_all(&mut self) {
    for lane in [
      RetentionLane::Protected,
      RetentionLane::High,
      RetentionLane::Low,
    ] {
      self.queue_mut(lane).clear();
    }
    self.protected_bytes = 0;
    self.evictable_bytes = 0;
    self.high_bytes = 0;
    self.low_bytes = 0;
  }

  fn add_bytes(&mut self, lane: RetentionLane, bytes: usize) {
    match lane {
      RetentionLane::Low => {
        self.evictable_bytes += bytes;
        self.low_bytes += bytes;
      },
      RetentionLane::High => {
        self.evictable_bytes += bytes;
        self.high_bytes += bytes;
      },
      RetentionLane::Protected => self.protected_bytes += bytes,
    }
  }

  fn remove_bytes(&mut self, lane: RetentionLane, bytes: usize) {
    match lane {
      RetentionLane::Low => {
        self.evictable_bytes -= bytes;
        self.low_bytes -= bytes;
      },
      RetentionLane::High => {
        self.evictable_bytes -= bytes;
        self.high_bytes -= bytes;
      },
      RetentionLane::Protected => self.protected_bytes -= bytes,
    }
  }

  fn queue(&self, lane: RetentionLane) -> &VecDeque<QueuedEntry<T>> {
    match lane {
      RetentionLane::Low => &self.low,
      RetentionLane::High => &self.high,
      RetentionLane::Protected => &self.protected,
    }
  }

  fn queue_mut(&mut self, lane: RetentionLane) -> &mut VecDeque<QueuedEntry<T>> {
    match lane {
      RetentionLane::Low => &mut self.low,
      RetentionLane::High => &mut self.high,
      RetentionLane::Protected => &mut self.protected,
    }
  }

  fn total_bytes(&self) -> usize {
    self.protected_bytes + self.evictable_bytes
  }
  fn log_bytes_over_limit(&self) -> usize {
    self
      .evictable_bytes
      .saturating_sub(self.limits.log_limit_bytes)
  }
  fn total_bytes_over_limit(&self) -> usize {
    self
      .total_bytes()
      .saturating_sub(self.limits.total_limit_bytes)
  }
  fn evictable_bytes_available_to(&self, lane: RetentionLane) -> usize {
    match lane {
      RetentionLane::Low => 0,
      RetentionLane::High => self.low_bytes,
      RetentionLane::Protected => self.low_bytes + self.high_bytes,
    }
  }
}
