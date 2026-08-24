// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_client_common::PlatformMutex;
use bd_log_primitives::size::MemorySized;
use bd_log_primitives::{DataValue, LogLevel, LogLine, log_level};
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::MemoryPressureLevel;
use bd_proto::protos::logging::payload::LogType;
use std::sync::Arc;
use tokio::sync::Notify;

mod retention;

#[cfg(test)]
#[path = "./event_buffer_prop_test.rs"]
mod prop_tests;
#[cfg(test)]
#[path = "./lib_test.rs"]
mod tests;

pub use retention::EventBufferState;

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

#[derive(Debug)]
pub struct CapturedLog {
  pub log: LogLine,
  completion: Option<bd_completion::Sender<()>>,
  pub blocking: bool,
}
impl CapturedLog {
  #[must_use]
  pub fn new(log: LogLine, blocking: bool, completion: Option<bd_completion::Sender<()>>) -> Self {
    Self {
      log,
      completion,
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
      completion.send(());
    }
  }
  fn take_completion(&mut self) -> Option<bd_completion::Sender<()>> {
    match self {
      Self::Log(log) => log.completion.take(),
      Self::State(StateUpdateMessage::FlushState(completion)) => completion.take(),
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
}

impl EventBuffer {
  #[must_use]
  pub fn new(limits: EventBufferLimits) -> Self {
    Self {
      inner: Arc::new(EventBufferInner {
        state: PlatformMutex::new(LoggerEventBufferState {
          retention: EventBufferState::new(limits),
        }),
        notify: Notify::new(),
      }),
    }
  }
  pub fn set_pending_limits(&self, limits: EventBufferLimits) {
    self.inner.state.lock().retention.set_pending_limits(limits);
  }
  #[must_use]
  pub fn admit(&self, entry: EventBufferEntry) -> AdmissionOutcome {
    let outcome = {
      let mut state = self.inner.state.lock();
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
}

//
// RetentionLane
//

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RetentionLane {
  /// Verbose, non-blocking logs that yield to every other lane.
  Low,
  /// Normal logs that yield only to protected entries.
  High,
  /// State updates and logs that must not be evicted.
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
  /// Maximum bytes used by evictable log entries across the low and high lanes.
  pub log_limit_bytes: usize,
  /// Maximum bytes used by every entry, including protected state updates.
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
