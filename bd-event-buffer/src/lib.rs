// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_client_common::PlatformMutex;
use bd_log_primitives::{AnnotatedLogFields, DataValue, LogFields, LogLevel, LogLine, log_level};
use bd_macros::ApproximateSize;
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::MemoryPressureLevel;
use bd_proto::protos::logging::payload::LogType;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::sync::Notify;

#[cfg(test)]
#[path = "./event_buffer_prop_test.rs"]
mod prop_tests;
#[cfg(test)]
#[path = "./lib_test.rs"]
mod tests;

mod retention;

pub use retention::EventBufferState;

#[derive(ApproximateSize, Debug)]
pub enum StateUpdateMessage {
  AddLogField(String, DataValue),
  UpdateOotbLogField(String, DataValue),
  RemoveLogField(String),
  SetMemoryPressureLevel {
    #[approximate_size(skip)]
    level: MemoryPressureLevel,
  },
  SetEntityId(Option<String>),
  FlushState(#[approximate_size(skip)] Option<bd_completion::Sender<()>>),
}

//
// ProviderSnapshot
//

/// Results returned by field providers at the producer-side capture point.
#[derive(ApproximateSize, Debug)]
pub struct ProviderSnapshot {
  #[approximate_size(skip)]
  pub timestamp: OffsetDateTime,
  #[approximate_size(with = bd_log_primitives::approximate_ahash_map_children_bytes)]
  pub ootb_fields: LogFields,
  #[approximate_size(with = bd_log_primitives::approximate_ahash_map_children_bytes)]
  pub custom_fields: LogFields,
}

//
// CurrentProcessContext
//

/// Immutable current-process context used by logs and workflow-replayed state operations.
#[derive(ApproximateSize, Debug)]
pub struct CurrentProcessContext {
  pub session_id: String,
  pub provider: ProviderSnapshot,
  /// This snapshot is shared with logger field-map accounting and is not charged per entry.
  pub logger_fields: Arc<AnnotatedLogFields>,
  pub admitted_at: OffsetDateTime,
}

//
// CapturedContext
//

/// Context captured for an `EventBuffer` entry before ALB processes it.
#[derive(ApproximateSize, Debug)]
pub enum CapturedContext {
  CurrentProcess(CurrentProcessContext),
  /// Previous-process logs are finalized against prior global state on the consumer.
  PreviousProcess,
}

//
// CapturedEventPayload
//

#[derive(ApproximateSize, Debug)]
pub enum CapturedEventPayload {
  Log(LogLine),
  FeatureFlagExposure {
    flag: String,
    variant: Option<String>,
  },
}

//
// CapturedEvent
//

/// A producer-side snapshot retained until ALB finishes processing the event.
#[derive(ApproximateSize, Debug)]
pub struct CapturedEvent {
  pub context: CapturedContext,
  pub payload: CapturedEventPayload,
  #[approximate_size(skip)]
  completion: Option<bd_completion::Sender<()>>,
}

impl CapturedEvent {
  #[must_use]
  pub fn log(
    log: LogLine,
    context: CapturedContext,
    completion: Option<bd_completion::Sender<()>>,
  ) -> Self {
    Self {
      context,
      payload: CapturedEventPayload::Log(log),
      completion,
    }
  }

  #[must_use]
  pub fn feature_flag_exposure(
    flag: String,
    variant: Option<String>,
    context: CurrentProcessContext,
  ) -> Self {
    Self {
      context: CapturedContext::CurrentProcess(context),
      payload: CapturedEventPayload::FeatureFlagExposure { flag, variant },
      completion: None,
    }
  }
}

#[derive(ApproximateSize, Debug)]
pub enum EventBufferEntry {
  Captured(Box<CapturedEvent>),
  State(StateUpdateMessage),
}

impl EventBufferEntry {
  #[must_use]
  pub fn captured(event: CapturedEvent) -> Self {
    Self::Captured(Box::new(event))
  }

  #[must_use]
  pub fn lane(&self) -> RetentionLane {
    match self {
      Self::Captured(event) => match &event.payload {
        CapturedEventPayload::Log(log) => retention_lane(log.log_type, log.log_level),
        CapturedEventPayload::FeatureFlagExposure { .. } => RetentionLane::Protected,
      },
      Self::State(_) => RetentionLane::Protected,
    }
  }

  pub fn complete(mut self) {
    if let Some(completion) = self.take_completion() {
      completion.send(());
    }
  }

  fn take_completion(&mut self) -> Option<bd_completion::Sender<()>> {
    match self {
      Self::Captured(event) => event.completion.take(),
      Self::State(StateUpdateMessage::FlushState(completion)) => completion.take(),
      Self::State(_) => None,
    }
  }
}

#[must_use]
pub fn retention_lane(log_type: LogType, log_level: LogLevel) -> RetentionLane {
  if matches!(log_type, LogType::LIFECYCLE | LogType::DEVICE) {
    RetentionLane::Protected
  } else if log_level <= log_level::DEBUG {
    RetentionLane::Low
  } else {
    RetentionLane::High
  }
}

//
// EventBuffer
//

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
      state
        .retention
        .admit(entry.lane(), entry.approximate_size_bytes(), entry)
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
