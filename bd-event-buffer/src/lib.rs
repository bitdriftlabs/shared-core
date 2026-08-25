// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_client_common::PlatformMutex;
use bd_log_primitives::{DataValue, LogFields, LogLevel, LogLine, log_level};
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

pub use retention::{AdmissionResult, EventBufferState, TerminalEntries};

//
// LoggerControl
//

/// A state mutation or flush request processed in FIFO order with logger ingress events.
#[derive(ApproximateSize, Debug)]
pub enum LoggerControl {
  AddLogField(String, DataValue),
  UpdateOotbLogField(String, DataValue),
  RemoveLogField(String),
  // Feature flags will move to `LoggerIngressPayload` when producer-side capture is wired into
  // ALB. Retain the control form until then so the existing logger path remains ordered.
  SetFeatureFlagExposure(String, Option<String>),
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
///
/// As we deprecate `FieldProviders` this will eventually just hold the timestamp.
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
// AdmissionContext
//

/// Immutable current-process context captured when an event is admitted.
#[derive(ApproximateSize, Debug)]
pub struct AdmissionContext {
  pub session_id: String,
  pub provider: ProviderSnapshot,
  pub admitted_at: OffsetDateTime,
}

//
// EventContext
//

/// Context captured for an `EventBuffer` entry before ALB processes it.
#[derive(ApproximateSize, Debug)]
pub enum EventContext {
  CurrentProcess(AdmissionContext),
  /// Previous-process logs are finalized against prior global state on the consumer.
  PreviousProcess,
}

//
// LoggerIngressPayload
//

#[derive(ApproximateSize, Debug)]
pub enum LoggerIngressPayload {
  Log(LogLine),
  FeatureFlagExposure {
    flag: String,
    variant: Option<String>,
  },
}

//
// LoggerIngressEvent
//

/// A producer-side snapshot retained until ALB finishes processing the event.
#[derive(ApproximateSize, Debug)]
pub struct LoggerIngressEvent {
  pub context: EventContext,
  pub payload: LoggerIngressPayload,
  #[approximate_size(skip)]
  completion: Option<bd_completion::Sender<()>>,
}

impl LoggerIngressEvent {
  #[must_use]
  pub fn log(
    log: LogLine,
    context: EventContext,
    completion: Option<bd_completion::Sender<()>>,
  ) -> Self {
    Self {
      context,
      payload: LoggerIngressPayload::Log(log),
      completion,
    }
  }

  #[must_use]
  pub fn feature_flag_exposure(
    flag: String,
    variant: Option<String>,
    context: AdmissionContext,
  ) -> Self {
    Self {
      context: EventContext::CurrentProcess(context),
      payload: LoggerIngressPayload::FeatureFlagExposure { flag, variant },
      completion: None,
    }
  }
}

//
// EventBufferEntry
//

/// A FIFO event-buffer entry.
///
/// Ingress events are boxed so control entries do not inherit the larger ingress-event layout in
/// the backing `VecDeque`.
#[derive(ApproximateSize, Debug)]
pub enum EventBufferEntry {
  Ingress(Box<LoggerIngressEvent>),
  Control(LoggerControl),
}

impl EventBufferEntry {
  #[must_use]
  pub fn ingress(event: LoggerIngressEvent) -> Self {
    Self::Ingress(Box::new(event))
  }

  #[must_use]
  pub fn lane(&self) -> RetentionLane {
    match self {
      Self::Ingress(event) => match &event.payload {
        LoggerIngressPayload::Log(log) => retention_lane(log.log_type, log.log_level),
        LoggerIngressPayload::FeatureFlagExposure { .. } => RetentionLane::Protected,
      },
      Self::Control(_) => RetentionLane::Protected,
    }
  }

  pub fn complete(mut self) {
    if let Some(completion) = self.take_completion() {
      completion.send(());
    }
  }

  fn take_completion(&mut self) -> Option<bd_completion::Sender<()>> {
    match self {
      Self::Ingress(event) => event.completion.take(),
      Self::Control(LoggerControl::FlushState(completion)) => completion.take(),
      Self::Control(_) => None,
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
  #[cfg(test)]
  waiting_consumers: tokio::sync::watch::Sender<usize>,
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
        #[cfg(test)]
        waiting_consumers: tokio::sync::watch::channel(0).0,
      }),
    }
  }

  pub fn set_pending_limits(&self, limits: EventBufferLimits) {
    self.inner.state.lock().retention.set_pending_limits(limits);
  }

  #[must_use]
  pub fn admit(&self, entry: EventBufferEntry) -> AdmissionOutcome {
    let admission = {
      let mut state = self.inner.state.lock();
      state
        .retention
        .admit(entry.lane(), entry.approximate_size_bytes(), entry)
    };
    let outcome = admission.outcome();
    drop(admission.into_terminal_entries());
    if outcome == AdmissionOutcome::Admitted {
      self.inner.notify.notify_one();
    }
    outcome
  }
  pub async fn next_batch(&self, max_entries: usize) -> Vec<EventBufferEntry> {
    assert!(max_entries > 0, "next_batch requires a non-zero batch size");
    loop {
      let notified = self.inner.notify.notified();
      tokio::pin!(notified);
      notified.as_mut().enable();
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
      #[cfg(test)]
      self
        .inner
        .waiting_consumers
        .send_modify(|waiting| *waiting += 1);
      notified.await;
      #[cfg(test)]
      self
        .inner
        .waiting_consumers
        .send_modify(|waiting| *waiting -= 1);
    }
  }

  #[cfg(test)]
  async fn wait_for_waiting_consumers(&self, expected: usize) {
    let mut waiting_consumers = self.inner.waiting_consumers.subscribe();
    while *waiting_consumers.borrow_and_update() < expected {
      waiting_consumers
        .changed()
        .await
        .expect("the event buffer always owns the waiting-consumer sender");
    }
  }

  pub fn close(&self) {
    let terminal_entries = {
      let mut state = self.inner.state.lock();
      state.retention.close()
    };
    drop(terminal_entries);
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
