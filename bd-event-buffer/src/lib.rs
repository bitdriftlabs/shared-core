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

use retention::EventBufferState;

//
// LoggerControl
//

/// A state mutation or flush request processed in FIFO order with logger ingress events.
#[derive(ApproximateSize, Debug)]
pub enum LoggerControl {
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
///
/// As we deprecate `FieldProviders` this will eventually just hold the timestamp.
#[derive(ApproximateSize, Debug, Clone)]
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
#[derive(ApproximateSize, Debug, Clone)]
pub struct AdmissionContext {
  pub session_id: Arc<str>,
  pub provider: ProviderSnapshot,
  pub admitted_at: OffsetDateTime,
}

//
// EventContext
//

/// Context captured for an `EventBuffer` entry before ALB processes it.
#[derive(ApproximateSize, Debug, Clone)]
pub enum EventContext {
  CurrentProcess(AdmissionContext),
  /// Previous-process logs retain their admission-time `_logged_at` value, but are finalized
  /// against prior global state on the consumer.
  PreviousProcess {
    /// Timestamp-provider result captured when this previous-process log enters `EventBuffer`.
    logged_at: OffsetDateTime,
  },
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

  /// Splits an admitted event so ALB can process its payload before resolving a blocking caller.
  #[must_use]
  pub fn into_parts(
    self,
  ) -> (
    EventContext,
    LoggerIngressPayload,
    Option<bd_completion::Sender<()>>,
  ) {
    (self.context, self.payload, self.completion)
  }
}

//
// EventBufferEntry
//

// Keep ingress events inline until the layout benchmark establishes whether the saved control-entry
// slot space outweighs the allocation and indirection cost of boxing them.
#[allow(clippy::large_enum_variant)]
#[derive(ApproximateSize, Debug)]
pub enum EventBufferEntry {
  Ingress(LoggerIngressEvent),
  Control(LoggerControl),
}

impl EventBufferEntry {
  #[must_use]
  pub fn ingress(event: LoggerIngressEvent) -> Self {
    Self::Ingress(event)
  }

  #[must_use]
  pub fn lane(&self) -> RetentionLane {
    match self {
      Self::Ingress(event) if matches!(&event.context, EventContext::PreviousProcess { .. }) => {
        RetentionLane::Protected
      },
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

  fn is_previous_process(&self) -> bool {
    matches!(
      self,
      Self::Ingress(LoggerIngressEvent {
        context: EventContext::PreviousProcess { .. },
        ..
      })
    )
  }

  fn is_blocking_flush(&self) -> bool {
    matches!(self, Self::Control(LoggerControl::FlushState(Some(_))))
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
  gate_notify: Notify,
  pipeline_notify: Notify,
  #[cfg(test)]
  test_hooks: Option<Arc<dyn TestHooks>>,
}

struct LoggerEventBufferState {
  retention: EventBufferState<EventBufferEntry>,
  pipeline_ready: bool,
}

impl EventBuffer {
  #[must_use]
  pub fn new(limits: EventBufferLimits) -> Self {
    #[cfg(test)]
    {
      Self::new_with_test_hooks(limits, None)
    }
    #[cfg(not(test))]
    {
      Self::new_inner(limits)
    }
  }

  #[cfg(test)]
  fn new_with_test_hooks(
    limits: EventBufferLimits,
    test_hooks: Option<Arc<dyn TestHooks>>,
  ) -> Self {
    Self::new_inner(limits, test_hooks)
  }

  fn new_inner(
    limits: EventBufferLimits,
    #[cfg(test)] test_hooks: Option<Arc<dyn TestHooks>>,
  ) -> Self {
    Self {
      inner: Arc::new(EventBufferInner {
        state: PlatformMutex::new(LoggerEventBufferState {
          retention: EventBufferState::new(limits),
          pipeline_ready: false,
        }),
        notify: Notify::new(),
        gate_notify: Notify::new(),
        pipeline_notify: Notify::new(),
        #[cfg(test)]
        test_hooks,
      }),
    }
  }

  pub fn set_pending_limits(&self, limits: EventBufferLimits) {
    self.inner.state.lock().retention.set_pending_limits(limits);
  }

  /// Marks that ALB has finished constructing a processing pipeline. This linearizes the
  /// transition between startup's hard gate—where flushes are intentionally no-ops—and the soft
  /// replay gate, where a blocking flush is an ordered barrier.
  pub fn mark_pipeline_ready(&self) {
    self.inner.state.lock().pipeline_ready = true;
    self.inner.pipeline_notify.notify_waiters();
  }

  /// Returns whether ALB has completed the hard startup gate by constructing its processing
  /// pipeline.
  #[must_use]
  pub fn is_pipeline_ready(&self) -> bool {
    self.inner.state.lock().pipeline_ready
  }

  /// Waits until ALB has constructed its processing pipeline.
  pub async fn wait_for_pipeline_ready(&self) {
    loop {
      let notified = self.inner.pipeline_notify.notified();
      tokio::pin!(notified);
      notified.as_mut().enable();
      if self.is_pipeline_ready() {
        return;
      }
      notified.await;
    }
  }

  /// Returns whether a flush should complete without queueing because no processing pipeline
  /// exists yet. The check shares the `EventBuffer` mutex with `mark_pipeline_ready`, so a flush
  /// is deterministically either an early no-op or a normal ordered entry.
  #[must_use]
  pub fn skips_flush_before_pipeline_ready(&self) -> bool {
    let state = self.inner.state.lock();
    !state.pipeline_ready && !state.retention.is_closed()
  }

  #[must_use]
  pub fn admit(&self, entry: EventBufferEntry) -> AdmissionOutcome {
    let lane = entry.lane();
    let previous_process = entry.is_previous_process();
    let blocking_flush = entry.is_blocking_flush();
    let (outcome, gate_requested, notify_consumer) = {
      let mut state = self.inner.state.lock();
      let outcome = state.retention.admit_with_evictions(
        lane,
        previous_process,
        entry.approximate_size_bytes(),
        entry,
        |_| {},
      );
      let gate_requested =
        Self::request_startup_gate_release(&mut state.retention, outcome, lane, blocking_flush);
      (outcome, gate_requested, state.retention.is_gate_open())
    };
    if outcome == AdmissionOutcome::Admitted && notify_consumer {
      self.inner.notify.notify_one();
    }
    if gate_requested {
      self.inner.gate_notify.notify_one();
    }
    outcome
  }

  /// Admits a group of already-prepared entries without allowing another producer to interleave.
  ///
  /// Each entry retains its own admission outcome, so a full buffer can reject part of a batch.
  #[must_use]
  pub fn admit_batch(
    &self,
    entries: impl IntoIterator<Item = EventBufferEntry>,
  ) -> Vec<AdmissionOutcome> {
    let (outcomes, gate_requested, notify_consumer) = {
      let mut state = self.inner.state.lock();
      #[cfg(test)]
      if let Some(test_hooks) = &self.inner.test_hooks {
        test_hooks.batch_admission_started();
      }
      let mut gate_requested = false;
      let outcomes = entries
        .into_iter()
        .map(|entry| {
          let lane = entry.lane();
          let previous_process = entry.is_previous_process();
          let blocking_flush = entry.is_blocking_flush();
          let outcome = state.retention.admit_with_evictions(
            lane,
            previous_process,
            entry.approximate_size_bytes(),
            entry,
            |_| {},
          );
          gate_requested |=
            Self::request_startup_gate_release(&mut state.retention, outcome, lane, blocking_flush);
          outcome
        })
        .collect::<Vec<_>>();
      (outcomes, gate_requested, state.retention.is_gate_open())
    };
    if outcomes.contains(&AdmissionOutcome::Admitted) && notify_consumer {
      self.inner.notify.notify_one();
    }
    if gate_requested {
      self.inner.gate_notify.notify_one();
    }
    outcomes
  }

  pub async fn next_batch(&self, max_entries: usize) -> Vec<EventBufferEntry> {
    debug_assert!(max_entries > 0, "next_batch requires a non-zero batch size");
    if max_entries == 0 {
      return vec![];
    }
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
      if let Some(test_hooks) = &self.inner.test_hooks {
        test_hooks.consumer_waiting();
      }
      notified.await;
    }
  }

  pub fn close(&self) {
    {
      let mut state = self.inner.state.lock();
      state.retention.close();
    }
    self.inner.notify.notify_waiters();
  }

  /// Opens the startup drain gate. Once open, it cannot be closed again.
  #[must_use]
  pub fn open_gate(&self) -> bool {
    let opened = self.inner.state.lock().retention.open_gate();
    if opened {
      self.inner.notify.notify_waiters();
    }
    opened
  }

  #[must_use]
  pub fn is_gate_open(&self) -> bool {
    self.inner.state.lock().retention.is_gate_open()
  }

  /// Reports whether protected work retained behind the startup gate has reached its high
  /// watermark. The consumer uses this after configuration becomes ready so work that arrived
  /// before configuration can release immediately instead of waiting for the replay timer.
  #[must_use]
  pub fn reaches_protected_high_watermark(&self) -> bool {
    self
      .inner
      .state
      .lock()
      .retention
      .reaches_protected_high_watermark()
  }

  /// Waits for a pressure or blocking-flush request that the configured consumer may use to
  /// release the startup gate.
  pub async fn wait_for_gate_release_request(&self) -> StartupGateReleaseRequest {
    loop {
      let notified = self.inner.gate_notify.notified();
      tokio::pin!(notified);
      notified.as_mut().enable();
      if let Some(request) = self
        .inner
        .state
        .lock()
        .retention
        .take_gate_release_request()
      {
        return request;
      }
      notified.await;
    }
  }

  fn request_startup_gate_release(
    retention: &mut EventBufferState<EventBufferEntry>,
    outcome: AdmissionOutcome,
    lane: RetentionLane,
    blocking_flush: bool,
  ) -> bool {
    if outcome != AdmissionOutcome::Admitted {
      return false;
    }

    let request = if blocking_flush {
      Some(StartupGateReleaseRequest::BlockingFlush)
    } else if lane == RetentionLane::Protected && retention.reaches_protected_high_watermark() {
      Some(StartupGateReleaseRequest::ProtectedHighWatermark)
    } else {
      None
    };
    request.is_some_and(|request| retention.request_gate_release(request))
  }
}

//
// TestHooks
//

#[cfg(test)]
trait TestHooks: Send + Sync {
  fn consumer_waiting(&self) {}

  fn batch_admission_started(&self) {}
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

/// A protected admission that can release `EventBuffer`'s startup drain gate early.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartupGateReleaseRequest {
  ProtectedHighWatermark,
  BlockingFlush,
}
