// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_client_common::PlatformMutex;
use bd_client_stats_store::{Counter, Scope};
use bd_log_primitives::{DataValue, LogFields, LogLevel, LogLine, log_level};
use bd_macros::ApproximateSize;
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::MemoryPressureLevel;
use bd_proto::protos::logging::payload::LogType;
use bd_stats_common::{Counter as _, labels};
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
  stats: Option<EventBufferStats>,
  #[cfg(test)]
  test_hooks: Option<Arc<dyn TestHooks>>,
}

struct LoggerEventBufferState {
  retention: EventBufferState<EventBufferEntry>,
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
      Self::new_inner(limits, None)
    }
  }

  /// Creates an `EventBuffer` that emits bounded per-lane admission outcome metrics.
  #[must_use]
  pub fn new_with_stats(limits: EventBufferLimits, scope: &Scope) -> Self {
    #[cfg(test)]
    {
      Self::new_inner(limits, Some(EventBufferStats::new(scope)), None)
    }
    #[cfg(not(test))]
    {
      Self::new_inner(limits, Some(EventBufferStats::new(scope)))
    }
  }

  #[cfg(test)]
  fn new_with_test_hooks(
    limits: EventBufferLimits,
    test_hooks: Option<Arc<dyn TestHooks>>,
  ) -> Self {
    Self::new_inner(limits, None, test_hooks)
  }

  fn new_inner(
    limits: EventBufferLimits,
    stats: Option<EventBufferStats>,
    #[cfg(test)] test_hooks: Option<Arc<dyn TestHooks>>,
  ) -> Self {
    Self {
      inner: Arc::new(EventBufferInner {
        state: PlatformMutex::new(LoggerEventBufferState {
          retention: EventBufferState::new(limits),
        }),
        notify: Notify::new(),
        stats,
        #[cfg(test)]
        test_hooks,
      }),
    }
  }

  pub fn set_pending_limits(&self, limits: EventBufferLimits) {
    self.inner.state.lock().retention.set_pending_limits(limits);
  }

  #[must_use]
  pub fn admit(&self, entry: EventBufferEntry) -> AdmissionOutcome {
    let lane = entry.lane();
    let outcome = {
      let mut state = self.inner.state.lock();
      state.retention.admit_with_evictions(
        lane,
        entry.approximate_size_bytes(),
        entry,
        |evicted_lane| self.record_eviction(evicted_lane),
      )
    };
    self.record_outcome(lane, outcome);
    if outcome == AdmissionOutcome::Admitted {
      self.inner.notify.notify_one();
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
    let outcomes = {
      let mut state = self.inner.state.lock();
      #[cfg(test)]
      if let Some(test_hooks) = &self.inner.test_hooks {
        test_hooks.batch_admission_started();
      }
      entries
        .into_iter()
        .map(|entry| {
          let lane = entry.lane();
          let outcome = state.retention.admit_with_evictions(
            lane,
            entry.approximate_size_bytes(),
            entry,
            |evicted_lane| self.record_eviction(evicted_lane),
          );
          self.record_outcome(lane, outcome);
          outcome
        })
        .collect::<Vec<_>>()
    };
    if outcomes.contains(&AdmissionOutcome::Admitted) {
      self.inner.notify.notify_one();
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

  fn record_outcome(&self, lane: RetentionLane, outcome: AdmissionOutcome) {
    if let Some(stats) = &self.inner.stats {
      stats.record_outcome(lane, outcome);
    }
  }

  fn record_eviction(&self, lane: RetentionLane) {
    if let Some(stats) = &self.inner.stats {
      stats.record_eviction(lane);
    }
  }
}

//
// EventBufferStats
//

/// Bounded `EventBuffer` outcome metrics, labeled only by the fixed retention lane and outcome.
struct EventBufferStats {
  admitted: LaneCounters,
  evicted: LaneCounters,
  rejected_full: LaneCounters,
  rejected_oversized: LaneCounters,
  closed: LaneCounters,
}

impl EventBufferStats {
  fn new(scope: &Scope) -> Self {
    Self {
      admitted: LaneCounters::new(scope, "admitted"),
      evicted: LaneCounters::new(scope, "evicted"),
      rejected_full: LaneCounters::new(scope, "rejected_full"),
      rejected_oversized: LaneCounters::new(scope, "rejected_oversized"),
      closed: LaneCounters::new(scope, "closed"),
    }
  }

  fn record_outcome(&self, lane: RetentionLane, outcome: AdmissionOutcome) {
    match outcome {
      AdmissionOutcome::Admitted => self.admitted.inc(lane),
      AdmissionOutcome::RejectedFull => self.rejected_full.inc(lane),
      AdmissionOutcome::RejectedOversized => self.rejected_oversized.inc(lane),
      AdmissionOutcome::Closed => self.closed.inc(lane),
    }
  }

  fn record_eviction(&self, lane: RetentionLane) {
    self.evicted.inc(lane);
  }
}

//
// LaneCounters
//

struct LaneCounters {
  low: Counter,
  high: Counter,
  protected: Counter,
}

impl LaneCounters {
  fn new(scope: &Scope, outcome: &'static str) -> Self {
    Self {
      low: scope.counter_with_labels(
        "entry_outcomes",
        labels!("lane" => "low", "outcome" => outcome),
      ),
      high: scope.counter_with_labels(
        "entry_outcomes",
        labels!("lane" => "high", "outcome" => outcome),
      ),
      protected: scope.counter_with_labels(
        "entry_outcomes",
        labels!("lane" => "protected", "outcome" => outcome),
      ),
    }
  }

  fn inc(&self, lane: RetentionLane) {
    match lane {
      RetentionLane::Low => self.low.inc(),
      RetentionLane::High => self.high.inc(),
      RetentionLane::Protected => self.protected.inc(),
    }
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
