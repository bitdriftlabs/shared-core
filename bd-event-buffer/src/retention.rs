// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{AdmissionOutcome, EventBufferLimits, RetentionLane};
use std::collections::VecDeque;

//
// AdmissionResult
//

/// The outcome of an admission attempt and entries no longer retained by the buffer.
///
/// Retention only detaches terminal entries. Its owner drops this result after releasing any
/// synchronization guard, so completion senders never wake a consumer while that guard is held.
#[must_use]
pub struct AdmissionResult<T> {
  outcome: AdmissionOutcome,
  terminal_entries: TerminalEntries<T>,
}

impl<T> AdmissionResult<T> {
  #[must_use]
  pub const fn outcome(&self) -> AdmissionOutcome {
    self.outcome
  }

  #[must_use]
  pub fn into_terminal_entries(self) -> TerminalEntries<T> {
    self.terminal_entries
  }
}

//
// TerminalEntries
//

/// Entries removed from retention and awaiting destruction by the owner.
///
/// Each lane keeps its backing allocation when it is detached during close. Before eviction,
/// terminal storage is reserved fallibly; moving the planned entries afterward cannot allocate.
pub struct TerminalEntries<T> {
  protected: VecDeque<QueuedEntry<T>>,
  high: VecDeque<QueuedEntry<T>>,
  low: VecDeque<QueuedEntry<T>>,
  rejected: Option<T>,
}

impl<T> TerminalEntries<T> {
  fn empty() -> Self {
    Self {
      protected: VecDeque::new(),
      high: VecDeque::new(),
      low: VecDeque::new(),
      rejected: None,
    }
  }

  fn reject(&mut self, entry: T) {
    debug_assert!(
      self.rejected.is_none(),
      "an admission has only one rejected entry"
    );
    self.rejected = Some(entry);
  }

  fn try_reserve(&mut self, plan: EvictionPlan) -> bool {
    self.low.try_reserve(plan.low).is_ok() && self.high.try_reserve(plan.high).is_ok()
  }

  fn entries_mut(&mut self, lane: RetentionLane) -> &mut VecDeque<QueuedEntry<T>> {
    match lane {
      RetentionLane::Low => &mut self.low,
      RetentionLane::High => &mut self.high,
      RetentionLane::Protected => &mut self.protected,
    }
  }
}

//
// EvictionPlan
//

/// A precomputed suffix length for each lane that can be reserved and moved without allocation.
#[derive(Clone, Copy, Default)]
struct EvictionPlan {
  high: usize,
  low: usize,
  freed_bytes: usize,
}

impl EvictionPlan {
  fn count(self, lane: RetentionLane) -> usize {
    match lane {
      RetentionLane::Low => self.low,
      RetentionLane::High => self.high,
      RetentionLane::Protected => 0,
    }
  }

  fn add(&mut self, lane: RetentionLane, count: usize, freed_bytes: usize) {
    match lane {
      RetentionLane::Low => self.low += count,
      RetentionLane::High => self.high += count,
      RetentionLane::Protected => debug_assert_eq!(0, count),
    }
    self.freed_bytes += freed_bytes;
  }
}

//
// EventBufferState
//

/// A synchronous, priority-aware retention state machine.
///
/// The owner supplies synchronization and associates each payload with a lane and accounting
/// size. Each lane is FIFO, while `admission_id` lets `take_batch` merge their fronts back into
/// global admission order. Low entries never displace existing entries; high entries can displace
/// low entries; protected entries can displace low, then high, entries. Protected entries are
/// never evicted.
pub struct EventBufferState<T> {
  limits: EventBufferLimits,
  pending_limits: Option<EventBufferLimits>,
  next_admission_id: u64,
  closed: bool,
  protected: LaneState<T>,
  high: LaneState<T>,
  low: LaneState<T>,
}

impl<T> EventBufferState<T> {
  #[must_use]
  pub fn new(limits: EventBufferLimits) -> Self {
    Self {
      limits,
      pending_limits: None,
      next_admission_id: 0,
      closed: false,
      protected: LaneState::new(),
      high: LaneState::new(),
      low: LaneState::new(),
    }
  }

  /// Stages a limit update for the next admission; a newer update replaces an older staged one.
  pub fn set_pending_limits(&mut self, limits: EventBufferLimits) {
    self.pending_limits = Some(limits);
  }

  /// Applies staged limits, then admits an entry or detaches it without changing retained entries.
  pub fn admit(&mut self, lane: RetentionLane, bytes: usize, entry: T) -> AdmissionResult<T> {
    let mut terminal_entries = TerminalEntries::empty();
    if !self.apply_pending_limits(&mut terminal_entries) {
      terminal_entries.reject(entry);
      return AdmissionResult {
        outcome: AdmissionOutcome::RejectedFull,
        terminal_entries,
      };
    }
    if self.closed {
      terminal_entries.reject(entry);
      return AdmissionResult {
        outcome: AdmissionOutcome::Closed,
        terminal_entries,
      };
    }
    if bytes > self.limits.total_limit_bytes
      || (lane.is_evictable() && bytes > self.limits.log_limit_bytes)
    {
      terminal_entries.reject(entry);
      return AdmissionResult {
        outcome: AdmissionOutcome::RejectedOversized,
        terminal_entries,
      };
    }

    // Build and reserve the eviction handoff before mutating any lane. A rejected entry therefore
    // never grows retained queues, partially evicts, or allocates while it is being evicted.
    let Some(eviction_plan) = self.admission_eviction_plan(lane, bytes) else {
      terminal_entries.reject(entry);
      return AdmissionResult {
        outcome: AdmissionOutcome::RejectedFull,
        terminal_entries,
      };
    };
    if !terminal_entries.try_reserve(eviction_plan) || !self.reserve(lane) {
      terminal_entries.reject(entry);
      return AdmissionResult {
        outcome: AdmissionOutcome::RejectedFull,
        terminal_entries,
      };
    }
    self.apply_eviction_plan(eviction_plan, &mut terminal_entries);

    let admission_id = self.next_admission_id;
    self.next_admission_id = self.next_admission_id.wrapping_add(1);
    self.lane_mut(lane).push(QueuedEntry {
      admission_id,
      bytes,
      entry,
    });
    AdmissionResult {
      outcome: AdmissionOutcome::Admitted,
      terminal_entries,
    }
  }

  #[must_use]
  pub fn take_batch(&mut self, max_entries: usize) -> Vec<T> {
    let mut result = Vec::new();
    while result.len() < max_entries {
      let Some(lane) = self.oldest_retained_lane() else {
        break;
      };
      let entry = self
        .lane_mut(lane)
        .pop_oldest()
        .expect("the oldest retained lane must contain an entry");
      result.push(entry.entry);
    }
    result
  }

  /// Closes the buffer and detaches all retained entries for destruction by the owner.
  pub fn close(&mut self) -> TerminalEntries<T> {
    if !self.closed {
      self.closed = true;
      return TerminalEntries {
        protected: self.protected.take_entries(),
        high: self.high.take_entries(),
        low: self.low.take_entries(),
        rejected: None,
      };
    }
    TerminalEntries::empty()
  }

  #[must_use]
  pub const fn is_closed(&self) -> bool {
    self.closed
  }

  fn reserve(&mut self, lane: RetentionLane) -> bool {
    self.lane_mut(lane).reserve()
  }

  fn oldest_retained_lane(&self) -> Option<RetentionLane> {
    // The front of each lane is its oldest retained entry. Taking the minimum admission ID
    // therefore preserves global ordering without sacrificing lane-specific eviction policy.
    [
      RetentionLane::Protected,
      RetentionLane::High,
      RetentionLane::Low,
    ]
    .into_iter()
    .filter_map(|lane| self.lane(lane).oldest_admission_id().map(|id| (lane, id)))
    .min_by_key(|(_, admission_id)| *admission_id)
    .map(|(lane, _)| lane)
  }

  fn apply_pending_limits(&mut self, terminal_entries: &mut TerminalEntries<T>) -> bool {
    let Some(limits) = self.pending_limits else {
      return true;
    };

    let eviction_plan = self.budget_shrink_eviction_plan(limits);
    if !terminal_entries.try_reserve(eviction_plan) {
      return false;
    }

    self.pending_limits = None;
    self.limits = limits;
    self.apply_eviction_plan(eviction_plan, terminal_entries);
    true
  }

  fn admission_eviction_plan(
    &self,
    lane: RetentionLane,
    incoming_bytes: usize,
  ) -> Option<EvictionPlan> {
    let bytes_needed = self.required_eviction_bytes(lane, incoming_bytes);
    let lanes = match lane {
      RetentionLane::Low => &[][..],
      RetentionLane::High => &[RetentionLane::Low],
      RetentionLane::Protected => &[RetentionLane::Low, RetentionLane::High],
    };
    let (remaining_bytes, plan) = self.plan_eviction(bytes_needed, EvictionPlan::default(), lanes);
    (remaining_bytes == 0).then_some(plan)
  }

  fn required_eviction_bytes(&self, lane: RetentionLane, incoming_bytes: usize) -> usize {
    // An evicted log reduces both budgets. Satisfying the larger overage therefore satisfies both
    // the evictable-log budget and the total budget for this admission.
    let log_needed = if lane.is_evictable() {
      self
        .evictable_bytes()
        .saturating_add(incoming_bytes)
        .saturating_sub(self.limits.log_limit_bytes)
    } else {
      0
    };
    let total_needed = self
      .total_bytes()
      .saturating_add(incoming_bytes)
      .saturating_sub(self.limits.total_limit_bytes);
    log_needed.max(total_needed)
  }

  fn budget_shrink_eviction_plan(&self, limits: EventBufferLimits) -> EvictionPlan {
    // The log and total limits constrain different sets: the log limit covers only evictable
    // entries, while the total limit also includes protected entries. Enforce them separately,
    // recalculating the total overage after the first eviction pass. Both passes evict low before
    // high and never evict protected entries.
    let (remaining_bytes, plan) = self.plan_eviction(
      self
        .evictable_bytes()
        .saturating_sub(limits.log_limit_bytes),
      EvictionPlan::default(),
      &[RetentionLane::Low, RetentionLane::High],
    );
    debug_assert_eq!(
      0, remaining_bytes,
      "evictable entries can always satisfy the log budget"
    );
    let (_remaining_bytes, plan) = self.plan_eviction(
      self
        .total_bytes()
        .saturating_sub(plan.freed_bytes)
        .saturating_sub(limits.total_limit_bytes),
      plan,
      &[RetentionLane::Low, RetentionLane::High],
    );
    plan
  }

  fn plan_eviction(
    &self,
    mut bytes_needed: usize,
    mut plan: EvictionPlan,
    lanes: &[RetentionLane],
  ) -> (usize, EvictionPlan) {
    for lane in lanes {
      let (count, freed_bytes) = self
        .lane(*lane)
        .newest_entries_for_bytes(bytes_needed, plan.count(*lane));
      plan.add(*lane, count, freed_bytes);
      bytes_needed = bytes_needed.saturating_sub(freed_bytes);
    }
    (bytes_needed, plan)
  }

  fn apply_eviction_plan(&mut self, plan: EvictionPlan, terminal_entries: &mut TerminalEntries<T>) {
    for lane in [RetentionLane::Low, RetentionLane::High] {
      self
        .lane_mut(lane)
        .evict_newest(plan.count(lane), terminal_entries.entries_mut(lane));
    }
  }

  fn lane(&self, lane: RetentionLane) -> &LaneState<T> {
    match lane {
      RetentionLane::Low => &self.low,
      RetentionLane::High => &self.high,
      RetentionLane::Protected => &self.protected,
    }
  }

  fn lane_mut(&mut self, lane: RetentionLane) -> &mut LaneState<T> {
    match lane {
      RetentionLane::Low => &mut self.low,
      RetentionLane::High => &mut self.high,
      RetentionLane::Protected => &mut self.protected,
    }
  }

  fn evictable_bytes(&self) -> usize {
    self.low.bytes + self.high.bytes
  }

  fn total_bytes(&self) -> usize {
    self.protected.bytes + self.evictable_bytes()
  }
}

//
// LaneState
//

/// A FIFO retention lane whose byte count is updated together with its entries.
struct LaneState<T> {
  entries: VecDeque<QueuedEntry<T>>,
  bytes: usize,
}

impl<T> LaneState<T> {
  fn new() -> Self {
    Self {
      entries: VecDeque::new(),
      bytes: 0,
    }
  }

  fn reserve(&mut self) -> bool {
    self.entries.try_reserve(1).is_ok()
  }

  fn oldest_admission_id(&self) -> Option<u64> {
    self.entries.front().map(|entry| entry.admission_id)
  }

  fn push(&mut self, entry: QueuedEntry<T>) {
    self.bytes += entry.bytes;
    self.entries.push_back(entry);
  }

  fn pop_oldest(&mut self) -> Option<QueuedEntry<T>> {
    let entry = self.entries.pop_front()?;
    self.bytes -= entry.bytes;
    Some(entry)
  }

  fn newest_entries_for_bytes(&self, bytes_needed: usize, skip_newest: usize) -> (usize, usize) {
    let mut count = 0;
    let mut freed_bytes = 0;
    for entry in self.entries.iter().rev().skip(skip_newest) {
      if freed_bytes >= bytes_needed {
        break;
      }
      count += 1;
      freed_bytes += entry.bytes;
    }
    (count, freed_bytes)
  }

  fn evict_newest(&mut self, count: usize, terminal_entries: &mut VecDeque<QueuedEntry<T>>) {
    // `TerminalEntries::try_reserve` runs before this mutation, so moving these entries cannot
    // allocate or drop a completion sender while the owner holds its synchronization guard.
    for _ in 0 .. count {
      let entry = self
        .entries
        .pop_back()
        .expect("eviction plan cannot outgrow its source lane");
      self.bytes -= entry.bytes;
      terminal_entries.push_back(entry);
    }
  }

  fn take_entries(&mut self) -> VecDeque<QueuedEntry<T>> {
    self.bytes = 0;
    std::mem::take(&mut self.entries)
  }
}

//
// QueuedEntry
//

struct QueuedEntry<T> {
  admission_id: u64,
  bytes: usize,
  entry: T,
}
