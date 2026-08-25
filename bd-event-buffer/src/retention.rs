// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{AdmissionOutcome, EventBufferLimits, RetentionLane};
use std::collections::VecDeque;

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
/// Each lane keeps its backing allocation when it is detached during close. Evicted suffixes are
/// appended here until the owner releases the mutex and drops the result.
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
    assert!(
      self.rejected.is_none(),
      "an admission has only one rejected entry"
    );
    self.rejected = Some(entry);
  }

  fn append(&mut self, lane: RetentionLane, entries: VecDeque<QueuedEntry<T>>) {
    match lane {
      RetentionLane::Low => self.low.extend(entries),
      RetentionLane::High => self.high.extend(entries),
      RetentionLane::Protected => self.protected.extend(entries),
    }
  }
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
    self.apply_pending_limits(&mut terminal_entries);
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

    // Check admission without changing queue capacity first. A rejected entry must not transiently
    // grow a full lane's backing allocation beyond the configured retained-byte budget.
    if !self.can_make_room(lane, bytes) || !self.reserve(lane) {
      terminal_entries.reject(entry);
      return AdmissionResult {
        outcome: AdmissionOutcome::RejectedFull,
        terminal_entries,
      };
    }
    self.make_room(lane, bytes, &mut terminal_entries);

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

  fn apply_pending_limits(&mut self, terminal_entries: &mut TerminalEntries<T>) {
    let Some(limits) = self.pending_limits.take() else {
      return;
    };
    self.limits = limits;
    // The log and total limits constrain different sets: the log limit covers only evictable
    // entries, while the total limit also includes protected entries. Enforce them separately,
    // recalculating the total overage after the first eviction pass. Both passes evict low before
    // high and never evict protected entries.
    self.evict_for_budget_shrink(self.log_bytes_over_limit(), terminal_entries);
    self.evict_for_budget_shrink(self.total_bytes_over_limit(), terminal_entries);
  }

  fn can_make_room(&self, lane: RetentionLane, incoming_bytes: usize) -> bool {
    let bytes_needed = self.required_eviction_bytes(lane, incoming_bytes);

    // A newly admitted entry may only displace less-important lanes. Check that enough eligible
    // bytes exist before mutating the queues, so a rejected admission cannot partially evict.
    self.evictable_bytes_available_to(lane) >= bytes_needed
  }

  fn make_room(
    &mut self,
    lane: RetentionLane,
    incoming_bytes: usize,
    terminal_entries: &mut TerminalEntries<T>,
  ) {
    let bytes_needed = self.required_eviction_bytes(lane, incoming_bytes);
    self.evict_for_limit(lane, bytes_needed, terminal_entries);
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

  fn evict_for_limit(
    &mut self,
    incoming_lane: RetentionLane,
    mut bytes_needed: usize,
    terminal_entries: &mut TerminalEntries<T>,
  ) {
    // Evict the newest eligible entries first. This retains the oldest entries in each lane and
    // ensures an incoming entry only displaces work of lower priority.
    match incoming_lane {
      RetentionLane::Low => {
        assert_eq!(0, bytes_needed, "low entries cannot displace entries");
      },
      RetentionLane::High => {
        bytes_needed = self.evict_from_lane(RetentionLane::Low, bytes_needed, terminal_entries);
        assert_eq!(
          0, bytes_needed,
          "admission preflight guarantees enough lower-priority bytes"
        );
      },
      RetentionLane::Protected => {
        bytes_needed = self.evict_from_lane(RetentionLane::Low, bytes_needed, terminal_entries);
        bytes_needed = self.evict_from_lane(RetentionLane::High, bytes_needed, terminal_entries);
        assert_eq!(
          0, bytes_needed,
          "admission preflight guarantees enough lower-priority bytes"
        );
      },
    }
  }

  fn evict_for_budget_shrink(
    &mut self,
    mut bytes_needed: usize,
    terminal_entries: &mut TerminalEntries<T>,
  ) {
    // A limit change has no incoming priority, so it uses the least-destructive policy: discard
    // low entries first, then high entries, and leave protected entries intact even when they
    // alone exceed the new total limit.
    for lane in [RetentionLane::Low, RetentionLane::High] {
      bytes_needed = self.evict_from_lane(lane, bytes_needed, terminal_entries);
    }
  }

  fn evict_from_lane(
    &mut self,
    lane: RetentionLane,
    bytes_needed: usize,
    terminal_entries: &mut TerminalEntries<T>,
  ) -> usize {
    let (freed_bytes, entries) = self.lane_mut(lane).evict_newest(bytes_needed);
    terminal_entries.append(lane, entries);
    bytes_needed.saturating_sub(freed_bytes)
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

  fn log_bytes_over_limit(&self) -> usize {
    self
      .evictable_bytes()
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
      RetentionLane::High => self.low.bytes,
      RetentionLane::Protected => self.low.bytes + self.high.bytes,
    }
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

  fn evict_newest(&mut self, bytes_needed: usize) -> (usize, VecDeque<QueuedEntry<T>>) {
    if bytes_needed == 0 {
      return (0, VecDeque::new());
    }

    // Remove a newest-first suffix. The remaining prefix retains its FIFO ordering.
    let mut start = self.entries.len();
    let mut freed_bytes = 0;
    while start > 0 && freed_bytes < bytes_needed {
      start -= 1;
      freed_bytes += self.entries[start].bytes;
    }
    let evicted = self.entries.split_off(start);
    self.bytes -= freed_bytes;
    (freed_bytes, evicted)
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
