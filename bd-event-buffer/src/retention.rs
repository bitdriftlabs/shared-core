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

  /// Applies staged limits, then admits or drops an entry while the owner holds its mutex.
  #[cfg(test)]
  pub(crate) fn admit(&mut self, lane: RetentionLane, bytes: usize, entry: T) -> AdmissionOutcome {
    self.admit_with_evictions(lane, bytes, entry, |_| {})
  }

  /// Admits an entry and reports every retained entry displaced by pressure or a budget shrink.
  pub(crate) fn admit_with_evictions(
    &mut self,
    lane: RetentionLane,
    bytes: usize,
    entry: T,
    mut on_eviction: impl FnMut(RetentionLane),
  ) -> AdmissionOutcome {
    self.apply_pending_limits(&mut on_eviction);
    if self.closed {
      return AdmissionOutcome::Closed;
    }
    if bytes > self.limits.total_limit_bytes
      || (lane.is_evictable() && bytes > self.limits.log_limit_bytes)
    {
      return AdmissionOutcome::RejectedOversized;
    }

    // Check admission without changing queue capacity first. A rejected entry must not transiently
    // grow a full lane's backing allocation beyond the configured retained-byte budget.
    if !self.can_make_room(lane, bytes) || !self.reserve(lane) {
      return AdmissionOutcome::RejectedFull;
    }
    self.make_room(lane, bytes, &mut on_eviction);

    let admission_id = self.next_admission_id;
    self.next_admission_id = self.next_admission_id.wrapping_add(1);
    self.lane_mut(lane).push(QueuedEntry {
      admission_id,
      bytes,
      entry,
    });
    AdmissionOutcome::Admitted
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

  /// Closes the buffer and drops all retained entries while the owner holds its mutex.
  pub(crate) fn close(&mut self) {
    if !self.closed {
      self.closed = true;
      self.protected.clear();
      self.high.clear();
      self.low.clear();
    }
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

  fn apply_pending_limits(&mut self, on_eviction: &mut impl FnMut(RetentionLane)) {
    let Some(limits) = self.pending_limits.take() else {
      return;
    };
    self.limits = limits;
    // The log and total limits constrain different sets: the log limit covers only evictable
    // entries, while the total limit also includes protected entries. Enforce them separately,
    // recalculating the total overage after the first eviction pass. Both passes evict low before
    // high and never evict protected entries.
    self.evict_for_budget_shrink(self.log_bytes_over_limit(), on_eviction);
    self.evict_for_budget_shrink(self.total_bytes_over_limit(), on_eviction);
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
    on_eviction: &mut impl FnMut(RetentionLane),
  ) {
    let bytes_needed = self.required_eviction_bytes(lane, incoming_bytes);
    self.evict_for_limit(lane, bytes_needed, on_eviction);
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
    on_eviction: &mut impl FnMut(RetentionLane),
  ) {
    // Evict the newest eligible entries first. This retains the oldest entries in each lane and
    // ensures an incoming entry only displaces work of lower priority.
    match incoming_lane {
      RetentionLane::Low => {
        debug_assert_eq!(0, bytes_needed, "low entries cannot displace entries");
      },
      RetentionLane::High => {
        bytes_needed = self.evict_from_lane(RetentionLane::Low, bytes_needed, on_eviction);
        debug_assert_eq!(
          0, bytes_needed,
          "admission preflight guarantees enough lower-priority bytes"
        );
      },
      RetentionLane::Protected => {
        bytes_needed = self.evict_from_lane(RetentionLane::Low, bytes_needed, on_eviction);
        bytes_needed = self.evict_from_lane(RetentionLane::High, bytes_needed, on_eviction);
        debug_assert_eq!(
          0, bytes_needed,
          "admission preflight guarantees enough lower-priority bytes"
        );
      },
    }
  }

  fn evict_for_budget_shrink(
    &mut self,
    mut bytes_needed: usize,
    on_eviction: &mut impl FnMut(RetentionLane),
  ) {
    // A limit change has no incoming priority, so it uses the least-destructive policy: discard
    // low entries first, then high entries, and leave protected entries intact even when they
    // alone exceed the new total limit.
    for lane in [RetentionLane::Low, RetentionLane::High] {
      bytes_needed = self.evict_from_lane(lane, bytes_needed, on_eviction);
    }
  }

  fn evict_from_lane(
    &mut self,
    lane: RetentionLane,
    bytes_needed: usize,
    on_eviction: &mut impl FnMut(RetentionLane),
  ) -> usize {
    let freed_bytes = self
      .lane_mut(lane)
      .evict_newest(bytes_needed, || on_eviction(lane));
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

  fn evict_newest(&mut self, bytes_needed: usize, mut on_eviction: impl FnMut()) -> usize {
    // Remove and drop a newest-first suffix. The remaining prefix retains its FIFO ordering.
    let mut freed_bytes = 0;
    while freed_bytes < bytes_needed {
      let Some(entry) = self.entries.pop_back() else {
        break;
      };
      freed_bytes += entry.bytes;
      self.bytes -= entry.bytes;
      on_eviction();
    }
    freed_bytes
  }

  fn clear(&mut self) {
    self.bytes = 0;
    self.entries.clear();
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
