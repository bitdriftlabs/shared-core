#![allow(clippy::unwrap_used)]

use super::{AdmissionOutcome, EventBufferLimits, EventBufferState, RetentionLane};
use proptest::prelude::*;
use std::env;

const DEFAULT_PROPTEST_CASES: u32 = 512;

fn proptest_config() -> ProptestConfig {
  let mut config = ProptestConfig::with_cases(DEFAULT_PROPTEST_CASES);
  if let Ok(value) = env::var("PROPTEST_CASES") {
    config.cases = value
      .parse()
      .expect("PROPTEST_CASES must be a positive integer");
    assert!(
      config.cases > 0,
      "PROPTEST_CASES must be a positive integer"
    );
  }
  config
}

//
// Entry
//

#[derive(Clone, Debug, PartialEq, Eq)]
struct Entry {
  id: u64,
  lane: RetentionLane,
  bytes: usize,
}

impl Entry {
  const fn new(id: u64, lane: RetentionLane, bytes: usize) -> Self {
    Self { id, lane, bytes }
  }
}

//
// Operation
//

#[derive(Clone, Debug)]
enum Operation {
  Admit { lane: RetentionLane, bytes: usize },
  UpdateLimits(EventBufferLimits),
  TakeBatch { max_entries: usize },
  Close,
}

fn operation_strategy() -> impl Strategy<Value = Operation> {
  prop_oneof![
    4 => (retention_lane_strategy(), 1_usize .. 65)
      .prop_map(|(lane, bytes)| Operation::Admit { lane, bytes }),
    1 => (1_usize .. 65, 1_usize .. 65)
      .prop_map(|(log_limit_bytes, total_limit_bytes)| {
        Operation::UpdateLimits(EventBufferLimits {
          log_limit_bytes,
          total_limit_bytes,
        })
      }),
    1 => (1_usize .. 65).prop_map(|max_entries| Operation::TakeBatch { max_entries }),
    1 => Just(Operation::Close),
  ]
}

fn retention_lane_strategy() -> impl Strategy<Value = RetentionLane> {
  prop_oneof![
    Just(RetentionLane::Low),
    Just(RetentionLane::High),
    Just(RetentionLane::Protected),
  ]
}

fn scenario_strategy() -> impl Strategy<Value = Vec<Operation>> {
  prop::collection::vec(operation_strategy(), 1 .. 128)
}

//
// TestSubject
//

struct TestSubject {
  actual: EventBufferState<Entry>,
  reference: ReferenceState,
  next_id: u64,
}

impl TestSubject {
  fn new(limits: EventBufferLimits) -> Self {
    Self {
      actual: EventBufferState::new(limits),
      reference: ReferenceState::new(limits),
      next_id: 0,
    }
  }

  fn apply(&mut self, operation: &Operation) {
    match operation {
      Operation::Admit { lane, bytes } => {
        let entry = Entry::new(self.next_id, *lane, *bytes);
        self.next_id += 1;
        assert_eq!(
          self.actual.admit(entry.lane, entry.bytes, entry.clone()),
          self.reference.admit(entry),
        );
      },
      Operation::UpdateLimits(limits) => {
        self.actual.set_pending_limits(*limits);
        self.reference.set_pending_limits(*limits);
      },
      Operation::TakeBatch { max_entries } => {
        assert_eq!(
          self.actual.take_batch(*max_entries),
          self.reference.take_batch(*max_entries),
        );
      },
      Operation::Close => {
        self.actual.close();
        self.reference.close();
      },
    }
  }
}

//
// ReferenceState
//

/// An intentionally simple model that enumerates retained lane prefixes instead of replaying the
/// production eviction loops.
struct ReferenceState {
  limits: EventBufferLimits,
  pending_limits: Option<EventBufferLimits>,
  closed: bool,
  entries: Vec<Entry>,
}

impl ReferenceState {
  fn new(limits: EventBufferLimits) -> Self {
    Self {
      limits,
      pending_limits: None,
      closed: false,
      entries: vec![],
    }
  }

  fn set_pending_limits(&mut self, limits: EventBufferLimits) {
    self.pending_limits = Some(limits);
  }

  fn admit(&mut self, entry: Entry) -> AdmissionOutcome {
    self.apply_pending_limits();
    if self.closed {
      return AdmissionOutcome::Closed;
    }
    if entry.bytes > self.limits.total_limit_bytes
      || (entry.lane.is_evictable() && entry.bytes > self.limits.log_limit_bytes)
    {
      return AdmissionOutcome::RejectedOversized;
    }

    let Some(retained) = best_retained_entries(
      &self.entries,
      self.limits,
      Some(&entry),
      evictable_lanes(entry.lane),
      TotalBudget::Strict,
    ) else {
      return AdmissionOutcome::RejectedFull;
    };
    self.entries = retained;
    self.entries.push(entry);
    AdmissionOutcome::Admitted
  }

  fn take_batch(&mut self, max_entries: usize) -> Vec<Entry> {
    let count = max_entries.min(self.entries.len());
    self.entries.drain(.. count).collect()
  }

  fn close(&mut self) {
    self.closed = true;
    self.entries.clear();
  }

  fn apply_pending_limits(&mut self) {
    let Some(limits) = self.pending_limits.take() else {
      return;
    };
    self.limits = limits;
    // A limit change has no incoming lane. It always removes low entries before high entries and
    // never removes protected entries, even if they alone exceed the new total limit.
    self.entries = best_retained_entries(
      &self.entries,
      self.limits,
      None,
      &[RetentionLane::Low, RetentionLane::High],
      TotalBudget::PreserveProtected,
    )
    .expect("removing every evictable entry always produces a valid retained set");
  }
}

fn evictable_lanes(incoming_lane: RetentionLane) -> &'static [RetentionLane] {
  match incoming_lane {
    RetentionLane::Low => &[],
    RetentionLane::High => &[RetentionLane::Low],
    RetentionLane::Protected => &[RetentionLane::Low, RetentionLane::High],
  }
}

#[derive(Clone, Copy)]
enum TotalBudget {
  Strict,
  PreserveProtected,
}

/// Enumerates valid retained lane prefixes. The unique best answer keeps as many of the oldest
/// eligible entries as possible, preferring to evict low before high. This specification never
/// calculates bytes to free or replays the production eviction loops.
fn best_retained_entries(
  entries: &[Entry],
  limits: EventBufferLimits,
  incoming: Option<&Entry>,
  evictable_lanes: &[RetentionLane],
  total_budget: TotalBudget,
) -> Option<Vec<Entry>> {
  let low_count = entries
    .iter()
    .filter(|entry| entry.lane == RetentionLane::Low)
    .count();
  let high_count = entries
    .iter()
    .filter(|entry| entry.lane == RetentionLane::High)
    .count();

  let mut best: Option<(usize, usize, Vec<Entry>)> = None;
  for retained_low in 0 ..= low_count {
    for retained_high in 0 ..= high_count {
      if !is_valid_retained_prefix(
        retained_low,
        retained_high,
        low_count,
        high_count,
        evictable_lanes,
      ) {
        continue;
      }
      let retained = retain_lane_prefixes(entries, retained_low, retained_high);
      if !fits(&retained, limits, incoming, total_budget) {
        continue;
      }

      // The cascade constraint makes this equivalent to evicting the newest low suffix before
      // touching high entries.
      let score = (retained_low, retained_high);
      if best
        .as_ref()
        .is_none_or(|(best_low, best_high, _)| score > (*best_low, *best_high))
      {
        best = Some((retained_low, retained_high, retained));
      }
    }
  }
  best.map(|(_, _, entries)| entries)
}

fn is_valid_retained_prefix(
  retained_low: usize,
  retained_high: usize,
  low_count: usize,
  high_count: usize,
  evictable_lanes: &[RetentionLane],
) -> bool {
  let evicts_ineligible_low =
    !evictable_lanes.contains(&RetentionLane::Low) && retained_low != low_count;
  let evicts_ineligible_high =
    !evictable_lanes.contains(&RetentionLane::High) && retained_high != high_count;
  // A high entry is evicted only after every eligible low entry has gone. This also captures the
  // intentional whole-entry overshoot when the final low eviction was not sufficient.
  let evicts_high_before_low = retained_high < high_count && retained_low != 0;
  !(evicts_ineligible_low || evicts_ineligible_high || evicts_high_before_low)
}

fn retain_lane_prefixes(
  entries: &[Entry],
  retained_low: usize,
  retained_high: usize,
) -> Vec<Entry> {
  let mut seen_low = 0;
  let mut seen_high = 0;
  entries
    .iter()
    .filter(|entry| match entry.lane {
      RetentionLane::Low => {
        seen_low += 1;
        seen_low <= retained_low
      },
      RetentionLane::High => {
        seen_high += 1;
        seen_high <= retained_high
      },
      RetentionLane::Protected => true,
    })
    .cloned()
    .collect()
}

fn fits(
  entries: &[Entry],
  limits: EventBufferLimits,
  incoming: Option<&Entry>,
  total_budget: TotalBudget,
) -> bool {
  let evictable_bytes = entries
    .iter()
    .filter(|entry| entry.lane.is_evictable())
    .map(|entry| entry.bytes)
    .sum::<usize>();
  let retained_bytes = entries.iter().map(|entry| entry.bytes).sum::<usize>();
  let protected_bytes = entries
    .iter()
    .filter(|entry| entry.lane == RetentionLane::Protected)
    .map(|entry| entry.bytes)
    .sum::<usize>();
  let incoming_bytes = incoming.map_or(0, |entry| entry.bytes);
  let incoming_evictable_bytes = incoming
    .filter(|entry| entry.lane.is_evictable())
    .map_or(0, |entry| entry.bytes);
  let total_limit = match total_budget {
    TotalBudget::Strict => limits.total_limit_bytes,
    TotalBudget::PreserveProtected => limits.total_limit_bytes.max(protected_bytes),
  };

  evictable_bytes + incoming_evictable_bytes <= limits.log_limit_bytes
    && retained_bytes + incoming_bytes <= total_limit
}

proptest! {
  #![proptest_config(proptest_config())]

  #[test]
  fn operations_match_reference_model(operations in scenario_strategy()) {
    let initial_limits = EventBufferLimits {
      log_limit_bytes: 64,
      total_limit_bytes: 64,
    };
    let mut subject = TestSubject::new(initial_limits);

    for operation in operations {
      subject.apply(&operation);
    }
  }
}
