#![allow(clippy::unwrap_used)]

use super::{AdmissionOutcome, EventBufferLimits, EventBufferState, RetentionLane};
use proptest::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Entry {
  id: u64,
  kind: EntryKind,
  bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EntryKind {
  Log(RetentionLane),
  State,
}

impl Entry {
  const fn lane(&self) -> RetentionLane {
    match self.kind {
      EntryKind::Log(lane) => lane,
      EntryKind::State => RetentionLane::Protected,
    }
  }
}

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
      || (entry.lane().is_evictable() && entry.bytes > self.limits.log_limit_bytes)
    {
      return AdmissionOutcome::RejectedOversized;
    }

    let candidates = candidates_for_admission(entry.lane());
    let Some(retained) =
      best_retained_entries(&self.entries, self.limits, Some(&entry), &candidates, false)
    else {
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
    // A limit change has no inbound lane. Its policy permits evicting low entries first and high
    // entries only once retaining an additional high entry would exceed a budget.
    self.entries = best_retained_entries(
      &self.entries,
      self.limits,
      None,
      &[RetentionLane::Low, RetentionLane::High],
      true,
    )
    .expect("removing every evictable entry always produces a valid retained set");
  }
}

fn candidates_for_admission(incoming_lane: RetentionLane) -> Vec<RetentionLane> {
  match incoming_lane {
    RetentionLane::Low => vec![],
    RetentionLane::High => vec![RetentionLane::Low],
    RetentionLane::Protected => vec![RetentionLane::Low, RetentionLane::High],
  }
}

/// Enumerates the valid retained sets of lane prefixes. The policy has a unique best answer: keep
/// the oldest eligible entries possible, preferring to evict low before high. This specification
/// deliberately never calculates required bytes or replays the production eviction loops.
fn best_retained_entries(
  entries: &[Entry],
  limits: EventBufferLimits,
  incoming: Option<&Entry>,
  candidates: &[RetentionLane],
  allow_protected_budget_debt: bool,
) -> Option<Vec<Entry>> {
  let low_count = entries
    .iter()
    .filter(|entry| entry.lane() == RetentionLane::Low)
    .count();
  let high_count = entries
    .iter()
    .filter(|entry| entry.lane() == RetentionLane::High)
    .count();

  let mut best: Option<(usize, usize, Vec<Entry>)> = None;
  for retained_low in 0 ..= low_count {
    for retained_high in 0 ..= high_count {
      if (!candidates.contains(&RetentionLane::Low) && retained_low != low_count)
        || (!candidates.contains(&RetentionLane::High) && retained_high != high_count)
        // A high entry is evicted only after every eligible low entry has gone. This also captures
        // the intentional whole-entry overshoot when the final low eviction was not sufficient.
        || (retained_high < high_count && retained_low != 0)
      {
        continue;
      }
      let retained = retain_lane_prefixes(entries, retained_low, retained_high);
      if !fits(&retained, limits, incoming, allow_protected_budget_debt) {
        continue;
      }

      // Among policy-valid states, retain as much as possible. The cascade constraint above makes
      // this equivalent to evicting the newest low suffix before touching high entries.
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

fn retain_lane_prefixes(
  entries: &[Entry],
  retained_low: usize,
  retained_high: usize,
) -> Vec<Entry> {
  let mut seen_low = 0;
  let mut seen_high = 0;
  entries
    .iter()
    .filter(|entry| match entry.lane() {
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
  allow_protected_budget_debt: bool,
) -> bool {
  let evictable_bytes = entries
    .iter()
    .filter(|entry| entry.lane().is_evictable())
    .map(|entry| entry.bytes)
    .sum::<usize>();
  let retained_bytes = entries.iter().map(|entry| entry.bytes).sum::<usize>();
  let protected_bytes = entries
    .iter()
    .filter(|entry| entry.lane() == RetentionLane::Protected)
    .map(|entry| entry.bytes)
    .sum::<usize>();
  let incoming_bytes = incoming.map_or(0, |entry| entry.bytes);
  let incoming_evictable_bytes = incoming
    .filter(|entry| entry.lane().is_evictable())
    .map_or(0, |entry| entry.bytes);
  let total_limit = if allow_protected_budget_debt {
    limits.total_limit_bytes.max(protected_bytes)
  } else {
    limits.total_limit_bytes
  };
  evictable_bytes + incoming_evictable_bytes <= limits.log_limit_bytes
    && retained_bytes + incoming_bytes <= total_limit
}

fn log_lane(value: u8) -> RetentionLane {
  match value % 3 {
    0 => RetentionLane::Low,
    1 => RetentionLane::High,
    _ => RetentionLane::Protected,
  }
}

fn entry_kind(value: u8) -> EntryKind {
  if value % 4 == 3 {
    EntryKind::State
  } else {
    EntryKind::Log(log_lane(value))
  }
}

fn operations() -> impl Strategy<Value = Vec<(u8, u8, usize, usize)>> {
  prop::collection::vec(
    (0_u8 .. 4, 0_u8 .. 4, 1_usize .. 65, 1_usize .. 65),
    1 .. 128,
  )
}

proptest! {
  #![proptest_config(ProptestConfig::with_cases(512))]

  #[test]
  fn operations_match_reference_model(operations in operations()) {
    let initial_limits = EventBufferLimits {
      log_limit_bytes: 64,
      total_limit_bytes: 64,
    };
    let mut actual = EventBufferState::new(initial_limits);
    let mut reference = ReferenceState::new(initial_limits);
    let mut next_id = 0;

    for (kind, entry_kind_value, bytes, secondary) in operations {
      match kind {
        0 => {
          let entry = Entry {
            id: next_id,
            kind: entry_kind(entry_kind_value),
            bytes,
          };
          next_id += 1;
          let expected = reference.admit(entry.clone());
          let observed = actual.admit(entry.lane(), entry.bytes, entry);
          prop_assert_eq!(observed, expected);
        },
        1 => {
          let limits = EventBufferLimits {
            log_limit_bytes: bytes,
            total_limit_bytes: secondary,
          };
          actual.set_pending_limits(limits);
          reference.set_pending_limits(limits);
        },
        2 => {
          let expected = reference.take_batch(secondary);
          let observed = actual.take_batch(secondary);
          prop_assert_eq!(observed, expected);
        },
        _ => {
          actual.close();
          reference.close();
        },
      }
    }
  }
}
