// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::{ExponentialBackoffBuilder, ExponentialBackoffFinite, Finite};
use crate::clock::Clock;
use crate::{FiniteBackoff as _, InfiniteBackoff as _};
use std::cell::RefCell;
use std::time::Instant;
use time::Duration;

struct Inner {
  i: Duration,
  start: Instant,
}

struct TestClock(RefCell<Inner>);

impl TestClock {
  fn new(i: Duration, start: Instant) -> Self {
    Self(RefCell::new(Inner { i, start }))
  }
}

impl Clock for TestClock {
  fn now(&self) -> Instant {
    let mut inner = self.0.borrow_mut();
    let t = inner.start + inner.i.unsigned_abs();
    inner.i += Duration::seconds(1);
    t
  }
}

impl Default for TestClock {
  fn default() -> Self {
    Self::new(Duration::seconds(1), Instant::now())
  }
}

#[test]
fn get_elapsed_time() {
  let clock = TestClock::new(Duration::seconds(0), Instant::now());
  let mut exp = ExponentialBackoffBuilder::<TestClock, Finite>::new()
    .with_max_elapsed_time(Duration::minutes(15))
    .build();
  exp.state.clock = clock;
  exp.reset();

  let elapsed_time = exp.state.get_elapsed_time();
  assert_eq!(elapsed_time, Duration::seconds(1));
}

#[test]
fn max_elapsed_time() {
  let clock = TestClock::new(Duration::seconds(0), Instant::now());
  let mut exp = ExponentialBackoffBuilder::<TestClock, Finite>::new()
    .with_max_elapsed_time(Duration::seconds(1))
    .build();
  exp.state.clock = clock;
  exp.state.start_time = Instant::now()
    .checked_sub(std::time::Duration::from_secs(1000))
    .unwrap();
  assert!(exp.next_backoff().is_none());

  let clock = TestClock::default();
  let mut exp = ExponentialBackoffFinite {
    state: super::ExponentialBackoffState {
      max_elapsed_time: Some(Duration::seconds(1)),
      current_interval: Duration::milliseconds(500),
      start_time: clock
        .now()
        .checked_sub(std::time::Duration::from_millis(900))
        .unwrap(),
      clock,
      ..ExponentialBackoffBuilder::<TestClock, Finite>::new()
        .build()
        .state
    },
  };
  assert!(exp.next_backoff().is_none());
}

#[test]
fn backoff() {
  let mut exp = ExponentialBackoffBuilder::<TestClock, Finite>::new()
    .with_initial_interval(Duration::milliseconds(500))
    .with_randomization_factor(0.1)
    .with_multiplier(2.0)
    .with_max_interval(Duration::seconds(5))
    .with_max_elapsed_time(Duration::minutes(16))
    .build();

  let expected_results_millis = [500, 1000, 2000, 4000, 5000, 5000, 5000, 5000, 5000, 5000];
  for expected in expected_results_millis {
    assert_eq!(Duration::milliseconds(expected), exp.state.current_interval);
    exp.next_backoff();
  }
}

#[test]
fn infinite_never_none() {
  let max_interval = Duration::seconds(2);
  let mut exp = ExponentialBackoffBuilder::<TestClock, Finite>::new_infinite()
    .with_max_interval(max_interval)
    .build();

  for _ in 0 .. 10 {
    let delay = exp.next_backoff();
    assert!(delay >= Duration::ZERO);
    assert!(exp.current_interval() <= max_interval);
  }
}
