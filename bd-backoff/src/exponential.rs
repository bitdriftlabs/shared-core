// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::backoff::{FiniteBackoff, InfiniteBackoff};
use crate::clock::Clock;
use crate::default;
use rand::random;
use std::marker::PhantomData;
use time::Duration;

#[cfg(test)]
#[path = "./exponential_test.rs"]
mod tests;

#[derive(Debug, Clone, Copy)]
pub struct Finite;

#[derive(Debug, Clone, Copy)]
pub struct Infinite;

#[derive(Debug, Clone)]
pub struct ExponentialBackoffBuilder<C, E> {
  initial_interval: Duration,
  randomization_factor: f64,
  multiplier: f64,
  max_interval: Duration,
  max_elapsed_time: Option<Duration>,
  clock: C,
  _elapsed: PhantomData<E>,
}

#[derive(Debug, Clone)]
struct ExponentialBackoffState<C> {
  current_interval: Duration,
  initial_interval: Duration,
  randomization_factor: f64,
  multiplier: f64,
  max_interval: Duration,
  max_elapsed_time: Option<Duration>,
  start_time: std::time::Instant,
  clock: C,
}

#[derive(Debug, Clone)]
pub struct ExponentialBackoffFinite<C> {
  state: ExponentialBackoffState<C>,
}

#[derive(Debug, Clone)]
pub struct ExponentialBackoffInfinite<C> {
  state: ExponentialBackoffState<C>,
}

impl<C> ExponentialBackoffBuilder<C, Finite>
where
  C: Clock + Default,
{
  #[must_use]
  pub fn new() -> Self {
    Self {
      initial_interval: Duration::milliseconds(default::INITIAL_INTERVAL_MILLIS.cast_signed()),
      randomization_factor: default::RANDOMIZATION_FACTOR,
      multiplier: default::MULTIPLIER,
      max_interval: Duration::milliseconds(default::MAX_INTERVAL_MILLIS.cast_signed()),
      max_elapsed_time: Some(Duration::milliseconds(
        default::MAX_ELAPSED_TIME_MILLIS.cast_signed(),
      )),
      clock: C::default(),
      _elapsed: PhantomData,
    }
  }

  #[must_use]
  pub fn new_infinite() -> ExponentialBackoffBuilder<C, Infinite> {
    Self::new().with_no_elapsed_time()
  }

  #[must_use]
  pub fn with_max_elapsed_time(mut self, max_elapsed_time: Duration) -> Self {
    self.max_elapsed_time = Some(max_elapsed_time);
    self
  }

  #[must_use]
  pub fn with_no_elapsed_time(self) -> ExponentialBackoffBuilder<C, Infinite> {
    let Self {
      initial_interval,
      randomization_factor,
      multiplier,
      max_interval,
      clock,
      ..
    } = self;
    ExponentialBackoffBuilder {
      initial_interval,
      randomization_factor,
      multiplier,
      max_interval,
      max_elapsed_time: None,
      clock,
      _elapsed: PhantomData,
    }
  }
}

impl<C, E> ExponentialBackoffBuilder<C, E> {
  #[must_use]
  pub fn with_initial_interval(mut self, initial_interval: Duration) -> Self {
    self.initial_interval = initial_interval;
    self
  }

  #[must_use]
  pub fn with_randomization_factor(mut self, randomization_factor: f64) -> Self {
    self.randomization_factor = randomization_factor;
    self
  }

  #[must_use]
  pub fn with_multiplier(mut self, multiplier: f64) -> Self {
    self.multiplier = multiplier;
    self
  }

  #[must_use]
  pub fn with_max_interval(mut self, max_interval: Duration) -> Self {
    self.max_interval = max_interval;
    self
  }
}

impl<C> Default for ExponentialBackoffBuilder<C, Finite>
where
  C: Clock + Default,
{
  fn default() -> Self {
    Self::new()
  }
}

impl<C> ExponentialBackoffBuilder<C, Finite>
where
  C: Clock,
{
  pub fn build(self) -> ExponentialBackoffFinite<C> {
    ExponentialBackoffFinite {
      state: ExponentialBackoffState::new(self),
    }
  }
}

impl<C> ExponentialBackoffBuilder<C, Infinite>
where
  C: Clock,
{
  pub fn build(self) -> ExponentialBackoffInfinite<C> {
    ExponentialBackoffInfinite {
      state: ExponentialBackoffState::new(self),
    }
  }
}

impl<C> ExponentialBackoffState<C>
where
  C: Clock,
{
  fn new<E>(builder: ExponentialBackoffBuilder<C, E>) -> Self {
    let mut state = Self {
      current_interval: builder.initial_interval,
      initial_interval: builder.initial_interval,
      randomization_factor: builder.randomization_factor,
      multiplier: builder.multiplier,
      max_interval: builder.max_interval,
      max_elapsed_time: builder.max_elapsed_time,
      clock: builder.clock,
      start_time: std::time::Instant::now(),
    };
    state.reset();
    state
  }

  fn reset(&mut self) {
    self.current_interval = self.initial_interval;
    self.start_time = self.clock.now();
  }

  fn get_elapsed_time(&self) -> Duration {
    let elapsed = self.clock.now().duration_since(self.start_time);
    Duration::new(
      elapsed.as_secs().cast_signed(),
      elapsed.subsec_nanos().cast_signed(),
    )
  }

  fn next_backoff(&mut self) -> Option<Duration> {
    let elapsed_time = self.get_elapsed_time();

    match self.max_elapsed_time {
      Some(max_elapsed_time) if elapsed_time > max_elapsed_time => None,
      _ => {
        let random_value = random::<f64>();
        let randomized_interval = Self::randomized_interval(
          self.randomization_factor,
          random_value,
          self.current_interval,
        );
        self.current_interval = self.increment_current_interval();

        self
          .max_elapsed_time
          .map_or(Some(randomized_interval), |max_elapsed_time| {
            if elapsed_time + randomized_interval <= max_elapsed_time {
              Some(randomized_interval)
            } else {
              None
            }
          })
      },
    }
  }

  fn increment_current_interval(&self) -> Duration {
    let current_interval_nanos = duration_to_nanos(self.current_interval);
    let max_interval_nanos = duration_to_nanos(self.max_interval);
    if current_interval_nanos >= max_interval_nanos / self.multiplier {
      self.max_interval
    } else {
      nanos_to_duration(current_interval_nanos * self.multiplier)
    }
  }

  fn randomized_interval(
    randomization_factor: f64,
    random: f64,
    current_interval: Duration,
  ) -> Duration {
    let current_interval_nanos = duration_to_nanos(current_interval);
    let delta = randomization_factor * current_interval_nanos;
    let min_interval = current_interval_nanos - delta;
    let max_interval = current_interval_nanos + delta;
    let diff = max_interval - min_interval;
    let nanos = random.mul_add(diff + 1.0, min_interval);
    nanos_to_duration(nanos)
  }
}

impl<C> FiniteBackoff for ExponentialBackoffFinite<C>
where
  C: Clock,
{
  fn reset(&mut self) {
    self.state.reset();
  }

  fn next_backoff(&mut self) -> Option<Duration> {
    self.state.next_backoff()
  }
}

impl<C> InfiniteBackoff for ExponentialBackoffInfinite<C>
where
  C: Clock,
{
  fn reset(&mut self) {
    self.state.reset();
  }

  fn next_backoff(&mut self) -> Duration {
    match self.state.next_backoff() {
      Some(delay) => delay,
      None => self.state.max_interval,
    }
  }
}

impl<C> ExponentialBackoffFinite<C> {
  pub fn initial_interval(&self) -> Duration {
    self.state.initial_interval
  }

  pub fn current_interval(&self) -> Duration {
    self.state.current_interval
  }
}

impl<C> ExponentialBackoffInfinite<C> {
  pub fn initial_interval(&self) -> Duration {
    self.state.initial_interval
  }

  pub fn current_interval(&self) -> Duration {
    self.state.current_interval
  }
}

fn duration_to_nanos(duration: Duration) -> f64 {
  duration.as_seconds_f64() * 1_000_000_000.0
}

fn nanos_to_duration(nanos: f64) -> Duration {
  Duration::seconds_f64(nanos / 1_000_000_000.0)
}
