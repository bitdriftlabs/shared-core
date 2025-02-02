// Taken from https://github.com/ihrwein/backoff/blob/master/src/exponential.rs and modified for
// WASM compatibility. Originally licensed dual MIT/Apache.

#![allow(
  clippy::cast_precision_loss,
  clippy::cast_possible_truncation,
  clippy::cast_sign_loss,
  clippy::float_cmp
)]

use bd_time::Instant;
use std::time::Duration;

/// The default initial interval value in milliseconds (0.5 seconds).
pub const INITIAL_INTERVAL_MILLIS: u64 = 500;
/// The default randomization factor (0.5 which results in a random period ranging between 50%
/// below and 50% above the retry interval).
pub const RANDOMIZATION_FACTOR: f64 = 0.5;
/// The default multiplier value (1.5 which is 50% increase per back off).
pub const MULTIPLIER: f64 = 1.5;
/// The default maximum back off time in milliseconds (1 minute).
pub const MAX_INTERVAL_MILLIS: u64 = 60_000;
/// The default maximum elapsed time in milliseconds (15 minutes).
pub const MAX_ELAPSED_TIME_MILLIS: u64 = 900_000;

#[derive(Debug, Clone)]
pub struct ExponentialBackoff {
  /// The current retry interval.
  pub current_interval: Duration,
  /// The initial retry interval.
  pub initial_interval: Duration,
  /// The randomization factor to use for creating a range around the retry interval.
  ///
  /// A randomization factor of 0.5 results in a random period ranging between 50% below and 50%
  /// above the retry interval.
  pub randomization_factor: f64,
  /// The value to multiply the current interval with for each retry attempt.
  pub multiplier: f64,
  /// The maximum value of the back off period. Once the retry interval reaches this
  /// value it stops increasing.
  pub max_interval: Duration,
  /// The system time. It is calculated when an
  /// [`ExponentialBackoff`](struct.ExponentialBackoff.html) instance is created and is reset
  /// when [`retry`](../trait.Operation.html#method.retry) is called.
  pub start_time: Instant,
  /// The maximum elapsed time after instantiating
  /// [`ExponentialBackfff`](struct.ExponentialBackoff.html) or calling [`reset`](trait.Backoff.
  /// html#method.reset) after which [`next_backoff`](../trait.Backoff.html#method.reset) returns
  /// `None`.
  pub max_elapsed_time: Option<Duration>,
}

impl Default for ExponentialBackoff {
  fn default() -> Self {
    let mut eb = Self {
      current_interval: Duration::from_millis(INITIAL_INTERVAL_MILLIS),
      initial_interval: Duration::from_millis(INITIAL_INTERVAL_MILLIS),
      randomization_factor: RANDOMIZATION_FACTOR,
      multiplier: MULTIPLIER,
      max_interval: Duration::from_millis(MAX_INTERVAL_MILLIS),
      max_elapsed_time: Some(Duration::from_millis(MAX_ELAPSED_TIME_MILLIS)),
      start_time: Instant::now(),
    };
    eb.reset();
    eb
  }
}

impl ExponentialBackoff {
  /// Returns the elapsed time since `start_time`.
  #[must_use]
  pub fn get_elapsed_time(&self) -> Duration {
    Instant::now().duration_since(self.start_time)
  }

  fn get_random_value_from_interval(
    randomization_factor: f64,
    random: f64,
    current_interval: Duration,
  ) -> Duration {
    let current_interval_nanos = duration_to_nanos(current_interval);

    let delta = randomization_factor * current_interval_nanos;
    let min_interval = current_interval_nanos - delta;
    let max_interval = current_interval_nanos + delta;
    // Get a random value from the range [minInterval, maxInterval].
    // The formula used below has a +1 because if the minInterval is 1 and the maxInterval is 3 then
    // we want a 33% chance for selecting either 1, 2 or 3.
    let diff = max_interval - min_interval;
    let nanos = random.mul_add(diff + 1.0, min_interval);
    nanos_to_duration(nanos)
  }

  fn increment_current_interval(&self) -> Duration {
    let current_interval_nanos = duration_to_nanos(self.current_interval);
    let max_interval_nanos = duration_to_nanos(self.max_interval);
    // Check for overflow, if overflow is detected set the current interval to the max interval.
    if current_interval_nanos >= max_interval_nanos / self.multiplier {
      self.max_interval
    } else {
      let nanos = current_interval_nanos * self.multiplier;
      nanos_to_duration(nanos)
    }
  }
}

fn duration_to_nanos(d: Duration) -> f64 {
  (d.as_secs() as f64).mul_add(1_000_000_000.0, f64::from(d.subsec_nanos()))
}

fn nanos_to_duration(nanos: f64) -> Duration {
  let secs = nanos / 1_000_000_000.0;
  let nanos = nanos as u64 % 1_000_000_000;
  Duration::new(secs as u64, nanos as u32)
}

impl ExponentialBackoff {
  pub fn reset(&mut self) {
    self.current_interval = self.initial_interval;
    self.start_time = Instant::now();
  }

  pub fn next_backoff(&mut self) -> Option<Duration> {
    let elapsed_time = self.get_elapsed_time();

    match self.max_elapsed_time {
      Some(v) if elapsed_time > v => None,
      _ => {
        let random = rand::random::<f64>();
        let randomized_interval = Self::get_random_value_from_interval(
          self.randomization_factor,
          random,
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
}

/// Builder for [`ExponentialBackoff`](type.ExponentialBackoff.html).
///
/// TODO: Example
#[derive(Debug)]
pub struct ExponentialBackoffBuilder {
  initial_interval: Duration,
  randomization_factor: f64,
  multiplier: f64,
  max_interval: Duration,
  max_elapsed_time: Option<Duration>,
}

impl Default for ExponentialBackoffBuilder {
  fn default() -> Self {
    Self {
      initial_interval: Duration::from_millis(INITIAL_INTERVAL_MILLIS),
      randomization_factor: RANDOMIZATION_FACTOR,
      multiplier: MULTIPLIER,
      max_interval: Duration::from_millis(MAX_INTERVAL_MILLIS),
      max_elapsed_time: Some(Duration::from_millis(MAX_ELAPSED_TIME_MILLIS)),
    }
  }
}

impl ExponentialBackoffBuilder {
  #[must_use]
  pub fn new() -> Self {
    Self::default()
  }

  /// The initial retry interval.
  pub fn with_initial_interval(&mut self, initial_interval: Duration) -> &mut Self {
    self.initial_interval = initial_interval;
    self
  }

  /// The maximum value of the back off period. Once the retry interval reaches this
  /// value it stops increasing.
  pub fn with_max_interval(&mut self, max_interval: Duration) -> &mut Self {
    self.max_interval = max_interval;
    self
  }

  /// The maximum elapsed time after instantiating
  /// [`ExponentialBackfff`](struct.ExponentialBackoff.html) or calling [`reset`](trait.Backoff.
  /// html#method.reset) after which [`next_backoff`](../trait.Backoff.html#method.reset) returns
  /// `None`.
  pub fn with_max_elapsed_time(&mut self, max_elapsed_time: Option<Duration>) -> &mut Self {
    self.max_elapsed_time = max_elapsed_time;
    self
  }

  #[must_use]
  pub fn build(&self) -> ExponentialBackoff {
    ExponentialBackoff {
      current_interval: self.initial_interval,
      initial_interval: self.initial_interval,
      randomization_factor: self.randomization_factor,
      multiplier: self.multiplier,
      max_interval: self.max_interval,
      max_elapsed_time: self.max_elapsed_time,
      start_time: Instant::now(),
    }
  }
}

#[test]
fn get_randomized_interval() {
  // 33% chance of being 1.
  let f = ExponentialBackoff::get_random_value_from_interval;
  assert_eq!(Duration::new(0, 1), f(0.5, 0.0, Duration::new(0, 2)));
  assert_eq!(Duration::new(0, 1), f(0.5, 0.33, Duration::new(0, 2)));
  // 33% chance of being 2.
  assert_eq!(Duration::new(0, 2), f(0.5, 0.34, Duration::new(0, 2)));
  assert_eq!(Duration::new(0, 2), f(0.5, 0.66, Duration::new(0, 2)));
  // 33% chance of being 3.
  assert_eq!(Duration::new(0, 3), f(0.5, 0.67, Duration::new(0, 2)));
  assert_eq!(Duration::new(0, 3), f(0.5, 0.99, Duration::new(0, 2)));
}

#[test]
fn exponential_backoff_builder() {
  let initial_interval = Duration::from_secs(1);
  let max_interval = Duration::from_secs(2);
  let backoff: ExponentialBackoff = ExponentialBackoffBuilder::new()
    .with_initial_interval(initial_interval)
    .with_max_interval(max_interval)
    .with_max_elapsed_time(None)
    .build();
  assert_eq!(backoff.initial_interval, initial_interval);
  assert_eq!(backoff.current_interval, initial_interval);
  assert_eq!(backoff.max_interval, max_interval);
  assert_eq!(backoff.max_elapsed_time, None);
}

#[test]
fn exponential_backoff_default_builder() {
  let backoff: ExponentialBackoff = ExponentialBackoffBuilder::new().build();
  assert_eq!(
    backoff.initial_interval,
    Duration::from_millis(INITIAL_INTERVAL_MILLIS)
  );
  assert_eq!(
    backoff.current_interval,
    Duration::from_millis(INITIAL_INTERVAL_MILLIS)
  );
  assert_eq!(backoff.multiplier, MULTIPLIER);
  assert_eq!(backoff.randomization_factor, RANDOMIZATION_FACTOR);
  assert_eq!(
    backoff.max_interval,
    Duration::from_millis(MAX_INTERVAL_MILLIS)
  );
  assert_eq!(
    backoff.max_elapsed_time,
    Some(Duration::from_millis(MAX_ELAPSED_TIME_MILLIS))
  );
}
