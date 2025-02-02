use crate::{MissedTickBehavior, TimeDurationExt};
use rand::{rng, Rng};
use std::future::{Future, IntoFuture};
use std::time::Duration;
use tokio::time::{advance, interval, interval_at, MissedTickBehavior as TokioMissedTickBehavior};
pub use tokio::time::{sleep, Instant, Interval, Sleep, Timeout};

impl MissedTickBehavior {
  #[must_use]
  pub const fn to_tokio(&self) -> TokioMissedTickBehavior {
    match self {
      Self::Delay => TokioMissedTickBehavior::Delay,
    }
  }
}

impl TimeDurationExt for time::Duration {
  fn advance(self) -> impl Future<Output = ()> {
    advance(self.unsigned_abs())
  }

  fn sleep(self) -> Sleep {
    sleep(self.unsigned_abs())
  }

  fn interval(self, behavior: MissedTickBehavior) -> Interval {
    let mut i = interval(self.unsigned_abs());
    i.set_missed_tick_behavior(behavior.to_tokio());
    i
  }

  fn interval_at(self, behavior: MissedTickBehavior) -> Interval {
    let mut i = interval_at(self.add_now(), self.unsigned_abs());
    i.set_missed_tick_behavior(behavior.to_tokio());
    i
  }

  fn jittered_interval_at(self, behavior: MissedTickBehavior) -> Interval {
    let millis: u64 = self.whole_milliseconds().try_into().unwrap();
    let jittered = Duration::from_millis(rng().random_range(0 ..= millis));
    let mut i = interval_at(Instant::now() + jittered, self.unsigned_abs());
    i.set_missed_tick_behavior(behavior.to_tokio());
    i
  }

  fn timeout<F: IntoFuture>(self, f: F) -> Timeout<F::IntoFuture> {
    tokio::time::timeout(self.unsigned_abs(), f)
  }

  fn add_now(self) -> Instant {
    Instant::now() + self.unsigned_abs()
  }

  fn add_instant(self, instant: Instant) -> Instant {
    instant + self.unsigned_abs()
  }
}
