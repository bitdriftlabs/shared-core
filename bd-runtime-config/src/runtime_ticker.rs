// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::feature_flags::FeatureFlagsWatch;
use async_trait::async_trait;
use bd_time::{SystemTimeProvider, Ticker, TimeProvider};
use std::sync::Arc;

#[derive(Clone, Copy)]
enum Unit {
  Seconds,
  Milliseconds,
}

pub struct RuntimeFlagTicker {
  feature_flags: FeatureFlagsWatch,
  interval_flag: String,
  default_interval: u64,
  unit: Unit,
  disabled_recheck_interval: time::Duration,
  time_provider: Arc<dyn TimeProvider>,
}

impl RuntimeFlagTicker {
  /// Creates a ticker that reloads `interval_flag` before each tick.
  ///
  /// Values are interpreted as seconds. If the configured value is `0`, the ticker sleeps for
  /// `disabled_recheck_interval` and then re-checks the flag until polling is re-enabled.
  #[must_use]
  pub fn new(
    feature_flags: FeatureFlagsWatch,
    interval_flag: impl Into<String>,
    default_interval_seconds: u64,
  ) -> Self {
    Self::new_with_unit(
      feature_flags,
      interval_flag,
      default_interval_seconds,
      Unit::Seconds,
    )
  }

  /// Creates a ticker that reloads `interval_flag` before each tick.
  ///
  /// Values are interpreted as milliseconds. If the configured value is `0`, the ticker sleeps for
  /// `disabled_recheck_interval` and then re-checks the flag until polling is re-enabled.
  #[must_use]
  pub fn new_milliseconds(
    feature_flags: FeatureFlagsWatch,
    interval_flag: impl Into<String>,
    default_interval_milliseconds: u64,
  ) -> Self {
    Self::new_with_unit(
      feature_flags,
      interval_flag,
      default_interval_milliseconds,
      Unit::Milliseconds,
    )
  }

  fn new_with_unit(
    feature_flags: FeatureFlagsWatch,
    interval_flag: impl Into<String>,
    default_interval: u64,
    unit: Unit,
  ) -> Self {
    Self {
      feature_flags,
      interval_flag: interval_flag.into(),
      default_interval,
      unit,
      disabled_recheck_interval: time::Duration::seconds(5),
      time_provider: Arc::new(SystemTimeProvider),
    }
  }

  #[must_use]
  pub fn with_time_provider(mut self, time_provider: Arc<dyn TimeProvider>) -> Self {
    self.time_provider = time_provider;
    self
  }

  #[must_use]
  pub fn with_disabled_recheck_interval(mut self, interval: time::Duration) -> Self {
    self.disabled_recheck_interval = interval;
    self
  }

  fn interval(&self) -> time::Duration {
    let interval = self
      .feature_flags
      .borrow()
      .as_ref()
      .map_or(self.default_interval, |flags| {
        flags.get_integer(&self.interval_flag, self.default_interval)
      });

    let Ok(interval) = interval.try_into() else {
      log::warn!(
        "Invalid value for {}: {}, using fallback of {} seconds",
        self.interval_flag,
        interval,
        time::Duration::minutes(5).whole_seconds()
      );
      return time::Duration::minutes(5);
    };

    match self.unit {
      Unit::Seconds => time::Duration::seconds(interval),
      Unit::Milliseconds => time::Duration::milliseconds(interval),
    }
  }
}

#[async_trait]
impl Ticker for RuntimeFlagTicker {
  async fn tick(&mut self) {
    loop {
      let interval = self.interval();
      if interval.is_zero() {
        self
          .time_provider
          .sleep(self.disabled_recheck_interval)
          .await;
        continue;
      }

      self.time_provider.sleep(interval).await;
      return;
    }
  }
}
