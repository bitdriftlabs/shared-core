// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![deny(
  clippy::expect_used,
  clippy::panic,
  clippy::todo,
  clippy::unimplemented,
  clippy::unreachable,
  clippy::unwrap_used
)]

#[cfg(test)]
#[path = "./reporter_test.rs"]
mod reporter_test;

use bd_client_common::maybe_await_interval;
use bd_runtime::runtime::resource_utilization::{
  ResourceUtilizationEnabledFlag,
  ResourceUtilizationReportingIntervalFlag,
};
use bd_runtime::runtime::{BoolWatch, ConfigLoader, DurationWatch};
use bd_shutdown::{ComponentShutdown, ComponentShutdownTrigger};
use bd_time::TimeDurationExt;
use std::sync::Arc;
use time::Duration;
use tokio::time::{Interval, MissedTickBehavior};

#[cfg(test)]
#[ctor::ctor]
fn test_global_init() {
  bd_test_helpers::test_global_init();
}

//
// Target
//

pub trait Target {
  /// Called to indicate that the target is supposed to prepare and emit a resource utilization log.
  fn tick(&self);
}

//
// Reporter
//

/// Responsible for informing the target to prepare and emit a resource utilization log at a
/// runtime-controlled interval.
pub struct Reporter {
  target: Box<dyn Target + Send + Sync>,

  is_enabled: bool,
  reporting_interval_rate: Duration,
  reporting_interval: Option<Interval>,

  is_enabled_flag: BoolWatch<ResourceUtilizationEnabledFlag>,
  reporting_interval_flag: DurationWatch<ResourceUtilizationReportingIntervalFlag>,
}

impl Reporter {
  pub fn new(target: Box<dyn Target + Send + Sync>, runtime_loader: &Arc<ConfigLoader>) -> Self {
    let mut is_enabled_flag = ResourceUtilizationEnabledFlag::register(runtime_loader);
    let mut reporting_interval_flag =
      ResourceUtilizationReportingIntervalFlag::register(runtime_loader);

    let rate = *reporting_interval_flag.read_mark_update();
    let is_enabled = *is_enabled_flag.read_mark_update();

    Self {
      target,

      is_enabled,
      reporting_interval_rate: rate,

      reporting_interval: None,

      is_enabled_flag,
      reporting_interval_flag,
    }
  }

  fn create_interval(interval: Duration, fire_immediately: bool) -> tokio::time::Interval {
    let interval = if fire_immediately {
      interval.interval(MissedTickBehavior::Delay)
    } else {
      interval.interval_at(MissedTickBehavior::Delay)
    };

    log::debug!(
      "resource utilization reporter interval is {:?}",
      interval.period()
    );

    interval
  }

  pub async fn run(&mut self) {
    let shutdown_trigger = ComponentShutdownTrigger::default();
    self
      .run_with_shutdown(shutdown_trigger.make_shutdown())
      .await;
  }

  pub async fn run_with_shutdown(&mut self, mut shutdown: ComponentShutdown) {
    if self.reporting_interval.is_none() {
      self.reporting_interval = Some(Self::create_interval(self.reporting_interval_rate, true));
    }

    let local_shutdown = shutdown.cancelled();
    tokio::pin!(local_shutdown);

    loop {
      tokio::select! {
        () = maybe_await_interval(self.reporting_interval.as_mut()), if self.is_enabled => {
          log::debug!("resource utilization reporter tick");
          self.target.tick();
        },
        _ = self.reporting_interval_flag.changed() => {
          self.reporting_interval = Some(
            Self::create_interval(
              *self.reporting_interval_flag.read_mark_update(),
              false
            )
          );
        },
        _ = self.is_enabled_flag.changed() => {
          self.is_enabled = *self.is_enabled_flag.read_mark_update();
        },
        () = &mut local_shutdown => {
          return;
        },
      }
    }
  }
}
