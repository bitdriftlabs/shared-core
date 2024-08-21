// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./reporter_test.rs"]
mod reporter_test;
use bd_runtime::runtime::resource_utilization::{
  ResourceUtilizationEnabledFlag,
  ResourceUtilizationReportingIntervalFlag,
};
use bd_runtime::runtime::{ConfigLoader, Watch};
use bd_shutdown::{ComponentShutdown, ComponentShutdownTrigger};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, Interval, MissedTickBehavior};

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

  is_enabled_flag: Watch<bool, ResourceUtilizationEnabledFlag>,
  reporting_interval_flag: Watch<u32, ResourceUtilizationReportingIntervalFlag>,
}

impl Reporter {
  pub fn new(target: Box<dyn Target + Send + Sync>, runtime_loader: &Arc<ConfigLoader>) -> Self {
    let mut is_enabled_flag: Watch<bool, ResourceUtilizationEnabledFlag> =
      runtime_loader.register_watch().unwrap();
    let mut reporting_interval_flag: Watch<u32, ResourceUtilizationReportingIntervalFlag> =
      runtime_loader.register_watch().unwrap();

    let rate = Duration::from_millis(reporting_interval_flag.read_mark_update().into());

    Self {
      target,

      is_enabled: is_enabled_flag.read_mark_update(),
      reporting_interval_rate: rate,

      reporting_interval: None,

      is_enabled_flag,
      reporting_interval_flag,
    }
  }

  fn create_interval(interval: Duration, fire_immediately: bool) -> tokio::time::Interval {
    let mut interval = if fire_immediately {
      tokio::time::interval(interval)
    } else {
      tokio::time::interval_at(Instant::now() + interval, interval)
    };

    log::debug!(
      "resource utilization reporter interval is {:?}",
      interval.period()
    );

    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
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
        () = async {
          if let Some(reporting_interval) = &mut self.reporting_interval {
            reporting_interval.tick().await;
          }
        }, if self.reporting_interval.is_some() && self.is_enabled => {
          log::debug!("resource utilization reporter tick");
          self.target.tick();
        },
        _ = self.reporting_interval_flag.changed() => {
          self.reporting_interval = Some(
            Self::create_interval(
              Duration::from_millis(self.reporting_interval_flag.read().into()),
              false
            )
          );
        },
        _ = self.is_enabled_flag.changed() => {
          self.is_enabled = self.is_enabled_flag.read();
        },
        () = &mut local_shutdown => {
          return;
        },
      }
    }
  }
}
