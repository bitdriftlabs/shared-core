// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
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
#[path = "./recorder_test.rs"]
mod recorder_test;

use bd_client_common::maybe_await_interval;
use bd_runtime::runtime::{BoolWatch, ConfigLoader, DurationWatch, session_replay};
use bd_shutdown::{ComponentShutdown, ComponentShutdownTrigger};
use bd_time::TimeDurationExt;
use std::sync::Arc;
use tokio::time::{Interval, MissedTickBehavior};

#[cfg(test)]
#[ctor::ctor(unsafe)]
fn test_global_init() {
  bd_test_helpers_core::test_global_init();
}

//
// Target
//

// An interface implementing the act of capturing user screens.
pub trait Target {
  // Instruct the target to capture a privacy-preserving and bandwidth-efficient representation
  // of the user's screen. The target should capture the screen and send it using the
  // `logger::log_session_replay_screen` method. The target is expected to operate
  // asynchronously, as accessing the application view hierarchy requires executing on the main
  // thread, and the `capture_screen` method is always called from a non-main thread.
  fn capture_screen(&self);
}

//
// Recorder
//

pub struct Recorder {
  target: Arc<dyn Target + Send + Sync>,

  is_periodic_reporting_enabled_flag: BoolWatch<session_replay::PeriodicScreensEnabledFlag>,
  is_periodic_reporting_enabled: bool,
  reporting_interval_rate_flag: DurationWatch<session_replay::ReportingIntervalFlag>,
  reporting_interval_rate: time::Duration,
  reporting_interval: Option<Interval>,
}

impl Recorder {
  pub fn new(target: Box<dyn Target + Send + Sync>, runtime_loader: &Arc<ConfigLoader>) -> Self {
    let mut is_periodic_reporting_enabled_flag =
      session_replay::PeriodicScreensEnabledFlag::register(runtime_loader);
    let is_periodic_reporting_enabled = *is_periodic_reporting_enabled_flag.read_mark_update();

    let reporting_interval_rate =
      *session_replay::ReportingIntervalFlag::register(runtime_loader).read_mark_update();

    let target: Arc<dyn Target + Send + Sync> = target.into();
    Self {
      target,
      is_periodic_reporting_enabled_flag,
      is_periodic_reporting_enabled,
      reporting_interval_rate_flag: session_replay::ReportingIntervalFlag::register(runtime_loader),
      reporting_interval_rate,
      reporting_interval: None,
    }
  }

  fn create_interval(interval: time::Duration, fire_immediately: bool) -> tokio::time::Interval {
    let interval = if fire_immediately {
      interval.interval(MissedTickBehavior::Delay)
    } else {
      interval.interval_at(MissedTickBehavior::Delay)
    };

    log::debug!(
      "session replay recorder interval is {:?}",
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
        () = maybe_await_interval(self.reporting_interval.as_mut()),
        if self.is_periodic_reporting_enabled => {
          log::debug!("session replay recorder capturing screen");
          // We capture a screen once per 3s (by default) so backlogging shouldn't be a problem
          // here (since taking a screenshot takes not more than ~tens of ms).
          // TODO(Augustyniak): Consider changing the implementation so that we do not ask platform layer
          // for more screens until we receive the previous one.
          self.target.capture_screen();
        },
        _ = self.reporting_interval_rate_flag.changed() => {
          self.reporting_interval = Some(
            Self::create_interval(
              *self.reporting_interval_rate_flag.read_mark_update(),
              false
            )
          );
        },
        _ = self.is_periodic_reporting_enabled_flag.changed() => {
          self.is_periodic_reporting_enabled
            = *self.is_periodic_reporting_enabled_flag.read_mark_update();
        },
        () = &mut local_shutdown => {
          return;
        },
      }
    }
  }
}
