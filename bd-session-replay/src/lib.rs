// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./recorder_test.rs"]
mod recorder_test;

use bd_runtime::runtime::{session_replay, BoolWatch, ConfigLoader, DurationWatch};
use bd_shutdown::{ComponentShutdown, ComponentShutdownTrigger};
use bd_time::TimeDurationExt;
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::Interval;

#[cfg(test)]
#[ctor::ctor]
fn test_global_init() {
  bd_test_helpers::test_global_init();
}

//
// Target
//

// An interface implementing the act of capturing user screens.
pub trait Target {
  // Instrument the target to capture a privacy-preserving and bandwidth-efficient representation
  // of the user's screen. The target should capture the wireframe and send it using the
  // `logger::log_session_replay_wireframe` method. The target is expected to operate
  // asynchronously, as accessing the application view hierarchy requires executing on the main
  // thread, and the `capture_wireframe` method is always called from a non-main thread.
  fn capture_wireframe(&self);

  // Instrument the target to capture a pixel-perfect representation of the user's screen.
  // The target should capture the wireframe and send it using the
  // `logger::log_session_screenshot` method. The target is expected to operate asynchronously,
  // as accessing the application view hierarchy requires executing on the main thread, and the
  // `take_screenshot` method is always called from a non-main thread.
  fn take_screenshot(&self);
}

//
// Recorder
//

pub struct Recorder {
  target: Box<dyn Target + Send + Sync>,

  is_periodic_reporting_enabled_flag: BoolWatch<session_replay::PeriodicWireframesEnabledFlag>,
  is_periodic_reporting_enabled: bool,
  reporting_interval_rate_flag: DurationWatch<session_replay::ReportingIntervalFlag>,
  reporting_interval_rate: time::Duration,
  reporting_interval: Option<Interval>,

  is_take_screenshots_enabled_flag: BoolWatch<session_replay::ScreenshotsEnabledFlag>,
  is_take_screenshots_enabled: bool,
  take_screenshot_rx: Receiver<()>,
}

impl Recorder {
  pub fn new(
    target: Box<dyn Target + Send + Sync>,
    runtime_loader: &Arc<ConfigLoader>,
  ) -> (Self, Sender<()>) {
    // Limit the buffer size to 1 to reduce the risk of putting too much pressure on the
    // application's main thread when dequeuing the screenshot actions from the channel and
    // passing them to the platform layer, which performs the actual screenshotting on the main
    // thread.
    let (take_screenshot_tx, take_screenshot_rx) = channel(1);

    let mut is_periodic_reporting_enabled_flag: bd_runtime::runtime::Watch<
      bool,
      session_replay::PeriodicWireframesEnabledFlag,
    > = session_replay::PeriodicWireframesEnabledFlag::register(runtime_loader).unwrap();
    let is_periodic_reporting_enabled = is_periodic_reporting_enabled_flag.read_mark_update();

    let reporting_interval_rate = session_replay::ReportingIntervalFlag::register(runtime_loader)
      .unwrap()
      .read_mark_update();

    let mut is_take_screenshots_enabled_flag =
      session_replay::ScreenshotsEnabledFlag::register(runtime_loader).unwrap();
    let is_take_screenshots_enabled = is_take_screenshots_enabled_flag.read_mark_update();

    (
      Self {
        target,
        is_periodic_reporting_enabled_flag,
        is_periodic_reporting_enabled,
        reporting_interval_rate_flag: session_replay::ReportingIntervalFlag::register(
          runtime_loader,
        )
        .unwrap(),
        reporting_interval_rate,
        reporting_interval: None,
        is_take_screenshots_enabled_flag,
        is_take_screenshots_enabled,
        take_screenshot_rx,
      },
      take_screenshot_tx,
    )
  }

  fn create_interval(interval: time::Duration, fire_immediately: bool) -> tokio::time::Interval {
    let mut interval = if fire_immediately {
      interval.interval()
    } else {
      interval.interval_at()
    };

    log::debug!(
      "session replay recorder interval is {:?}",
      interval.period()
    );

    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
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
        () = async { self.reporting_interval.as_mut().unwrap().tick().await; },
          if self.reporting_interval.is_some() && self.is_periodic_reporting_enabled => {
          log::debug!("session replay recorder capturing wireframe");
          self.target.capture_wireframe();
        },
        _ = self.reporting_interval_rate_flag.changed() => {
          self.reporting_interval = Some(
            Self::create_interval(
              self.reporting_interval_rate_flag.read_mark_update(),
              false
            )
          );
        },
        _ = self.take_screenshot_rx.recv(), if self.is_take_screenshots_enabled => {
          log::debug!("session replay recorder taking screenshot");
          self.target.take_screenshot();
        },
        _ = self.is_periodic_reporting_enabled_flag.changed() => {
          self.is_periodic_reporting_enabled
            = self.is_periodic_reporting_enabled_flag.read_mark_update();
        },
        _ = self.is_take_screenshots_enabled_flag.changed() => {
          self.is_take_screenshots_enabled
            = self.is_take_screenshots_enabled_flag.read_mark_update();
        },
        () = &mut local_shutdown => {
          return;
        },
      }
    }
  }
}
