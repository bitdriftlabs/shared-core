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
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::oneshot;
use tokio::time::{Interval, MissedTickBehavior};

#[cfg(test)]
#[ctor::ctor(unsafe)]
fn test_global_init() {
  bd_test_helpers_core::test_global_init();
}

//
// Target
//

pub type DeviceCommandScreenshotCompletion = Box<dyn FnOnce(Result<Vec<u8>, String>) + Send>;

// An interface implementing the act of capturing user screens.
pub trait Target {
  // Instruct the target to capture a privacy-preserving and bandwidth-efficient representation
  // of the user's screen. The target should capture the screen and send it using the
  // `logger::log_session_replay_screen` method. The target is expected to operate
  // asynchronously, as accessing the application view hierarchy requires executing on the main
  // thread, and the `capture_screen` method is always called from a non-main thread.
  fn capture_screen(&self);

  // Instruct the target to capture a screenshot for a remote device command. Targets that do not
  // support returning screenshot bytes explicitly reject the request rather than leaving the
  // command pending.
  fn capture_device_command_screenshot(&self, completion: DeviceCommandScreenshotCompletion) {
    completion(Err("remote screenshot capture is unavailable".to_string()));
  }
}

//
// RemoteScreenshotCaptureHandler
//

#[derive(Clone)]
pub struct RemoteScreenshotCaptureHandler {
  target: Arc<dyn Target + Send + Sync>,
  active_capture_id: Arc<AtomicU64>,
  next_capture_id: Arc<AtomicU64>,
  capture_timeout: std::time::Duration,
}

struct ActiveCaptureGuard {
  active_capture_id: Arc<AtomicU64>,
  capture_id: u64,
}

impl Drop for ActiveCaptureGuard {
  fn drop(&mut self) {
    let _ = self.active_capture_id.compare_exchange(
      self.capture_id,
      0,
      Ordering::AcqRel,
      Ordering::Acquire,
    );
  }
}

impl RemoteScreenshotCaptureHandler {
  pub fn new(target: Arc<dyn Target + Send + Sync>) -> Self {
    Self::with_timeout(target, std::time::Duration::from_secs(30))
  }

  fn with_timeout(
    target: Arc<dyn Target + Send + Sync>,
    capture_timeout: std::time::Duration,
  ) -> Self {
    Self {
      target,
      active_capture_id: Arc::new(AtomicU64::new(0)),
      next_capture_id: Arc::new(AtomicU64::new(1)),
      capture_timeout,
    }
  }

  pub async fn capture(&self) -> Result<Vec<u8>, String> {
    let capture_id = self.next_capture_id.fetch_add(1, Ordering::Relaxed);
    if self
      .active_capture_id
      .compare_exchange(0, capture_id, Ordering::AcqRel, Ordering::Acquire)
      .is_err()
    {
      return Err("a remote screenshot capture is already in progress".to_string());
    }

    let _active_capture_guard = ActiveCaptureGuard {
      active_capture_id: self.active_capture_id.clone(),
      capture_id,
    };
    let (completion_tx, completion_rx) = oneshot::channel();
    let active_capture_id = self.active_capture_id.clone();
    self
      .target
      .capture_device_command_screenshot(Box::new(move |result| {
        let _ =
          active_capture_id.compare_exchange(capture_id, 0, Ordering::AcqRel, Ordering::Acquire);
        let _ = completion_tx.send(result);
      }));

    match tokio::time::timeout(self.capture_timeout, completion_rx).await {
      Ok(Ok(result)) => result,
      Ok(Err(_)) => Err("remote screenshot capture was interrupted".to_string()),
      Err(_) => Err("remote screenshot capture timed out".to_string()),
    }
  }
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
  pub fn new(
    target: Box<dyn Target + Send + Sync>,
    runtime_loader: &Arc<ConfigLoader>,
  ) -> (Self, RemoteScreenshotCaptureHandler) {
    let mut is_periodic_reporting_enabled_flag =
      session_replay::PeriodicScreensEnabledFlag::register(runtime_loader);
    let is_periodic_reporting_enabled = *is_periodic_reporting_enabled_flag.read_mark_update();

    let reporting_interval_rate =
      *session_replay::ReportingIntervalFlag::register(runtime_loader).read_mark_update();

    let target: Arc<dyn Target + Send + Sync> = target.into();
    let remote_screenshot_capture_handler = RemoteScreenshotCaptureHandler::new(target.clone());

    (
      Self {
        target,
        is_periodic_reporting_enabled_flag,
        is_periodic_reporting_enabled,
        reporting_interval_rate_flag: session_replay::ReportingIntervalFlag::register(
          runtime_loader,
        ),
        reporting_interval_rate,
        reporting_interval: None,
      },
      remote_screenshot_capture_handler,
    )
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
