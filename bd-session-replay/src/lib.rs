// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./recorder_test.rs"]
mod recorder_test;

use bd_client_stats_store::{Counter, Scope};
use bd_log_primitives::LogType;
use bd_runtime::runtime::{session_replay, BoolWatch, ConfigLoader, DurationWatch};
use bd_shutdown::{ComponentShutdown, ComponentShutdownTrigger};
use bd_stats_common::labels;
use bd_time::TimeDurationExt;
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{Interval, MissedTickBehavior};

pub const SESSION_REPLAY_SCREENSHOT_LOG_MESSAGE: &str = "Screenshot captured";

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
  // Instruct the target to capture a privacy-preserving and bandwidth-efficient representation
  // of the user's screen. The target should capture the screen and send it using the
  // `logger::log_session_replay_screen` method. The target is expected to operate
  // asynchronously, as accessing the application view hierarchy requires executing on the main
  // thread, and the `capture_screen` method is always called from a non-main thread.
  fn capture_screen(&self);

  // Instruct the target to capture a pixel-perfect representation of the user's screen.
  // The target should capture the screenshot and send it using the
  // `logger::log_session_replay_screenshot` method. The target is expected to operate
  // asynchronously, as accessing the application view hierarchy requires executing on the main
  // thread, and the `take_screenshot` method is always called from a non-main thread.
  //
  // The recorder implementation guarantees that the consecutive `take_screenshot` method calls are
  // not made after the previous screenshot is taken.
  fn capture_screenshot(&self);
}

//
// Recorder
//

pub struct Recorder {
  target: Box<dyn Target + Send + Sync>,

  is_periodic_reporting_enabled_flag: BoolWatch<session_replay::PeriodicScreensEnabledFlag>,
  is_periodic_reporting_enabled: bool,
  reporting_interval_rate_flag: DurationWatch<session_replay::ReportingIntervalFlag>,
  reporting_interval_rate: time::Duration,
  reporting_interval: Option<Interval>,

  is_capture_screenshots_enabled_flag: BoolWatch<session_replay::ScreenshotsEnabledFlag>,
  is_capture_screenshots_enabled: bool,
  capture_screenshot_rx: Receiver<()>,

  // A flag indicating whether the recorder is ready to take a screenshot. This flag ensures
  // that the recorder does not request a screenshot from the platform layer while it is still
  // processing a previously requested screenshot.
  // Each time a screenshot is requested, the flag is set to `false`, and it is set back to `true`
  // when the screenshot log is intercepted.
  is_ready_to_capture_screenshot: bool,
  next_screenshot_rx: Receiver<()>,

  stats: Stats,
}

impl Recorder {
  pub fn new(
    target: Box<dyn Target + Send + Sync>,
    runtime_loader: &Arc<ConfigLoader>,
    scope: &Scope,
  ) -> (Self, CaptureScreenshotHandler, ScreenshotLogInterceptor) {
    // Limit the buffer size to 1 to reduce the risk of putting too much pressure on the
    // application's main thread when dequeuing the screenshot actions from the channel and
    // passing them to the platform layer, which performs the actual screenshotting on the main
    // thread.
    let (capture_screenshot_tx, capture_screenshot_rx) = channel(1);
    let (next_screenshot_tx, next_screenshot_rx) = channel(1);

    let mut is_periodic_reporting_enabled_flag =
      session_replay::PeriodicScreensEnabledFlag::register(runtime_loader).unwrap();
    let is_periodic_reporting_enabled = is_periodic_reporting_enabled_flag.read_mark_update();

    let reporting_interval_rate = session_replay::ReportingIntervalFlag::register(runtime_loader)
      .unwrap()
      .read_mark_update();

    let mut is_capture_screenshots_enabled_flag =
      session_replay::ScreenshotsEnabledFlag::register(runtime_loader).unwrap();
    let is_capture_screenshots_enabled = is_capture_screenshots_enabled_flag.read_mark_update();

    let stats = Stats::new(scope);

    let capture_screenshot_handler = CaptureScreenshotHandler {
      capture_screenshot_tx,
      channel_full: stats.channel_full.clone(),
    };

    let screenshot_log_interceptor = ScreenshotLogInterceptor { next_screenshot_tx };

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
        is_capture_screenshots_enabled_flag,
        is_capture_screenshots_enabled,
        capture_screenshot_rx,
        is_ready_to_capture_screenshot: true,
        next_screenshot_rx,
        stats,
      },
      capture_screenshot_handler,
      screenshot_log_interceptor,
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
        () = async { self.reporting_interval.as_mut().unwrap().tick().await; },
          if self.reporting_interval.is_some() && self.is_periodic_reporting_enabled => {
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
              self.reporting_interval_rate_flag.read_mark_update(),
              false
            )
          );
        },
        Some(()) = self.capture_screenshot_rx.recv() => {
          if !self.is_capture_screenshots_enabled {
            self.stats.disabled.inc();
            log::debug!("session replay recorder ignored screenshot: capturing screenshots is disabled");
            continue;
          }

          if !self.is_ready_to_capture_screenshot {
            self.stats.not_ready.inc();
            log::debug!("session replay recorder ignored screenshot: not ready to capture screenshot");
            continue;
          }

          log::debug!("session replay recorder taking screenshot");

          self.stats.success.inc();
          self.target.capture_screenshot();

          // Reset the flag to indicate that the recorder is not ready to take a screenshot until
          // `ScreenshotLogInterceptor` intercepts a log containing a screenshot that was just requested.
          self.is_ready_to_capture_screenshot = false;
        },
        Some(()) = async { self.next_screenshot_rx.recv().await } => {
          log::debug!("session replay recorder received screenshot");

          self.stats.received.inc();
          self.is_ready_to_capture_screenshot = true;
        },
        _ = self.is_periodic_reporting_enabled_flag.changed() => {
          self.is_periodic_reporting_enabled
            = self.is_periodic_reporting_enabled_flag.read_mark_update();
        },
        _ = self.is_capture_screenshots_enabled_flag.changed() => {
          self.is_capture_screenshots_enabled
            = self.is_capture_screenshots_enabled_flag.read_mark_update();
        },
        () = &mut local_shutdown => {
          return;
        },
      }
    }
  }
}

//
// Stats
//

#[allow(clippy::struct_field_names)]
struct Stats {
  success: Counter,
  channel_full: Counter,
  disabled: Counter,
  not_ready: Counter,
  received: Counter,
}

impl Stats {
  fn new(scope: &Scope) -> Self {
    let scope = scope.scope("screenshots");
    Self {
      success: scope.counter_with_labels("requests_total", labels!("type" => "success")),
      channel_full: scope.counter_with_labels("requests_total", labels!("type" => "channel_full")),
      disabled: scope.counter_with_labels("requests_total", labels!("type" => "disabled")),
      not_ready: scope.counter_with_labels("requests_total", labels!("type" => "not_ready")),
      received: scope.counter("received_total"),
    }
  }
}

//
// CaptureScreenshotHandler
//

#[derive(Clone)]
pub struct CaptureScreenshotHandler {
  capture_screenshot_tx: Sender<()>,
  channel_full: Counter,
}

impl CaptureScreenshotHandler {
  pub fn capture_screenshot(&self) {
    // We use a channel with a capacity of one and employ `try_send` to avoid waiting for space in
    // the channel before sending a new signal. This is especially important as the method is
    // expected to be called on logging hot path.
    // The channel may become full if "capture screenshot" requests arrive faster than the platform
    // layer can process them. In such cases, new requests are ignored.
    if let Err(e) = self.capture_screenshot_tx.try_send(()) {
      self.channel_full.inc();
      log::debug!("failed to send capture screenshot signal: {:?}", e);
    }
  }
}

//
// ScreenshotLogInterceptor
//

pub struct ScreenshotLogInterceptor {
  next_screenshot_tx: Sender<()>,
}

impl bd_log_primitives::LogInterceptor for ScreenshotLogInterceptor {
  fn process(
    &self,
    _log_level: bd_log_primitives::LogLevel,
    log_type: bd_log_primitives::LogType,
    msg: &bd_log_primitives::LogMessage,
    _fields: &mut bd_log_primitives::AnnotatedLogFields,
    _matching_fields: &mut bd_log_primitives::AnnotatedLogFields,
  ) {
    if !(log_type == LogType::Replay && msg.as_str() == Some(SESSION_REPLAY_SCREENSHOT_LOG_MESSAGE))
    {
      return;
    }

    bd_client_common::error::handle_unexpected(
      self.next_screenshot_tx.try_send(()),
      "failed to send ready for next screenshot signal",
    );
  }
}
