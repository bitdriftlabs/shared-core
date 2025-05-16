// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//
// Setup
//

use crate::{
  CaptureScreenshotHandler,
  Recorder,
  ScreenshotLogInterceptor,
  Target,
  SESSION_REPLAY_SCREENSHOT_LOG_MESSAGE,
};
use bd_client_stats_store::test::StatsHelper;
use bd_client_stats_store::Collector;
use bd_log_primitives::{log_level, LogInterceptor, LogType};
use bd_runtime::runtime::{ConfigLoader, FeatureFlag};
use bd_shutdown::ComponentShutdownTrigger;
use bd_stats_common::labels;
use bd_test_helpers::runtime::{make_simple_update, ValueKind};
use bd_time::TimeDurationExt;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tempfile::TempDir;
use time::ext::NumericalDuration;
use time::Duration;
use tokio_test::assert_ok;

struct Setup {
  _directory: Arc<TempDir>,
  runtime: Arc<ConfigLoader>,
  collector: Collector,
}

impl Setup {
  fn new() -> Self {
    let directory = Arc::new(tempfile::TempDir::with_prefix("bd-resource-utilization").unwrap());
    let runtime = ConfigLoader::new(directory.path());
    Self {
      _directory: directory,
      runtime,
      collector: Collector::default(),
    }
  }

  fn create_recorder(
    &self,
    target: Box<dyn Target + Send + Sync>,
  ) -> (Recorder, CaptureScreenshotHandler, ScreenshotLogInterceptor) {
    Recorder::new(target, &self.runtime, &self.collector.scope(""))
  }

  async fn update_reporting_interval(&self, interval: Duration) {
    self
      .runtime
      .update_snapshot(&make_simple_update(vec![
        (
          bd_runtime::runtime::session_replay::PeriodicScreensEnabledFlag::path(),
          ValueKind::Bool(true),
        ),
        (
          bd_runtime::runtime::session_replay::ReportingIntervalFlag::path(),
          #[allow(clippy::cast_possible_truncation)]
          ValueKind::Int(interval.whole_milliseconds().try_into().unwrap()),
        ),
      ]))
      .await;
  }
}

//
// MockTarget
//

#[derive(Default)]
struct MockTarget {
  capture_screen_count: Arc<AtomicUsize>,
  capture_screenshot_count: Arc<AtomicUsize>,
}

impl Target for MockTarget {
  fn capture_screen(&self) {
    self
      .capture_screen_count
      .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
  }

  fn capture_screenshot(&self) {
    self
      .capture_screenshot_count
      .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
  }
}

#[tokio::test]
async fn does_not_report_if_disabled() {
  let setup = Setup::new();
  setup
    .runtime
    .update_snapshot(&make_simple_update(vec![
      (
        bd_runtime::runtime::session_replay::PeriodicScreensEnabledFlag::path(),
        ValueKind::Bool(false),
      ),
      (
        bd_runtime::runtime::session_replay::ReportingIntervalFlag::path(),
        ValueKind::Int(10),
      ),
    ]))
    .await;

  let target = Box::<MockTarget>::default();
  let capture_screen_count = target.capture_screen_count.clone();
  let (mut reporter, ..) = setup.create_recorder(target);

  let shutdown_trigger = ComponentShutdownTrigger::default();
  let shutdown = shutdown_trigger.make_shutdown();

  let recorder_task = tokio::task::spawn(async move {
    () = reporter.run_with_shutdown(shutdown).await;
  });

  100.milliseconds().sleep().await;

  shutdown_trigger.shutdown().await;
  assert_ok!(recorder_task.await);

  assert_eq!(
    0,
    capture_screen_count.load(std::sync::atomic::Ordering::Relaxed)
  );
}

#[tokio::test]
async fn does_not_report_if_there_are_no_fields() {
  let setup = Setup::new();
  setup.update_reporting_interval(10.milliseconds()).await;

  let target = Box::<MockTarget>::default();
  let capture_screen_count = target.capture_screen_count.clone();
  let (mut reporter, ..) = setup.create_recorder(target);

  let shutdown_trigger = ComponentShutdownTrigger::default();
  let shutdown = shutdown_trigger.make_shutdown();

  let recorder_task = tokio::task::spawn(async move {
    () = reporter.run_with_shutdown(shutdown).await;
  });

  100.milliseconds().sleep().await;

  shutdown_trigger.shutdown().await;
  assert_ok!(recorder_task.await);

  assert!(capture_screen_count.load(std::sync::atomic::Ordering::Relaxed) > 0);
}

#[tokio::test]
async fn taking_screenshots_is_wired_up() {
  let setup = Setup::new();

  let target: Box<MockTarget> = Box::<MockTarget>::default();
  let capture_screenshot_count = target.capture_screenshot_count.clone();
  let (mut recorder, take_screenshot_handler, _) = setup.create_recorder(target);

  let shutdown_trigger = ComponentShutdownTrigger::default();
  let shutdown = shutdown_trigger.make_shutdown();

  let recorder_task = tokio::task::spawn(async move {
    () = recorder.run_with_shutdown(shutdown).await;
  });

  take_screenshot_handler.capture_screenshot();

  100.milliseconds().sleep().await;

  // No screenshot taken since screenshot feature is disabled.
  assert_eq!(
    0,
    capture_screenshot_count.load(std::sync::atomic::Ordering::Relaxed)
  );

  setup
    .runtime
    .update_snapshot(&make_simple_update(vec![(
      bd_runtime::runtime::session_replay::ScreenshotsEnabledFlag::path(),
      ValueKind::Bool(true),
    )]))
    .await;

  100.milliseconds().sleep().await;

  take_screenshot_handler.capture_screenshot();

  100.milliseconds().sleep().await;

  shutdown_trigger.shutdown().await;
  assert_ok!(recorder_task.await);

  // Screenshot taken.
  assert_eq!(
    1,
    capture_screenshot_count.load(std::sync::atomic::Ordering::Relaxed)
  );
}

#[tokio::test]
async fn limits_the_number_of_concurrent_screenshots_to_one() {
  let setup = Setup::new();

  setup
    .runtime
    .update_snapshot(&make_simple_update(vec![(
      bd_runtime::runtime::session_replay::ScreenshotsEnabledFlag::path(),
      ValueKind::Bool(true),
    )]))
    .await;

  let target = Box::<MockTarget>::default();
  let capture_screenshot_count = target.capture_screenshot_count.clone();
  let (mut recorder, take_screenshot_handler, screenshot_log_interceptor) =
    setup.create_recorder(target);

  let shutdown_trigger = ComponentShutdownTrigger::default();
  let shutdown = shutdown_trigger.make_shutdown();

  let recorder_task = tokio::task::spawn(async move {
    recorder.run_with_shutdown(shutdown.clone()).await;
  });

  // First request to take a screenshot goes through successfully.
  take_screenshot_handler.capture_screenshot();
  // The next two requests fail because the channel is full, as we did not allow the recorder to
  // process signals from the channel.
  take_screenshot_handler.capture_screenshot();
  take_screenshot_handler.capture_screenshot();

  setup.collector.assert_counter_eq(
    2,
    "screenshots:requests_total",
    labels!("type" => "channel_full"),
  );

  300.milliseconds().sleep().await;

  // One screenshot is taken. Other requests are ignored, as only one concurrent screenshot
  // operation is allowed.
  assert_eq!(
    1,
    capture_screenshot_count.load(std::sync::atomic::Ordering::Relaxed)
  );

  // Simulate a screenshot log.
  screenshot_log_interceptor.process(
    log_level::INFO,
    LogType::Replay,
    &SESSION_REPLAY_SCREENSHOT_LOG_MESSAGE.into(),
    &mut [].into(),
    &mut [].into(),
  );

  // Request taking a screenshot goes through successfully as the previous screenshot log was
  // received.
  300.milliseconds().sleep().await;
  take_screenshot_handler.capture_screenshot();

  // Request taking a screenshot fails as only one concurrent take screenshot operation is allowed.
  300.milliseconds().sleep().await;
  take_screenshot_handler.capture_screenshot();

  // Request taking a screenshot fails again as only one concurrent take screenshot operation is
  // allowed.
  300.milliseconds().sleep().await;
  take_screenshot_handler.capture_screenshot();

  300.milliseconds().sleep().await;
  setup.collector.assert_counter_eq(
    2,
    "screenshots:requests_total",
    labels!("type" => "channel_full"),
  );
  setup.collector.assert_counter_eq(
    2,
    "screenshots:requests_total",
    labels!("type" => "not_ready"),
  );

  screenshot_log_interceptor.process(
    log_level::INFO,
    LogType::Replay,
    &SESSION_REPLAY_SCREENSHOT_LOG_MESSAGE.into(),
    &mut [].into(),
    &mut [].into(),
  );

  300.milliseconds().sleep().await;

  // Another screenshot is taken. Other requests are ignored, as only one concurrent screenshot
  // operation is allowed.
  assert_eq!(
    2,
    capture_screenshot_count.load(std::sync::atomic::Ordering::Relaxed)
  );

  setup.collector.assert_counter_eq(
    2,
    "screenshots:requests_total",
    labels!("type" => "success"),
  );
  setup
    .collector
    .assert_counter_eq(2, "screenshots:received_total", labels!());

  shutdown_trigger.shutdown().await;
  assert_ok!(recorder_task.await);
}
