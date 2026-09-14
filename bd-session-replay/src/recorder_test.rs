// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//
// Setup
//

use crate::{DeviceCommandScreenshotCompletion, Recorder, RemoteScreenshotCaptureHandler, Target};
use bd_runtime::runtime::{ConfigLoader, FeatureFlag};
use bd_shutdown::ComponentShutdownTrigger;
use bd_test_helpers::runtime::{ValueKind, make_simple_update};
use bd_time::TimeDurationExt;
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tempfile::TempDir;
use time::Duration;
use time::ext::NumericalDuration;
use tokio_test::assert_ok;

struct Setup {
  _directory: Arc<TempDir>,
  runtime: Arc<ConfigLoader>,
}

impl Setup {
  fn new() -> Self {
    let directory = Arc::new(tempfile::TempDir::with_prefix("bd-resource-utilization").unwrap());
    let runtime = ConfigLoader::new(directory.path());
    Self {
      _directory: directory,
      runtime,
    }
  }

  fn create_recorder(&self, target: Box<dyn Target + Send + Sync>) -> Recorder {
    let (recorder, _) = Recorder::new(target, &self.runtime);
    recorder
  }

  async fn update_reporting_interval(&self, interval: Duration) {
    self
      .runtime
      .update_snapshot(make_simple_update(vec![
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
      .await
      .unwrap();
  }
}

//
// MockTarget
//

#[derive(Default)]
struct MockTarget {
  capture_screen_count: Arc<AtomicUsize>,
}

impl Target for MockTarget {
  fn capture_screen(&self) {
    self
      .capture_screen_count
      .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
  }
}

struct DeferredScreenshotTarget {
  completions: Mutex<Vec<DeviceCommandScreenshotCompletion>>,
  capture_started_tx: tokio::sync::mpsc::UnboundedSender<()>,
}

impl DeferredScreenshotTarget {
  fn complete_next(&self, result: Result<Vec<u8>, String>) {
    self.completions.lock().remove(0)(result);
  }
}

impl Target for DeferredScreenshotTarget {
  fn capture_screen(&self) {}

  fn capture_device_command_screenshot(&self, completion: DeviceCommandScreenshotCompletion) {
    self.completions.lock().push(completion);
    self.capture_started_tx.send(()).unwrap();
  }
}

#[tokio::test(start_paused = true)]
async fn late_screenshot_callback_does_not_release_newer_capture() {
  let (capture_started_tx, mut capture_started_rx) = tokio::sync::mpsc::unbounded_channel();
  let target = Arc::new(DeferredScreenshotTarget {
    completions: Mutex::default(),
    capture_started_tx,
  });
  let handler =
    RemoteScreenshotCaptureHandler::with_timeout(target.clone(), std::time::Duration::from_secs(1));

  let first_handler = handler.clone();
  let first_capture = tokio::spawn(async move { first_handler.capture().await });
  capture_started_rx.recv().await.unwrap();
  tokio::time::advance(std::time::Duration::from_secs(1)).await;
  assert_eq!(
    first_capture.await.unwrap(),
    Err("remote screenshot capture timed out".to_string())
  );

  let second_handler = handler.clone();
  let second_capture = tokio::spawn(async move { second_handler.capture().await });
  capture_started_rx.recv().await.unwrap();

  target.complete_next(Ok(vec![1]));
  assert_eq!(
    handler.capture().await,
    Err("a remote screenshot capture is already in progress".to_string())
  );
  target.complete_next(Ok(vec![2]));
  assert_eq!(second_capture.await.unwrap(), Ok(vec![2]));
}

#[tokio::test]
async fn cancelled_screenshot_capture_releases_slot() {
  let (capture_started_tx, mut capture_started_rx) = tokio::sync::mpsc::unbounded_channel();
  let target = Arc::new(DeferredScreenshotTarget {
    completions: Mutex::default(),
    capture_started_tx,
  });
  let handler =
    RemoteScreenshotCaptureHandler::with_timeout(target.clone(), std::time::Duration::from_secs(1));

  let first_handler = handler.clone();
  let first_capture = tokio::spawn(async move { first_handler.capture().await });
  capture_started_rx.recv().await.unwrap();
  first_capture.abort();
  assert!(first_capture.await.unwrap_err().is_cancelled());

  let second_handler = handler.clone();
  let second_capture = tokio::spawn(async move { second_handler.capture().await });
  capture_started_rx.recv().await.unwrap();
  target.complete_next(Ok(vec![1]));
  target.complete_next(Ok(vec![2]));
  assert_eq!(second_capture.await.unwrap(), Ok(vec![2]));
}

#[tokio::test]
async fn does_not_report_if_disabled() {
  let setup = Setup::new();
  setup
    .runtime
    .update_snapshot(make_simple_update(vec![
      (
        bd_runtime::runtime::session_replay::PeriodicScreensEnabledFlag::path(),
        ValueKind::Bool(false),
      ),
      (
        bd_runtime::runtime::session_replay::ReportingIntervalFlag::path(),
        ValueKind::Int(10),
      ),
    ]))
    .await
    .unwrap();

  let target = Box::<MockTarget>::default();
  let capture_screen_count = target.capture_screen_count.clone();
  let mut reporter = setup.create_recorder(target);

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
  let mut reporter = setup.create_recorder(target);

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
