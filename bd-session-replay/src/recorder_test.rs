// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//
// Setup
//

use crate::{Recorder, Target};
use bd_runtime::runtime::{ConfigLoader, FeatureFlag};
use bd_shutdown::ComponentShutdownTrigger;
use bd_test_helpers::runtime::{make_simple_update, ValueKind};
use bd_time::TimeDurationExt;
use std::sync::Arc;
use tempfile::TempDir;
use time::ext::NumericalDuration;
use time::Duration;
use tokio::sync::mpsc::Sender;
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

  fn create_recorder(&self, target: Box<dyn Target + Send + Sync>) -> (Recorder, Sender<()>) {
    Recorder::new(target, &self.runtime)
  }

  fn update_reporting_interval(&self, interval: Duration) {
    self.runtime.update_snapshot(&make_simple_update(vec![
      (
        bd_runtime::runtime::session_replay::PeriodicWireframesEnabledFlag::path(),
        ValueKind::Bool(true),
      ),
      (
        bd_runtime::runtime::session_replay::ReportingIntervalFlag::path(),
        #[allow(clippy::cast_possible_truncation)]
        ValueKind::Int(interval.whole_milliseconds().try_into().unwrap()),
      ),
    ]));
  }
}

//
// MockTarget
//

#[derive(Default)]
struct MockTarget {
  capture_wireframe_count: Arc<parking_lot::Mutex<usize>>,
  take_screenshot_count: Arc<parking_lot::Mutex<usize>>,
}

impl Target for MockTarget {
  fn capture_wireframe(&self) {
    *self.capture_wireframe_count.lock() += 1;
  }

  fn take_screenshot(&self) {
    *self.take_screenshot_count.lock() += 1;
  }
}

#[tokio::test]
async fn does_not_report_if_disabled() {
  let setup = Setup::new();
  setup.runtime.update_snapshot(&make_simple_update(vec![
    (
      bd_runtime::runtime::session_replay::PeriodicWireframesEnabledFlag::path(),
      ValueKind::Bool(false),
    ),
    (
      bd_runtime::runtime::session_replay::ReportingIntervalFlag::path(),
      ValueKind::Int(10),
    ),
  ]));

  let target = Box::<MockTarget>::default();
  let capture_wireframe_count = target.capture_wireframe_count.clone();
  let (mut reporter, _) = setup.create_recorder(target);

  let shutdown_trigger = ComponentShutdownTrigger::default();
  let shutdown = shutdown_trigger.make_shutdown();

  let recorder_task = tokio::task::spawn(async move {
    () = reporter.run_with_shutdown(shutdown).await;
  });

  100.milliseconds().sleep().await;

  shutdown_trigger.shutdown().await;
  assert_ok!(recorder_task.await);

  assert_eq!(0, *capture_wireframe_count.lock());
}

#[tokio::test]
async fn does_not_report_if_there_are_no_fields() {
  let setup = Setup::new();
  setup.update_reporting_interval(10.milliseconds());

  let target = Box::<MockTarget>::default();
  let capture_wireframe_count = target.capture_wireframe_count.clone();
  let (mut reporter, _) = setup.create_recorder(target);

  let shutdown_trigger = ComponentShutdownTrigger::default();
  let shutdown = shutdown_trigger.make_shutdown();

  let recorder_task = tokio::task::spawn(async move {
    () = reporter.run_with_shutdown(shutdown).await;
  });

  100.milliseconds().sleep().await;

  shutdown_trigger.shutdown().await;
  assert_ok!(recorder_task.await);

  assert!(*capture_wireframe_count.lock() > 0);
}

#[tokio::test]
async fn taking_screenshots_is_wired() {
  let setup = Setup::new();
  setup.update_reporting_interval(10.milliseconds());

  let target = Box::<MockTarget>::default();
  let take_screenshot_count = target.take_screenshot_count.clone();
  let (mut reporter, take_screenshot_tx) = setup.create_recorder(target);

  let shutdown_trigger = ComponentShutdownTrigger::default();
  let shutdown = shutdown_trigger.make_shutdown();

  let reporter_task = tokio::task::spawn(async move {
    () = reporter.run_with_shutdown(shutdown).await;
  });

  take_screenshot_tx.send(()).await.unwrap();

  100.milliseconds().sleep().await;

  // No screenshot taken since screenshot feature is disabled.
  assert!(*take_screenshot_count.lock() == 0);

  setup.runtime.update_snapshot(&make_simple_update(vec![(
    bd_runtime::runtime::session_replay::ScreenshotsEnabledFlag::path(),
    ValueKind::Bool(true),
  )]));

  100.milliseconds().sleep().await;

  take_screenshot_tx.send(()).await.unwrap();

  100.milliseconds().sleep().await;

  shutdown_trigger.shutdown().await;
  assert_ok!(reporter_task.await);

  // Screenshot taken.
  assert!(*take_screenshot_count.lock() == 1);
}
