// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/licenses/strict/1.0.0

//
// Setup
//

use crate::{Recorder, Target};
use bd_runtime::runtime::{ConfigLoader, FeatureFlag};
use bd_shutdown::ComponentShutdownTrigger;
use bd_test_helpers::runtime::{ValueKind, make_simple_update};
use bd_time::TimeDurationExt;
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
    Recorder::new(target, &self.runtime)
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
