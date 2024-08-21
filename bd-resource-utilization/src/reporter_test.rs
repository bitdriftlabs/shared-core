// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{Reporter, Target};
use bd_runtime::runtime::{ConfigLoader, FeatureFlag};
use bd_shutdown::ComponentShutdownTrigger;
use bd_test_helpers::runtime::{make_simple_update, ValueKind};
use bd_time::TimeDurationExt;
use std::sync::Arc;
use tempdir::TempDir;
use time::ext::NumericalDuration;
use time::Duration;
use tokio_test::assert_ok;

//
// Setup
//

struct Setup {
  _directory: Arc<TempDir>,
  runtime: Arc<ConfigLoader>,
}

impl Setup {
  fn new() -> Self {
    let directory = Arc::new(tempdir::TempDir::new("bd-resource-utilization").unwrap());
    let runtime = ConfigLoader::new(directory.path());
    Self {
      _directory: directory,
      runtime,
    }
  }

  fn create_reporter(&self, target: Box<dyn Target + Send + Sync>) -> Reporter {
    Reporter::new(target, &self.runtime)
  }

  fn update_reporting_interval(&self, interval: Duration) {
    self.runtime.update_snapshot(&make_simple_update(vec![
      (
        bd_runtime::runtime::resource_utilization::ResourceUtilizationEnabledFlag::path(),
        ValueKind::Bool(true),
      ),
      (
        bd_runtime::runtime::resource_utilization::ResourceUtilizationReportingIntervalFlag::path(),
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
  ticks_count: Arc<parking_lot::Mutex<i32>>,
}

impl Target for MockTarget {
  fn tick(&self) {
    *self.ticks_count.lock() += 1;
  }
}

#[tokio::test]
async fn does_not_report_if_disabled() {
  let setup = Setup::new();
  setup.runtime.update_snapshot(&make_simple_update(vec![
    (
      bd_runtime::runtime::resource_utilization::ResourceUtilizationEnabledFlag::path(),
      ValueKind::Bool(false),
    ),
    (
      bd_runtime::runtime::resource_utilization::ResourceUtilizationReportingIntervalFlag::path(),
      ValueKind::Int(10),
    ),
  ]));

  let target = Box::<MockTarget>::default();
  let ticks_count = target.ticks_count.clone();
  let mut reporter = setup.create_reporter(target);

  let shutdown_trigger = ComponentShutdownTrigger::default();
  let shutdown = shutdown_trigger.make_shutdown();

  let reporter_task = tokio::task::spawn(async move {
    () = reporter.run_with_shutdown(shutdown).await;
  });

  500.milliseconds().sleep().await;

  shutdown_trigger.shutdown().await;
  assert_ok!(reporter_task.await);

  assert_eq!(0, *ticks_count.lock());
}

#[tokio::test]
async fn does_not_report_if_there_are_no_fields() {
  let setup = Setup::new();
  setup.update_reporting_interval(10.milliseconds());

  let target = Box::<MockTarget>::default();
  let ticks_count = target.ticks_count.clone();
  let mut reporter = setup.create_reporter(target);

  let shutdown_trigger = ComponentShutdownTrigger::default();
  let shutdown = shutdown_trigger.make_shutdown();

  let reporter_task = tokio::task::spawn(async move {
    () = reporter.run_with_shutdown(shutdown).await;
  });

  500.milliseconds().sleep().await;

  shutdown_trigger.shutdown().await;
  assert_ok!(reporter_task.await);

  assert!(*ticks_count.lock() > 0);
}
