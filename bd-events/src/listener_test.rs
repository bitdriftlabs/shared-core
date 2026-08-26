// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{Listener, ListenerTarget};
use bd_runtime::runtime::{ConfigLoader, FeatureFlag};
use bd_shutdown::ComponentShutdownTrigger;
use bd_test_helpers::runtime::{ValueKind, make_simple_update};
use std::sync::Arc;
use tempfile::TempDir;
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
    let directory = Arc::new(tempfile::TempDir::with_prefix("bd-resource-utilization").unwrap());
    let runtime = ConfigLoader::new(directory.path());
    Self {
      _directory: directory,
      runtime,
    }
  }

  fn create_listener(&self, target: Box<dyn ListenerTarget + Send + Sync>) -> Listener {
    Listener::new(target, &self.runtime)
  }

  async fn make_runtime_update(&self, events_listener_enabled: bool) {
    self
      .runtime
      .update_snapshot(make_simple_update(vec![(
        bd_runtime::runtime::platform_events::ListenerEnabledFlag::path(),
        ValueKind::Bool(events_listener_enabled),
      )]))
      .await
      .unwrap();
  }
}

//
// State
//

#[derive(Default)]
struct State {
  start_calls_count: u32,
  stop_calls_count: u32,
  is_active: Option<bool>,
}

//
// MockListenerTarget
//

#[derive(Default)]
struct MockListenerTarget {
  state: Arc<parking_lot::Mutex<State>>,
  started: Arc<tokio::sync::Notify>,
  stopped: Arc<tokio::sync::Notify>,
}

impl super::ListenerTarget for MockListenerTarget {
  fn start(&self) {
    let mut state = self.state.lock();
    *state = State {
      start_calls_count: state.start_calls_count + 1,
      stop_calls_count: state.stop_calls_count,
      is_active: Some(true),
    };
    self.started.notify_one();
  }

  fn stop(&self) {
    let mut state = self.state.lock();
    *state = State {
      start_calls_count: state.start_calls_count,
      stop_calls_count: state.stop_calls_count + 1,
      is_active: Some(false),
    };
    self.stopped.notify_one();
  }
}

#[tokio::test]
async fn starts_target_by_default() {
  let setup = Setup::new();

  let target = Box::new(MockListenerTarget::default());
  let state = target.state.clone();
  let started = target.started.clone();

  let mut listener = setup.create_listener(target);

  let shutdown_trigger = ComponentShutdownTrigger::default();
  let shutdown = shutdown_trigger.make_shutdown();

  let listener_task = tokio::task::spawn(async move {
    () = listener.run_with_shutdown(shutdown).await;
  });

  started.notified().await;

  shutdown_trigger.shutdown().await;
  assert_ok!(listener_task.await);

  assert_eq!(1, state.lock().start_calls_count);
  assert_eq!(0, state.lock().stop_calls_count);
  assert!(state.lock().is_active.unwrap());
}

#[tokio::test]
async fn switches_target_off_and_back_on() {
  let setup = Setup::new();

  let target = Box::new(MockListenerTarget::default());
  let state = target.state.clone();
  let started = target.started.clone();
  let stopped = target.stopped.clone();

  let mut listener = setup.create_listener(target);

  let shutdown_trigger = ComponentShutdownTrigger::default();
  let shutdown = shutdown_trigger.make_shutdown();

  let listener_task = tokio::task::spawn(async move {
    () = listener.run_with_shutdown(shutdown).await;
  });

  started.notified().await;

  setup.make_runtime_update(false).await;
  stopped.notified().await;

  setup.make_runtime_update(true).await;
  started.notified().await;

  shutdown_trigger.shutdown().await;
  assert_ok!(listener_task.await);

  assert_eq!(2, state.lock().start_calls_count);
  assert_eq!(1, state.lock().stop_calls_count);
  assert!(state.lock().is_active.unwrap());
}
