// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{Callbacks, UUIDCallbacks};
use crate::Strategy;
use pretty_assertions::assert_eq;
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use tempfile::TempDir;
use uuid::Uuid;

struct NoopWake;

impl Wake for NoopWake {
  fn wake(self: Arc<Self>) {}
}

fn expect_ready<T>(future: impl Future<Output = T>) -> T {
  let waker = Waker::from(Arc::new(NoopWake));
  let mut context = Context::from_waker(&waker);
  let mut future = pin!(future);
  match future.as_mut().poll(&mut context) {
    Poll::Ready(value) => value,
    Poll::Pending => panic!("future unexpectedly pending"),
  }
}

#[derive(Default)]
struct MockCallbacks {
  generated_session_ids: parking_lot::Mutex<Vec<String>>,
}

impl Callbacks for MockCallbacks {
  fn generate_session_id(&self) -> anyhow::Result<String> {
    let id = Uuid::new_v4().to_string();
    self.generated_session_ids.lock().push(id.clone());
    Ok(id)
  }
}

#[tokio::test]
async fn test_session_id() {
  let sdk_directory = TempDir::new().unwrap();
  let callbacks = Arc::new(MockCallbacks::default());
  let strategy = Strategy::fixed(sdk_directory.path(), callbacks.clone());

  let session_id = strategy.session_id().await.unwrap();
  let previous_session_id = strategy.previous_process_session_id();

  assert_eq!(1, callbacks.generated_session_ids.lock().len());
  assert_eq!(callbacks.generated_session_ids.lock()[0], session_id);
  assert_eq!(None, previous_session_id);
}

#[tokio::test]
async fn test_start_new_session() {
  let sdk_directory = TempDir::new().unwrap();
  let callbacks = Arc::new(MockCallbacks::default());
  let strategy = Strategy::fixed(sdk_directory.path(), callbacks.clone());

  let session_id = strategy.session_id().await.unwrap();

  assert_eq!(1, callbacks.generated_session_ids.lock().len());
  assert_eq!(callbacks.generated_session_ids.lock()[0], session_id);

  strategy.start_new_session().await;
  let next_session_id = strategy.session_id().await.unwrap();

  assert_eq!(next_session_id, strategy.session_id().await.unwrap());
  assert_eq!(2, callbacks.generated_session_ids.lock().len());
  assert_eq!(callbacks.generated_session_ids.lock()[1], next_session_id);
  assert_eq!(None, strategy.previous_process_session_id());
}

#[tokio::test]
async fn test_previous_process_session_id() {
  let sdk_directory = TempDir::new().unwrap();
  let strategy = Strategy::fixed(sdk_directory.path(), Arc::new(UUIDCallbacks));
  strategy.session_id().await.unwrap();
  strategy.start_new_session().await;
  let session_id = strategy.session_id().await.unwrap();

  assert!(strategy.previous_process_session_id().is_none());

  let strategy = Strategy::fixed(sdk_directory.path(), Arc::new(UUIDCallbacks));
  assert_eq!(Some(session_id), strategy.previous_process_session_id());
}

#[derive(Default)]
struct ReEntryCallbacks {
  session_strategy: parking_lot::Mutex<Option<Arc<Strategy>>>,
  inner_session_id_error: parking_lot::Mutex<Option<String>>,
}

impl Callbacks for ReEntryCallbacks {
  fn generate_session_id(&self) -> anyhow::Result<String> {
    if let Some(strategy) = &*self.session_strategy.lock() {
      expect_ready(strategy.start_new_session());
      let error = expect_ready(strategy.session_id()).unwrap_err();
      *self.inner_session_id_error.lock() = Some(error.to_string());
      anyhow::bail!(error);
    }

    Ok("should not happen".to_string())
  }
}

#[tokio::test]
async fn handles_re_entry() {
  let sdk_directory = TempDir::new().unwrap();

  let callbacks = Arc::new(ReEntryCallbacks::default());
  let strategy = Arc::new(Strategy::fixed(sdk_directory.path(), callbacks.clone()));

  callbacks.session_strategy.lock().replace(strategy.clone());

  // Confirm that it doesn't deadlock and returns a reasonable ID.
  let session_id = strategy.session_id().await.unwrap();
  assert_eq!(36, session_id.len());
  assert_eq!(
    Some("session_id cannot be called from within a session callback".to_string()),
    callbacks.inner_session_id_error.lock().clone()
  );

  // Confirm that it doesn't deadlock and returns a reasonable ID.
  strategy.start_new_session().await;
  let new_session_id = strategy.session_id().await.unwrap();
  assert_eq!(36, new_session_id.len());

  assert_ne!(session_id, new_session_id);
}
