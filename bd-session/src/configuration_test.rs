// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.

use super::Callbacks;
use crate::Strategy as SessionStrategy;
use bd_time::TestTimeProvider;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use tempfile::TempDir;
use time::{Duration, OffsetDateTime};

#[derive(Default)]
struct TestCallbacks(parking_lot::Mutex<Vec<String>>);

impl Callbacks for TestCallbacks {
  fn session_id_changed(&self, session_id: &str) {
    self.0.lock().push(session_id.to_string());
  }
}

#[tokio::test]
async fn seeded_and_explicit_sessions_notify_after_transition() {
  let directory = TempDir::new().unwrap();
  let callbacks = Arc::new(TestCallbacks::default());
  let session = SessionStrategy::configuration(
    directory.path(),
    Some("initial".into()),
    None,
    callbacks.clone(),
    Arc::new(TestTimeProvider::new(OffsetDateTime::now_utc())),
  );
  let strategy = session.strategy();

  assert_eq!("initial", strategy.session_id().unwrap().as_ref());
  assert_eq!(vec!["initial"], *callbacks.0.lock());

  strategy.start_new_session(Some("provided".into())).unwrap();
  assert_eq!(
    "provided",
    strategy.try_current_session_id().unwrap().as_ref()
  );
  assert_eq!(vec!["initial", "provided"], *callbacks.0.lock());
}

#[tokio::test]
async fn empty_initial_and_explicit_session_ids_are_treated_as_absent() {
  let directory = TempDir::new().unwrap();
  let callbacks = Arc::new(TestCallbacks::default());
  let session = SessionStrategy::configuration(
    directory.path(),
    Some(String::new().into()),
    None,
    callbacks.clone(),
    Arc::new(TestTimeProvider::new(OffsetDateTime::now_utc())),
  );
  let strategy = session.strategy();

  let initial_session_id = strategy.session_id().unwrap();
  assert!(!initial_session_id.is_empty());

  strategy
    .start_new_session(Some(String::new().into()))
    .unwrap();
  let explicit_session_id = strategy.try_current_session_id().unwrap();
  assert!(!explicit_session_id.is_empty());
  assert_ne!(initial_session_id, explicit_session_id);
  assert_eq!(
    vec![initial_session_id.as_ref(), explicit_session_id.as_ref()],
    callbacks
      .0
      .lock()
      .iter()
      .map(String::as_str)
      .collect::<Vec<_>>()
  );
}

#[tokio::test]
async fn configuration_without_timeout_uses_initial_id_after_restart() {
  let directory = TempDir::new().unwrap();
  let time_provider = Arc::new(TestTimeProvider::new(OffsetDateTime::now_utc()));
  let first = SessionStrategy::configuration(
    directory.path(),
    Some("initial".into()),
    None,
    Arc::new(TestCallbacks::default()),
    time_provider.clone(),
  );
  let first_strategy = first.strategy();
  assert_eq!("initial", first_strategy.session_id().unwrap().as_ref());
  crate::test::flush(first_strategy, first.into_parts().1).await;

  time_provider.advance(Duration::hours(1));
  let second = SessionStrategy::configuration(
    directory.path(),
    Some("initial".into()),
    None,
    Arc::new(TestCallbacks::default()),
    time_provider,
  );
  assert_eq!("initial", second.strategy().session_id().unwrap().as_ref());
}

#[tokio::test]
async fn configuration_without_timeout_uses_initial_id_after_timeout_configuration() {
  let directory = TempDir::new().unwrap();
  let time_provider = Arc::new(TestTimeProvider::new(OffsetDateTime::now_utc()));
  let timeout_configuration = SessionStrategy::configuration(
    directory.path(),
    Some("activity-initial".into()),
    Some(Duration::minutes(30)),
    Arc::new(TestCallbacks::default()),
    time_provider.clone(),
  );
  let timeout_strategy = timeout_configuration.strategy();
  assert_eq!(
    "activity-initial",
    timeout_strategy.session_id().unwrap().as_ref()
  );
  crate::test::flush(timeout_strategy, timeout_configuration.into_parts().1).await;

  let no_timeout_configuration = SessionStrategy::configuration(
    directory.path(),
    Some("no-timeout-initial".into()),
    None,
    Arc::new(TestCallbacks::default()),
    time_provider,
  );
  assert_eq!(
    "no-timeout-initial",
    no_timeout_configuration
      .strategy()
      .session_id()
      .unwrap()
      .as_ref()
  );
}

#[tokio::test]
async fn inactivity_configuration_reuses_then_rotates_persisted_session() {
  let directory = TempDir::new().unwrap();
  let time_provider = Arc::new(TestTimeProvider::new(OffsetDateTime::now_utc()));
  let first = SessionStrategy::configuration(
    directory.path(),
    Some("initial".into()),
    Some(Duration::minutes(30)),
    Arc::new(TestCallbacks::default()),
    time_provider.clone(),
  );
  let first_strategy = first.strategy();
  assert_eq!("initial", first_strategy.session_id().unwrap().as_ref());
  crate::test::flush(first_strategy, first.into_parts().1).await;

  time_provider.advance(Duration::minutes(5));
  let active = SessionStrategy::configuration(
    directory.path(),
    Some("ignored-while-active".into()),
    Some(Duration::minutes(30)),
    Arc::new(TestCallbacks::default()),
    time_provider.clone(),
  );
  assert_eq!("initial", active.strategy().session_id().unwrap().as_ref());

  time_provider.advance(Duration::minutes(31));
  let expired = SessionStrategy::configuration(
    directory.path(),
    Some("ignored-after-inactivity".into()),
    Some(Duration::minutes(30)),
    Arc::new(TestCallbacks::default()),
    time_provider,
  );
  let session_id = expired.strategy().session_id().unwrap();
  assert_ne!("initial", session_id.as_ref());
  assert_ne!("ignored-after-inactivity", session_id.as_ref());
}

#[tokio::test]
async fn inactivity_timeout_rotates_an_initialized_session() {
  let directory = TempDir::new().unwrap();
  let now = OffsetDateTime::now_utc();
  let time_provider = Arc::new(TestTimeProvider::new(now));
  let callbacks = Arc::new(TestCallbacks::default());
  let session = SessionStrategy::configuration(
    directory.path(),
    Some("initial".into()),
    Some(Duration::minutes(30)),
    callbacks.clone(),
    time_provider.clone(),
  );
  let strategy = session.strategy();
  let initial_session_id = strategy.session_id().unwrap();
  strategy.acknowledge_state_update(&strategy.pending_state_update().unwrap());

  time_provider.advance(Duration::minutes(31));
  let rotated_session_id = strategy.session_id().unwrap();

  assert_ne!(initial_session_id, rotated_session_id);
  assert_eq!(
    vec![initial_session_id.as_ref(), rotated_session_id.as_ref()],
    callbacks
      .0
      .lock()
      .iter()
      .map(String::as_str)
      .collect::<Vec<_>>()
  );
  assert_eq!(
    vec![rotated_session_id.as_ref()],
    strategy
      .pending_state_update()
      .unwrap()
      .request()
      .started_sessions
      .iter()
      .map(|session| session.session_id.as_str())
      .collect::<Vec<_>>()
  );
}

#[tokio::test]
async fn inactivity_timeout_rotates_an_initialized_session_when_time_moves_backward() {
  let directory = TempDir::new().unwrap();
  let now = OffsetDateTime::now_utc();
  let time_provider = Arc::new(TestTimeProvider::new(now));
  let callbacks = Arc::new(TestCallbacks::default());
  let session = SessionStrategy::configuration(
    directory.path(),
    Some("initial".into()),
    Some(Duration::minutes(30)),
    callbacks.clone(),
    time_provider.clone(),
  );
  let strategy = session.strategy();
  let initial_session_id = strategy.session_id().unwrap();
  strategy.acknowledge_state_update(&strategy.pending_state_update().unwrap());

  time_provider.set_time(now - Duration::seconds(1));
  let rotated_session_id = strategy.session_id().unwrap();

  assert_ne!(initial_session_id, rotated_session_id);
  assert_eq!(
    vec![initial_session_id.as_ref(), rotated_session_id.as_ref()],
    callbacks
      .0
      .lock()
      .iter()
      .map(String::as_str)
      .collect::<Vec<_>>()
  );
  assert_eq!(
    vec![rotated_session_id.as_ref()],
    strategy
      .pending_state_update()
      .unwrap()
      .request()
      .started_sessions
      .iter()
      .map(|session| session.session_id.as_str())
      .collect::<Vec<_>>()
  );
}
