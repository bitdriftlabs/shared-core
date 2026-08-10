// shared-core - bitdrift's common client SDK
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
    Some("initial".to_string()),
    None,
    callbacks.clone(),
    Arc::new(TestTimeProvider::new(OffsetDateTime::now_utc())),
  );
  let strategy = session.strategy();

  assert_eq!("initial", strategy.session_id().unwrap());
  assert_eq!(vec!["initial"], *callbacks.0.lock());

  strategy
    .start_new_session(Some("provided".to_string()))
    .unwrap();
  assert_eq!("provided", strategy.try_current_session_id().unwrap());
  assert_eq!(vec!["initial", "provided"], *callbacks.0.lock());
}

#[tokio::test]
async fn configuration_without_timeout_uses_initial_id_after_restart() {
  let directory = TempDir::new().unwrap();
  let time_provider = Arc::new(TestTimeProvider::new(OffsetDateTime::now_utc()));
  let first = SessionStrategy::configuration(
    directory.path(),
    Some("initial".to_string()),
    None,
    Arc::new(TestCallbacks::default()),
    time_provider.clone(),
  );
  let first_strategy = first.strategy();
  assert_eq!("initial", first_strategy.session_id().unwrap());
  crate::test::flush(first_strategy, first.into_parts().1).await;

  time_provider.advance(Duration::hours(1));
  let second = SessionStrategy::configuration(
    directory.path(),
    Some("initial".to_string()),
    None,
    Arc::new(TestCallbacks::default()),
    time_provider,
  );
  assert_eq!("initial", second.strategy().session_id().unwrap());
}

#[tokio::test]
async fn configuration_without_timeout_uses_initial_id_after_timeout_configuration() {
  let directory = TempDir::new().unwrap();
  let time_provider = Arc::new(TestTimeProvider::new(OffsetDateTime::now_utc()));
  let timeout_configuration = SessionStrategy::configuration(
    directory.path(),
    Some("activity-initial".to_string()),
    Some(Duration::minutes(30)),
    Arc::new(TestCallbacks::default()),
    time_provider.clone(),
  );
  let timeout_strategy = timeout_configuration.strategy();
  assert_eq!("activity-initial", timeout_strategy.session_id().unwrap());
  crate::test::flush(timeout_strategy, timeout_configuration.into_parts().1).await;

  let no_timeout_configuration = SessionStrategy::configuration(
    directory.path(),
    Some("no-timeout-initial".to_string()),
    None,
    Arc::new(TestCallbacks::default()),
    time_provider,
  );
  assert_eq!(
    "no-timeout-initial",
    no_timeout_configuration.strategy().session_id().unwrap()
  );
}

#[tokio::test]
async fn inactivity_configuration_reuses_then_rotates_persisted_session() {
  let directory = TempDir::new().unwrap();
  let time_provider = Arc::new(TestTimeProvider::new(OffsetDateTime::now_utc()));
  let first = SessionStrategy::configuration(
    directory.path(),
    Some("initial".to_string()),
    Some(Duration::minutes(30)),
    Arc::new(TestCallbacks::default()),
    time_provider.clone(),
  );
  let first_strategy = first.strategy();
  assert_eq!("initial", first_strategy.session_id().unwrap());
  crate::test::flush(first_strategy, first.into_parts().1).await;

  time_provider.advance(Duration::minutes(5));
  let active = SessionStrategy::configuration(
    directory.path(),
    Some("ignored-while-active".to_string()),
    Some(Duration::minutes(30)),
    Arc::new(TestCallbacks::default()),
    time_provider.clone(),
  );
  assert_eq!("initial", active.strategy().session_id().unwrap());

  time_provider.advance(Duration::minutes(31));
  let expired = SessionStrategy::configuration(
    directory.path(),
    Some("ignored-after-inactivity".to_string()),
    Some(Duration::minutes(30)),
    Arc::new(TestCallbacks::default()),
    time_provider,
  );
  let session_id = expired.strategy().session_id().unwrap();
  assert_ne!("initial", session_id);
  assert_ne!("ignored-after-inactivity", session_id);
}
