// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::test::flush;
use super::{PendingStateUpdate, Strategy, StrategyWithWorker};
use crate::persistence::{ActivityState, PersistedSessionState, Store};
use crate::configuration;
use bd_proto::protos::client::api::StateUpdateRequest;
use bd_time::TestTimeProvider;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use tempfile::TempDir;
use time::{Duration, OffsetDateTime};

//
// TestCallbacks
//

struct TestCallbacks;

impl configuration::Callbacks for TestCallbacks {
  fn session_id_changed(&self, _session_id: &str) {}
}

fn no_timeout_strategy(sdk_directory: &TempDir, initial_session_id: &str) -> StrategyWithWorker {
  Strategy::configuration(
    sdk_directory.path(),
    Some(initial_session_id.to_string()),
    None,
    Arc::new(TestCallbacks),
    Arc::new(TestTimeProvider::new(OffsetDateTime::now_utc())),
  )
}

fn inactivity_timeout_strategy(sdk_directory: &TempDir, now: OffsetDateTime) -> StrategyWithWorker {
  Strategy::configuration(
    sdk_directory.path(),
    None,
    Some(Duration::minutes(30)),
    Arc::new(TestCallbacks),
    Arc::new(TestTimeProvider::new(now)),
  )
}

fn persisted_state(sdk_directory: &TempDir) -> PersistedSessionState {
  Store::new(sdk_directory.path()).load_state().unwrap()
}

fn started_session_ids(request: &StateUpdateRequest) -> Vec<String> {
  request
    .started_sessions
    .iter()
    .map(|session| session.session_id.clone())
    .collect()
}

#[tokio::test]
async fn persistence_flusher_coalesces_to_latest_state_on_shutdown() {
  let sdk_directory = TempDir::new().unwrap();
  let StrategyWithWorker {
    strategy,
    persistence_worker: worker,
  } = no_timeout_strategy(&sdk_directory, "session-1");
  let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
  let flusher = tokio::spawn(worker.run(
    async move {
      let _ignored = shutdown_rx.await;
    },
    || {},
  ));

  let first_session_id = strategy.session_id().unwrap();
  strategy.start_new_session(Some("session-2".to_string())).unwrap();
  let second_session_id = strategy.session_id().unwrap();

  let _ignored = shutdown_tx.send(());
  flusher.await.unwrap();

  let persisted = persisted_state(&sdk_directory);
  assert_eq!(second_session_id, persisted.current_session_id);
  assert_eq!(
    vec![first_session_id, second_session_id],
    Store::new(sdk_directory.path())
      .load_pending_started_sessions()
      .into_iter()
      .map(|started| started.session_id)
      .collect::<Vec<_>>()
  );
}

#[tokio::test]
async fn flush_request_waits_for_persistence_worker() {
  let sdk_directory = TempDir::new().unwrap();
  let StrategyWithWorker {
    strategy,
    persistence_worker: worker,
  } = no_timeout_strategy(&sdk_directory, "session-1");
  let session_id = strategy.session_id().unwrap();

  let flush_strategy = strategy.clone();
  let flush = tokio::spawn(async move { flush_strategy.flush().await });
  tokio::task::yield_now().await;
  assert!(!flush.is_finished());

  let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
  let flusher = tokio::spawn(worker.run(
    async move {
      let _ignored = shutdown_rx.await;
    },
    || {},
  ));

  flush.await.unwrap();
  let _ignored = shutdown_tx.send(());
  flusher.await.unwrap();

  assert_eq!(
    session_id,
    persisted_state(&sdk_directory).current_session_id
  );
}

#[tokio::test]
async fn flush_retries_persistence_after_a_failed_write() {
  let sdk_directory = TempDir::new().unwrap();
  std::fs::write(sdk_directory.path().join("state"), "not a directory").unwrap();
  let StrategyWithWorker {
    strategy,
    persistence_worker: worker,
  } = no_timeout_strategy(&sdk_directory, "session-1");
  let session_id = strategy.session_id().unwrap();

  let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
  let flusher = tokio::spawn(worker.run(
    async move {
      let _ignored = shutdown_rx.await;
    },
    || {},
  ));

  strategy.flush().await;
  assert!(Store::new(sdk_directory.path()).load_state().is_none());

  std::fs::remove_file(sdk_directory.path().join("state")).unwrap();
  strategy.flush().await;

  let _ignored = shutdown_tx.send(());
  flusher.await.unwrap();

  assert_eq!(
    session_id,
    persisted_state(&sdk_directory).current_session_id
  );
}

#[tokio::test]
async fn handshake_synthesizes_current_session_after_pending_queue_is_acked() {
  let sdk_directory = TempDir::new().unwrap();
  let StrategyWithWorker { strategy, .. } = no_timeout_strategy(&sdk_directory, "session-1");

  let session_id = strategy.session_id().unwrap();
  let pending = strategy.pending_state_update().unwrap();

  assert_eq!(
    vec![session_id.clone()],
    started_session_ids(pending.request())
  );

  strategy.acknowledge_state_update(&pending);

  assert!(strategy.pending_state_update().is_none());

  let handshake = strategy.handshake_state_update();
  assert_eq!(vec![session_id], started_session_ids(handshake.request()));
  assert!(handshake.started_sessions.is_empty());
}

#[tokio::test]
async fn acknowledge_state_update_ignores_non_prefix_updates() {
  let sdk_directory = TempDir::new().unwrap();
  let StrategyWithWorker { strategy, .. } =
    no_timeout_strategy(&sdk_directory, "session-1");

  strategy.session_id().unwrap();
  strategy.start_new_session(Some("session-2".to_string())).unwrap();

  let pending = strategy.pending_state_update().unwrap();
  assert_eq!(
    vec!["session-1".to_string(), "session-2".to_string()],
    started_session_ids(pending.request())
  );

  let fake_update = PendingStateUpdate {
    request: StateUpdateRequest::default(),
    started_sessions: vec![pending.started_sessions[1].clone()],
  };

  strategy.acknowledge_state_update(&fake_update);

  let still_pending = strategy.pending_state_update().unwrap();
  assert_eq!(
    vec!["session-1".to_string(), "session-2".to_string()],
    started_session_ids(still_pending.request())
  );
}

#[tokio::test]
async fn subscribe_updates_changes_on_initialization_and_acknowledgement() {
  let sdk_directory = TempDir::new().unwrap();
  let StrategyWithWorker { strategy, .. } = no_timeout_strategy(&sdk_directory, "session-1");
  let updates = strategy.subscribe_updates();

  assert_eq!(0, *updates.borrow());

  let pending = strategy.pending_state_update().unwrap();
  assert_eq!(1, *updates.borrow());

  strategy.acknowledge_state_update(&pending);
  assert_eq!(2, *updates.borrow());
}

#[tokio::test]
async fn restart_rebuilds_pending_queue_from_persisted_state() {
  let sdk_directory = TempDir::new().unwrap();

  let StrategyWithWorker {
    strategy: first_strategy,
    persistence_worker: first_worker,
  } = no_timeout_strategy(&sdk_directory, "session-1");
  let first_session_id = first_strategy.session_id().unwrap();
  flush(first_strategy.clone(), first_worker).await;
  drop(first_strategy);

  let StrategyWithWorker {
    strategy: restarted_strategy,
    ..
  } = no_timeout_strategy(&sdk_directory, "session-2");
  let pending = restarted_strategy.pending_state_update().unwrap();

  assert_eq!(
    vec![first_session_id, "session-2".to_string()],
    started_session_ids(pending.request())
  );
  assert_eq!(
    Some("session-1".to_string()),
    restarted_strategy.previous_process_session_id()
  );
}

#[tokio::test]
async fn handshake_does_not_duplicate_current_session_when_queue_already_contains_it() {
  let sdk_directory = TempDir::new().unwrap();
  let StrategyWithWorker { strategy, .. } = no_timeout_strategy(&sdk_directory, "session-1");

  strategy.session_id().unwrap();

  let handshake = strategy.handshake_state_update();
  assert_eq!(
    vec!["session-1".to_string()],
    started_session_ids(handshake.request())
  );
  assert_eq!(1, handshake.started_sessions.len());
}

#[tokio::test]
async fn enabling_inactivity_timeout_resyncs_persisted_state() {
  let sdk_directory = TempDir::new().unwrap();

  let StrategyWithWorker {
    strategy: first_strategy,
    persistence_worker: first_worker,
  } = no_timeout_strategy(&sdk_directory, "no-timeout-session");
  let first_session_id = first_strategy.session_id().unwrap();
  flush(first_strategy.clone(), first_worker).await;
  drop(first_strategy);

  let StrategyWithWorker {
    strategy: restarted_strategy,
    persistence_worker: restarted_worker,
  } = inactivity_timeout_strategy(&sdk_directory, OffsetDateTime::now_utc());
  let pending = restarted_strategy.pending_state_update().unwrap();
  let restarted_session_id = restarted_strategy.try_current_session_id().unwrap();
  flush(restarted_strategy.clone(), restarted_worker).await;

  assert_ne!(first_session_id, restarted_session_id);
  assert_eq!(
    Some(first_session_id.clone()),
    restarted_strategy.previous_process_session_id()
  );
  assert_eq!(
    vec![first_session_id.clone(), restarted_session_id],
    started_session_ids(pending.request())
  );
  assert!(matches!(
    persisted_state(&sdk_directory).activity_state,
    ActivityState::InactivityTimeout { .. }
  ));
}

#[tokio::test]
async fn disabling_inactivity_timeout_resyncs_persisted_state() {
  let sdk_directory = TempDir::new().unwrap();

  let StrategyWithWorker {
    strategy: first_strategy,
    persistence_worker: first_worker,
  } = inactivity_timeout_strategy(&sdk_directory, OffsetDateTime::now_utc());
  let first_session_id = first_strategy.session_id().unwrap();
  flush(first_strategy.clone(), first_worker).await;
  drop(first_strategy);

  let StrategyWithWorker {
    strategy: restarted_strategy,
    persistence_worker: restarted_worker,
  } = no_timeout_strategy(&sdk_directory, "no-timeout-session");
  let pending = restarted_strategy.pending_state_update().unwrap();
  let restarted_session_id = restarted_strategy.try_current_session_id().unwrap();
  flush(restarted_strategy.clone(), restarted_worker).await;

  assert_ne!(first_session_id, restarted_session_id);
  assert_eq!(
    Some(first_session_id.clone()),
    restarted_strategy.previous_process_session_id()
  );
  assert_eq!(
    vec![first_session_id.clone(), restarted_session_id],
    started_session_ids(pending.request())
  );
  assert!(matches!(
    persisted_state(&sdk_directory).activity_state,
    ActivityState::NoInactivityTimeout
  ));
}
