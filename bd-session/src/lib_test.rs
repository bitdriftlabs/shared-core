// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::test::start_new_session;
use super::{PendingStateUpdate, Strategy};
use crate::persistence::{BackendState, PersistedSessionState, Store};
use crate::{activity_based, fixed};
use bd_proto::protos::client::api::StateUpdateRequest;
use bd_time::TestTimeProvider;
use pretty_assertions::assert_eq;
use std::collections::VecDeque;
use std::sync::Arc;
use tempfile::TempDir;
use time::{Duration, OffsetDateTime};

//
// FixedCallbacks
//

struct FixedCallbacks {
  session_ids: parking_lot::Mutex<VecDeque<String>>,
}

impl FixedCallbacks {
  fn new(session_ids: &[&str]) -> Self {
    Self {
      session_ids: parking_lot::Mutex::new(
        session_ids
          .iter()
          .map(|session_id| (*session_id).to_string())
          .collect(),
      ),
    }
  }
}

impl fixed::Callbacks for FixedCallbacks {
  fn generate_session_id(&self) -> anyhow::Result<String> {
    self
      .session_ids
      .lock()
      .pop_front()
      .ok_or_else(|| anyhow::anyhow!("missing test session id"))
  }
}

struct ActivityCallbacks;

impl activity_based::Callbacks for ActivityCallbacks {
  fn session_id_changed(&self, _session_id: &str) {}
}

fn fixed_strategy(sdk_directory: &TempDir, session_ids: &[&str]) -> Strategy {
  Strategy::fixed(
    sdk_directory.path(),
    Arc::new(FixedCallbacks::new(session_ids)),
  )
}

fn activity_strategy(sdk_directory: &TempDir, now: OffsetDateTime) -> Strategy {
  Strategy::activity_based(
    sdk_directory.path(),
    Duration::minutes(30),
    Arc::new(ActivityCallbacks),
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
async fn handshake_synthesizes_current_session_after_pending_queue_is_acked() {
  let sdk_directory = TempDir::new().unwrap();
  let strategy = fixed_strategy(&sdk_directory, &["session-1"]);

  let session_id = strategy.session_id().await.unwrap();
  let pending = strategy.pending_state_update().await.unwrap();

  assert_eq!(
    vec![session_id.clone()],
    started_session_ids(pending.request())
  );

  strategy.acknowledge_state_update(&pending).await;

  assert!(strategy.pending_state_update().await.is_none());

  let handshake = strategy.handshake_state_update().await;
  assert_eq!(vec![session_id], started_session_ids(handshake.request()));
  assert!(handshake.started_sessions.is_empty());
}

#[tokio::test]
async fn acknowledge_state_update_ignores_non_prefix_updates() {
  let sdk_directory = TempDir::new().unwrap();
  let strategy = fixed_strategy(&sdk_directory, &["session-1", "session-2"]);

  strategy.session_id().await.unwrap();
  start_new_session(&strategy).await;

  let pending = strategy.pending_state_update().await.unwrap();
  assert_eq!(
    vec!["session-1".to_string(), "session-2".to_string()],
    started_session_ids(pending.request())
  );

  let fake_update = PendingStateUpdate {
    request: StateUpdateRequest::default(),
    started_sessions: vec![pending.started_sessions[1].clone()],
  };

  strategy.acknowledge_state_update(&fake_update).await;

  let still_pending = strategy.pending_state_update().await.unwrap();
  assert_eq!(
    vec!["session-1".to_string(), "session-2".to_string()],
    started_session_ids(still_pending.request())
  );
}

#[tokio::test]
async fn subscribe_updates_changes_on_initialization_and_acknowledgement() {
  let sdk_directory = TempDir::new().unwrap();
  let strategy = fixed_strategy(&sdk_directory, &["session-1"]);
  let updates = strategy.subscribe_updates();

  assert_eq!(0, *updates.borrow());

  let pending = strategy.pending_state_update().await.unwrap();
  assert_eq!(1, *updates.borrow());

  strategy.acknowledge_state_update(&pending).await;
  assert_eq!(2, *updates.borrow());
}

#[tokio::test]
async fn restart_rebuilds_pending_queue_from_persisted_state() {
  let sdk_directory = TempDir::new().unwrap();

  let first_strategy = fixed_strategy(&sdk_directory, &["session-1"]);
  let first_session_id = first_strategy.session_id().await.unwrap();
  drop(first_strategy);

  let restarted_strategy = fixed_strategy(&sdk_directory, &["session-2"]);
  let pending = restarted_strategy.pending_state_update().await.unwrap();

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
  let strategy = fixed_strategy(&sdk_directory, &["session-1"]);

  strategy.session_id().await.unwrap();

  let handshake = strategy.handshake_state_update().await;
  assert_eq!(
    vec!["session-1".to_string()],
    started_session_ids(handshake.request())
  );
  assert_eq!(1, handshake.started_sessions.len());
}

#[tokio::test]
async fn switching_from_fixed_to_activity_resyncs_persisted_backend() {
  let sdk_directory = TempDir::new().unwrap();

  let first_strategy = fixed_strategy(&sdk_directory, &["fixed-session"]);
  let first_session_id = first_strategy.session_id().await.unwrap();
  drop(first_strategy);

  let restarted_strategy = activity_strategy(&sdk_directory, OffsetDateTime::now_utc());
  let pending = restarted_strategy.pending_state_update().await.unwrap();
  let restarted_session_id = restarted_strategy.try_current_session_id().unwrap();

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
    persisted_state(&sdk_directory).backend,
    BackendState::ActivityBased { .. }
  ));
}

#[tokio::test]
async fn switching_from_activity_to_fixed_resyncs_persisted_backend() {
  let sdk_directory = TempDir::new().unwrap();

  let first_strategy = activity_strategy(&sdk_directory, OffsetDateTime::now_utc());
  let first_session_id = first_strategy.session_id().await.unwrap();
  drop(first_strategy);

  let restarted_strategy = fixed_strategy(&sdk_directory, &["fixed-session"]);
  let pending = restarted_strategy.pending_state_update().await.unwrap();
  let restarted_session_id = restarted_strategy.try_current_session_id().unwrap();

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
    persisted_state(&sdk_directory).backend,
    BackendState::Fixed
  ));
}
