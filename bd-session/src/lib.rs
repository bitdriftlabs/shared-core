// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Session management is split into three layers:
//! 1. A backend-specific strategy decides when a session ID should change.
//! 2. This module persists the current session plus a durable queue of started sessions that still
//!    need to be announced to the server.
//! 3. The API layer consumes `PendingStateUpdate` values from that durable queue and acknowledges
//!    them once the server has accepted the update.
//!
//! The important consequence is that session creation and session announcement are intentionally
//! decoupled. A process can rotate or create a session locally, crash, and still reconstruct the
//! correct state update to send on the next startup.

#![deny(
  clippy::expect_used,
  clippy::panic,
  clippy::todo,
  clippy::unimplemented,
  clippy::unreachable,
  clippy::unwrap_used
)]

pub mod activity_based;
pub mod fixed;
mod persistence;

#[cfg(test)]
#[path = "./lib_test.rs"]
mod lib_test;

#[cfg(test)]
#[ctor::ctor]
fn test_global_init() {
  bd_test_helpers::test_global_init();
}

use bd_proto::protos::client::api::StateUpdateRequest;
use bd_proto::protos::client::api::state_update_request::StartedSession;
use bd_time::{OffsetDateTimeExt as _, TimeProvider};
use persistence::{PersistedSessionState, StartedSessionRecord, Store};
use std::cell::Cell;
use std::path::Path;
use std::sync::Arc;
use thread_local::ThreadLocal;
use time::OffsetDateTime;
use tokio::sync::watch;

//
// LoadedState
//

/// In-memory session state combines the persisted wire format with transient bookkeeping that is
/// only meaningful for the current process.
#[derive(Clone, Debug)]
pub(crate) struct LoadedState {
  persisted: PersistedSessionState,
  pending_started_sessions: Vec<StartedSessionRecord>,
  last_activity_write: Option<OffsetDateTime>,
}

//
// Mutation
//

#[derive(Clone, Debug, Default)]
pub(crate) struct Mutation {
  persist_state: bool,
  persist_pending: bool,
  callback: Option<DeferredCallback>,
}

//
// Initialization
//

#[derive(Clone, Debug)]
pub(crate) struct Initialization {
  state: LoadedState,
  mutation: Mutation,
}

//
// DeferredCallback
//

#[derive(Clone, Debug)]
pub(crate) enum DeferredCallback {
  ActivitySessionChanged(String),
}

//
// PendingStateUpdate
//

#[derive(Clone, Debug)]
pub struct PendingStateUpdate {
  request: StateUpdateRequest,
  started_sessions: Vec<StartedSessionRecord>,
}

impl PendingStateUpdate {
  #[must_use]
  pub const fn request(&self) -> &StateUpdateRequest {
    &self.request
  }
}

//
// Backend
//

enum Backend {
  Fixed(fixed::Strategy),
  ActivityBased(activity_based::Strategy),
}

//
// Strategy
//

pub struct Strategy {
  backend: Backend,
  store: Store,
  state: parking_lot::Mutex<Option<LoadedState>>,
  update_tx: watch::Sender<u64>,
  callback_in_progress: Box<ThreadLocal<Cell<bool>>>,
}

impl Strategy {
  #[must_use]
  pub fn fixed(sdk_directory: impl AsRef<Path>, callbacks: Arc<dyn fixed::Callbacks>) -> Self {
    let (update_tx, _) = watch::channel(0);
    Self {
      backend: Backend::Fixed(fixed::Strategy::new(callbacks)),
      store: Store::new(sdk_directory),
      state: parking_lot::Mutex::new(None),
      update_tx,
      callback_in_progress: Box::new(ThreadLocal::new()),
    }
  }

  #[must_use]
  pub fn activity_based(
    sdk_directory: impl AsRef<Path>,
    inactivity_threshold: time::Duration,
    callbacks: Arc<dyn activity_based::Callbacks>,
    time_provider: Arc<dyn TimeProvider>,
  ) -> Self {
    let (update_tx, _) = watch::channel(0);
    Self {
      backend: Backend::ActivityBased(activity_based::Strategy::new(
        inactivity_threshold,
        callbacks,
        time_provider,
      )),
      store: Store::new(sdk_directory),
      state: parking_lot::Mutex::new(None),
      update_tx,
      callback_in_progress: Box::new(ThreadLocal::new()),
    }
  }

  #[must_use]
  pub fn subscribe_updates(&self) -> watch::Receiver<u64> {
    self.update_tx.subscribe()
  }

  pub async fn session_id(&self) -> anyhow::Result<String> {
    self.ensure_not_in_callback("session_id")?;

    let (session_id, state, mutation) = {
      let mut guard = self.state.lock();
      if let Some(state) = guard.as_mut() {
        // Session reads and activity updates share the same mutation path so activity-based
        // sessions can rotate or persist last-activity state while fixed sessions stay unchanged.
        let mutation = match &self.backend {
          Backend::Fixed(_) => fixed::Strategy::on_session_id(state),
          Backend::ActivityBased(strategy) => strategy.on_session_id(state),
        };
        let session_id = state.persisted.current_session_id.clone();
        (session_id, state.clone(), mutation)
      } else {
        // The first read initializes from durable state and may enqueue a deferred callback if the
        // backend decides the current access should create or rotate a session.
        let initialization = self.initialize_state();
        let session_id = initialization.state.persisted.current_session_id.clone();
        *guard = Some(initialization.state.clone());
        (session_id, initialization.state, initialization.mutation)
      }
    };

    let callback = self.finish_mutation(state, mutation).await;
    self.run_callback(callback);
    Ok(session_id)
  }

  pub async fn start_new_session(&self) {
    if let Err(e) = self.ensure_not_in_callback("start_new_session") {
      log::error!("bitdrift Capture failed to start new session: {e:?}");
      return;
    }

    let (session_id, state, mutation) = {
      let mut guard = self.state.lock();
      self.start_new_session_locked(&mut guard)
    };

    let callback = self.finish_mutation(state, mutation).await;
    self.run_callback(callback);
    log::info!("bitdrift Capture started new session: {session_id:?}");
  }

  fn ensure_not_in_callback(&self, operation: &str) -> anyhow::Result<()> {
    if self.callback_in_progress.get_or_default().get() {
      anyhow::bail!("{operation} cannot be called from within a session callback");
    }

    Ok(())
  }

  fn with_callback_guard<T>(&self, f: impl FnOnce() -> T) -> T {
    let cell = self.callback_in_progress.get_or_default();
    let was_in_progress = cell.replace(true);
    let result = f();
    cell.set(was_in_progress);
    result
  }

  pub async fn flush(&self) {
    let Some(state) = self.state.lock().clone() else {
      log::debug!("no session state to flush");
      return;
    };

    if let Err(e) = self.store.persist_state(&state.persisted).await {
      log::warn!("failed to persist session state during flush: {e}");
    }
  }

  /// The last active session ID from the previous SDK run.
  pub fn previous_process_session_id(&self) -> Option<String> {
    self.state.lock().as_ref().map_or_else(
      || {
        self
          .store
          .load_state()
          .map(|state| state.current_session_id)
      },
      |state| state.persisted.previous_process_session_id.clone(),
    )
  }

  pub async fn handshake_state_update(&self) -> PendingStateUpdate {
    let state = self.load_state_for_update().await;

    let mut request_started_sessions = state.pending_started_sessions.clone();
    // Handshakes must always include the current session. If the queue only contains older pending
    // starts, synthesize the current entry without mutating durable state.
    if !request_started_sessions
      .iter()
      .any(|started| started.session_id == state.persisted.current_session_id)
    {
      request_started_sessions.push(StartedSessionRecord {
        session_id: state.persisted.current_session_id.clone(),
        start_time: state.persisted.current_session_start,
      });
    }

    PendingStateUpdate {
      request: StateUpdateRequest {
        started_sessions: request_started_sessions
          .iter()
          .map(StartedSessionRecord::to_proto)
          .collect(),
        ..Default::default()
      },
      started_sessions: state.pending_started_sessions,
    }
  }

  pub async fn pending_state_update(&self) -> Option<PendingStateUpdate> {
    // Mid-stream state updates only send the durable queue. Unlike the handshake, they do not
    // synthesize the current session because the queue should already contain every unsent start.
    let state = self.load_state_for_update().await;

    if state.pending_started_sessions.is_empty() {
      return None;
    }

    let started_sessions = state.pending_started_sessions;
    Some(PendingStateUpdate {
      request: StateUpdateRequest {
        started_sessions: started_sessions
          .iter()
          .map(StartedSessionRecord::to_proto)
          .collect(),
        ..Default::default()
      },
      started_sessions,
    })
  }

  pub async fn acknowledge_state_update(&self, update: &PendingStateUpdate) {
    if update.started_sessions.is_empty() {
      return;
    }

    let pending_started_sessions = {
      let mut guard = self.state.lock();
      if guard.is_none() {
        *guard = Some(LoadedState {
          persisted: self.store.load_state().unwrap_or_default(),
          pending_started_sessions: self.store.load_pending_started_sessions(),
          last_activity_write: None,
        });
      }

      let Some(state) = guard.as_mut() else {
        return;
      };

      // Responses are not correlated, so we only advance the durable queue when the acknowledged
      // set matches the prefix we most recently sent.
      if !starts_with_sessions(&state.pending_started_sessions, &update.started_sessions) {
        return;
      }

      state
        .pending_started_sessions
        .drain(.. update.started_sessions.len());
      state.pending_started_sessions.clone()
    };

    if let Err(e) = self
      .store
      .persist_pending_started_sessions(&pending_started_sessions)
      .await
    {
      log::warn!("failed to persist acknowledged started sessions: {e}");
    } else {
      self.notify_update();
    }
  }

  /// Pretty name of the strategy.
  pub const fn type_name(&self) -> &'static str {
    match self.backend {
      Backend::Fixed(_) => "fixed",
      Backend::ActivityBased(_) => "activity_based",
    }
  }

  fn initialize_state(&self) -> Initialization {
    // The persisted current session and the persisted pending queue are loaded together so the
    // backend makes decisions from a consistent snapshot of durable state.
    let persisted = self.store.load_state();
    let pending_started_sessions = self.store.load_pending_started_sessions();

    match &self.backend {
      Backend::Fixed(strategy) => {
        self.with_callback_guard(|| strategy.initialize(persisted, pending_started_sessions))
      },
      Backend::ActivityBased(strategy) => strategy.initialize(persisted, pending_started_sessions),
    }
  }

  async fn load_state_for_update(&self) -> LoadedState {
    let (state, mutation) = {
      let mut guard = self.state.lock();
      if let Some(state) = guard.clone() {
        return state;
      }

      // State-update callers can be the first code path to touch session state. Initialize lazily,
      // then drop the lock before invoking any deferred callback.
      let initialization = self.initialize_state();
      *guard = Some(initialization.state.clone());
      (initialization.state, initialization.mutation)
    };

    let callback = self.finish_mutation(state.clone(), mutation).await;
    self.run_callback(callback);
    state
  }

  fn start_new_session_locked(
    &self,
    guard: &mut parking_lot::MutexGuard<'_, Option<LoadedState>>,
  ) -> (String, LoadedState, Mutation) {
    // Explicit session rotation re-reads persisted state so the durable queue remains the source
    // of truth even if this process has not initialized the in-memory cache yet.
    let initialization = match &self.backend {
      Backend::Fixed(strategy) => self.with_callback_guard(|| {
        strategy.start_new_session(
          guard.as_ref(),
          self.store.load_state(),
          self.store.load_pending_started_sessions(),
        )
      }),
      Backend::ActivityBased(strategy) => strategy.start_new_session(
        guard.as_ref(),
        self.store.load_state(),
        self.store.load_pending_started_sessions(),
      ),
    };

    let session_id = initialization.state.persisted.current_session_id.clone();
    **guard = Some(initialization.state.clone());
    (session_id, initialization.state, initialization.mutation)
  }

  async fn finish_mutation(
    &self,
    state: LoadedState,
    mutation: Mutation,
  ) -> Option<DeferredCallback> {
    let pending_changed = mutation.persist_pending;
    let callback = mutation.callback.clone();
    self.persist_loaded_state(&state, &mutation).await;
    if pending_changed {
      self.notify_update();
    }
    callback
  }

  async fn persist_loaded_state(&self, state: &LoadedState, mutation: &Mutation) {
    // The strategy returns a mutation describing which durable pieces changed. Persist only those
    // pieces so reads can stay cheap while the on-disk view remains crash-safe.
    if mutation.persist_state
      && let Err(e) = self.store.persist_state(&state.persisted).await
    {
      log::warn!("failed to persist session state: {e}");
    }

    if mutation.persist_pending
      && let Err(e) = self
        .store
        .persist_pending_started_sessions(&state.pending_started_sessions)
        .await
    {
      log::warn!("failed to persist started sessions: {e}");
    }
  }

  fn run_callback(&self, callback: Option<DeferredCallback>) {
    // Deferred callbacks run after state is persisted and locks are dropped so user code cannot
    // observe half-applied transitions or deadlock against session APIs.
    match callback {
      Some(DeferredCallback::ActivitySessionChanged(session_id)) => {
        if let Backend::ActivityBased(strategy) = &self.backend {
          self.with_callback_guard(|| strategy.run_callback(&session_id));
        }
      },
      None => {},
    }
  }

  fn notify_update(&self) {
    self
      .update_tx
      .send_modify(|version| *version = version.wrapping_add(1));
  }
}

fn starts_with_sessions(
  pending: &[StartedSessionRecord],
  acknowledged: &[StartedSessionRecord],
) -> bool {
  // State-update responses are uncorrelated, so the best we can do is verify that the ack matches
  // the prefix we most recently sent before trimming the durable queue.
  pending.starts_with(acknowledged)
}

impl StartedSessionRecord {
  fn to_proto(&self) -> StartedSession {
    StartedSession {
      session_id: self.session_id.clone(),
      start_time: OffsetDateTime::from(self.start_time).into_proto(),
      ..Default::default()
    }
  }
}
