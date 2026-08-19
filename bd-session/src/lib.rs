// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Session management is split into three layers:
//! 1. A configuration decides when a session ID should change.
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

pub mod configuration;
mod persistence;
pub mod test;

#[cfg(test)]
#[path = "./lib_test.rs"]
mod lib_test;

#[cfg(test)]
#[path = "./persistence_test.rs"]
mod persistence_test;

#[cfg(test)]
#[ctor::ctor(unsafe)]
fn test_global_init() {
  bd_test_helpers_core::test_global_init();
}

use bd_client_common::PlatformMutex;
use bd_proto::protos::client::api::StateUpdateRequest;
use bd_proto::protos::client::api::state_update_request::StartedSession;
use bd_time::OffsetDateTimeExt as _;
use persistence::{PersistedSessionState, StartedSessionRecord, Store};
use std::cell::Cell;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use thread_local::ThreadLocal;
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot, watch};

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
  persistence_pending: bool,
}

//
// TransitionEffects
//

#[derive(Clone, Debug, Default)]
pub(crate) struct TransitionEffects {
  persist: bool,
  notify_update: bool,
  callback: Option<DeferredCallback>,
}

//
// Transition
//

#[derive(Clone, Debug)]
pub(crate) struct Transition {
  state: LoadedState,
  effects: TransitionEffects,
}

//
// DeferredCallback
//

#[derive(Clone, Debug)]
pub(crate) enum DeferredCallback {
  SessionIdChanged(String),
}

//
// PersistenceRequest
//

enum PersistenceRequest {
  Persist,
  Flush(oneshot::Sender<()>),
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
// Strategy
//

pub struct Strategy {
  configuration: configuration::Configuration,
  callbacks: Arc<dyn configuration::Callbacks>,
  store: Store,
  state: PlatformMutex<Option<LoadedState>>,
  persistence_tx: mpsc::Sender<PersistenceRequest>,
  update_tx: watch::Sender<u64>,
  callback_in_progress: Box<ThreadLocal<Cell<bool>>>,
}

/// Owns the single persistence receiver for a [`Strategy`].
///
/// Run this worker for the lifetime of the logger. It serializes persistence and retains queued
/// flush requests until it starts, while callers of [`Strategy`] only synchronize access to the
/// in-memory session state.
pub struct PersistenceWorker {
  strategy: Arc<Strategy>,
  receiver: mpsc::Receiver<PersistenceRequest>,
}

/// A session strategy paired with its single persistence worker.
///
/// Pass this to the logger builder, which splits and owns both components for the logger's
/// lifetime. Call [`Self::strategy`] when a platform needs the strategy handle before then.
#[must_use]
pub struct StrategyWithWorker {
  strategy: Arc<Strategy>,
  persistence_worker: PersistenceWorker,
}

impl StrategyWithWorker {
  /// Returns the strategy handle without consuming the paired persistence worker.
  #[must_use]
  pub fn strategy(&self) -> Arc<Strategy> {
    self.strategy.clone()
  }

  /// Separates the strategy from its worker for the logger's internal lifecycle management.
  #[must_use]
  pub fn into_parts(self) -> (Arc<Strategy>, PersistenceWorker) {
    (self.strategy, self.persistence_worker)
  }
}

impl Strategy {
  /// Creates a session strategy from the canonical mobile SDK configuration.
  ///
  /// Empty initial session IDs are treated as absent and replaced with generated UUIDs.
  pub fn configuration(
    sdk_directory: impl AsRef<Path>,
    initial_session_id: Option<String>,
    inactivity_timeout: Option<time::Duration>,
    callbacks: Arc<dyn configuration::Callbacks>,
    time_provider: Arc<dyn bd_time::TimeProvider>,
  ) -> StrategyWithWorker {
    Self::make_parts(
      configuration::Configuration::new(initial_session_id, inactivity_timeout, time_provider),
      callbacks,
      sdk_directory,
    )
  }

  fn make_parts(
    configuration: configuration::Configuration,
    callbacks: Arc<dyn configuration::Callbacks>,
    sdk_directory: impl AsRef<Path>,
  ) -> StrategyWithWorker {
    let (update_tx, _) = watch::channel(0);
    let (persistence_tx, persistence_rx) = mpsc::channel(1);
    let strategy = Arc::new(Self {
      configuration,
      callbacks,
      store: Store::new(sdk_directory),
      state: PlatformMutex::new(None),
      persistence_tx,
      update_tx,
      callback_in_progress: Box::new(ThreadLocal::new()),
    });
    StrategyWithWorker {
      strategy: strategy.clone(),
      persistence_worker: PersistenceWorker {
        strategy,
        receiver: persistence_rx,
      },
    }
  }

  #[must_use]
  pub fn subscribe_updates(&self) -> watch::Receiver<u64> {
    self.update_tx.subscribe()
  }

  pub fn try_current_session_id(&self) -> anyhow::Result<String> {
    self.ensure_not_in_callback("try_current_session_id")?;

    let guard = self.state.lock();
    let state = guard
      .as_ref()
      .ok_or_else(|| anyhow::anyhow!("current session ID is not loaded"))?;

    Self::loaded_session_id(state)
  }

  /// Resolves the current session ID and schedules any resulting durable transition.
  ///
  /// This is synchronous so caller-thread APIs do not depend on the logger task making progress.
  /// Persistence is coalesced by the background flusher after this method returns.
  pub fn session_id(&self) -> anyhow::Result<String> {
    self.ensure_not_in_callback("session_id")?;

    let (current_session_id, effects) = {
      let mut guard = self.state.lock();
      if let Some(state) = guard.as_mut() {
        let effects = self.configuration.on_session_id(state);

        let current_session_id = Self::loaded_session_id(state)?;
        (current_session_id, effects)
      } else {
        // The first read initializes from durable state and may enqueue a deferred callback if the
        // configuration decides whether the current access should create or rotate a session.
        let initialization = self.initialize_state();
        *guard = Some(initialization.state.clone());

        let current_session_id = Self::loaded_session_id(&initialization.state)?;
        log::debug!(
          "initialized session state on first read: configuration={}, current_session_id={}, \
           persist={}, notify_update={}, callback={}, pending_started_sessions={}",
          self.type_name(),
          current_session_id,
          initialization.effects.persist,
          initialization.effects.notify_update,
          initialization.effects.callback.is_some(),
          initialization.state.pending_started_sessions.len()
        );

        (current_session_id, initialization.effects)
      }
    };

    self.apply_effects(effects);
    Ok(current_session_id)
  }

  /// Creates a new session and schedules persistence without waiting for disk I/O.
  ///
  /// An empty session ID is treated as absent and replaced with a generated UUID.
  pub fn start_new_session(&self, session_id: Option<String>) -> anyhow::Result<()> {
    self.ensure_not_in_callback("start_new_session")?;

    let effects = {
      let mut guard = self.state.lock();
      self.start_new_session_locked(&mut guard, session_id)
    };

    self.apply_effects(effects);
    Ok(())
  }

  fn ensure_not_in_callback(&self, operation: &str) -> anyhow::Result<()> {
    if self.callback_in_progress.get_or_default().get() {
      anyhow::bail!("{operation} cannot be called from within a session callback");
    }

    Ok(())
  }

  fn loaded_session_id(state: &LoadedState) -> anyhow::Result<String> {
    if state.persisted.current_session_id.is_empty() {
      anyhow::bail!("current session ID is unavailable");
    }

    Ok(state.persisted.current_session_id.clone())
  }

  fn with_callback_guard<T>(&self, f: impl FnOnce() -> T) -> T {
    let cell = self.callback_in_progress.get_or_default();
    let was_in_progress = cell.replace(true);
    let result = f();
    cell.set(was_in_progress);
    result
  }

  /// Requests that the persistence worker drain the latest coalesced session snapshot.
  ///
  /// The request remains queued until the worker runs, then completes after its best-effort write
  /// attempt. Session APIs never await this path; only an explicit flush waits for completion.
  pub async fn flush(&self) {
    if self.state.lock().is_none() {
      log::debug!("no session state to flush");
      return;
    }

    let (completion_tx, completion_rx) = oneshot::channel();
    if self
      .persistence_tx
      .send(PersistenceRequest::Flush(completion_tx))
      .await
      .is_err()
    {
      log::debug!("session persistence worker stopped before flush could be queued");
      return;
    }

    if completion_rx.await.is_err() {
      log::debug!("session persistence worker stopped before completing flush");
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

  pub fn handshake_state_update(&self) -> PendingStateUpdate {
    let state = self.load_state_for_update();

    let mut request_started_sessions = state.pending_started_sessions.clone();
    // Handshakes must always include the current session. If the queue only contains older pending
    // starts, synthesize the current entry without mutating durable state.
    if !request_started_sessions
      .iter()
      .any(|started| started.session_id == state.persisted.current_session_id)
    {
      log::debug!(
        "synthesizing current session into handshake: configuration={}, current_session_id={}, \
         pending_started_sessions={}",
        self.type_name(),
        state.persisted.current_session_id,
        state.pending_started_sessions.len()
      );

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

  pub fn pending_state_update(&self) -> Option<PendingStateUpdate> {
    // Mid-stream state updates only send the durable queue. Unlike the handshake, they do not
    // synthesize the current session because the queue should already contain every unsent start.
    let state = self.load_state_for_update();

    if state.pending_started_sessions.is_empty() {
      log::debug!(
        "no pending state update to emit: configuration={}, current_session_id={}",
        self.type_name(),
        state.persisted.current_session_id
      );
      return None;
    }

    let started_sessions = state.pending_started_sessions;
    log::debug!(
      "emitting pending state update: configuration={}, current_session_id={}, \
       pending_started_sessions={}",
      self.type_name(),
      state.persisted.current_session_id,
      started_sessions.len()
    );

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

  pub fn acknowledge_state_update(&self, update: &PendingStateUpdate) {
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
          persistence_pending: false,
        });
      }

      let Some(state) = guard.as_mut() else {
        return;
      };

      // Responses are not correlated, so we only advance the durable queue when the acknowledged
      // set matches the prefix we most recently sent.
      if !starts_with_sessions(&state.pending_started_sessions, &update.started_sessions) {
        log::debug!(
          "ignoring non-prefix state update acknowledgement: configuration={}, \
           current_pending={}, acknowledged={}",
          self.type_name(),
          state.pending_started_sessions.len(),
          update.started_sessions.len()
        );
        return;
      }

      state
        .pending_started_sessions
        .drain(.. update.started_sessions.len());
      state.persistence_pending = true;
      state.pending_started_sessions.clone()
    };

    log::debug!(
      "acknowledged pending started sessions: configuration={}, remaining_pending={}",
      self.type_name(),
      pending_started_sessions.len()
    );
    self.apply_effects(TransitionEffects {
      persist: true,
      notify_update: true,
      callback: None,
    });
  }

  /// Pretty name of the active session configuration.
  pub const fn type_name(&self) -> &'static str {
    self.configuration.type_name()
  }

  fn initialize_state(&self) -> Transition {
    // The persisted current session and the persisted pending queue are loaded together so the
    // configuration makes decisions from a consistent snapshot of durable state.
    let persisted = self.store.load_state();
    let pending_started_sessions = self.store.load_pending_started_sessions();

    log::debug!(
      "loading session state snapshot: active_configuration={}, has_persisted_state={}, \
       pending_started_sessions={}",
      self.type_name(),
      persisted.is_some(),
      pending_started_sessions.len()
    );

    if self.configuration.has_inactivity_timeout()
      && let Some(persisted) = persisted.as_ref()
      && !self
        .configuration
        .matches_persisted_activity_state(&persisted.activity_state)
    {
      log::debug!(
        "resyncing session state after inactivity configuration change: active_configuration={}, \
         previous_current_session_id={}, pending_started_sessions={}",
        self.type_name(),
        persisted.current_session_id,
        pending_started_sessions.len()
      );

      return self
        .initialize_after_inactivity_timeout_enabled(persisted.clone(), pending_started_sessions);
    }

    self
      .configuration
      .initialize(persisted, pending_started_sessions)
  }

  fn initialize_after_inactivity_timeout_enabled(
    &self,
    persisted: PersistedSessionState,
    pending_started_sessions: Vec<StartedSessionRecord>,
  ) -> Transition {
    self
      .configuration
      .start_new_session(None, None, Some(persisted), pending_started_sessions)
  }

  fn load_state_for_update(&self) -> LoadedState {
    let (state, effects) = {
      let mut guard = self.state.lock();
      if let Some(state) = guard.clone() {
        log::debug!(
          "serving state update from cached session state: configuration={}, \
           current_session_id={}, pending_started_sessions={}",
          self.type_name(),
          state.persisted.current_session_id,
          state.pending_started_sessions.len()
        );
        return state;
      }

      // State-update callers can be the first code path to touch session state. Initialize lazily,
      // then drop the lock before invoking any deferred callback.
      let initialization = self.initialize_state();
      *guard = Some(initialization.state.clone());
      log::debug!(
        "initialized session state for update flow: configuration={}, current_session_id={}, \
         persist={}, notify_update={}, callback={}, pending_started_sessions={}",
        self.type_name(),
        initialization.state.persisted.current_session_id,
        initialization.effects.persist,
        initialization.effects.notify_update,
        initialization.effects.callback.is_some(),
        initialization.state.pending_started_sessions.len()
      );
      (initialization.state.clone(), initialization.effects)
    };

    self.apply_effects(effects);
    state
  }

  fn start_new_session_locked(
    &self,
    state: &mut Option<LoadedState>,
    session_id: Option<String>,
  ) -> TransitionEffects {
    // Once a process has initialized session state, its in-memory snapshot is newer than disk
    // until the coalescing writer catches up. Re-reading disk here would drop rapid rotations.
    let (persisted, pending_started_sessions) = state.as_ref().map_or_else(
      || {
        (
          self.store.load_state(),
          self.store.load_pending_started_sessions(),
        )
      },
      |state| {
        (
          Some(state.persisted.clone()),
          state.pending_started_sessions.clone(),
        )
      },
    );
    log::debug!(
      "starting explicit session rotation: configuration={}, cached_state={}, \
       has_persisted_state={}, pending_started_sessions={}",
      self.type_name(),
      state.is_some(),
      persisted.is_some(),
      pending_started_sessions.len()
    );

    let initialization = self.configuration.start_new_session(
      session_id,
      state.as_ref(),
      persisted,
      pending_started_sessions,
    );

    log::debug!(
      "computed explicit session rotation: configuration={}, current_session_id={}, persist={}, \
       notify_update={}, callback={}, pending_started_sessions={}",
      self.type_name(),
      initialization.state.persisted.current_session_id,
      initialization.effects.persist,
      initialization.effects.notify_update,
      initialization.effects.callback.is_some(),
      initialization.state.pending_started_sessions.len()
    );

    *state = Some(initialization.state);
    initialization.effects
  }

  fn apply_effects(&self, effects: TransitionEffects) {
    if !effects.persist && !effects.notify_update && effects.callback.is_none() {
      return;
    }

    log::debug!(
      "applying session transition effects: configuration={}, persist={}, notify_update={}, \
       callback={}, pending_started_sessions={}",
      self.type_name(),
      effects.persist,
      effects.notify_update,
      effects.callback.is_some(),
      self
        .state
        .lock()
        .as_ref()
        .map_or(0, |state| state.pending_started_sessions.len())
    );

    if effects.persist {
      // A full channel already holds a request that will persist the newest in-memory snapshot.
      let _ignored = self.persistence_tx.try_send(PersistenceRequest::Persist);
    }
    if effects.notify_update {
      self.notify_update();
    }
    self.run_callback(effects.callback);
  }

  async fn persist_current_state(&self, on_persistence_failure: &(dyn Fn() + Send + Sync)) {
    let Some(snapshot) = self.state.lock().as_mut().and_then(|state| {
      state.persistence_pending.then(|| {
        state.persistence_pending = false;
        state.clone()
      })
    }) else {
      return;
    };

    log::debug!(
      "persisting coalesced session snapshot: configuration={}, current_session_id={}, \
       pending_started_sessions={}",
      self.type_name(),
      snapshot.persisted.current_session_id,
      snapshot.pending_started_sessions.len()
    );

    let result = async {
      self.store.persist_state(&snapshot.persisted).await?;
      self
        .store
        .persist_pending_started_sessions(&snapshot.pending_started_sessions)
        .await
    }
    .await;

    if let Err(e) = result {
      // Keep the latest in-memory state dirty so the next flush or mutation retries this
      // best-effort write. Mutations that occurred during I/O are retained by the same flag.
      if let Some(state) = self.state.lock().as_mut() {
        state.persistence_pending = true;
      }
      on_persistence_failure();
      log::warn!("failed to persist coalesced session snapshot: {e}");
    }
  }

  fn run_callback(&self, callback: Option<DeferredCallback>) {
    // Callbacks run on the calling thread after the state lock is dropped. This keeps platform
    // integrations responsive and avoids coupling session rotation to best-effort disk I/O.
    match callback {
      Some(DeferredCallback::SessionIdChanged(session_id)) => {
        log::debug!("dispatching configured session callback: current_session_id={session_id}");
        self.with_callback_guard(|| self.callbacks.session_id_changed(&session_id));
      },
      None => {},
    }
  }

  fn notify_update(&self) {
    let mut old_version = 0;
    let mut new_version = 0;
    self.update_tx.send_modify(|version| {
      old_version = *version;
      *version = version.wrapping_add(1);
      new_version = *version;
    });
    log::debug!(
      "advanced session update version: configuration={}, old_version={}, new_version={}",
      self.type_name(),
      old_version,
      new_version
    );
  }
}

impl PersistenceWorker {
  /// Runs the dedicated coalescing writer until `shutdown` resolves.
  pub async fn run<F>(
    mut self,
    shutdown: F,
    on_persistence_failure: impl Fn() + Send + Sync + 'static,
  ) where
    F: Future<Output = ()> + Send,
  {
    tokio::pin!(shutdown);
    loop {
      tokio::select! {
        Some(request) = self.receiver.recv() => {
          match request {
            PersistenceRequest::Persist => {
              self.strategy.persist_current_state(&on_persistence_failure).await;
            },
            PersistenceRequest::Flush(completion_tx) => {
              // A mutation may have coalesced behind this barrier while it occupied the channel.
              self.strategy.persist_current_state(&on_persistence_failure).await;
              let _ignored = completion_tx.send(());
            },
          }
        },
        () = &mut shutdown => {
          self.strategy.persist_current_state(&on_persistence_failure).await;
          while let Ok(request) = self.receiver.try_recv() {
            match request {
              PersistenceRequest::Persist => {},
              PersistenceRequest::Flush(completion_tx) => {
                let _ignored = completion_tx.send(());
              },
            }
          }
          return;
        },
      }
    }
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
