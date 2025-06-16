// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./activity_based_test.rs"]
mod activity_based_test;

use crate::SessionId;
use bd_key_value::{Key, Storable, Store};
use bd_time::TimeProvider;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

/// The key used to store the state of the session strategy.
pub(crate) static STATE_KEY: Key<State> = Key::new("session_strategy.activity_based.state.1");

//
// Strategy
//

/// A session strategy that stores the current session ID in the provided store, potentially
/// persisting the session ID between SDK launches. It tracks the time elapsed since the last access
/// of the session ID and regenerates the session ID after an inactivity period threshold.
///
/// Writes to the store are throttled to avoid excessive writes.
pub struct Strategy {
  callbacks: Arc<dyn Callbacks>,
  inactivity_threshold: time::Duration,
  store: Arc<Store>,
  time_provider: Arc<dyn TimeProvider>,

  // The current state of the strategy. Starts off as `None` and is initialized lazily on the first
  // access of the session identifier. This allows potentially heavy storage operations to be
  // offloaded to a later time (which in practice can end up being handled by a background
  // thread).
  state: parking_lot::Mutex<Option<InMemoryState>>,

  // The minimum duration between consecutive writes of the last activity time.
  max_write_interval: time::Duration,
}

impl Strategy {
  pub fn new(
    inactivity_threshold: time::Duration,
    store: Arc<Store>,
    callbacks: Arc<dyn Callbacks>,
    time_provider: Arc<dyn bd_time::TimeProvider>,
  ) -> Self {
    Self {
      callbacks,
      inactivity_threshold,
      store,
      time_provider,
      state: parking_lot::Mutex::new(None),
      max_write_interval: Duration::seconds(15),
    }
  }

  fn generate_session_id() -> String {
    Uuid::new_v4().to_string()
  }

  pub(crate) fn session_id(&self) -> SessionId {
    let mut guard = self.state.lock();

    let now = self.time_provider.now();

    let mut session_changed = false;
    let mut state = guard.as_ref().map_or_else(
      || {
        if let Some(state) = self.store.get(&STATE_KEY) {
          InMemoryState {
            session_id: state.session_id.clone(),
            previous_process_session_id: Some(state.session_id),
            last_activity: state.last_activity,
            last_activity_write: None,
          }
        } else {
          let state = InMemoryState {
            session_id: Self::generate_session_id(),
            previous_process_session_id: None,
            last_activity: now,
            last_activity_write: None,
          };

          session_changed = true;

          log::info!(
            "bitdrift Capture initialized with session ID: {:?}",
            state.session_id,
          );

          state
        }
      },
      std::clone::Clone::clone,
    );

    let is_now_before_last_activity = now < state.last_activity;
    let is_inactivity_threshold_exceeded = now - state.last_activity > self.inactivity_threshold;

    // We debounce writes to the store to avoid excessive writes, so only perform a state write if
    // we haven't written to the store yet or if the last write was more than `max_write_interval`
    // ago. If the session changes we always write to the store.
    let last_activity_storage_needs_write = state
      .last_activity_write
      .is_none_or(|last_activity_write| now - last_activity_write > self.max_write_interval);

    state.last_activity = now;

    if is_now_before_last_activity || is_inactivity_threshold_exceeded {
      let session_id = Self::generate_session_id();

      state.session_id.clone_from(&session_id);
      state.last_activity_write = Some(now);

      self.store.set(&STATE_KEY, &state.clone().into());

      session_changed = true;
    } else if last_activity_storage_needs_write {
      state.last_activity_write = Some(now);

      self.store.set(&STATE_KEY, &state.clone().into());
    }

    let session_id = state.session_id.clone();
    *guard = Some(state);

    // Make sure we call the callback with the lock released. There are legitimate cases where
    // this function may get called again from the context of the callback.
    drop(guard);
    if session_changed {
      self.callbacks.session_id_changed(&session_id);

      return SessionId::New(session_id);
    }

    SessionId::Existing(session_id)
  }

  pub(crate) fn start_new_session(&self) -> String {
    let mut guard = self.state.lock();

    let session_id = Self::generate_session_id();
    let now = self.time_provider.now();

    let previous_process_session_id = guard.as_ref().map_or_else(
      || {
        if let Some(state) = self.store.get(&STATE_KEY) {
          Some(state.session_id)
        } else {
          None
        }
      },
      |state| state.previous_process_session_id.clone(),
    );

    let state = InMemoryState {
      session_id: session_id.clone(),
      previous_process_session_id,
      last_activity: now,
      last_activity_write: Some(now),
    };

    self.store.set(&STATE_KEY, &state.clone().into());

    *guard = Some(state);

    session_id
  }

  pub fn previous_process_session_id(&self) -> Option<String> {
    self.state.lock().as_ref().map_or_else(
      || self.store.get(&STATE_KEY).map(|state| state.session_id),
      |state| state.previous_process_session_id.clone(),
    )
  }

  pub(crate) fn flush(&self) {
    log::debug!("flushing session state");
    if let Some(state) = self.state.lock().as_ref() {
      self.store.set(&STATE_KEY, &state.clone().into());
    } else {
      log::debug!("no session state to flush");
    }
  }
}

//
// Callbacks
//

pub trait Callbacks: Send + Sync {
  fn session_id_changed(&self, session_id: &str);
}

//
// InMemoryState
//

#[derive(Clone)]
struct InMemoryState {
  session_id: String,
  /// The last active session ID from the previous SDK run.
  previous_process_session_id: Option<String>,
  last_activity: OffsetDateTime,
  last_activity_write: Option<OffsetDateTime>,
}

//
// State
//

#[cfg_attr(test, derive(Clone, PartialEq, Eq))]
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct State {
  session_id: String,
  #[serde(with = "time::serde::rfc3339")]
  last_activity: OffsetDateTime,
}

impl Storable for State {}

impl From<InMemoryState> for State {
  fn from(state: InMemoryState) -> Self {
    Self {
      session_id: state.session_id,
      last_activity: state.last_activity,
    }
  }
}
