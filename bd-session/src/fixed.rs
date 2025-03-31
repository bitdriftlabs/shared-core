// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./fixed_test.rs"]
mod fixed_test;

use bd_key_value::{Key, Storable, Store};
use bd_log::warn_every;
use std::cell::Cell;
use std::sync::Arc;
use thread_local::ThreadLocal;
use time::ext::NumericalDuration;
use uuid::Uuid;

/// The key used to store the state of the session strategy.
pub static STATE_KEY: Key<State> = Key::new("session_strategy.fixed.state.1");

//
// Strategy
//

/// A session strategy that generates a new session ID on each SDK launch.
pub struct Strategy {
  store: Arc<Store>,
  callbacks: Arc<dyn Callbacks>,

  // Used to prevent a case where a Capture SDK customer calls into the logger to start a new
  // session or obtain a session ID as part of the `generated_session_id` callback.
  is_callback_in_progress: Box<ThreadLocal<Cell<bool>>>,

  state: parking_lot::Mutex<Option<InMemoryState>>,
}

impl Strategy {
  pub fn new(store: Arc<Store>, callbacks: Arc<dyn Callbacks>) -> Self {
    Self {
      store,
      callbacks,
      is_callback_in_progress: Box::new(ThreadLocal::new()),
      state: parking_lot::Mutex::new(None),
    }
  }

  /// Generates a new session ID using the provided callback or a random UUID if the callback
  /// fails.
  fn generate_session_id(&self) -> String {
    // Cannot log anything using `handle_unexpected` or similar as it would cause a cycle between
    // `ErrorReporter` and `Strategy`. As a reminder, `ErrorReporter` calls into `SessionProvider`
    // as part of error reporting flow.
    let cell = self.is_callback_in_progress.get_or_default();
    cell.set(true);

    let session_id = self.callbacks.generate_session_id().unwrap_or_else(|_| {
      let id = Self::generate_uuid();

      log::warn!("failed to generate a new session ID, using a random UUID instead {id:?}");
      id
    });
    cell.set(false);

    session_id
  }

  fn generate_uuid() -> String {
    Uuid::new_v4().to_string()
  }

  /// Returns the current session ID. If the session ID has not been generated yet we call through
  /// to the provided callback to generate a new session ID.
  ///
  /// Note that if this is called from within the `generateSessionID` callback, we will return a
  /// random UUID instead of the actual session ID to prevent deadlocks.
  pub(crate) fn session_id(&self) -> String {
    // Protect against cases where an attempt to read a session ID is made while already holding a
    // lock. In practice, this happens when a customer of the SDK reads session ID from within a
    // `generateSessionID` closure that they are allowed to provide to the SDK.
    //
    // In this case we cannot proceed to the logic below as we risk deadlocking, so we stand
    // in a random UUID instead.
    if self.is_callback_in_progress.get_or_default().get() {
      warn_every!(
        15.seconds(),
        "cannot obtain session ID from within 'generatedSessionID' {}",
        "callback"
      );

      // TODO(snowp): This probably never does what the user expects as this session ID is not a
      // real session ID but a random UUID. We should probably return an error here or just rework
      // how all this works.
      return Self::generate_uuid();
    }

    let mut guard = self.state.lock();
    // Clippy's proposal leads to code that doesn't compile.
    #[allow(clippy::option_if_let_else)]
    if let Some(state) = guard.as_ref() {
      state.session_id.clone()
    } else {
      let previous_process_session_id = if let Some(state) = self.store.get(&STATE_KEY) {
        Some(state.session_id)
      } else {
        None
      };

      let session_id = self.generate_session_id();

      let state = InMemoryState {
        session_id: session_id.clone(),
        previous_process_session_id,
      };

      *guard = Some(state.clone());

      self.store.set(&STATE_KEY, &state.into());

      log::info!("bitdrift Capture initialized with session ID: {session_id:?}");

      session_id
    }
  }

  /// Starts a new session by generating a new session ID and storing it in the state.
  ///
  /// Note that if this is called from within the `generateSessionID` callback, we will return an
  /// error instead of starting a new session to prevent deadlocks.
  pub(crate) fn start_new_session(&self) -> anyhow::Result<String> {
    // Protect against cases where an attempt to start a new session is made while already holding a
    // lock. In practice, this happens when a customer of the SDK starts a new session from
    // within a `generateSessionID` closure that they are allowed to provide to the SDK.
    if self.is_callback_in_progress.get_or_default().get() {
      anyhow::bail!("cannot start new session from within 'generatedSessionID' callback");
    };

    let mut guard = self.state.lock();

    let session_id = self.generate_session_id();

    let state = guard.as_ref().map_or_else(
      || match self.store.get(&STATE_KEY) {
        Some(state) => InMemoryState {
          session_id: session_id.clone(),
          previous_process_session_id: Some(state.session_id),
        },
        None => InMemoryState {
          session_id: session_id.clone(),
          previous_process_session_id: None,
        },
      },
      |state| InMemoryState {
        session_id: session_id.clone(),
        previous_process_session_id: state.previous_process_session_id.clone(),
      },
    );

    self.store.set(&STATE_KEY, &state.clone().into());
    *guard = Some(state);

    Ok(session_id)
  }

  /// Returns the last session ID from the previous SDK run.
  pub fn previous_process_session_id(&self) -> Option<String> {
    self.state.lock().as_ref().map_or_else(
      || self.store.get(&STATE_KEY).map(|state| state.session_id),
      |state| state.previous_process_session_id.clone(),
    )
  }
}

//
// Callbacks
//

pub trait Callbacks: Send + Sync {
  fn generate_session_id(&self) -> anyhow::Result<String>;
}

//
// UUIDCallbacks
//

#[derive(Default)]
pub struct UUIDCallbacks;

impl Callbacks for UUIDCallbacks {
  fn generate_session_id(&self) -> anyhow::Result<String> {
    Ok(Uuid::new_v4().to_string())
  }
}

//
// State
//

#[cfg_attr(test, derive(Clone, PartialEq, Eq))]
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct State {
  /// The last active session ID. Used on SDK launch to retrieve the `previous_process_session_id`.
  pub session_id: String,
}

impl Storable for State {}

//
// InMemoryState
//

#[derive(Clone, Debug)]
struct InMemoryState {
  /// The current session ID.
  session_id: String,
  /// The last active session ID from the previous SDK run.
  previous_process_session_id: Option<String>,
}

impl From<InMemoryState> for State {
  fn from(state: InMemoryState) -> Self {
    Self {
      session_id: state.session_id,
    }
  }
}
