// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.

#[cfg(test)]
#[path = "./configuration_test.rs"]
mod configuration_test;

use crate::persistence::{ActivityState, PersistedSessionState, StartedSessionRecord};
use crate::{DeferredCallback, LoadedState, Transition, TransitionEffects};
use bd_time::TimeProvider;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

const MAX_ACTIVITY_WRITE_INTERVAL: Duration = Duration::seconds(15);

/// The canonical session configuration used by the mobile SDKs.
pub struct Configuration {
  initial_session_id: Option<String>,
  inactivity_timeout: Option<Duration>,
  time_provider: Arc<dyn TimeProvider>,
}

impl Configuration {
  pub fn new(
    initial_session_id: Option<String>,
    inactivity_timeout: Option<Duration>,
    time_provider: Arc<dyn TimeProvider>,
  ) -> Self {
    Self {
      initial_session_id: Self::non_empty_session_id(initial_session_id),
      inactivity_timeout,
      time_provider,
    }
  }

  pub(crate) const fn has_inactivity_timeout(&self) -> bool {
    self.inactivity_timeout.is_some()
  }

  pub(crate) const fn type_name(&self) -> &'static str {
    if self.has_inactivity_timeout() {
      "inactivity_timeout"
    } else {
      "no_inactivity_timeout"
    }
  }

  pub(crate) const fn matches_persisted_activity_state(
    &self,
    activity_state: &ActivityState,
  ) -> bool {
    matches!(
      (self.inactivity_timeout, activity_state),
      (None, ActivityState::NoInactivityTimeout)
        | (Some(_), ActivityState::InactivityTimeout { .. })
    )
  }

  pub(crate) fn initialize(
    &self,
    persisted: Option<PersistedSessionState>,
    pending_started_sessions: Vec<StartedSessionRecord>,
  ) -> Transition {
    match (self.inactivity_timeout, persisted) {
      (Some(timeout), Some(persisted)) => {
        let mut state = LoadedState {
          persisted: PersistedSessionState {
            previous_process_session_id: Some(persisted.current_session_id.clone()),
            ..persisted
          },
          pending_started_sessions,
          last_activity_write: None,
          persistence_pending: false,
        };
        let mut effects = self.on_session_id_with_timeout(&mut state, timeout);
        if !state
          .pending_started_sessions
          .iter()
          .any(|started| started.session_id == state.persisted.current_session_id)
        {
          state
            .pending_started_sessions
            .push(StartedSessionRecord::new(
              state.persisted.current_session_id.clone(),
              OffsetDateTime::from(state.persisted.current_session_start),
            ));
          state.persistence_pending = true;
          effects.persist = true;
          effects.notify_update = true;
        }
        Transition { state, effects }
      },
      (_, persisted) => self.new_session(
        self
          .initial_session_id
          .clone()
          .unwrap_or_else(Self::generate_session_id),
        persisted.map(|state| state.current_session_id),
        pending_started_sessions,
      ),
    }
  }

  pub(crate) fn on_session_id(&self, state: &mut LoadedState) -> TransitionEffects {
    self
      .inactivity_timeout
      .map_or_else(TransitionEffects::default, |timeout| {
        self.on_session_id_with_timeout(state, timeout)
      })
  }

  pub(crate) fn start_new_session(
    &self,
    session_id: Option<String>,
    state: Option<&LoadedState>,
    persisted: Option<PersistedSessionState>,
    pending_started_sessions: Vec<StartedSessionRecord>,
  ) -> Transition {
    let previous_process_session_id = state.as_ref().map_or_else(
      || persisted.map(|state| state.current_session_id),
      |state| state.persisted.previous_process_session_id.clone(),
    );
    self.new_session(
      Self::non_empty_session_id(session_id).unwrap_or_else(Self::generate_session_id),
      previous_process_session_id,
      pending_started_sessions,
    )
  }

  fn on_session_id_with_timeout(
    &self,
    state: &mut LoadedState,
    timeout: Duration,
  ) -> TransitionEffects {
    let now = self.time_provider.now();
    let ActivityState::InactivityTimeout { last_activity } = &mut state.persisted.activity_state
    else {
      return TransitionEffects::default();
    };
    let previous_last_activity = OffsetDateTime::from(*last_activity);
    let last_activity_storage_needs_write = state
      .last_activity_write
      .is_none_or(|last_activity_write| now - last_activity_write > MAX_ACTIVITY_WRITE_INTERVAL);
    *last_activity = now.into();

    if now < previous_last_activity || now - previous_last_activity > timeout {
      let session_id = Self::generate_session_id();
      state.persisted.current_session_id.clone_from(&session_id);
      state.persisted.current_session_start = now.into();
      state
        .pending_started_sessions
        .push(StartedSessionRecord::new(session_id.clone(), now));
      state.last_activity_write = Some(now);
      state.persistence_pending = true;
      return TransitionEffects {
        persist: true,
        notify_update: true,
        callback: Some(DeferredCallback::SessionIdChanged(session_id)),
      };
    }

    if last_activity_storage_needs_write {
      state.last_activity_write = Some(now);
      state.persistence_pending = true;
      TransitionEffects {
        persist: true,
        ..Default::default()
      }
    } else {
      TransitionEffects::default()
    }
  }

  fn new_session(
    &self,
    session_id: String,
    previous_process_session_id: Option<String>,
    mut pending_started_sessions: Vec<StartedSessionRecord>,
  ) -> Transition {
    let now = self.time_provider.now();
    pending_started_sessions.push(StartedSessionRecord::new(session_id.clone(), now));
    let activity_state = self
      .inactivity_timeout
      .map_or(ActivityState::NoInactivityTimeout, |_| {
        ActivityState::InactivityTimeout {
          last_activity: now.into(),
        }
      });

    Transition {
      state: LoadedState {
        persisted: PersistedSessionState {
          current_session_id: session_id.clone(),
          current_session_start: now.into(),
          previous_process_session_id,
          activity_state,
        },
        pending_started_sessions,
        last_activity_write: Some(now),
        persistence_pending: true,
      },
      effects: TransitionEffects {
        persist: true,
        notify_update: true,
        callback: Some(DeferredCallback::SessionIdChanged(session_id)),
      },
    }
  }

  fn generate_session_id() -> String {
    Uuid::new_v4().to_string()
  }

  fn non_empty_session_id(session_id: Option<String>) -> Option<String> {
    session_id.filter(|session_id| !session_id.is_empty())
  }
}

pub trait Callbacks: Send + Sync {
  /// Receives the ID associated with a session transition after the in-memory state is updated.
  ///
  /// Callbacks from overlapping transitions can be delivered in a different order from those
  /// transitions. Implementations should treat `session_id` as the ID for that individual
  /// transition, not as an ordered view of the current session. Query
  /// [`crate::Strategy::session_id`] when the current session ID is required.
  fn session_id_changed(&self, session_id: &str);
}

#[derive(Default)]
pub struct NoopCallbacks;

impl Callbacks for NoopCallbacks {
  fn session_id_changed(&self, _session_id: &str) {}
}
