// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./activity_based_test.rs"]
mod activity_based_test;

use crate::persistence::{BackendState, PersistedSessionState, StartedSessionRecord};
use crate::{DeferredCallback, Initialization, LoadedState, Mutation};
use bd_time::TimeProvider;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

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
  time_provider: Arc<dyn TimeProvider>,

  // The minimum duration between consecutive writes of the last activity time.
  max_write_interval: time::Duration,
}

impl Strategy {
  pub fn new(
    inactivity_threshold: time::Duration,
    callbacks: Arc<dyn Callbacks>,
    time_provider: Arc<dyn bd_time::TimeProvider>,
  ) -> Self {
    log::debug!(
      "configured activity-based session strategy: inactivity_threshold={inactivity_threshold:?}, \
       max_write_interval={:?}",
      Duration::seconds(15)
    );

    Self {
      callbacks,
      inactivity_threshold,
      time_provider,
      max_write_interval: Duration::seconds(15),
    }
  }

  fn generate_session_id() -> String {
    Uuid::new_v4().to_string()
  }

  pub(crate) fn initialize(
    &self,
    persisted: Option<PersistedSessionState>,
    mut pending_started_sessions: Vec<StartedSessionRecord>,
  ) -> Initialization {
    let now = self.time_provider.now();
    if let Some(persisted) = persisted {
      log::debug!(
        "initializing activity-based session from persisted state: current_session_id={}, \
         pending_started_sessions={}, current_session_start={:?}",
        persisted.current_session_id,
        pending_started_sessions.len(),
        OffsetDateTime::from(persisted.current_session_start)
      );

      // Reuse the persisted session as input to the normal activity transition logic so restart
      // behavior matches a normal foreground access.
      let previous_process_session_id = Some(persisted.current_session_id.clone());
      let mut state = LoadedState {
        persisted: PersistedSessionState {
          previous_process_session_id,
          ..persisted
        },
        pending_started_sessions,
        last_activity_write: None,
      };
      let mut mutation = self.on_session_id(&mut state);
      // Handshake uploads must be able to announce the current session after a restart even if the
      // process died before flushing a pending-started-sessions queue entry.
      if !state
        .pending_started_sessions
        .iter()
        .any(|started| started.session_id == state.persisted.current_session_id)
      {
        log::debug!(
          "re-queueing current activity session for handshake upload after restart: session_id={}",
          state.persisted.current_session_id
        );

        state
          .pending_started_sessions
          .push(StartedSessionRecord::new(
            state.persisted.current_session_id.clone(),
            OffsetDateTime::from(state.persisted.current_session_start),
          ));
        mutation.persist_pending = true;
      }

      log::debug!(
        "initialized activity-based session from persisted state: current_session_id={}, \
         previous_process_session_id={:?}, persist_state={}, persist_pending={}, \
         pending_started_sessions={}",
        state.persisted.current_session_id,
        state.persisted.previous_process_session_id,
        mutation.persist_state,
        mutation.persist_pending,
        state.pending_started_sessions.len()
      );

      Initialization { state, mutation }
    } else {
      let session_id = Self::generate_session_id();
      let session_start = now;
      pending_started_sessions.push(StartedSessionRecord::new(session_id.clone(), session_start));

      log::debug!(
        "initializing new activity-based session: session_id={session_id}, \
         session_start={session_start:?}, pending_started_sessions={}",
        pending_started_sessions.len()
      );

      Initialization {
        state: LoadedState {
          persisted: PersistedSessionState {
            current_session_id: session_id.clone(),
            current_session_start: session_start.into(),
            previous_process_session_id: None,
            backend: BackendState::ActivityBased {
              last_activity: now.into(),
            },
          },
          pending_started_sessions,
          last_activity_write: Some(now),
        },
        mutation: Mutation {
          persist_state: true,
          persist_pending: true,
          callback: Some(DeferredCallback::ActivitySessionChanged(session_id)),
        },
      }
    }
  }

  pub(crate) fn on_session_id(&self, state: &mut LoadedState) -> Mutation {
    let now = self.time_provider.now();
    let BackendState::ActivityBased { last_activity } = &mut state.persisted.backend else {
      // `initialize_state()` should already resync any persisted backend mismatch before an
      // activity-based strategy reaches its steady-state access path. Keep the branch as a
      // defensive fallback so debug builds catch any future invariant drift immediately.
      debug_assert!(
        false,
        "activity-based session observed incompatible backend state after initialization"
      );
      log::debug!(
        "activity-based session observed incompatible backend state after initialization; leaving \
         state unchanged"
      );
      return Mutation::default();
    };

    let previous_session_id = state.persisted.current_session_id.clone();
    let previous_last_activity = OffsetDateTime::from(*last_activity);
    let inactivity_elapsed = now - previous_last_activity;
    let is_now_before_last_activity = now < previous_last_activity;
    let is_inactivity_threshold_exceeded = inactivity_elapsed > self.inactivity_threshold;
    let last_activity_storage_needs_write = state
      .last_activity_write
      .is_none_or(|last_activity_write| now - last_activity_write > self.max_write_interval);

    log::debug!(
      "evaluating activity-based session access: current_session_id={}, now={now:?}, \
       previous_last_activity={previous_last_activity:?}, \
       inactivity_elapsed={inactivity_elapsed:?}, threshold={:?}, clock_moved_backwards={}, \
       persist_last_activity={}",
      previous_session_id,
      self.inactivity_threshold,
      is_now_before_last_activity,
      last_activity_storage_needs_write
    );

    *last_activity = now.into();

    if is_now_before_last_activity || is_inactivity_threshold_exceeded {
      // Either the clock moved backwards or the inactivity threshold expired. In both cases we
      // rotate the session and enqueue a state update so the backend can observe the boundary.
      let session_id = Self::generate_session_id();
      state.persisted.current_session_id.clone_from(&session_id);
      state.persisted.current_session_start = now.into();
      state
        .pending_started_sessions
        .push(StartedSessionRecord::new(session_id.clone(), now));
      state.last_activity_write = Some(now);

      log::debug!(
        "rotating activity-based session: previous_session_id={}, new_session_id={}, reason={}, \
         pending_started_sessions={}",
        previous_session_id,
        session_id,
        if is_now_before_last_activity {
          "clock_moved_backwards"
        } else {
          "inactivity_threshold_exceeded"
        },
        state.pending_started_sessions.len()
      );

      Mutation {
        persist_state: true,
        persist_pending: true,
        callback: Some(DeferredCallback::ActivitySessionChanged(session_id)),
      }
    } else if last_activity_storage_needs_write {
      // The session itself is unchanged, but we periodically persist the last-activity timestamp so
      // a restart can continue the inactivity window from roughly the right point in time.
      state.last_activity_write = Some(now);

      log::debug!(
        "persisting activity-based last-activity without session rotation: \
         current_session_id={previous_session_id}, last_activity={now:?}"
      );

      Mutation {
        persist_state: true,
        ..Default::default()
      }
    } else {
      log::debug!(
        "leaving activity-based session unchanged: current_session_id={previous_session_id}, no \
         durable write required"
      );

      Mutation::default()
    }
  }

  pub(crate) fn start_new_session(
    &self,
    state: Option<&LoadedState>,
    persisted: Option<PersistedSessionState>,
    mut pending_started_sessions: Vec<StartedSessionRecord>,
  ) -> Initialization {
    // An explicit rotation behaves like a brand-new activity session, but it preserves the last
    // previous-process session marker for post-restart reporting.
    let session_id = Self::generate_session_id();
    let now = self.time_provider.now();
    let previous_process_session_id = state.as_ref().map_or_else(
      || persisted.map(|state| state.current_session_id),
      |state| state.persisted.previous_process_session_id.clone(),
    );

    pending_started_sessions.push(StartedSessionRecord::new(session_id.clone(), now));

    log::debug!(
      "starting explicit new activity-based session: new_session_id={}, \
       previous_process_session_id={previous_process_session_id:?}, pending_started_sessions={}",
      session_id,
      pending_started_sessions.len()
    );

    Initialization {
      state: LoadedState {
        persisted: PersistedSessionState {
          current_session_id: session_id,
          current_session_start: now.into(),
          previous_process_session_id,
          backend: BackendState::ActivityBased {
            last_activity: now.into(),
          },
        },
        pending_started_sessions,
        last_activity_write: Some(now),
      },
      mutation: Mutation {
        persist_state: true,
        persist_pending: true,
        callback: None,
      },
    }
  }

  pub(crate) fn run_callback(&self, session_id: &str) {
    log::debug!("dispatching activity-based session callback: session_id={session_id}");
    self.callbacks.session_id_changed(session_id);
  }
}

pub trait Callbacks: Send + Sync {
  fn session_id_changed(&self, session_id: &str);
}
