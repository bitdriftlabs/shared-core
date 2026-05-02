// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./fixed_test.rs"]
mod fixed_test;

use crate::persistence::{BackendState, PersistedSessionState, StartedSessionRecord};
use crate::{Initialization, LoadedState, Mutation};
use std::sync::Arc;
use time::OffsetDateTime;
use uuid::Uuid;

//
// Strategy
//

/// A session strategy that generates a new session ID on each SDK launch.
pub struct Strategy {
  callbacks: Arc<dyn Callbacks>,
}

impl Strategy {
  pub fn new(callbacks: Arc<dyn Callbacks>) -> Self {
    Self { callbacks }
  }

  /// Generates a new session ID using the provided callback or a random UUID if the callback
  /// fails.
  fn generate_session_id(&self) -> String {
    // Cannot log anything using `handle_unexpected` or similar as it would cause a cycle between
    // `ErrorReporter` and `Strategy`. As a reminder, `ErrorReporter` calls into `SessionProvider`
    // as part of error reporting flow.
    self.callbacks.generate_session_id().unwrap_or_else(|_| {
      let id = Self::generate_uuid();

      log::warn!("failed to generate a new session ID, using a random UUID instead {id:?}");
      id
    })
  }

  fn generate_uuid() -> String {
    Uuid::new_v4().to_string()
  }

  pub(crate) fn initialize(
    &self,
    persisted: Option<PersistedSessionState>,
    mut pending_started_sessions: Vec<StartedSessionRecord>,
  ) -> Initialization {
    // Fixed sessions always create a fresh session on startup. The persisted current session is
    // only used to seed `previous_process_session_id` for crash/error attribution.
    let session_id = self.generate_session_id();
    let session_start = OffsetDateTime::now_utc();
    let previous_process_session_id = persisted.map(|state| state.current_session_id);

    pending_started_sessions.push(StartedSessionRecord::new(session_id.clone(), session_start));

    Initialization {
      state: LoadedState {
        persisted: PersistedSessionState {
          current_session_id: session_id,
          current_session_start: session_start.into(),
          previous_process_session_id,
          backend: BackendState::Fixed,
        },
        pending_started_sessions,
        last_activity_write: None,
      },
      mutation: Mutation {
        persist_state: true,
        persist_pending: true,
        callback: None,
      },
    }
  }

  pub(crate) fn on_session_id(_state: &mut LoadedState) -> Mutation {
    Mutation::default()
  }

  pub(crate) fn start_new_session(
    &self,
    state: Option<&LoadedState>,
    persisted: Option<PersistedSessionState>,
    mut pending_started_sessions: Vec<StartedSessionRecord>,
  ) -> Initialization {
    // Explicit session starts should preserve whatever we already consider the previous-process
    // session rather than replacing it with the session we are rotating away from in this run.
    let session_id = self.generate_session_id();
    let session_start = OffsetDateTime::now_utc();
    let previous_process_session_id = state.as_ref().map_or_else(
      || persisted.map(|state| state.current_session_id),
      |state| state.persisted.previous_process_session_id.clone(),
    );

    pending_started_sessions.push(StartedSessionRecord::new(session_id.clone(), session_start));

    Initialization {
      state: LoadedState {
        persisted: PersistedSessionState {
          current_session_id: session_id,
          current_session_start: session_start.into(),
          previous_process_session_id,
          backend: BackendState::Fixed,
        },
        pending_started_sessions,
        last_activity_write: None,
      },
      mutation: Mutation {
        persist_state: true,
        persist_pending: true,
        callback: None,
      },
    }
  }
}

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
