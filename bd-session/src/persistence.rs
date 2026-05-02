// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_client_common::file::{read_compressed, write_compressed};
use bd_client_common::file_system::{delete_file_if_exists_async, write_file_atomic};
use bd_macros::proto_serializable;
use bd_proto_util::serialization::{
  ProtoMessageDeserialize,
  ProtoMessageSerialize,
  TimestampMicros,
};
use std::path::{Path, PathBuf};
use time::OffsetDateTime;

const STATE_FILE_NAME: &str = "current_session.pb";
const PENDING_STARTED_SESSIONS_FILE_NAME: &str = "pending_started_sessions.pb";

//
// PersistedSessionState
//

#[proto_serializable]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PersistedSessionState {
  #[field(id = 1)]
  pub current_session_id: String,
  #[field(id = 2)]
  pub current_session_start: TimestampMicros,
  #[field(id = 3)]
  pub previous_process_session_id: Option<String>,
  #[field(id = 4)]
  pub backend: BackendState,
}

//
// BackendState
//

#[proto_serializable]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum BackendState {
  #[field(id = 1)]
  #[field(deserialize)]
  #[default]
  Fixed,
  #[field(id = 2)]
  #[field(deserialize)]
  ActivityBased {
    #[field(id = 1)]
    last_activity: TimestampMicros,
  },
}

//
// PendingStartedSessions
//

#[proto_serializable]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PendingStartedSessions {
  #[field(id = 1)]
  pub sessions: Vec<StartedSessionRecord>,
}

//
// StartedSessionRecord
//

#[proto_serializable]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StartedSessionRecord {
  #[field(id = 1)]
  pub session_id: String,
  #[field(id = 2)]
  pub start_time: TimestampMicros,
}

impl StartedSessionRecord {
  pub fn new(session_id: String, start_time: OffsetDateTime) -> Self {
    Self {
      session_id,
      start_time: start_time.into(),
    }
  }
}

//
// Store
//

#[derive(Clone, Debug)]
pub struct Store {
  directory: PathBuf,
}

impl Store {
  #[must_use]
  pub fn new(sdk_directory: impl AsRef<Path>) -> Self {
    Self {
      directory: sdk_directory.as_ref().join("state").join("session"),
    }
  }

  pub fn load_state(&self) -> Option<PersistedSessionState> {
    Self::read_message::<PersistedSessionState>(&self.state_file_path())
  }

  pub async fn persist_state(&self, state: &PersistedSessionState) -> anyhow::Result<()> {
    Self::write_message(&self.state_file_path(), state).await
  }

  pub fn load_pending_started_sessions(&self) -> Vec<StartedSessionRecord> {
    Self::read_message::<PendingStartedSessions>(&self.pending_started_sessions_file_path())
      .map_or_else(Vec::new, |pending| pending.sessions)
  }

  pub async fn persist_pending_started_sessions(
    &self,
    sessions: &[StartedSessionRecord],
  ) -> anyhow::Result<()> {
    // An empty queue is represented by the file being absent so that a restart cannot
    // accidentally resurrect stale session announcements.
    if sessions.is_empty() {
      return delete_file_if_exists_async(&self.pending_started_sessions_file_path()).await;
    }

    Self::write_message(
      &self.pending_started_sessions_file_path(),
      &PendingStartedSessions {
        sessions: sessions.to_vec(),
      },
    )
    .await
  }

  fn state_file_path(&self) -> PathBuf {
    self.directory.join(STATE_FILE_NAME)
  }

  fn pending_started_sessions_file_path(&self) -> PathBuf {
    self.directory.join(PENDING_STARTED_SESSIONS_FILE_NAME)
  }

  fn read_message<T: ProtoMessageDeserialize>(path: &Path) -> Option<T> {
    let bytes = std::fs::read(path).ok()?;
    match read_compressed(&bytes).and_then(|bytes| T::deserialize_message_from_bytes(&bytes)) {
      Ok(message) => Some(message),
      Err(e) => {
        // Treat unreadable state as self-healing corruption. We drop the file so the caller can
        // rebuild from a clean slate rather than failing every startup.
        log::warn!("failed to read session state from {}: {e}", path.display());
        let _ = std::fs::remove_file(path);
        None
      },
    }
  }

  async fn write_message<T: ProtoMessageSerialize>(path: &Path, message: &T) -> anyhow::Result<()> {
    let bytes = message.serialize_message_to_bytes()?;
    let compressed = write_compressed(&bytes)?;
    write_file_atomic(path, &compressed).await
  }
}
