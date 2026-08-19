// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.

use super::persistence::{ActivityState, PersistedSessionState};
use bd_macros::proto_serializable;
use bd_proto_util::serialization::{
  ProtoMessageDeserialize,
  ProtoMessageSerialize,
  TimestampMicros,
};
use pretty_assertions::assert_eq;
use time::OffsetDateTime;

#[proto_serializable]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct LegacyPersistedSessionState {
  #[field(id = 1)]
  current_session_id: String,
  #[field(id = 2)]
  current_session_start: TimestampMicros,
  #[field(id = 3)]
  previous_process_session_id: Option<String>,
  #[field(id = 4)]
  backend: LegacyBackendState,
}

#[proto_serializable]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
enum LegacyBackendState {
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

#[test]
fn persisted_state_reads_legacy_backend_wire_format() {
  let start = OffsetDateTime::UNIX_EPOCH.into();
  let last_activity = (OffsetDateTime::UNIX_EPOCH + time::Duration::minutes(5)).into();
  let cases = [
    (
      LegacyBackendState::Fixed,
      ActivityState::NoInactivityTimeout,
    ),
    (
      LegacyBackendState::ActivityBased { last_activity },
      ActivityState::InactivityTimeout { last_activity },
    ),
  ];

  for (backend, activity_state) in cases {
    let legacy = LegacyPersistedSessionState {
      current_session_id: "session".to_string(),
      current_session_start: start,
      previous_process_session_id: Some("previous".to_string()),
      backend,
    };

    let deserialized = PersistedSessionState::deserialize_message_from_bytes(
      &legacy.serialize_message_to_bytes().unwrap(),
    )
    .unwrap();

    assert_eq!("session", deserialized.current_session_id);
    assert_eq!(start, deserialized.current_session_start);
    assert_eq!(
      Some("previous".to_string()),
      deserialized.previous_process_session_id
    );
    assert_eq!(activity_state, deserialized.activity_state);
  }
}
