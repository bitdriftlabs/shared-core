#![allow(clippy::unwrap_used)]

use super::RawConfigurationUpdate;
use bd_proto::protos::client::api::configuration_update::{StateOfTheWorld, Update_type};
use bd_proto::protos::client::api::{ClientStateUpdate, ConfigurationUpdate};
use protobuf::Message;

#[test]
fn extracts_state_updates_without_decoding_or_changing_configuration() {
  let mut generated = ConfigurationUpdate {
    version_nonce: "version".to_owned(),
    update_type: Some(Update_type::StateOfTheWorld(StateOfTheWorld::default())),
    client_state_updates: vec![ClientStateUpdate::default()],
    ..Default::default()
  };
  let raw = RawConfigurationUpdate::new(&generated.write_to_bytes().unwrap()).unwrap();
  assert_eq!(raw.version_nonce, "version");
  assert_eq!(raw.client_state_updates, generated.client_state_updates);
  generated.client_state_updates.clear();
  assert_eq!(raw.bytes, generated.write_to_bytes().unwrap());
}

#[test]
fn retains_uninterpreted_configuration_payload() {
  let bytes = [10, 1, b'v', 18, 3, 34, 1, 0];
  assert!(ConfigurationUpdate::parse_from_bytes(&bytes).is_err());
  let raw = RawConfigurationUpdate::new(&bytes).unwrap();
  assert_eq!(raw.bytes, bytes);
  assert_eq!(raw.version_nonce, "v");
}
