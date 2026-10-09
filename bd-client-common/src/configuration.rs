#[cfg(test)]
#[path = "./configuration_test.rs"]
mod tests;

use bd_macros::proto_deserialize;
use bd_proto::protos::client::api::ClientStateUpdate;
use bd_proto_util::serialization::inline::{Message, ProtoDeserialize};

//
// RawConfigurationUpdate
//

/// A configuration's original wire payload, without the separately applied state updates.
#[derive(Debug)]
#[proto_deserialize]
pub struct RawConfigurationUpdate {
  #[field(message, deserialize_with = "configuration_payload")]
  pub bytes: Vec<u8>,
  #[field(id = 1)]
  pub version_nonce: String,
  #[field(id = 3)]
  pub client_state_updates: Vec<ClientStateUpdate>,
}

impl RawConfigurationUpdate {
  pub fn new(bytes: &[u8]) -> anyhow::Result<Self> {
    Self::from_proto_bytes(bytes)
  }
}

fn configuration_payload(message: &Message<'_>) -> anyhow::Result<Vec<u8>> {
  message.without_fields(&[3])
}
