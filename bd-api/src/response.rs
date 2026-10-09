#[cfg(test)]
#[path = "./response_test.rs"]
mod tests;

use bd_client_common::RawConfigurationUpdate;
use bd_grpc_codec::{DecodingResult, Error, Result};
use bd_macros::proto_deserialize;
use bd_proto::protos::client::api::{
  ApiResponse as ProtoResponse,
  DeviceCommandUpdateAck,
  ErrorShutdown,
  FlushBuffers,
  HandshakeResponse,
  LogUploadIntentResponse,
  LogUploadResponse,
  RuntimeUpdate,
  SankeyIntentResponse,
  SankeyPathUploadResponse,
  StatsUploadResponse,
  UploadArtifactIntentResponse,
  UploadArtifactResponse,
};
use bd_proto_util::serialization::inline::ProtoDeserialize;
use bytes::Bytes;

//
// Response
//

#[proto_deserialize]
pub(super) enum Response {
  #[field(id = 1)]
  Handshake(HandshakeResponse),
  #[field(id = 2)]
  LogUpload(LogUploadResponse),
  #[field(id = 3)]
  Pong,
  #[field(id = 4)]
  ConfigurationUpdate(RawConfigurationUpdate),
  #[field(id = 5)]
  RuntimeUpdate(RuntimeUpdate),
  #[field(id = 6)]
  ErrorShutdown(ErrorShutdown),
  #[field(id = 7)]
  StatsUpload(StatsUploadResponse),
  #[field(id = 8)]
  LogUploadIntent(LogUploadIntentResponse),
  #[field(id = 9)]
  FlushBuffers(FlushBuffers),
  #[field(id = 12)]
  SankeyDiagramUpload(SankeyPathUploadResponse),
  #[field(id = 13)]
  SankeyIntentResponse(SankeyIntentResponse),
  #[field(id = 14)]
  ArtifactUpload(UploadArtifactResponse),
  #[field(id = 15)]
  ArtifactIntent(UploadArtifactIntentResponse),
  #[field(id = 16)]
  StateUpdate,
  #[field(id = 17)]
  DeviceCommandUpdateAck(DeviceCommandUpdateAck),
}

//
// ApiResponse
//

#[proto_deserialize]
pub(super) struct ApiResponse {
  #[field(oneof)]
  pub response_type: Option<Response>,
}

impl ApiResponse {
  #[inline(never)]
  fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
    Self::from_proto_bytes(bytes)
  }
}

impl DecodingResult for ApiResponse {
  type Message = ProtoResponse;

  fn from_flags_and_bytes(_flags: u8, bytes: Bytes) -> Result<Self> {
    Self::decode(&bytes).map_err(|error| {
      log::debug!("failed to decode inline API response: {error}");
      Error::Protocol("invalid protobuf API response")
    })
  }

  fn message(&self) -> Option<&Self::Message> {
    None
  }
}
