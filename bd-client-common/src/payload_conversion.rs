// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// This file contains all the trait implementations necessary to wire up de-/multiplexing of both
// the client and server APIs. We keep this centralized to reuse helper macros to reduce some of
// the boilerplate - in prod the build should be targeting one of the types and compile out the
// unnecessary types.

// Since both client configuration and runtime wraps the same ConfigurationUpdateAck proto we need
// to wrap them up in additional type information in order to differentiate between the two of
// them.

use bd_proto::protos::client::api::api_request::Request_type;
use bd_proto::protos::client::api::api_response::Response_type;
use bd_proto::protos::client::api::{
  ApiRequest,
  ApiResponse,
  ConfigurationUpdate,
  ConfigurationUpdateAck,
  ErrorShutdown,
  FlushBuffers,
  HandshakeRequest,
  HandshakeResponse,
  LogUploadIntentRequest,
  LogUploadIntentResponse,
  LogUploadRequest,
  LogUploadResponse,
  PingRequest,
  PongResponse,
  RuntimeUpdate,
  SankeyIntentRequest,
  SankeyIntentResponse,
  SankeyPathUploadRequest,
  SankeyPathUploadResponse,
  StatsUploadRequest,
  StatsUploadResponse,
  UploadArtifactIntentRequest,
  UploadArtifactIntentResponse,
  UploadArtifactRequest,
  UploadArtifactResponse,
};

//
// ResponseKind
//

/// A transport independent representation of the possible multiplexed response types.
pub enum ResponseKind {
  Handshake(HandshakeResponse),
  ErrorShutdown(ErrorShutdown),
  Pong(PongResponse),
  LogUpload(LogUploadResponse),
  LogUploadIntent(LogUploadIntentResponse),
  StatsUpload(StatsUploadResponse),
  FlushBuffers(FlushBuffers),
  SankeyPathUpload(SankeyPathUploadResponse),
  SankeyPathUploadIntent(SankeyIntentResponse),
  ArtifactUploadIntent(UploadArtifactIntentResponse),
  ArtifactUpload(UploadArtifactResponse),
  ConfigurationUpdate(ConfigurationUpdate),
  RuntimeUpdate(RuntimeUpdate),
}

//
// MuxResponse
//

/// Used to convert a response type into the transport independent `ResponseKind`.
pub trait MuxResponse {
  fn demux(self) -> Option<ResponseKind>;
}

/// Used to allow the API mux operate against multiple different transport APIs. The code is able
/// to operate against the inner types that can be wrapped up into the appropriate mux type
/// depending on the use case.
pub trait IntoRequest {
  fn into_request(self) -> ApiRequest;
}

//
// FromResponse
//

/// Used to unwrap a multiplexing `ResponseType` into an inner type.
pub trait FromResponse<ResponseType> {
  fn from_response(response: &ResponseType) -> Option<&Self>;
}

pub struct RuntimeConfigurationUpdate(pub ConfigurationUpdateAck);
pub struct ClientConfigurationUpdate(pub ConfigurationUpdateAck);

macro_rules! unwrap_response {
  ($wrapper:ty, $inner:ty, $field:path) => {
    impl crate::payload_conversion::FromResponse<$wrapper> for $inner {
      fn from_response(response: &$wrapper) -> Option<&Self> {
        match &response.response_type {
          Some($field(inner)) => Some(inner),
          _ => None,
        }
      }
    }
  };
}

// Helper macro for defining IntoRequest for a wrapper type where an inner request type is wrapped
// using the provided oneof branch.
macro_rules! into_api_request {
  ($type:tt, $oneof:expr) => {
    impl crate::payload_conversion::IntoRequest for $type {
      fn into_request(self) -> ApiRequest {
        ApiRequest {
          request_type: Some($oneof(self)),
          ..Default::default()
        }
      }
    }
  };
}

unwrap_response!(
  ApiResponse,
  ConfigurationUpdate,
  Response_type::ConfigurationUpdate
);
unwrap_response!(ApiResponse, RuntimeUpdate, Response_type::RuntimeUpdate);

impl crate::payload_conversion::IntoRequest for ApiRequest {
  fn into_request(self) -> Self {
    self
  }
}

into_api_request!(StatsUploadRequest, Request_type::StatsUpload);
into_api_request!(LogUploadRequest, Request_type::LogUpload);
into_api_request!(PingRequest, Request_type::Ping);
into_api_request!(HandshakeRequest, Request_type::Handshake);
into_api_request!(LogUploadIntentRequest, Request_type::LogUploadIntent);
into_api_request!(SankeyIntentRequest, Request_type::SankeyIntent);
into_api_request!(SankeyPathUploadRequest, Request_type::SankeyPathUpload);
into_api_request!(UploadArtifactRequest, Request_type::ArtifactUpload);
into_api_request!(UploadArtifactIntentRequest, Request_type::ArtifactIntent);

impl IntoRequest for RuntimeConfigurationUpdate {
  fn into_request(self) -> ApiRequest {
    ApiRequest {
      request_type: Some(Request_type::RuntimeUpdateAck(self.0)),
      ..Default::default()
    }
  }
}

impl IntoRequest for ClientConfigurationUpdate {
  fn into_request(self) -> ApiRequest {
    ApiRequest {
      request_type: Some(Request_type::ConfigurationUpdateAck(self.0)),
      ..Default::default()
    }
  }
}

impl MuxResponse for ApiResponse {
  fn demux(self) -> Option<ResponseKind> {
    match self.response_type? {
      Response_type::Handshake(handshake) => Some(ResponseKind::Handshake(handshake)),
      Response_type::LogUpload(log_upload) => Some(ResponseKind::LogUpload(log_upload)),
      Response_type::LogUploadIntent(intent) => Some(ResponseKind::LogUploadIntent(intent)),
      Response_type::StatsUpload(stats_upload) => Some(ResponseKind::StatsUpload(stats_upload)),
      Response_type::Pong(pong) => Some(ResponseKind::Pong(pong)),
      Response_type::ErrorShutdown(e) => Some(ResponseKind::ErrorShutdown(e)),
      Response_type::FlushBuffers(f) => Some(ResponseKind::FlushBuffers(f)),
      Response_type::SankeyDiagramUpload(s) => Some(ResponseKind::SankeyPathUpload(s)),
      Response_type::SankeyIntentResponse(s) => Some(ResponseKind::SankeyPathUploadIntent(s)),
      Response_type::ConfigurationUpdate(u) => Some(ResponseKind::ConfigurationUpdate(u)),
      Response_type::RuntimeUpdate(r) => Some(ResponseKind::RuntimeUpdate(r)),
      Response_type::ArtifactUpload(u) => Some(ResponseKind::ArtifactUpload(u)),
      Response_type::ArtifactIntent(u) => Some(ResponseKind::ArtifactUploadIntent(u)),
    }
  }
}
