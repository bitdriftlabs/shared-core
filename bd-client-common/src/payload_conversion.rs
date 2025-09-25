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
use bd_proto::protos::client::api::{
  ApiRequest,
  ConfigurationUpdateAck,
  DebugDataRequest,
  HandshakeRequest,
  LogUploadIntentRequest,
  LogUploadRequest,
  PingRequest,
  SankeyIntentRequest,
  SankeyPathUploadRequest,
  StatsUploadRequest,
  UploadArtifactIntentRequest,
  UploadArtifactRequest,
};

/// Used to allow the API mux operate against multiple different transport APIs. The code is able
/// to operate against the inner types that can be wrapped up into the appropriate mux type
/// depending on the use case.
pub trait IntoRequest {
  fn into_request(self) -> ApiRequest;
}

pub struct RuntimeConfigurationUpdateAck(pub ConfigurationUpdateAck);
pub struct ClientConfigurationUpdateAck(pub ConfigurationUpdateAck);

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
into_api_request!(DebugDataRequest, Request_type::DebugData);

impl IntoRequest for RuntimeConfigurationUpdateAck {
  fn into_request(self) -> ApiRequest {
    ApiRequest {
      request_type: Some(Request_type::RuntimeUpdateAck(self.0)),
      ..Default::default()
    }
  }
}

impl IntoRequest for ClientConfigurationUpdateAck {
  fn into_request(self) -> ApiRequest {
    ApiRequest {
      request_type: Some(Request_type::ConfigurationUpdateAck(self.0)),
      ..Default::default()
    }
  }
}
