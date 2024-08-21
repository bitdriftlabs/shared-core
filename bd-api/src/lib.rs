// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_proto::protos::client::api::log_upload_intent_response::Decision;
use bd_proto::protos::client::api::{
  ApiRequest,
  ApiResponse,
  ErrorShutdown,
  FlushBuffers,
  HandshakeRequest,
  HandshakeResponse,
  LogUploadIntentRequest,
  LogUploadIntentResponse,
  LogUploadRequest,
  LogUploadResponse,
  OpaqueRequest,
  OpaqueResponse,
  PongResponse,
  StatsUploadRequest,
  StatsUploadResponse,
};
use std::collections::HashMap;
use upload::Tracked;

pub mod api;
mod payload_conversion;
pub mod upload;

pub use bd_metadata::{Metadata, Platform};
pub use payload_conversion::{ClientConfigurationUpdate, RuntimeConfigurationUpdate};

#[cfg(test)]
#[ctor::ctor]
fn global_init() {
  bd_test_helpers::test_global_init();
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

/// Used to unwrap a mulitplexing `ResponseType` into an inner type.
pub trait FromResponse<ResponseType> {
  fn from_response(response: &ResponseType) -> Option<&Self>;
}

/// Used to define a configuration pipeline that receives configuration updates through the
/// multiplexing API. The configuration pipeline may optioanlly support disk persistence.
#[async_trait::async_trait]
pub trait ConfigurationUpdate: Send + Sync {
  /// Attempt to apply a new inbound configuration. Returns None if the response does not apply
  /// to this configuration type, otherwise returns the ack/nack after attempting to apply the
  /// config.
  async fn try_apply_config(&mut self, response: &ApiResponse) -> Option<ApiRequest>;

  /// Attempts to load persisted config from disk if supported by the configuration type.
  async fn try_load_persisted_config(&mut self);

  /// Provides a partial handshake that is used to include the version nonce for this configuration
  /// type into the handshake request.
  fn partial_handshake(&self) -> HandshakeRequest;

  /// Called to allow the configuration pipeline to react to the server being available.
  fn on_handshake_complete(&self);
}

//
// ResponseKind
//

/// A transport independent representation of the possible multiplexed response types.
pub enum ResponseKind<'a> {
  Handshake(&'a HandshakeResponse),
  ErrorShutdown(&'a ErrorShutdown),
  Pong(&'a PongResponse),
  LogUpload(&'a LogUploadResponse),
  LogUploadIntent(&'a LogUploadIntentResponse),
  StatsUpload(&'a StatsUploadResponse),
  FlushBuffers(&'a FlushBuffers),
  Opaque(&'a OpaqueResponse),
  Untyped,
}

//
// MuxResponse
//

/// Used to convert a response type into the transport indendent `ResponseKind`.
pub trait MuxResponse {
  fn demux(&self) -> Option<ResponseKind<'_>>;
}

/// Wrapper around an generic uploadable payload which can be converted into `RequestType`.
#[derive(Debug)]
pub enum DataUpload {
  /// A logs upload intent request sent to the server to ask whether a specific upload should be
  /// performed.
  LogsUploadIntentRequest(Tracked<LogUploadIntentRequest, Decision>),

  /// A logs upload request with an associated tracking id that is used to ensure delivery.
  LogsUploadRequest(Tracked<LogUploadRequest, bool>),

  /// A stats upload request with an associated tracking id that is used to ensure delivery.
  StatsUploadRequest(Tracked<StatsUploadRequest, bool>),

  /// An opaque request with an associated tracking id that is used to ensure delivery. This allows
  /// for uploading of payloads which are not directly typed to the mux.
  OpaqueRequest(Tracked<OpaqueRequest, bool>),
}

//
// TriggerUpload
//

/// A trigger upload.
#[derive(Clone, Debug)]
pub struct TriggerUpload {
  // The list of identifiers of the buffers whose content should be uploaded.
  pub buffer_ids: Vec<String>,
}

impl TriggerUpload {
  #[must_use]
  pub fn new(buffer_ids: Vec<String>) -> Self {
    Self { buffer_ids }
  }
}


//
// StreamEvent
//

// Describes an event received via the platform networking APIs.
// Either we're seeing data come in on an existing stream, or a stream is being closed.
#[derive(Debug)]
pub enum StreamEvent {
  /// We received raw data from the platform network.
  Data(Vec<u8>),

  /// The stream closed due to the provided reason.
  StreamClosed(String),
}

pub type StreamEventSender = tokio::sync::mpsc::Sender<StreamEvent>;

//
// PlatformNetworkManager
//

// This trait describes an implementation of the platform networking,
// allowing the api task to initiate and send data over an API stream that connects to the Loop
// backend.
#[async_trait::async_trait]
pub trait PlatformNetworkManager<RuntimeLoader>: Send + Sync {
  /// Starts a new stream with the channel sender to use to forward upstream events to the api
  /// task. Returns a handle that can be used to transmit data over the stream.
  /// This may fail if due to FFI issues.
  async fn start_stream(
    &self,
    event_tx: StreamEventSender,
    runtime: &RuntimeLoader,
    headers: &HashMap<&str, &str>,
  ) -> anyhow::Result<Box<dyn PlatformNetworkStream>>;
}

//
// PlatformNetworkStream
//

/// A trait describing a stream handle, which is used to transmit data over an active API stream.
#[async_trait::async_trait]
pub trait PlatformNetworkStream: Send {
  /// Sends data over the stream.
  /// This may fail due to FFI issues, e.g. an unhandled exception.
  async fn send_data(&mut self, data: &[u8]) -> anyhow::Result<()>;
}
