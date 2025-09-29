// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![deny(
  clippy::expect_used,
  clippy::panic,
  clippy::todo,
  clippy::unimplemented,
  clippy::unreachable,
  clippy::unwrap_used
)]

use backoff::SystemClock;
use backoff::exponential::{ExponentialBackoff, ExponentialBackoffBuilder};
use bd_proto::protos::client::api::{
  DebugDataRequest,
  LogUploadIntentRequest,
  LogUploadRequest,
  SankeyIntentRequest,
  SankeyPathUploadRequest,
  StatsUploadRequest,
  UploadArtifactIntentRequest,
  UploadArtifactRequest,
};
use bd_runtime::runtime::DurationWatch;
use bd_runtime::runtime::api::{InitialBackoffInterval, MaxBackoffInterval};
use std::collections::HashMap;
use upload::{TrackedIntent, TrackedUpload};

pub mod api;
pub mod upload;

pub use bd_metadata::{Metadata, Platform};

#[cfg(test)]
#[ctor::ctor]
fn global_init() {
  bd_test_helpers::test_global_init();
}

/// Wrapper around an generic uploadable payload which can be converted into `RequestType`.
#[derive(Debug)]
pub enum DataUpload {
  /// A logs upload intent request sent to the server to ask whether a specific upload should be
  /// performed.
  LogsUploadIntent(TrackedIntent<LogUploadIntentRequest>),

  /// A logs upload request with an associated tracking id that is used to ensure delivery.
  LogsUpload(TrackedUpload<LogUploadRequest>),

  /// An ackless logs upload request.
  AcklessLogsUpload(LogUploadRequest),

  /// A stats upload request with an associated tracking id that is used to ensure delivery.
  StatsUpload(TrackedUpload<StatsUploadRequest>),

  /// An intent to upload a Sankey path due collected by a workflow.
  SankeyPathUploadIntent(TrackedIntent<SankeyIntentRequest>),

  /// A Sankey path upload request.
  SankeyPathUpload(TrackedUpload<SankeyPathUploadRequest>),

  /// An intent to upload an generic artifact.
  ArtifactUploadIntent(TrackedIntent<UploadArtifactIntentRequest>),

  /// An generic artifact upload request.
  ArtifactUpload(TrackedUpload<UploadArtifactRequest>),

  /// A request to upload debug data.
  DebugData(DebugDataRequest),
}

//
// TriggerUpload
//

/// A trigger upload.
#[derive(Debug)]
pub struct TriggerUpload {
  // The list of identifiers of the buffers whose content should be uploaded.
  pub buffer_ids: Vec<String>,

  // A channel to notify the caller that the upload has been completed.
  pub response_tx: tokio::sync::oneshot::Sender<()>,
}

impl TriggerUpload {
  #[must_use]
  pub const fn new(buffer_ids: Vec<String>, response_tx: tokio::sync::oneshot::Sender<()>) -> Self {
    Self {
      buffer_ids,
      response_tx,
    }
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

/// Constructs a new `ExponentialBackoff` based on the current runtime values.
pub fn backoff_policy(
  initial_backoff_interval: &mut DurationWatch<InitialBackoffInterval>,
  max_backoff_interval: &mut DurationWatch<MaxBackoffInterval>,
) -> ExponentialBackoff<SystemClock> {
  ExponentialBackoffBuilder::<SystemClock>::new()
    .with_initial_interval(initial_backoff_interval.read_mark_update().unsigned_abs())
    .with_max_interval(max_backoff_interval.read_mark_update().unsigned_abs())
    .with_max_elapsed_time(None)
    .build()
}
