// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use anyhow::anyhow;
pub use bd_proto::protos::client::api::log_upload_intent_request::{
  Intent_type,
  WorkflowActionUpload,
};
pub use bd_proto::protos::client::api::log_upload_intent_response::Decision;
pub use bd_proto::protos::client::api::LogUploadIntentRequest;
use bd_proto::protos::client::api::{
  LogUploadRequest,
  SankeyIntentRequest,
  SankeyPathUploadRequest,
  StatsUploadRequest,
};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug)]
pub struct UploadResponse {
  pub success: bool,
  pub uuid: String,
}

/// Used to track pending upload requests. Requests are identified by a UUID recorded at
/// upload time, which is used to correlate it with incoming response. Each pending request
/// maintains a oneshot channel which is used to signal back to the uploader the result of the
/// upload attempt.
pub struct StateTracker {
  // A map of pending uploads to their response channel. This is used to
  // communicate back the result of a log or stats upload back to the upload task.
  pending_uploads: HashMap<String, tokio::sync::oneshot::Sender<UploadResponse>>,

  // A map of pending log intents to their response channel. This is used to communicate back the
  // result of a log intent request back to the upload task.
  pending_intents: HashMap<String, tokio::sync::oneshot::Sender<Decision>>,
}

impl StateTracker {
  #[must_use]
  pub fn new() -> Self {
    Self {
      pending_uploads: HashMap::new(),
      pending_intents: HashMap::new(),
    }
  }

  /// Track a log upload intent, converting it into an upload intent request.
  pub fn track_intent<T: Send + Sync>(&mut self, intent: Tracked<T, Decision>) -> T {
    let uuid = intent.uuid.clone();
    let (request, response_tx) = intent.into_parts();
    self.pending_intents.insert(uuid, response_tx);

    request
  }

  /// Track the upload object, converting it into an upload request.
  pub fn track_upload<T: Send + Sync>(&mut self, upload: Tracked<T, UploadResponse>) -> T {
    let uuid = upload.uuid.clone();
    let (request, response_tx) = upload.into_parts();
    self.pending_uploads.insert(uuid, response_tx);

    request
  }

  /// Resolve an upload response against pending uploads. This may fail if the upload UUID does not
  /// correspond to a pending request.
  pub fn resolve_pending_upload(&mut self, uuid: &str, error: &str) -> anyhow::Result<()> {
    // The receiver might be dropped on shutdown, so ignore failures.
    let _ignored = self
      .pending_uploads
      .remove(uuid)
      .ok_or_else(|| anyhow!("State for request with uuid {uuid:?} was inconsistent"))?
      .send(UploadResponse {
        success: error.is_empty(),
        uuid: uuid.to_string(),
      });

    Ok(())
  }

  pub fn resolve_intent(&mut self, uuid: &str, response: Decision) -> anyhow::Result<()> {
    let _ignored = self
      .pending_intents
      .remove(uuid)
      .ok_or_else(|| anyhow!("Log upload state for uuid {uuid:?} was inconsistent"))?
      .send(response);

    Ok(())
  }
}

impl Default for StateTracker {
  fn default() -> Self {
    Self::new()
  }
}

/// A tracked upload is an upload with an associated UUID and responses channel.
/// By inspecting responses and correlating it using the UUID, the API mux is able to notify the
/// caller about the result of uploading this upload by.
/// This is used by both stats and log uploads.
#[derive(Debug)]
pub struct Tracked<PayloadType, R> {
  // The UUID of the upload. This may be reused between attempts payload as an
  // idempotence token.
  pub uuid: String,

  // The upload payload.
  pub payload: PayloadType,

  // The response channel indicating the result of the upload.
  pub response_tx: tokio::sync::oneshot::Sender<R>,
}

impl<PayloadType: Send + Sync, R> Tracked<PayloadType, R> {
  // Creates a new upload and a receiver for the result of the upload attempt.
  pub fn new(uuid: String, payload: PayloadType) -> (Self, tokio::sync::oneshot::Receiver<R>) {
    let (response_tx, response_rx) = tokio::sync::oneshot::channel();

    (
      Self {
        uuid,
        payload,
        response_tx,
      },
      response_rx,
    )
  }

  #[must_use]
  pub fn upload_uuid() -> String {
    Uuid::new_v4().to_string()
  }

  fn into_parts(self) -> (PayloadType, tokio::sync::oneshot::Sender<R>) {
    (self.payload, self.response_tx)
  }

  /// Returns a new Tracked with the transformation function applied to the payload.
  pub fn map_payload<T>(self, f: impl FnOnce(PayloadType) -> T) -> Tracked<T, R> {
    Tracked {
      uuid: self.uuid,
      payload: f(self.payload),
      response_tx: self.response_tx,
    }
  }
}

/// A number of logs prepared for upload.
#[derive(Debug)]
pub struct LogBatch {
  /// A list of logs to be uploaded.
  pub logs: Vec<Vec<u8>>,

  /// The id of the buffer the logs are being uploaded from.
  pub buffer_id: String,
}

/// A batch of logs sent to be uploaded. The upload is wrapped in an Arc to allow for cheap retries.
pub type TrackedLogBatch = Tracked<LogUploadRequest, UploadResponse>;

pub type TrackedStatsUploadRequest = Tracked<StatsUploadRequest, UploadResponse>;

pub type TrackedSankeyPathUploadRequest = Tracked<SankeyPathUploadRequest, UploadResponse>;

pub type TrackedSankeyPathUploadIntentRequest = Tracked<SankeyIntentRequest, Decision>;

/// An intent to upload a buffer due to a listener triggering. This is communicated to the backend
/// in order to allow the server to make decisions on whether a buffer should be uploaded in
/// response to a specific listener.
pub type TrackedLogUploadIntent = Tracked<LogUploadIntentRequest, Decision>;
