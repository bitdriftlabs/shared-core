// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/licenses/strict/1.0.0

use anyhow::anyhow;
use bd_proto::protos::bdtail::bdtail_config::DeviceCommandRequest;
pub use bd_proto::protos::client::api::LogUploadIntentRequest;
use bd_proto::protos::client::api::device_command_update_ack::Disposition;
pub use bd_proto::protos::client::api::log_upload_intent_request::{
  Intent_type,
  WorkflowActionUpload,
};
pub use bd_proto::protos::client::api::log_upload_intent_response::Decision as LogsUploadDecision;
use bd_proto::protos::client::api::sankey_intent_response::Decision as SankeyPathUploadDecision;
use bd_proto::protos::client::api::{
  DeviceCommandResultContext,
  DeviceCommandUpdate,
  DeviceCommandUpdateAck,
  LogUploadRequest,
  SankeyIntentRequest,
  SankeyPathUploadRequest,
  StatsUploadRequest,
  UploadArtifactIntentRequest,
  UploadArtifactRequest,
  device_command_update,
  upload_artifact_intent_response,
};
use bd_proto::protos::logging::payload::Data;
use bd_proto::protos::logging::payload::data::Data_type;
use bd_time::TimeProvider;
use parking_lot::Mutex;
use protobuf::Message;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use upload_artifact_intent_response::Decision as ArtifactIntentDecision;
use uuid::Uuid;

//
// UploadResponse
//

#[derive(Debug)]
pub struct UploadResponse {
  pub success: bool,
  pub uuid: String,
}

//
// IntentDecision
//

#[derive(Clone, Debug)]
pub enum IntentDecision {
  Drop,
  UploadImmediately,
}

impl From<SankeyPathUploadDecision> for IntentDecision {
  fn from(decision: SankeyPathUploadDecision) -> Self {
    match decision {
      SankeyPathUploadDecision::Drop(_) => Self::Drop,
      SankeyPathUploadDecision::UploadImmediately(_) => Self::UploadImmediately,
    }
  }
}

impl From<LogsUploadDecision> for IntentDecision {
  fn from(decision: LogsUploadDecision) -> Self {
    match decision {
      LogsUploadDecision::Drop(_) => Self::Drop,
      LogsUploadDecision::UploadImmediately(_) => Self::UploadImmediately,
    }
  }
}

impl From<ArtifactIntentDecision> for IntentDecision {
  fn from(decision: ArtifactIntentDecision) -> Self {
    match decision {
      ArtifactIntentDecision::Drop(_) => Self::Drop,
      ArtifactIntentDecision::UploadImmediately(_) => Self::UploadImmediately,
    }
  }
}

//
// IntentResponse
//

#[derive(Clone, Debug)]
pub struct IntentResponse {
  pub uuid: String,
  pub decision: IntentDecision,
}

/// Used to track pending upload requests. Requests are identified by a UUID recorded at
/// upload time, which is used to correlate it with incoming response. Each pending request
/// maintains a oneshot channel which is used to signal back to the uploader the result of the
/// upload attempt.
pub struct StateTracker {
  // A map of pending uploads to their response channel. This is used to
  // communicate back the result of a log, sankey path or stats upload back to the upload task.
  pending_uploads: HashMap<String, tokio::sync::oneshot::Sender<UploadResponse>>,

  // A map of pending log intents to their response channel. This is used to communicate back the
  // result of an intent request back to the upload task.
  pending_intents: HashMap<String, tokio::sync::oneshot::Sender<IntentResponse>>,
}

impl StateTracker {
  #[must_use]
  pub fn new() -> Self {
    Self {
      pending_uploads: HashMap::new(),
      pending_intents: HashMap::new(),
    }
  }

  #[must_use]
  pub fn has_pending_requests(&self) -> bool {
    !self.pending_uploads.is_empty() || !self.pending_intents.is_empty()
  }

  /// Track a log upload intent, converting it into an upload intent request.
  pub fn track_intent<T: Send + Sync>(&mut self, intent: Tracked<T, IntentResponse>) -> T {
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

  pub fn resolve_intent(
    &mut self,
    uuid: &str,
    decision: Option<impl Into<IntentDecision>>,
  ) -> anyhow::Result<()> {
    let _ignored = self
      .pending_intents
      .remove(uuid)
      .ok_or_else(|| anyhow!("Upload intent state for uuid {uuid:?} was inconsistent"))?
      .send(IntentResponse {
        uuid: uuid.to_string(),
        decision: decision.map_or_else(|| IntentDecision::Drop, Into::into),
      });

    Ok(())
  }
}

//
// DeviceCommandState
//

pub const MAX_KNOWN_COMMANDS: usize = 64;
const MAX_PENDING_COMMAND_UPDATE_BYTES: usize = 4 * 1024 * 1024;

/// Admission and deduplication state for one live SDK, shared with every replacement API stream.
/// Nothing here is persisted: a recreated SDK must not inherit execution authority.
pub struct DeviceCommandState {
  /// Fresh per SDK, not per connection. Dispatches must name this owner before being admitted.
  pub sdk_instance_id: String,
  time_provider: Arc<dyn TimeProvider>,
  pending_update_bytes: Arc<Semaphore>,
  // Keep IDs through their original expiry, including after completion, so configuration
  // omission/reappearance cannot execute a command twice. Values contain no result payloads.
  commands: Mutex<BTreeMap<String, protobuf::well_known_types::timestamp::Timestamp>>,
}

impl DeviceCommandState {
  pub fn new(time_provider: Arc<dyn TimeProvider>) -> Self {
    Self {
      sdk_instance_id: Uuid::new_v4().to_string(),
      time_provider,
      pending_update_bytes: Arc::new(Semaphore::new(MAX_PENDING_COMMAND_UPDATE_BYTES)),
      commands: Mutex::default(),
    }
  }

  #[cfg(test)]
  pub fn known_command_ids(&self) -> Vec<String> {
    let now = self.time_provider.now();
    let mut commands = self.commands.lock();
    commands.retain(|_, deadline| {
      (deadline.seconds, deadline.nanos) > (now.unix_timestamp(), now.nanosecond().cast_signed())
    });
    commands.keys().cloned().collect()
  }

  /// Atomically claims a new invocation before execution can be spawned. Capacity is released by
  /// original expiry, never by an ACK or by removing a command from a configuration.
  pub fn admit(&self, command: &DeviceCommandRequest) -> bool {
    let Some(deadline) = command.expires_at.as_ref() else {
      return false;
    };
    let now = self.time_provider.now();
    if command.sdk_instance_id.as_str() != self.sdk_instance_id
      || Uuid::parse_str(&command.command_id).is_err()
      || (deadline.seconds, deadline.nanos)
        <= (now.unix_timestamp(), now.nanosecond().cast_signed())
    {
      return false;
    }
    let mut commands = self.commands.lock();
    commands.retain(|_, expiry| {
      (expiry.seconds, expiry.nanos) > (now.unix_timestamp(), now.nanosecond().cast_signed())
    });
    if commands.len() >= MAX_KNOWN_COMMANDS || commands.contains_key(command.command_id.as_str()) {
      return false;
    }
    commands.insert(command.command_id.to_string(), deadline.clone());
    true
  }

  /// Returns the earliest deadline worth retrying after capacity rejection. Invalid, expired,
  /// wrong-owner, and already-admitted commands must not enter the deferred admission queue.
  pub fn admission_retry_delay(
    &self,
    command: &DeviceCommandRequest,
  ) -> Option<std::time::Duration> {
    if command.sdk_instance_id.as_str() != self.sdk_instance_id
      || Uuid::parse_str(&command.command_id).is_err()
    {
      return None;
    }
    let deadline = command.expires_at.as_ref()?;
    let now = self.time_provider.now();
    let commands = self.commands.lock();
    if commands.contains_key(command.command_id.as_str())
      || (deadline.seconds, deadline.nanos)
        <= (now.unix_timestamp(), now.nanosecond().cast_signed())
    {
      return None;
    }
    let deadline = commands
      .values()
      .chain(std::iter::once(deadline))
      .min_by_key(|expiry| (expiry.seconds, expiry.nanos))?;
    let deadline = time::OffsetDateTime::from_unix_timestamp(deadline.seconds)
      .ok()?
      .replace_nanosecond(u32::try_from(deadline.nanos).ok()?)
      .ok()?;
    Some((deadline - now).try_into().unwrap_or_default())
  }

  /// Converts the immutable server deadline into a relative budget for an update waiter. A mux
  /// reconnect must reuse this budget's deadline rather than grant another full lifetime.
  pub fn remaining_time(&self, command_id: &str) -> Option<std::time::Duration> {
    let commands = self.commands.lock();
    let deadline = commands.get(command_id)?;
    let deadline = time::OffsetDateTime::from_unix_timestamp(deadline.seconds)
      .ok()?
      .replace_nanosecond(u32::try_from(deadline.nanos).ok()?)
      .ok()?;
    Some(
      (deadline - self.time_provider.now())
        .try_into()
        .unwrap_or_default(),
    )
  }
}

//
// DeviceCommandUpdateTracker
//

/// Retains command updates and their original waiters across streams, but not SDK restarts.
#[derive(Default)]
pub struct DeviceCommandUpdateTracker {
  // Sequence ordering makes replay deterministic. Expiry tasks hold only weak references, so
  // they cannot keep the tracker alive after the API/SDK is destroyed.
  pending_device_command_updates: Arc<Mutex<BTreeMap<(String, u64), PendingDeviceCommandUpdate>>>,
}

struct PendingDeviceCommandUpdate {
  update: DeviceCommandUpdate,
  response_tx: CommandUpdateSender,
}

impl DeviceCommandUpdateTracker {
  #[must_use]
  pub fn retained_update_count(&self) -> usize {
    self.pending_device_command_updates.lock().len()
  }

  #[must_use]
  pub fn has_pending_updates(&self) -> bool {
    self
      .pending_device_command_updates
      .lock()
      .values()
      .any(|pending| pending.response_tx.is_pending())
  }

  #[must_use]
  pub fn pending_updates(&self) -> Vec<DeviceCommandUpdate> {
    self
      .pending_device_command_updates
      .lock()
      .values()
      .filter(|pending| pending.response_tx.is_pending())
      .map(|pending| pending.update.clone())
      .collect()
  }

  /// Tracks a command update until its acknowledgement reports that it was durably persisted.
  pub fn track_device_command_update(
    &mut self,
    update: TrackedDeviceCommandUpdate,
  ) -> anyhow::Result<DeviceCommandUpdate> {
    let mut pending_updates = self.pending_device_command_updates.lock();
    pending_updates.retain(|_, pending| pending.response_tx.is_pending());
    let TrackedDeviceCommandUpdate {
      payload,
      mut response_tx,
      ..
    } = update;
    let key = (payload.command_id.clone(), payload.update_sequence_number);
    if pending_updates.contains_key(&key) {
      anyhow::bail!(
        "device command update is already pending: command_id={:?}, sequence={}",
        key.0,
        key.1
      );
    }

    let pending_bytes = pending_updates
      .values()
      .map(|pending| pending.update.compute_size())
      .sum::<u64>();
    if pending_updates.len() >= MAX_KNOWN_COMMANDS
      || pending_bytes + payload.compute_size() > MAX_PENDING_COMMAND_UPDATE_BYTES as u64
    {
      anyhow::bail!("pending device command update capacity exceeded");
    }

    let request = payload.clone();
    if let Some(expires_at) = response_tx.expires_at {
      // Release the payload and sender at the original deadline. This must run
      // independently of the stream loop, which may be parked offline or killed indefinitely.
      let pending = Arc::downgrade(&self.pending_device_command_updates);
      let key = key.clone();
      response_tx.expiry_task = Some(tokio::spawn(async move {
        tokio::time::sleep_until(expires_at).await;
        if let Some(pending) = pending.upgrade() {
          let mut pending = pending.lock();
          pending.remove(&key);
        }
      }));
    }
    pending_updates.insert(
      key,
      PendingDeviceCommandUpdate {
        update: payload,
        response_tx,
      },
    );
    log::debug!(
      "retained device command updates: count={}, bytes={}",
      pending_updates.len(),
      pending_bytes + request.compute_size()
    );
    Ok(request)
  }

  /// Only APPLIED releases execution successfully. RETIRED drops the sender to fail its waiter;
  /// transient or unknown dispositions retain the original payload and sender for exact replay.
  pub fn resolve_device_command_update(&mut self, acknowledgement: &DeviceCommandUpdateAck) {
    let disposition = acknowledgement.disposition.enum_value_or_default();
    if !matches!(
      disposition,
      Disposition::DISPOSITION_APPLIED | Disposition::DISPOSITION_RETIRED
    ) {
      // The stream's command-only timer paces retries. Do not clone a payload or retry directly
      // in response to a negative ACK, which could form an unbounded request/response loop.
      return;
    }
    let key = (
      acknowledgement.command_id.clone(),
      acknowledgement.update_sequence_number,
    );
    let mut pending_updates = self.pending_device_command_updates.lock();
    let Some(pending_update) = pending_updates.remove(&key) else {
      log::debug!("ignoring duplicate device command acknowledgement: {key:?}");
      return;
    };

    if disposition == Disposition::DISPOSITION_APPLIED {
      let _ignored = pending_update.response_tx.send(());
    }
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

pub type TrackedUpload<T> = Tracked<T, UploadResponse>;
pub type TrackedIntent<T> = Tracked<T, IntentResponse>;

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

pub type TrackedSankeyPathUploadIntentRequest = Tracked<SankeyIntentRequest, IntentResponse>;

/// An intent to upload a buffer due to a listener triggering. This is communicated to the backend
/// in order to allow the server to make decisions on whether a buffer should be uploaded in
/// response to a specific listener.
pub type TrackedLogUploadIntent = Tracked<LogUploadIntentRequest, IntentResponse>;

pub type TrackedArtifactUpload = Tracked<UploadArtifactRequest, UploadResponse>;
pub type TrackedArtifactIntent = Tracked<UploadArtifactIntentRequest, IntentResponse>;

#[derive(Debug)]
pub struct CommandUpdateSender {
  channel: Option<tokio::sync::oneshot::Sender<()>>,
  expires_at: Option<tokio::time::Instant>,
  expiry_task: Option<JoinHandle<()>>,
  byte_permit: Option<OwnedSemaphorePermit>,
}

impl Drop for CommandUpdateSender {
  fn drop(&mut self) {
    if let Some(task) = self.expiry_task.take() {
      task.abort();
    }
  }
}

impl CommandUpdateSender {
  fn is_pending(&self) -> bool {
    self
      .channel
      .as_ref()
      .is_some_and(|channel| !channel.is_closed())
      && self
        .expires_at
        .is_none_or(|deadline| deadline > tokio::time::Instant::now())
  }

  pub fn send(mut self, response: ()) -> anyhow::Result<()> {
    if !self.is_pending() {
      anyhow::bail!("device command update retired");
    }
    self
      .channel
      .take()
      .ok_or_else(|| anyhow!("device command update retired"))?
      .send(response)
      .map_err(|()| anyhow!("device command update waiter closed"))
  }
}

#[derive(Debug)]
pub struct TrackedDeviceCommandUpdate {
  pub uuid: String,
  pub payload: DeviceCommandUpdate,
  pub response_tx: CommandUpdateSender,
}

impl TrackedDeviceCommandUpdate {
  pub fn new(
    uuid: String,
    mut payload: DeviceCommandUpdate,
  ) -> (Self, tokio::sync::oneshot::Receiver<()>) {
    if payload.compute_size() > MAX_PENDING_COMMAND_UPDATE_BYTES as u64 {
      log::warn!("device command result exceeds update byte limit; reporting bounded failure");
      payload = DeviceCommandUpdate {
        command_id: payload.command_id,
        update_sequence_number: payload.update_sequence_number,
        update_type: Some(device_command_update::Update_type::Failed(
          device_command_update::Failed {
            context: Some(DeviceCommandResultContext {
              fields: [(
                "error".to_string(),
                Data {
                  data_type: Some(Data_type::StringData(
                    "device command result exceeds the 4 MiB update limit".to_string(),
                  )),
                  ..Default::default()
                },
              )]
              .into(),
              ..Default::default()
            })
            .into(),
            ..Default::default()
          },
        )),
        ..Default::default()
      };
    }
    let (sender, receiver) = tokio::sync::oneshot::channel();
    (
      Self {
        uuid,
        payload,
        response_tx: CommandUpdateSender {
          channel: Some(sender),
          expires_at: None,
          expiry_task: None,
          byte_permit: None,
        },
      },
      receiver,
    )
  }

  /// Reserves bytes across queued and tracked updates. The caller bounds this wait by command
  /// expiry.
  pub async fn with_byte_budget(mut self, state: &DeviceCommandState) -> anyhow::Result<Self> {
    let bytes = self.payload.compute_size();
    if bytes > MAX_PENDING_COMMAND_UPDATE_BYTES as u64 {
      anyhow::bail!("device command update identity exceeds byte limit");
    }
    log::debug!("reserving {bytes} bytes for a device command update");
    self.response_tx.byte_permit = Some(
      state
        .pending_update_bytes
        .clone()
        .acquire_many_owned(u32::try_from(bytes)?)
        .await?,
    );
    Ok(self)
  }

  #[must_use]
  pub fn with_remaining_time(mut self, remaining: std::time::Duration) -> Self {
    let expires_at = tokio::time::Instant::now() + remaining;
    self.response_tx.expires_at = Some(expires_at);
    // The caller bounds enqueueing and ACK waiting by this same budget. Only tracked payloads
    // need a separate expiry task, because API reconnect/backoff can stop polling them.
    self
  }
}
