// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! One submission queue and durable ownership boundary for every artifact producer.
//!
//! Crash reports, snapshots and command attachments differ in payload format and retention policy,
//! not in who publishes the upload index. Producer methods normalize metadata into a payload-free
//! request before entering this queue. `staging` owns admission, publication and acknowledgements;
//! `chunked` only supplies bounded chunk preparation and recovery checks. Whole-file and chunked
//! entries share the index, but their spool files stay separate and workflow payloads remain
//! borrowed.
//!
//! The whole-file transfer scheduler still owns intent negotiation and whole-artifact transfer.
//! Chunked producers and transport remain disabled until their later milestones; consolidation does
//! not change wire selection, restore invocation authority, or make persistence equivalent to
//! upload success.
//!
//! Optional lifecycle hooks let tests observe recovery and gate staging/upload completion without
//! changing the ownership algorithm. Notification bookkeeping belongs to the hook implementation,
//! not the payload rollback ledger; production construction leaves hooks unset.

#[cfg(test)]
#[path = "./uploader_test.rs"]
mod tests;

#[cfg(test)]
#[path = "./chunked_test.rs"]
mod chunked_tests;

mod chunked;

mod staging;

mod storage;

use bd_api::upload::{IntentDecision, TrackedArtifactIntent, TrackedArtifactUpload};
use bd_api::{DataUpload, RuntimeBackoffPolicy};
use bd_backoff::future::retry_notify;
use bd_backoff::{ExponentialBackoff, InfiniteBackoff};
use bd_bounded_buffer::SendCounters;
use bd_client_common::artifact::{
  CLIENT_REPORT_ARTIFACT_TYPE_ID,
  CRASH_PROFILE_ARTIFACT_TYPE_ID,
  STATE_SNAPSHOT_ARTIFACT_TYPE_ID,
};
use bd_client_common::error::InvariantError;
use bd_client_common::file::{read_checksummed_data, write_compressed_protobuf};
use bd_client_common::file_system::{FileSystem, OwnedFile};
use bd_client_common::maybe_await;
use bd_client_stats_store::{Collector, Counter, Scope};
use bd_error_reporter::reporter::handle_unexpected;
use bd_log_filter::ScrubRules;
use bd_log_primitives::{DataValue, LogFields};
use bd_macros::ApproximateSize;
use bd_proto::protos::client::api::{
  ArtifactPayloadEncoding,
  CrashProfileMetadata,
  UploadArtifactIntentRequest,
  UploadArtifactRequest,
};
use bd_proto::protos::client::artifact::artifact_upload_index::Artifact;
use bd_proto::protos::client::artifact::{ArtifactUploadIndex, StorageFormat};
use bd_proto::protos::client::feature_flag::FeatureFlag;
use bd_proto::protos::logging::payload::Data;
use bd_runtime::runtime::{ConfigLoader, IntWatch, artifact_upload, attachment};
use bd_shutdown::ComponentShutdown;
use bd_stats_common::Counter as _;
use bd_time::{OffsetDateTimeExt, TimeProvider, TimestampExt};
use mockall::automock;
use protobuf::Message as _;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use time::OffsetDateTime;
use tokio::io::AsyncRead;
use tokio::sync::{OwnedSemaphorePermit, oneshot, watch};
use tokio::time::{MissedTickBehavior, interval};
use uuid::Uuid;

/// Root directory for all files used for storage and uploading.
pub static ARTIFACT_UPLOAD_DIRECTORY: LazyLock<PathBuf> = LazyLock::new(|| "report_uploads".into());

/// The index file used for tracking all of the individual files.
pub static REPORT_INDEX_FILE: LazyLock<PathBuf> = LazyLock::new(|| "report_index.pb".into());

pub const WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID: &str = "workflow_attachment";
const INDEX_RETRY_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Default, Clone, Copy)]
pub enum ArtifactType {
  #[default]
  Report,
  StateSnapshot,
}

impl ArtifactType {
  fn to_type_id(self) -> &'static str {
    match self {
      Self::Report => CLIENT_REPORT_ARTIFACT_TYPE_ID,
      Self::StateSnapshot => STATE_SNAPSHOT_ARTIFACT_TYPE_ID,
    }
  }
}

//
// FeatureFlag
//

#[derive(ApproximateSize, Debug, Clone)]
pub struct SnappedFeatureFlag {
  name: String,
  variant: Option<String>,
  last_updated: OffsetDateTime,
}

impl SnappedFeatureFlag {
  #[must_use]
  pub fn new(name: String, variant: Option<String>, last_updated: OffsetDateTime) -> Self {
    Self {
      name,
      variant,
      last_updated,
    }
  }

  #[must_use]
  pub fn name(&self) -> &str {
    &self.name
  }

  #[must_use]
  pub fn variant(&self) -> Option<&str> {
    self.variant.as_deref()
  }

  #[must_use]
  pub fn last_updated(&self) -> OffsetDateTime {
    self.last_updated
  }
}

//
// NewUpload
//

// Metadata is represented once, regardless of the eventual payload format. In particular, chunked
// submissions no longer carry a second request alongside empty whole-file metadata fields.
#[derive(ApproximateSize, Debug)]
struct NewUpload {
  source: UploadSource,
  #[approximate_size(with = request_size)]
  request: UploadArtifactRequest,
  mode: PreparationMode,
  #[approximate_size(skip)]
  persisted_tx: Option<PersistenceAck>,
  #[approximate_size(skip)]
  completion_tx: Option<oneshot::Sender<std::result::Result<(), String>>>,
  #[approximate_size(skip)]
  source_lease: Option<OwnedSemaphorePermit>,
  #[approximate_size(skip)]
  source_cleanup: Option<OwnedFile>,
}

fn request_size(request: &UploadArtifactRequest) -> usize {
  // Serialized size counts linked IDs' contents but not their reserved heap capacity. Preserve
  // crash-profile queue accounting when the metadata moves into the common request template.
  let spare_profile_bytes = request.crash_profile.as_ref().map_or(0, |profile| {
    profile
      .report_ids
      .approximate_size_children_bytes()
      .saturating_sub(profile.report_ids.iter().map(String::len).sum())
  });
  usize::try_from(request.compute_size())
    .unwrap_or(usize::MAX)
    .saturating_add(spare_profile_bytes)
}

// Command and crash-profile attachments share compression, size limits and source ownership.
fn is_compressed_attachment(request: &UploadArtifactRequest) -> bool {
  request.command_id.is_some() || request.type_id == CRASH_PROFILE_ARTIFACT_TYPE_ID
}

//
// PreparationMode
//

#[derive(ApproximateSize, Clone, Copy, Debug, PartialEq, Eq)]
enum PreparationMode {
  WholeFile,
  Chunked,
}

impl NewUpload {
  fn new(source: UploadSource, request: UploadArtifactRequest, mode: PreparationMode) -> Self {
    // Command/crash-profile/chunk capture paths transfer even when enqueue is rejected. Snapshot
    // paths remain retryable inputs until publication; retained workflow paths stay borrowed.
    let source_cleanup = (mode == PreparationMode::Chunked || is_compressed_attachment(&request))
      .then(|| source.owned_file())
      .flatten();
    Self {
      source,
      request,
      mode,
      persisted_tx: None,
      completion_tx: None,
      source_lease: None,
      source_cleanup,
    }
  }
}

#[derive(Debug)]
pub enum UploadSource {
  // Bytes are copied directly to the durable report directory with a checksum before upload.
  Bytes(Vec<u8>),
  // When a file handle is provided the uploader will copy the contents of the file to disk and
  // append a CRC checksum to the end of the file to allow for integrity checking when we later
  // read.
  File(std::fs::File),
  // For raw files they are directly moved to the target location without modification. This is
  // intended for use cases where the data format is already self-validating (e.g. crc checksum or
  // zlib compression).
  Path(PathBuf),
  // A checksum-verified workflow payload borrowed in place through a durable upload-index lease.
  Retained(PathBuf),
}

impl UploadSource {
  /// Owns only a handed-over absolute capture path, never a borrowed retained payload.
  #[must_use]
  pub fn owned_file(&self) -> Option<OwnedFile> {
    match self {
      Self::Path(path) => OwnedFile::new(path),
      _ => None,
    }
  }

  /// Opens a bounded attachment input for both direct and workflow preparation.
  ///
  /// Root resolution belongs to the supplied filesystem. Regular-file and size checks happen here
  /// once; the bounded reader still enforces the limit while reading, so growth cannot yield a
  /// truncated success. Retained payloads are already encoded and must not be compressed again.
  pub async fn into_reader(
    self,
    filesystem: &dyn FileSystem,
    max_bytes: u64,
  ) -> anyhow::Result<Box<dyn AsyncRead + Unpin + Send>> {
    let file = match self {
      Self::Bytes(bytes) => {
        if bytes.len() as u64 > max_bytes {
          anyhow::bail!("attachment exceeds size limit");
        }
        return Ok(Box::new(io::Cursor::new(bytes)));
      },
      Self::File(file) => tokio::fs::File::from_std(file),
      Self::Path(path) => filesystem.open_regular_file(&path).await?,
      Self::Retained(_) => anyhow::bail!("cannot prepare an already retained attachment"),
    };
    let metadata = file.metadata().await?;
    if !metadata.is_file() || metadata.len() > max_bytes {
      anyhow::bail!("attachment must be a regular file within size limit");
    }
    Ok(Box::new(file))
  }
}

// Used for bounded_buffer logs
impl std::fmt::Display for NewUpload {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "NewUpload {{ uuid: {}, source: {:?} }}",
      self.request.artifact_id, self.source
    )
  }
}

impl ApproximateSize for UploadSource {
  fn approximate_size_children_bytes(&self) -> usize {
    match self {
      Self::Bytes(bytes) => bytes.capacity(),
      // File descriptors own no heap storage attributable to this queue entry. PathBuf exposes
      // the capacity of its owned path buffer, so account for its retained allocation directly.
      Self::File(_) => 0,
      Self::Path(path) | Self::Retained(path) => path.capacity(),
    }
  }
}

//
// Stats
//

struct Stats {
  uploaded: Counter,

  // TODO(snowp): For now we just emit metrics on drops but we probably want a more robust
  // mechanism for keeping track of the data that we're dropping either due to overflows or intent
  // rejections.
  dropped: Counter,
  dropped_intent: Counter,
  accepted_intent: Counter,
}

impl Stats {
  fn new(scope: &Scope) -> Self {
    Self {
      uploaded: scope.counter("uploaded"),
      dropped: scope.counter("dropped"),
      dropped_intent: scope.counter("dropped_intent"),
      accepted_intent: scope.counter("accepted_intent"),
    }
  }
}

#[derive(Debug, thiserror::Error)]
pub enum EnqueueError {
  #[error("upload queue full")]
  QueueFull,
  #[error("upload channel closed")]
  Closed,
  #[error("retryable persistence failure: {0}")]
  RetryablePersistence(anyhow::Error),
  #[error(transparent)]
  Other(#[from] anyhow::Error),
}

//
// PersistenceAck
//

#[derive(Debug)]
enum PersistenceAck {
  Upload(oneshot::Sender<std::result::Result<(), EnqueueError>>),
  Command(oneshot::Sender<std::result::Result<u64, EnqueueError>>),
}

impl PersistenceAck {
  fn send(self, result: std::result::Result<Option<u64>, EnqueueError>) {
    match self {
      Self::Upload(tx) => {
        let _ = tx.send(result.map(|_| ()));
      },
      Self::Command(tx) => {
        let result = result.and_then(|size_bytes| {
          size_bytes.ok_or_else(|| {
            EnqueueError::Other(anyhow::anyhow!("missing persisted command attachment size"))
          })
        });
        let _ = tx.send(result);
      },
    }
  }
}

fn retained_persistence_error(error: anyhow::Error) -> EnqueueError {
  if error
    .downcast_ref::<std::io::Error>()
    .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)
  {
    EnqueueError::Other(error)
  } else {
    EnqueueError::RetryablePersistence(error)
  }
}

#[automock]
pub trait Client: Send + Sync {
  /// Revisions of the atomically published queue index, including commits awaiting durability.
  fn upload_queue_revision(&self) -> watch::Receiver<u64>;

  /// Stages an owned, absolute regular-file path. Metadata is a payload-free capture request
  /// with a fixed deadline; size, descriptors, hashes and admission time are filled by the spool.
  /// Takes ownership even on rejection. Sources must share the SDK spool's filesystem so ownership
  /// can move atomically before preparation; cross-filesystem moves fail explicitly.
  /// Existing producers do not call this runtime-gated entry point until transport is supported.
  fn enqueue_chunked_file(
    &self,
    source_path: PathBuf,
    request: UploadArtifactRequest,
    persisted_tx: oneshot::Sender<std::result::Result<u64, EnqueueError>>,
  ) -> std::result::Result<(), EnqueueError>;

  /// Borrows the canonical payload. A workflow store must supply its cleanup-exclusion permit;
  /// the queue holds that permit until its durable index lease replaces it.
  fn enqueue_workflow_attachment(
    &self,
    artifact_id: Uuid,
    source_path: PathBuf,
    session_id: String,
    content_type: Option<String>,
    persisted_tx: Option<oneshot::Sender<std::result::Result<(), EnqueueError>>>,
    completion_tx: Option<oneshot::Sender<std::result::Result<(), String>>>,
    source_lease: Option<OwnedSemaphorePermit>,
  ) -> std::result::Result<(), EnqueueError>;

  fn enqueue_upload(
    &self,
    source: UploadSource,
    type_id: String,
    state: LogFields,
    timestamp: Option<OffsetDateTime>,
    session_id: String,
    feature_flags: Vec<SnappedFeatureFlag>,
    crash_profile: Option<CrashProfileMetadata>,
    persisted_tx: Option<oneshot::Sender<std::result::Result<(), EnqueueError>>>,
  ) -> std::result::Result<Uuid, EnqueueError>;

  /// Takes ownership of absolute path sources even on rejection. Persistence acknowledgements
  /// report the uncompressed payload size; dropping the caller's waits does not cancel the upload.
  fn enqueue_command_upload(
    &self,
    source: UploadSource,
    type_id: String,
    timestamp: Option<OffsetDateTime>,
    session_id: String,
    feature_flags: Vec<SnappedFeatureFlag>,
    command_id: String,
    content_type: Option<String>,
    persisted_tx: Option<oneshot::Sender<std::result::Result<u64, EnqueueError>>>,
    completion_tx: Option<oneshot::Sender<std::result::Result<(), String>>>,
  ) -> std::result::Result<Uuid, EnqueueError>;

  /// Queues an Android crash profile exactly like a command attachment, with its explicit parent
  /// report IDs. Takes ownership of absolute path sources even on rejection.
  fn enqueue_crash_profile(
    &self,
    source: UploadSource,
    timestamp: OffsetDateTime,
    session_id: String,
    content_type: String,
    crash_profile: CrashProfileMetadata,
    persisted_tx: Option<oneshot::Sender<std::result::Result<(), EnqueueError>>>,
  ) -> std::result::Result<Uuid, EnqueueError>;
}

pub struct UploadClient {
  upload_tx: bd_bounded_buffer::Sender<NewUpload>,
  queue_revision: watch::Receiver<u64>,
  counter_stats: SendCounters,
  command_artifact_metadata: LogFields,
}

impl UploadClient {
  fn submit(&self, upload: NewUpload) -> std::result::Result<(), EnqueueError> {
    // All formats use the same bounded queue and counters. Dropping a rejected submission also
    // drops its ownership guard, so producers cannot leak command files on synchronous rejection.
    let result = self
      .upload_tx
      .try_send(upload)
      .inspect_err(|error| log::warn!("failed to enqueue artifact upload: {error:?}"));
    self.counter_stats.record(&result);
    result.map_err(|error| match error {
      bd_bounded_buffer::TrySendError::FullSizeOverflow => EnqueueError::QueueFull,
      bd_bounded_buffer::TrySendError::Closed => EnqueueError::Closed,
    })
  }

  fn enqueue(
    &self,
    uuid: Uuid,
    source: UploadSource,
    type_id: String,
    state: LogFields,
    timestamp: Option<OffsetDateTime>,
    session_id: String,
    feature_flags: Vec<SnappedFeatureFlag>,
    command_id: Option<String>,
    content_type: Option<String>,
    crash_profile: Option<CrashProfileMetadata>,
    persisted_tx: Option<PersistenceAck>,
    completion_tx: Option<oneshot::Sender<std::result::Result<(), String>>>,
    source_lease: Option<OwnedSemaphorePermit>,
  ) -> std::result::Result<Uuid, EnqueueError> {
    let request = UploadArtifactRequest {
      artifact_id: uuid.to_string(),
      type_id,
      state_metadata: state
        .into_iter()
        .map(|(key, value)| (key.into(), value.into_proto()))
        .collect(),
      time: timestamp
        .map(OffsetDateTimeExt::into_proto)
        .unwrap_or_default(),
      session_id,
      feature_flags: feature_flags
        .into_iter()
        .map(|flag| FeatureFlag {
          name: flag.name,
          variant: flag.variant,
          last_updated: flag.last_updated.into_proto(),
          ..Default::default()
        })
        .collect(),
      command_id,
      content_type,
      crash_profile: crash_profile.into(),
      ..Default::default()
    };
    let mut upload = NewUpload::new(source, request, PreparationMode::WholeFile);
    upload.persisted_tx = persisted_tx;
    upload.completion_tx = completion_tx;
    upload.source_lease = source_lease;
    self.submit(upload)?;
    Ok(uuid)
  }
}

impl Client for UploadClient {
  fn upload_queue_revision(&self) -> watch::Receiver<u64> {
    self.queue_revision.clone()
  }

  fn enqueue_chunked_file(
    &self,
    source_path: PathBuf,
    request: UploadArtifactRequest,
    persisted_tx: oneshot::Sender<std::result::Result<u64, EnqueueError>>,
  ) -> std::result::Result<(), EnqueueError> {
    let mut upload = NewUpload::new(
      UploadSource::Path(source_path),
      request,
      PreparationMode::Chunked,
    );
    // Validate immutable capture shape before it can occupy queue memory. The owner performs
    // runtime/disk admission later, but need not repeat this unchanged metadata validation.
    chunked::validate_capture_request(&upload.request).map_err(|error| {
      log::warn!("invalid chunked capture metadata: {error}");
      EnqueueError::Other(error)
    })?;
    if upload.source_cleanup.is_none() {
      log::warn!("chunked capture requires an absolute owned path");
      return Err(EnqueueError::Other(anyhow::anyhow!(
        "chunked capture requires an absolute owned path"
      )));
    }
    upload.persisted_tx = Some(PersistenceAck::Command(persisted_tx));
    self.submit(upload)
  }

  fn enqueue_workflow_attachment(
    &self,
    artifact_id: Uuid,
    source_path: PathBuf,
    session_id: String,
    content_type: Option<String>,
    persisted_tx: Option<oneshot::Sender<std::result::Result<(), EnqueueError>>>,
    completion_tx: Option<oneshot::Sender<std::result::Result<(), String>>>,
    source_lease: Option<OwnedSemaphorePermit>,
  ) -> std::result::Result<(), EnqueueError> {
    if source_path != Path::new(&format!("workflow-attachments/{artifact_id}.payload")) {
      return Err(EnqueueError::Other(anyhow::anyhow!(
        "invalid workflow attachment path"
      )));
    }
    self.enqueue(
      artifact_id,
      UploadSource::Retained(source_path),
      WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID.to_string(),
      self.command_artifact_metadata.clone(),
      None,
      session_id,
      Vec::new(),
      None,
      content_type,
      None,
      persisted_tx.map(PersistenceAck::Upload),
      completion_tx,
      source_lease,
    )?;
    Ok(())
  }

  /// Dispatches a payload to be uploaded, returning the associated artifact UUID.
  fn enqueue_upload(
    &self,
    source: UploadSource,
    type_id: String,
    state: LogFields,
    timestamp: Option<OffsetDateTime>,
    session_id: String,
    feature_flags: Vec<SnappedFeatureFlag>,
    crash_profile: Option<CrashProfileMetadata>,
    persisted_tx: Option<oneshot::Sender<std::result::Result<(), EnqueueError>>>,
  ) -> std::result::Result<Uuid, EnqueueError> {
    self.enqueue(
      Uuid::new_v4(),
      source,
      type_id,
      state,
      timestamp,
      session_id,
      feature_flags,
      None,
      None,
      crash_profile,
      persisted_tx.map(PersistenceAck::Upload),
      None,
      None,
    )
  }

  fn enqueue_command_upload(
    &self,
    source: UploadSource,
    type_id: String,
    timestamp: Option<OffsetDateTime>,
    session_id: String,
    feature_flags: Vec<SnappedFeatureFlag>,
    command_id: String,
    content_type: Option<String>,
    persisted_tx: Option<oneshot::Sender<std::result::Result<u64, EnqueueError>>>,
    completion_tx: Option<oneshot::Sender<std::result::Result<(), String>>>,
  ) -> std::result::Result<Uuid, EnqueueError> {
    self.enqueue(
      Uuid::new_v4(),
      source,
      type_id,
      self.command_artifact_metadata.clone(),
      timestamp,
      session_id,
      feature_flags,
      Some(command_id),
      content_type,
      None,
      persisted_tx.map(PersistenceAck::Command),
      completion_tx,
      None,
    )
  }

  fn enqueue_crash_profile(
    &self,
    source: UploadSource,
    timestamp: OffsetDateTime,
    session_id: String,
    content_type: String,
    crash_profile: CrashProfileMetadata,
    persisted_tx: Option<oneshot::Sender<std::result::Result<(), EnqueueError>>>,
  ) -> std::result::Result<Uuid, EnqueueError> {
    self.enqueue(
      Uuid::new_v4(),
      source,
      CRASH_PROFILE_ARTIFACT_TYPE_ID.to_string(),
      self.command_artifact_metadata.clone(),
      Some(timestamp),
      session_id,
      Vec::new(),
      None,
      Some(content_type),
      Some(crash_profile),
      persisted_tx.map(PersistenceAck::Upload),
      None,
      None,
    )
  }
}

#[derive(thiserror::Error, Debug)]
enum Error {
  #[error("Task is shutting down")]
  Shutdown,
  #[error("Upload attempt did not complete successfully")]
  IncompleteUpload,
  #[error("Unhandled error: {0:#}")]
  Unhandled(anyhow::Error),
}

impl From<anyhow::Error> for Error {
  fn from(value: anyhow::Error) -> Self {
    Self::Unhandled(value)
  }
}

impl From<tokio::task::JoinError> for Error {
  fn from(value: tokio::task::JoinError) -> Self {
    Self::Unhandled(value.into())
  }
}

impl From<InvariantError> for Error {
  fn from(value: InvariantError) -> Self {
    Self::Unhandled(value.into())
  }
}

type Result<T> = std::result::Result<T, Error>;

//
// TestHooks
//

#[async_trait::async_trait]
trait TestHooks: Send + Sync {
  async fn initialized(&self);

  fn whole_file_preparation_started(&self, source: &UploadSource, request: &UploadArtifactRequest);

  fn payload_prepared(&self);

  // Sources and leases have been released and any persistence acknowledgement has been sent.
  // Tests may pause here without making production cleanup depend on a caller polling its result.
  async fn staging_completed(&self, id: &str, success: bool);

  async fn upload_complete(&self, id: &str);
}

pub struct Uploader {
  data_upload_tx: tokio::sync::mpsc::Sender<DataUpload>,
  upload_queued_rx: bd_bounded_buffer::Receiver<NewUpload>,
  shutdown: ComponentShutdown,
  time_provider: Arc<dyn TimeProvider>,
  file_system: Arc<dyn FileSystem>,

  index: VecDeque<Artifact>,
  chunked: chunked::Spool,
  queue_revision: watch::Sender<u64>,
  index_dirty: bool,
  index_sync_required: bool,
  upload_completions: HashMap<String, oneshot::Sender<std::result::Result<(), String>>>,

  max_entries: IntWatch<bd_runtime::runtime::artifact_upload::MaxPendingEntries>,
  max_attachment_bytes: IntWatch<attachment::MaxBytes>,
  backoff_policy: RuntimeBackoffPolicy<
    bd_runtime::runtime::retry_backoff::InitialBackoffInterval,
    bd_runtime::runtime::retry_backoff::MaxBackoffInterval,
    bd_runtime::runtime::retry_backoff::BackoffGrowthFactorBasisPoints,
  >,

  // The scrub rules from the most recently applied configuration. Metadata is scrubbed with
  // whatever rules are current when each request is built; sends never wait for rules.
  scrub_rules: watch::Receiver<Arc<ScrubRules>>,

  intent_task_handle: Option<tokio::task::JoinHandle<Result<IntentDecision>>>,
  upload_task_handle: Option<tokio::task::JoinHandle<Result<()>>>,

  stats: Stats,

  test_hooks: Option<Arc<dyn TestHooks>>,
}

impl Uploader {
  /// Reads the durable upload index without starting the uploader. Callers that need entries before
  /// any upload can remove them must read before `run` starts.
  pub async fn pending_artifacts(sdk_directory: &Path) -> io::Result<Vec<Artifact>> {
    Ok(Self::pending_index(sdk_directory).await?.artifact)
  }

  async fn pending_index(sdk_directory: &Path) -> io::Result<ArtifactUploadIndex> {
    let path = sdk_directory
      .join(&*ARTIFACT_UPLOAD_DIRECTORY)
      .join(&*REPORT_INDEX_FILE);
    let mut file = match bd_client_common::file_system::open_regular_file_async(&path).await {
      Ok(file) => file,
      Err(error) if error.kind() == io::ErrorKind::NotFound => {
        return Ok(ArtifactUploadIndex::default());
      },
      Err(error) => return Err(error),
    };
    chunked::read_index(&mut file).await
  }

  /// Reads durable workflow leases from both formats before payload cleanup can run.
  pub async fn pending_workflow_attachments(sdk_directory: &Path) -> io::Result<HashSet<Uuid>> {
    let index = Self::pending_index(sdk_directory).await?;
    let whole_file = index
      .artifact
      .into_iter()
      .filter(|entry| entry.type_id.as_deref() == Some(WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID))
      .map(|entry| {
        Uuid::parse_str(&entry.name)
          .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
      });
    let chunked = index
      .chunked_artifact
      .into_iter()
      .filter_map(|entry| {
        entry
          .request
          .into_option()
          .filter(|request| request.type_id == WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID)
      })
      .map(|request| {
        chunked::safe_id(&request.artifact_id)
          .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
      });
    whole_file.chain(chunked).collect()
  }

  fn payload_path(entry: &Artifact) -> anyhow::Result<PathBuf> {
    if Path::new(&entry.name)
      .file_name()
      .and_then(|name| name.to_str())
      != Some(&entry.name)
      || entry.name == "."
      || entry.name == ".."
    {
      anyhow::bail!("unsafe artifact filename");
    }
    if entry.type_id.as_deref() == Some(WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID) {
      let id = Uuid::parse_str(&entry.name)?;
      return Ok(PathBuf::from(format!("workflow-attachments/{id}.payload")));
    }
    Ok(ARTIFACT_UPLOAD_DIRECTORY.join(&entry.name))
  }

  pub fn new(
    file_system: Arc<dyn FileSystem>,
    data_upload_tx: tokio::sync::mpsc::Sender<DataUpload>,
    time_provider: Arc<dyn TimeProvider>,
    runtime: &ConfigLoader,
    collector: &Collector,
    command_artifact_metadata: HashMap<String, String>,
    scrub_rules: watch::Receiver<Arc<ScrubRules>>,
    shutdown: ComponentShutdown,
  ) -> (Self, UploadClient) {
    runtime.expect_initialized();

    let scope = collector.scope("artifact_upload");

    // TODO(snowp): It would be nice to not have to create a watch in order to use the typed
    // runtime flags. This buffer cannot be recreated on config change so we're only reading it on
    // startup.
    let buffer_memory_capacity = *runtime
      .register_int_watch::<artifact_upload::BufferByteLimit>()
      .read();

    let (upload_tx, upload_rx) =
      bd_bounded_buffer::channel(buffer_memory_capacity.try_into().unwrap_or_default());
    let (queue_revision_tx, queue_revision_rx) = watch::channel(0);

    let uploader = Self {
      data_upload_tx,
      upload_queued_rx: upload_rx,
      shutdown,
      time_provider,
      file_system,
      index: VecDeque::default(),
      chunked: chunked::Spool::new(runtime),
      queue_revision: queue_revision_tx,
      index_dirty: false,
      index_sync_required: false,
      upload_completions: HashMap::default(),
      max_entries: runtime.register_int_watch(),
      max_attachment_bytes: runtime.register_int_watch(),
      backoff_policy: RuntimeBackoffPolicy::new(runtime),
      scrub_rules,
      upload_task_handle: None,
      intent_task_handle: None,
      stats: Stats::new(&scope),
      test_hooks: None,
    };

    let client = UploadClient {
      upload_tx,
      queue_revision: queue_revision_rx,
      counter_stats: SendCounters::new(&scope, "enqueue"),
      command_artifact_metadata: command_artifact_metadata
        .into_iter()
        .map(|(key, value)| (key.into(), value.into()))
        .collect(),
    };

    (uploader, client)
  }

  fn request_metadata(&self, metadata: &HashMap<String, Data>) -> RequestMetadata {
    RequestMetadata {
      raw: metadata.clone(),
      scrub_rules: self.scrub_rules.clone(),
    }
  }

  pub async fn run(self) {
    if let Err(Error::Unhandled(e)) = self.run_inner().await {
      handle_unexpected(Err::<(), _>(e), "artifact uploader");
    }
  }

  async fn run_inner(mut self) -> Result<()> {
    self.initialize().await?;
    log::debug!("artifact spool recovery and orphan cleanup complete");
    if let Some(hooks) = &self.test_hooks {
      hooks.initialized().await;
    }
    // Startup may reset an unreadable index without writing a replacement.
    self
      .queue_revision
      .send_modify(|revision| *revision = revision.wrapping_add(1));
    let mut index_retry = interval(INDEX_RETRY_INTERVAL);
    index_retry.set_missed_tick_behavior(MissedTickBehavior::Skip);

    // The state machinery below relies on careful handling of the contents of the index list, as
    // we want to make sure that we don't lose entries due to process shutdown. The pending upload
    // remains at the head of the list during intent negotiation/uploads and is only removed after
    // the upload completes or we decide to not upload the file.
    loop {
      // If we're not currently processing an entry and there are pending work to do, check the
      // next entry in the list and perform the next step.
      if self.intent_task_handle.is_none()
        && self.upload_task_handle.is_none()
        && let Some(next) = self.index.front().cloned()
      {
        if next.pending_intent_negotiation {
          log::debug!(
            "starting intent negotiation: artifact_id={}, type={}",
            next.name,
            next.type_id.as_deref().unwrap_or("<missing>")
          );

          self.intent_task_handle = Some(tokio::spawn(Self::perform_intent_negotiation(
            self.data_upload_tx.clone(),
            next.name.clone(),
            next.type_id.clone().unwrap_or_default(),
            next.session_id.clone(),
            next.time.to_offset_date_time(),
            self.request_metadata(&next.metadata),
            next.crash_profile.as_ref().cloned(),
            self.backoff_policy.backoff_mark_update(),
          )));
          continue;
        }

        let contents = async {
          let file_path = Self::payload_path(&next)?;
          self.file_system.read_file(&file_path).await
        }
        .await;
        let Ok(contents) = contents else {
          log::warn!(
            "failed to read artifact {}: {:?}",
            next.name,
            contents.err()
          );
          let entry = self.index.pop_front().ok_or(InvariantError::Invariant)?;
          self
            .discard_upload(entry, "artifact upload file could not be read".to_string())
            .await;
          continue;
        };

        let contents = if next.storage_format.enum_value_or_default() == StorageFormat::RAW {
          Some(contents)
        } else {
          read_checksummed_data(&contents).ok()
        };
        let Some(contents) = contents else {
          log::warn!("artifact {} failed integrity validation", next.name);
          let entry = self.index.pop_front().ok_or(InvariantError::Invariant)?;
          self
            .discard_upload(
              entry,
              "artifact upload file failed integrity validation".to_string(),
            )
            .await;
          continue;
        };
        log::debug!(
          "starting file upload: artifact_id={}, type={}, payload_bytes={}",
          next.name,
          next.type_id.as_deref().unwrap_or("<missing>"),
          contents.len()
        );
        self.upload_task_handle = Some(tokio::spawn(Self::upload_artifact(
          self.data_upload_tx.clone(),
          contents,
          next.name.clone(),
          next.type_id.clone().unwrap_or_default(),
          next.time.to_offset_date_time(),
          next.session_id.clone(),
          self.backoff_policy.backoff_mark_update(),
          self.request_metadata(&next.metadata),
          next.feature_flags.clone(),
          next.command_id.clone(),
          next.content_type.clone(),
          next.payload_encoding.enum_value_or_default(),
          next.crash_profile.as_ref().cloned(),
        )));
      }

      // Only one task should ever be active at a time.
      debug_assert!(!(self.intent_task_handle.is_some() && self.upload_task_handle.is_some()));

      // At this point either wait for progress to be made to the current entry or wait for a new
      // entry to be submitted.
      tokio::select! {
        // Failed commits must be reconciled even if no further uploads arrive.
        _ = index_retry.tick() => {
          if self.index_dirty && let Err(error) = self.write_index().await {
            log::warn!("failed to reconcile artifact index; will retry: {error}");
          }
          if let Err(error) = self.retire_expired_chunks().await {
            log::warn!("failed to retire expired chunked artifacts; will retry: {error}");
          }
          if !self.index_dirty {
            self.clean_sources().await;
            if let Err(error) = self.clean_orphan_files().await {
              log::warn!("failed to remove orphaned artifact payloads; will retry: {error}");
            }
          }
        }
        () = self.shutdown.cancelled() => {
          log::debug!("shutting down uploader");
          self.stop_current_upload();

          return Err(Error::Shutdown);
        }
        Some(upload) = self.upload_queued_rx.recv() => {
          self.track_upload(upload).await;
        }
        intent_decision = maybe_await(&mut self.intent_task_handle) => {
            self.handle_intent_negotiation_decision(intent_decision??).await?;
        }
        result = maybe_await(&mut self.upload_task_handle) => {
            match result? {
              Ok(()) => {
                let name = self.handle_upload_complete().await?;
                self.complete_upload(&name, Ok(()));

                if let Some(hooks) = &self.test_hooks {
                    hooks.upload_complete(&name).await;
                }
              },
              Err(error) => {
                let entry = self.index.pop_front().ok_or(InvariantError::Invariant)?;
                self.discard_upload(entry, error.to_string()).await;
              },
            }
        }

      }
    }
  }

  // Initialize the uploader from the index file on disk.
  async fn initialize(&mut self) -> anyhow::Result<()> {
    let path = ARTIFACT_UPLOAD_DIRECTORY.join(&*REPORT_INDEX_FILE);
    log::debug!("initializing index: {}", path.display());
    let loaded = async {
      let mut file = self.file_system.open_regular_file(&path).await?;
      Ok::<_, anyhow::Error>(chunked::read_index(&mut file).await?)
    }
    .await;
    let index = match loaded {
      Ok(index) => index,
      Err(e) => {
        if !e.downcast_ref::<io::Error>().is_some_and(|error| {
          matches!(
            error.kind(),
            io::ErrorKind::NotFound | io::ErrorKind::InvalidData
          )
        }) {
          return Err(e);
        }
        log::debug!("unable to open index: {e}");
        log::debug!("creating new index");

        self
          .file_system
          .remove_dir(&ARTIFACT_UPLOAD_DIRECTORY)
          .await?;
        self
          .file_system
          .create_dir(&ARTIFACT_UPLOAD_DIRECTORY)
          .await?;
        ArtifactUploadIndex::default()
      },
    };
    self.index = index.artifact.into_iter().collect();
    self.chunked.entries = index.chunked_artifact;

    // Ensure that the files stored on disk pending upload and the index are in sync. If either the
    // file is missing for an index entry or a file exists without an index entry they can never
    // be uploaded, so just clean them up.

    // TODO(snowp): Should we check for crc integrity at this point? Currently we only do so when
    // we are considering a file for upload.

    let mut modified = false;
    let mut new_index = VecDeque::default();
    for entry in self.index.drain(..) {
      let path = match Self::payload_path(&entry) {
        Ok(path) => path,
        Err(error) => {
          log::warn!("discarding invalid queued artifact {}: {error}", entry.name);
          modified = true;
          self.index_sync_required = true;
          continue;
        },
      };
      let path_exists = self.file_system.exists(&path).await;
      match path_exists {
        Ok(false) => {
          log::debug!(
            "removing artifact {} from index, file does not exist",
            entry.name
          );
          modified = true;
          self.index_sync_required |=
            entry.type_id.as_deref() == Some(WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID);
          continue;
        },
        Err(error) => {
          log::warn!("failed to locate queued artifact {}: {error}", entry.name);
        },
        Ok(true) => {},
      }
      new_index.push_back(entry);
    }

    self.index = new_index;
    self.recover_chunks(&mut modified).await?;

    if modified {
      self.write_index().await?;
    }

    if let Err(error) = self.clean_orphan_files().await {
      log::warn!("failed to remove orphaned artifact files; will retry: {error}");
    }
    self.clean_sources().await;
    Ok(())
  }

  async fn handle_intent_negotiation_decision(&mut self, decision: IntentDecision) -> Result<()> {
    match decision {
      IntentDecision::Drop => {
        self.stats.dropped_intent.inc();
        let entry = self.index.pop_front().ok_or(InvariantError::Invariant)?;
        self
          .discard_upload(
            entry,
            "artifact upload was rejected during intent negotiation".to_string(),
          )
          .await;
      },
      IntentDecision::UploadImmediately => {
        self.stats.accepted_intent.inc();
        let entry = self.index.front_mut().ok_or(InvariantError::Invariant)?;
        log::debug!(
          "artifact intent accepted: artifact_id={}, type={}",
          entry.name,
          entry.type_id.as_deref().unwrap_or("<missing>")
        );
        // Mark the file as being ready for uploads and persist this to the index.
        entry.pending_intent_negotiation = false;
        if let Err(error) = self.write_index().await {
          log::warn!("failed to write artifact index: {error}");
        }
      },
    }
    Ok(())
  }

  async fn handle_upload_complete(&mut self) -> Result<String> {
    self.stats.uploaded.inc();

    let entry = self.index.pop_front().ok_or(InvariantError::Invariant)?;
    self.index_sync_required |=
      entry.type_id.as_deref() == Some(WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID);
    self.retire_whole_file(&entry).await;

    Ok(entry.name)
  }

  async fn discard_upload(&mut self, entry: Artifact, error: String) {
    log::debug!(
      "discarding artifact: artifact_id={}, type={}, reason={error}",
      entry.name,
      entry.type_id.as_deref().unwrap_or("<missing>")
    );
    self.index_sync_required |=
      entry.type_id.as_deref() == Some(WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID);
    self.retire_whole_file(&entry).await;
    self.complete_upload(&entry.name, Err(error));
  }

  async fn retire_whole_file(&mut self, entry: &Artifact) {
    let result = async {
      let paths = storage::StoredArtifact::WholeFile(entry).owned_paths()?;
      self.retire_files(&paths).await
    }
    .await;
    if let Err(error) = result {
      log::warn!("failed to retire artifact {}: {error}", entry.name);
    }
  }

  fn complete_upload(&mut self, artifact_id: &str, result: std::result::Result<(), String>) {
    if let Some(completion_tx) = self.upload_completions.remove(artifact_id) {
      let _ = completion_tx.send(result);
    }
  }

  fn stop_current_upload(&mut self) {
    if let Some(task) = self.upload_task_handle.take() {
      task.abort();
    }
    if let Some(task) = self.intent_task_handle.take() {
      task.abort();
    }
  }

  fn index_proto(&self) -> ArtifactUploadIndex {
    ArtifactUploadIndex {
      artifact: self.index.iter().cloned().collect(),
      chunked_artifact: self.chunked.entries.clone(),
      ..Default::default()
    }
  }

  async fn write_index(&mut self) -> std::result::Result<(), storage::IndexWriteError> {
    log::debug!("writing index to disk");
    self.index_dirty = true;

    let index_path = ARTIFACT_UPLOAD_DIRECTORY.join(&*REPORT_INDEX_FILE);
    let publication: anyhow::Result<()> = async {
      let index = self.index_proto();
      if index.compute_size() > chunked::MAX_INDEX_BYTES {
        anyhow::bail!("artifact index exceeds bounded recovery size");
      }
      let compressed = write_compressed_protobuf(&index)?;
      if compressed.len() as u64 > chunked::MAX_INDEX_BYTES {
        anyhow::bail!("compressed artifact index exceeds bounded recovery size");
      }
      let staging_path = index_path.with_extension("tmp");
      self
        .file_system
        .write_file(&staging_path, &compressed)
        .await?;
      self
        .file_system
        .rename_file(&staging_path, &index_path)
        .await
    }
    .await;
    publication.map_err(|error| storage::IndexWriteError {
      published: false,
      error,
    })?;
    // Readers can observe a rename even when the following durability check fails.
    self
      .queue_revision
      .send_modify(|revision| *revision = revision.wrapping_add(1));
    if self.index_sync_required || !self.chunked.entries.is_empty() {
      self
        .file_system
        .sync_file_and_parent(&index_path)
        .await
        .map_err(|error| storage::IndexWriteError {
          published: true,
          error,
        })?;
    }
    self.index_dirty = false;
    self.index_sync_required = false;
    Ok(())
  }

  async fn upload_artifact(
    data_upload_tx: tokio::sync::mpsc::Sender<DataUpload>,
    contents: Vec<u8>,
    name: String,
    type_id: String,
    timestamp: OffsetDateTime,
    session_id: String,
    mut retry_policy: ExponentialBackoff,
    state_metadata: RequestMetadata,
    feature_flags: Vec<FeatureFlag>,
    command_id: Option<String>,
    content_type: Option<String>,
    payload_encoding: ArtifactPayloadEncoding,
    crash_profile: Option<CrashProfileMetadata>,
  ) -> Result<()> {
    let path = ARTIFACT_UPLOAD_DIRECTORY.join(&name);
    log::debug!(
      "uploading artifact: artifact_id={name}, type={type_id}, path={}, payload_bytes={}, \
       encoding={payload_encoding:?}, linked_reports={}",
      path.display(),
      contents.len(),
      crash_profile
        .as_ref()
        .map_or(0, |profile| profile.report_ids.len())
    );

    // Use exponential backoff to avoid retrying over and over again in case something is going
    // wrong. We put no overall timeout as the device might be offline for a long time and we want
    // to give it whatever time it needs to perform the upload.

    retry_notify(
      || Some(retry_policy.next_backoff()),
      || async {
        let upload_uuid = TrackedArtifactUpload::upload_uuid();
        let (tracked, response) = TrackedArtifactUpload::new(
          upload_uuid.clone(),
          UploadArtifactRequest {
            upload_uuid,
            type_id: type_id.clone(),
            contents: contents.clone(),
            artifact_id: name.clone(),
            time: timestamp.into_proto(),
            session_id: session_id.clone(),
            state_metadata: state_metadata.current(),
            feature_flags: feature_flags.clone(),
            command_id: command_id.clone(),
            content_type: content_type.clone(),
            payload_encoding: payload_encoding.into(),
            crash_profile: crash_profile.clone().into(),
            ..Default::default()
          },
        );
        data_upload_tx
          .send(DataUpload::ArtifactUpload(tracked))
          .await
          .map_err(|_| Error::Shutdown)?;
        match response.await {
          Ok(response) if response.success => {
            log::debug!("artifact upload succeeded: artifact_id={name}, type={type_id}");
            Ok(())
          },
          Ok(_) | Err(_) => Err(Error::IncompleteUpload),
        }
      },
      |error| matches!(error, Error::IncompleteUpload),
      |error, delay| {
        log::debug!(
          "artifact upload failed: artifact_id={name}, type={type_id}, error={error:?}, \
           retry_delay={delay:?}"
        );
      },
    )
    .await
  }

  async fn perform_intent_negotiation(
    data_upload_tx: tokio::sync::mpsc::Sender<DataUpload>,
    id: String,
    type_id: String,
    session_id: String,
    timestamp: OffsetDateTime,
    state_metadata: RequestMetadata,
    crash_profile: Option<CrashProfileMetadata>,
    mut retry_policy: ExponentialBackoff,
  ) -> Result<IntentDecision> {
    retry_notify(
      || Some(retry_policy.next_backoff()),
      || async {
        let upload_uuid = TrackedArtifactIntent::upload_uuid();
        let (tracked, response) = TrackedArtifactIntent::new(
          upload_uuid.clone(),
          UploadArtifactIntentRequest {
            type_id: type_id.clone(),
            artifact_id: id.clone(),
            intent_uuid: upload_uuid.clone(),
            session_id: (!session_id.is_empty()).then(|| session_id.clone()),
            time: timestamp.into_proto(),
            metadata: state_metadata.current(),
            crash_profile: crash_profile.clone().into(),
            ..Default::default()
          },
        );
        data_upload_tx
          .send(DataUpload::ArtifactUploadIntent(tracked))
          .await
          .map_err(|_| Error::Shutdown)?;
        response
          .await
          .map(|response| response.decision)
          .map_err(|_| Error::IncompleteUpload)
      },
      |error| matches!(error, Error::IncompleteUpload),
      |error, delay| {
        log::debug!(
          "artifact intent failed: artifact_id={id}, type={type_id}, error={error:?}, \
           retry_delay={delay:?}"
        );
      },
    )
    .await
  }
}

/// The metadata of an artifact being sent. The index keeps it raw; every attempt to send an intent
/// or upload request scrubs it with the rules current at that moment, so rules applied while a
/// request is being retried, for example before any configuration has loaded, reach the next
/// attempt.
struct RequestMetadata {
  raw: HashMap<String, Data>,
  scrub_rules: watch::Receiver<Arc<ScrubRules>>,
}

impl RequestMetadata {
  fn current(&self) -> HashMap<String, Data> {
    scrub_metadata(&self.scrub_rules.borrow(), &self.raw)
  }
}

/// Applies scrub rules to artifact metadata. Values no rule changes are sent exactly as stored.
fn scrub_metadata(rules: &ScrubRules, metadata: &HashMap<String, Data>) -> HashMap<String, Data> {
  if rules.is_empty() {
    return metadata.clone();
  }
  metadata
    .iter()
    .filter_map(|(key, value)| {
      // A value that cannot be read as a field cannot be rewritten by a regex rule, but a rule
      // removing its key still applies, so it is checked with a placeholder value.
      let Some(original) = DataValue::from_proto(value.clone()) else {
        return rules
          .scrub_field(key, &mut DataValue::default())
          .then(|| (key.clone(), value.clone()));
      };
      let mut field = original.clone();
      if !rules.scrub_field(key, &mut field) {
        return None;
      }
      // Only a value a rule changed is re-encoded; the conversion drops details such as a
      // binary value's type.
      if field == original {
        Some((key.clone(), value.clone()))
      } else {
        Some((key.clone(), field.into_proto()))
      }
    })
    .collect()
}
