// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./uploader_test.rs"]
mod tests;

use bd_api::upload::{IntentDecision, TrackedArtifactIntent, TrackedArtifactUpload};
use bd_api::{DataUpload, RuntimeBackoffPolicy};
use bd_backoff::{ExponentialBackoff, InfiniteBackoff};
use bd_bounded_buffer::SendCounters;
use bd_client_common::artifact::{CLIENT_REPORT_ARTIFACT_TYPE_ID, STATE_SNAPSHOT_ARTIFACT_TYPE_ID};
use bd_client_common::error::InvariantError;
use bd_client_common::file::{
  async_write_checksummed_data,
  read_and_compress_limited,
  read_checksummed_data,
  read_compressed_protobuf,
  write_checksummed_data,
  write_compressed_protobuf,
};
use bd_client_common::file_system::FileSystem;
use bd_client_common::maybe_await;
use bd_client_stats_store::{Collector, Counter, Scope};
use bd_error_reporter::reporter::handle_unexpected;
use bd_log_primitives::LogFields;
use bd_macros::ApproximateSize;
use bd_proto::protos::client::api::{
  ArtifactPayloadEncoding,
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
use bd_time::{OffsetDateTimeExt, TimeDurationExt, TimeProvider, TimestampExt};
use mockall::automock;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
#[cfg(test)]
use tests::TestHooks;
use time::OffsetDateTime;
use tokio::io::{AsyncRead, AsyncWriteExt};
use tokio::sync::oneshot;
use uuid::Uuid;

/// Root directory for all files used for storage and uploading.
pub static ARTIFACT_UPLOAD_DIRECTORY: LazyLock<PathBuf> = LazyLock::new(|| "report_uploads".into());

/// The index file used for tracking all of the individual files.
pub static REPORT_INDEX_FILE: LazyLock<PathBuf> = LazyLock::new(|| "report_index.pb".into());

pub const WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID: &str = "workflow_attachment";

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

// TODO(snowp): Consider allowing passing an open file handle instead of having to hold the data in
// memory while entry is pending within the channel.
#[derive(ApproximateSize, Debug)]
struct NewUpload {
  uuid: Uuid,
  source: UploadSource,
  type_id: String,
  #[approximate_size(with = bd_log_primitives::approximate_ahash_map_children_bytes)]
  state: LogFields,
  timestamp: Option<OffsetDateTime>,
  session_id: String,
  feature_flags: Vec<SnappedFeatureFlag>,
  command_id: Option<String>,
  #[approximate_size(skip)]
  persisted_tx: Option<oneshot::Sender<std::result::Result<(), EnqueueError>>>,
  #[approximate_size(skip)]
  completion_tx: Option<oneshot::Sender<std::result::Result<(), String>>>,
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
  // A checksum-verified SDK payload shared with the upload queue using a hard link.
  Retained(PathBuf),
}

// Used for bounded_buffer logs
impl std::fmt::Display for NewUpload {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "NewUpload {{ uuid: {}, source: {:?} }}",
      self.uuid, self.source
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
  fn enqueue_workflow_attachment(
    &self,
    artifact_id: Uuid,
    source_path: PathBuf,
    session_id: String,
    persisted_tx: Option<oneshot::Sender<std::result::Result<(), EnqueueError>>>,
    completion_tx: Option<oneshot::Sender<std::result::Result<(), String>>>,
  ) -> std::result::Result<(), EnqueueError>;

  fn enqueue_upload(
    &self,
    source: UploadSource,
    type_id: String,
    state: LogFields,
    timestamp: Option<OffsetDateTime>,
    session_id: String,
    feature_flags: Vec<SnappedFeatureFlag>,
    persisted_tx: Option<oneshot::Sender<std::result::Result<(), EnqueueError>>>,
  ) -> std::result::Result<Uuid, EnqueueError>;

  fn enqueue_command_upload(
    &self,
    source: UploadSource,
    type_id: String,
    state: LogFields,
    timestamp: Option<OffsetDateTime>,
    session_id: String,
    feature_flags: Vec<SnappedFeatureFlag>,
    command_id: String,
    persisted_tx: Option<oneshot::Sender<std::result::Result<(), EnqueueError>>>,
    completion_tx: Option<oneshot::Sender<std::result::Result<(), String>>>,
  ) -> std::result::Result<Uuid, EnqueueError>;
}

pub struct UploadClient {
  upload_tx: bd_bounded_buffer::Sender<NewUpload>,
  counter_stats: SendCounters,
}

impl UploadClient {
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
    persisted_tx: Option<oneshot::Sender<std::result::Result<(), EnqueueError>>>,
    completion_tx: Option<oneshot::Sender<std::result::Result<(), String>>>,
  ) -> std::result::Result<Uuid, EnqueueError> {
    let result = self
      .upload_tx
      .try_send(NewUpload {
        uuid,
        source,
        type_id,
        state,
        timestamp,
        session_id,
        feature_flags,
        command_id,
        persisted_tx,
        completion_tx,
      })
      .inspect_err(|e| log::warn!("failed to enqueue artifact upload: {e:?}"));

    self.counter_stats.record(&result);
    result.map_err(|e| match e {
      bd_bounded_buffer::TrySendError::FullSizeOverflow => EnqueueError::QueueFull,
      bd_bounded_buffer::TrySendError::Closed => EnqueueError::Closed,
    })?;

    Ok(uuid)
  }
}

impl Client for UploadClient {
  fn enqueue_workflow_attachment(
    &self,
    artifact_id: Uuid,
    source_path: PathBuf,
    session_id: String,
    persisted_tx: Option<oneshot::Sender<std::result::Result<(), EnqueueError>>>,
    completion_tx: Option<oneshot::Sender<std::result::Result<(), String>>>,
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
      LogFields::default(),
      None,
      session_id,
      Vec::new(),
      None,
      persisted_tx,
      completion_tx,
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
      persisted_tx,
      None,
    )
  }

  fn enqueue_command_upload(
    &self,
    source: UploadSource,
    type_id: String,
    state: LogFields,
    timestamp: Option<OffsetDateTime>,
    session_id: String,
    feature_flags: Vec<SnappedFeatureFlag>,
    command_id: String,
    persisted_tx: Option<oneshot::Sender<std::result::Result<(), EnqueueError>>>,
    completion_tx: Option<oneshot::Sender<std::result::Result<(), String>>>,
  ) -> std::result::Result<Uuid, EnqueueError> {
    self.enqueue(
      Uuid::new_v4(),
      source,
      type_id,
      state,
      timestamp,
      session_id,
      feature_flags,
      Some(command_id),
      persisted_tx,
      completion_tx,
    )
  }
}

#[derive(thiserror::Error, Debug)]
enum Error {
  #[error("Task is shutting down")]
  Shutdown,
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

pub struct Uploader {
  data_upload_tx: tokio::sync::mpsc::Sender<DataUpload>,
  upload_queued_rx: bd_bounded_buffer::Receiver<NewUpload>,
  shutdown: ComponentShutdown,
  time_provider: Arc<dyn TimeProvider>,
  file_system: Arc<dyn FileSystem>,

  index: VecDeque<Artifact>,
  upload_completions: HashMap<String, oneshot::Sender<std::result::Result<(), String>>>,

  max_entries: IntWatch<bd_runtime::runtime::artifact_upload::MaxPendingEntries>,
  max_attachment_bytes: IntWatch<attachment::MaxBytes>,
  backoff_policy: RuntimeBackoffPolicy<
    bd_runtime::runtime::retry_backoff::InitialBackoffInterval,
    bd_runtime::runtime::retry_backoff::MaxBackoffInterval,
    bd_runtime::runtime::retry_backoff::BackoffGrowthFactorBasisPoints,
  >,

  intent_task_handle: Option<tokio::task::JoinHandle<Result<IntentDecision>>>,
  upload_task_handle: Option<tokio::task::JoinHandle<Result<()>>>,

  stats: Stats,

  #[cfg(test)]
  test_hooks: Option<TestHooks>,
}

impl Uploader {
  pub fn new(
    file_system: Arc<dyn FileSystem>,
    data_upload_tx: tokio::sync::mpsc::Sender<DataUpload>,
    time_provider: Arc<dyn TimeProvider>,
    runtime: &ConfigLoader,
    collector: &Collector,
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

    let uploader = Self {
      data_upload_tx,
      upload_queued_rx: upload_rx,
      shutdown,
      time_provider,
      file_system,
      index: VecDeque::default(),
      upload_completions: HashMap::default(),
      max_entries: runtime.register_int_watch(),
      max_attachment_bytes: runtime.register_int_watch(),
      backoff_policy: RuntimeBackoffPolicy::new(runtime),
      upload_task_handle: None,
      intent_task_handle: None,
      stats: Stats::new(&scope),
      #[cfg(test)]
      test_hooks: None,
    };

    let client = UploadClient {
      upload_tx,
      counter_stats: SendCounters::new(&scope, "enqueue"),
    };

    (uploader, client)
  }

  pub async fn run(self) {
    if let Err(Error::Unhandled(e)) = self.run_inner().await {
      handle_unexpected(Err::<(), _>(e), "artifact uploader");
    }
  }

  async fn run_inner(mut self) -> Result<()> {
    self.initialize().await;

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
          log::debug!("starting intent negotiation for {:?}", next.name);

          self.intent_task_handle = Some(tokio::spawn(Self::perform_intent_negotiation(
            self.data_upload_tx.clone(),
            next.name.clone(),
            next.type_id.clone().unwrap_or_default(),
            next.session_id.clone(),
            next.time.to_offset_date_time(),
            next.metadata.clone(),
            self.backoff_policy.backoff_mark_update(),
          )));
          continue;
        }

        let file_path = ARTIFACT_UPLOAD_DIRECTORY.join(&next.name);
        let Ok(contents) = self.file_system.read_file(&file_path).await else {
          log::debug!(
            "failed to read file for artifact {}, deleting and removing from index",
            next.name
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
        log::debug!("starting file upload for {:?}", next.name);
        self.upload_task_handle = Some(tokio::spawn(Self::upload_artifact(
          self.data_upload_tx.clone(),
          contents,
          next.name.clone(),
          next.type_id.clone().unwrap_or_default(),
          next.time.to_offset_date_time(),
          next.session_id.clone(),
          self.backoff_policy.backoff_mark_update(),
          next.metadata.clone(),
          next.feature_flags.clone(),
          next.command_id.clone(),
          next.payload_encoding.enum_value_or_default(),
        )));
      }

      // Only one task should ever be active at a time.
      debug_assert!(!(self.intent_task_handle.is_some() && self.upload_task_handle.is_some()));

      // At this point either wait for progress to be made to the current entry or wait for a new
      // entry to be submitted.
      tokio::select! {
        () = self.shutdown.cancelled() => {
          log::debug!("shutting down uploader");
          self.stop_current_upload();

          return Err(Error::Shutdown);
        }
        Some(NewUpload {
            uuid,
            source,
            type_id,
            state,
            timestamp,
            session_id,
            feature_flags,
            command_id,
            persisted_tx,
            completion_tx,
        }) = self.upload_queued_rx.recv() => {
          log::debug!("tracking artifact: {uuid} for upload");
          self
            .track_new_upload(
              uuid,
              source,
              type_id,
              state,
              session_id,
              timestamp,
              feature_flags,
              command_id,
              persisted_tx,
              completion_tx,
            )
            .await;
        }
        intent_decision = maybe_await(&mut self.intent_task_handle) => {
            self.handle_intent_negotiation_decision(intent_decision??).await?;
        }
        result = maybe_await(&mut self.upload_task_handle) => {
            match result? {
              Ok(()) => {
                let name = self.handle_upload_complete().await?;
                self.complete_upload(&name, Ok(()));

                #[cfg(test)]
                if let Some(hooks) = &self.test_hooks {
                    hooks.upload_complete_tx.send(name).await.unwrap();
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
  async fn initialize(&mut self) {
    let path = ARTIFACT_UPLOAD_DIRECTORY.join(&*REPORT_INDEX_FILE);
    log::debug!("initializing index: {}", path.display());
    self.index = match self
      .file_system
      .read_file(&path)
      .await
      .and_then(|contents| read_compressed_protobuf::<ArtifactUploadIndex>(&contents))
    {
      Ok(index) => index,
      Err(e) => {
        log::debug!("unable to open index: {e}");
        log::debug!("creating new index");

        let _ignored = self
          .file_system
          .remove_dir(&ARTIFACT_UPLOAD_DIRECTORY)
          .await;
        let _ignored = self
          .file_system
          .create_dir(&ARTIFACT_UPLOAD_DIRECTORY)
          .await;
        ArtifactUploadIndex::default()
      },
    }
    .artifact
    .into_iter()
    .collect();

    // Ensure that the files stored on disk pending upload and the index are in sync. If either the
    // file is missing for an index entry or a file exists without an index entry they can never
    // be uploaded, so just clean them up.

    // TODO(snowp): Should we check for crc integrity at this point? Currently we only do so when
    // we are considering a file for upload.

    let mut modified = false;
    let mut new_index = VecDeque::default();
    let mut filenames = HashSet::new();
    for mut entry in self.index.drain(..) {
      let file_path = ARTIFACT_UPLOAD_DIRECTORY.join(&entry.name);
      if !self
        .file_system
        .exists(&file_path)
        .await
        .unwrap_or_default()
      {
        log::debug!(
          "removing artifact {} from index, file does not exist",
          entry.name
        );
        modified = true;
        continue;
      }
      // Handle inserting a default type_id for entries that are missing it. This can happen for
      // older versions of the uploader that didn't persist the type_id to disk.
      // TODO(snowp): Remove this at some point in the future after.
      if entry.type_id.as_deref().unwrap_or_default().is_empty() {
        entry.type_id = Some(ArtifactType::default().to_type_id().to_string());
        modified = true;
      }
      filenames.insert(entry.name.clone());
      new_index.push_back(entry);
    }

    self.index = new_index;

    if modified && let Err(error) = self.write_index().await {
      log::warn!("failed to write artifact index: {error}");
    }

    // Remove any files left in the directory that isn't the index or a file referenced by the
    // index.
    let files = self
      .file_system
      .list_files(&ARTIFACT_UPLOAD_DIRECTORY)
      .await
      .unwrap_or_default();

    for file in files {
      if file.ends_with(REPORT_INDEX_FILE.to_string_lossy().as_ref()) {
        continue;
      }

      let Some(back) = file.split('/').next_back() else {
        continue;
      };

      if !filenames.contains(back) {
        log::debug!("removing artifact {file} from disk, not in index");
        if let Err(e) = self.file_system.delete_file(&PathBuf::from(&file)).await {
          log::warn!("failed to delete artifact {file:?}: {e}");
        }
      }
    }
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
    let file_path = ARTIFACT_UPLOAD_DIRECTORY.join(&entry.name);

    if let Err(e) = self.file_system.delete_file(&file_path).await {
      log::warn!("failed to delete artifact {:?}: {}", entry.name, e);
    }

    if let Err(error) = self.write_index().await {
      log::warn!("failed to write artifact index: {error}");
    }

    Ok(entry.name)
  }

  async fn discard_upload(&mut self, entry: Artifact, error: String) {
    if let Err(delete_error) = self
      .file_system
      .delete_file(&ARTIFACT_UPLOAD_DIRECTORY.join(&entry.name))
      .await
    {
      log::warn!(
        "failed to delete artifact {:?}: {}",
        entry.name,
        delete_error
      );
    }
    if let Err(error) = self.write_index().await {
      log::warn!("failed to write artifact index: {error}");
    }
    self.complete_upload(&entry.name, Err(error));
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

  async fn write_command_attachment(
    &self,
    source: UploadSource,
    target_path: &Path,
    path_source: &mut Option<(PathBuf, bool)>,
  ) -> anyhow::Result<()> {
    let max_attachment_bytes = u64::from(*self.max_attachment_bytes.read());
    let reader: Box<dyn AsyncRead + Unpin + Send> = match source {
      UploadSource::Bytes(contents) => Box::new(std::io::Cursor::new(contents)),
      UploadSource::File(file) => Box::new(tokio::fs::File::from_std(file)),
      UploadSource::Path(source_path) => {
        let file = self.file_system.open_file(&source_path).await?;
        *path_source = Some((source_path, false));
        Box::new(file)
      },
      UploadSource::Retained(_) => anyhow::bail!("command attachments cannot use retained sources"),
    };

    let contents = read_and_compress_limited(reader, max_attachment_bytes).await?;
    let mut target_file = self.file_system.create_file(target_path).await?;
    target_file.write_all(&contents).await?;
    Ok(())
  }

  async fn track_new_upload(
    &mut self,
    uuid: Uuid,
    source: UploadSource,
    type_id: String,
    state: LogFields,
    session_id: String,
    timestamp: Option<OffsetDateTime>,
    feature_flags: Vec<SnappedFeatureFlag>,
    command_id: Option<String>,
    mut persisted_tx: Option<oneshot::Sender<std::result::Result<(), EnqueueError>>>,
    completion_tx: Option<oneshot::Sender<std::result::Result<(), String>>>,
  ) {
    if let Some(existing) = self
      .index
      .iter()
      .find(|entry| entry.name == uuid.to_string())
    {
      let matching_workflow_attachment = existing.type_id.as_deref()
        == Some(WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID)
        && type_id == WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID
        && existing.session_id == session_id;
      if let Some(tx) = persisted_tx {
        let result = if existing.type_id.as_deref() == Some(WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID)
          && !matching_workflow_attachment
        {
          Err(EnqueueError::Other(anyhow::anyhow!(
            "workflow attachment ID belongs to another session or artifact type"
          )))
        } else {
          Ok(())
        };
        if result.is_ok()
          && matching_workflow_attachment
          && let Some(completion_tx) = completion_tx
        {
          self
            .upload_completions
            .entry(uuid.to_string())
            .or_insert(completion_tx);
        }
        let _ = tx.send(result);
      }
      return;
    }
    // Previously we would always drop the oldest entry when we hit capacity, but for state
    // snapshots this would result in us dropping uploads that we know we need to hydrate logs
    // that were scheduled for uploads. To mitigate this we treat state snapshots differently
    // and avoid dropping them when we hit capacity, which means that in the worst case if we have a
    // lot of state snapshots we might fill up this queue and apply backpressure to the state
    // snapshot producer, deferring the snapshot limit enforcement to the producer instead of the
    // uploader.

    // TODO(snowp): Consider also having a bound on the size of the files persisted to disk.
    // TODO(snowp): We should consider redoing how backpressure works for crash reports as well as
    // there are cases in which we drop reports. For now limit the backpressure mechanism to
    // StateSnapshots as we want stronger guarantees than what is currently provided for regular
    // crash reports.
    if self.index.len() == usize::try_from(*self.max_entries.read()).unwrap_or_default() {
      if let Some(index_to_drop) = self.index.iter().position(|entry| {
        entry.type_id.as_deref() != Some(ArtifactType::StateSnapshot.to_type_id())
          && entry.type_id.as_deref() != Some(WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID)
      }) {
        log::debug!("upload queue is full, dropping oldest non-state upload");
        self.stats.dropped.inc();
        if index_to_drop == 0 {
          self.stop_current_upload();
        }
        if let Some(entry) = self.index.remove(index_to_drop) {
          self
            .discard_upload(
              entry,
              "artifact upload was evicted from the upload queue".to_string(),
            )
            .await;
        }
      } else {
        self.stats.dropped.inc();
        if let Some(tx) = persisted_tx.take() {
          let _ = tx.send(Err(EnqueueError::QueueFull));
        }
        return;
      }
    }

    let retained = matches!(source, UploadSource::Retained(_));
    let command_attachment = command_id.is_some();
    let payload_encoding =
      if command_attachment || (retained && type_id == WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID) {
        ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_ZLIB
      } else {
        ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_RAW
      };
    let uuid = uuid.to_string();

    let target_path = ARTIFACT_UPLOAD_DIRECTORY.join(&uuid);
    let mut path_source = None;
    let (write_result, storage_format) = match source {
      source if command_attachment => (
        self
          .write_command_attachment(source, &target_path, &mut path_source)
          .await,
        StorageFormat::RAW,
      ),
      UploadSource::Bytes(bytes) => {
        let mut target_file = match self.file_system.create_file(&target_path).await {
          Ok(file) => file,
          Err(e) => {
            log::warn!("failed to create file for artifact: {uuid} on disk: {e}");
            if let Some(tx) = persisted_tx.take() {
              let _ = tx.send(Err(EnqueueError::Other(anyhow::anyhow!(
                "failed to create file for artifact {uuid}: {e}"
              ))));
            }
            return;
          },
        };
        (
          target_file
            .write_all(&write_checksummed_data(&bytes))
            .await
            .map_err(Into::into),
          StorageFormat::CHECKSUMMED,
        )
      },
      UploadSource::File(file) => {
        let target_file = match self.file_system.create_file(&target_path).await {
          Ok(file) => file,
          Err(e) => {
            log::warn!("failed to create file for artifact: {uuid} on disk: {e}");
            if let Some(tx) = persisted_tx.take() {
              let _ = tx.send(Err(EnqueueError::Other(anyhow::anyhow!(
                "failed to create file for artifact {uuid}: {e}"
              ))));
            }

            #[cfg(test)]
            if let Some(hooks) = &self.test_hooks {
              hooks.entry_received_tx.send(uuid.clone()).await.unwrap();
            }
            return;
          },
        };

        (
          async_write_checksummed_data(tokio::fs::File::from_std(file), target_file).await,
          StorageFormat::CHECKSUMMED,
        )
      },
      UploadSource::Path(source_path) => {
        let result = if let Err(e) = self
          .file_system
          .rename_file(&source_path, &target_path)
          .await
        {
          log::debug!("failed to move artifact source, falling back to copy: {e}");
          match tokio::fs::File::open(&source_path).await {
            Ok(source_file) => match self.file_system.create_file(&target_path).await {
              Ok(target_file) => {
                let result = async_write_checksummed_data(source_file, target_file).await;
                path_source = Some((source_path, false));
                result
              },
              Err(e) => Err(e),
            },
            Err(e) => Err(anyhow::anyhow!(
              "failed to open file for artifact {} on disk: {}",
              source_path.display(),
              e
            )),
          }
        } else {
          path_source = Some((source_path, true));
          Ok(())
        };

        (result, StorageFormat::RAW)
      },
      UploadSource::Retained(source_path) => (
        async {
          self
            .file_system
            .link_file(&source_path, &target_path)
            .await?;
          if let Err(error) = self.file_system.sync_file_and_parent(&target_path).await {
            if let Err(cleanup_error) = self.file_system.delete_file(&target_path).await {
              log::warn!(
                "failed to remove unsynced retained artifact {}: {cleanup_error}",
                target_path.display()
              );
            }
            return Err(error);
          }
          Ok(())
        }
        .await,
        StorageFormat::RAW,
      ),
    };

    if let Err(e) = write_result {
      log::warn!("failed to write artifact to disk: {uuid} to disk: {e}");
      if let Some(tx) = persisted_tx.take() {
        let error = if retained {
          retained_persistence_error(e)
        } else {
          EnqueueError::Other(anyhow::anyhow!(
            "failed to write artifact to disk {uuid}: {e}"
          ))
        };
        let _ = tx.send(Err(error));
      }

      #[cfg(test)]
      if let Some(hooks) = &self.test_hooks {
        hooks.entry_received_tx.send(uuid.clone()).await.unwrap();
      }
      return;
    }

    // Only write the index after we've written the report file to disk to try to minimze the risk
    // of the file being written without a corresponding entry.
    let type_id = if type_id.is_empty() {
      ArtifactType::default().to_type_id().to_string()
    } else {
      type_id
    };
    self.index.push_back(Artifact {
      name: uuid.clone(),
      type_id: Some(type_id),
      time: timestamp
        .unwrap_or_else(|| self.time_provider.now())
        .into_proto(),
      session_id,
      pending_intent_negotiation: true,
      metadata: state
        .into_iter()
        .map(|(key, value)| (key.into(), value.into_proto()))
        .collect(),
      storage_format: storage_format.into(),
      feature_flags: feature_flags
        .into_iter()
        .map(
          |SnappedFeatureFlag {
             name,
             variant,
             last_updated,
           }| FeatureFlag {
            name,
            variant,
            last_updated: last_updated.into_proto(),
            ..Default::default()
          },
        )
        .collect(),
      command_id,
      payload_encoding: payload_encoding.into(),
      ..Default::default()
    });

    let mut write_result = self.write_index().await;
    if write_result.is_ok() && retained {
      write_result = self
        .file_system
        .sync_file_and_parent(&ARTIFACT_UPLOAD_DIRECTORY.join(&*REPORT_INDEX_FILE))
        .await;
    }
    if let Err(error) = write_result {
      self.index.pop_back();
      if let Some((source_path, true)) = path_source.as_ref() {
        if let Err(restore_error) = self
          .file_system
          .rename_file(&target_path, source_path)
          .await
        {
          log::warn!(
            "failed to restore artifact source {}: {restore_error}",
            source_path.display()
          );
        }
      } else if let Err(delete_error) = self.file_system.delete_file(&target_path).await {
        log::warn!("failed to remove unindexed artifact {uuid}: {delete_error}");
      }
      if let Some(tx) = persisted_tx {
        let error = if retained {
          retained_persistence_error(error)
        } else {
          EnqueueError::Other(error)
        };
        let _ = tx.send(Err(error));
      }
      return;
    }
    if let Some((source_path, false)) = path_source
      && let Err(error) = self.file_system.delete_file(&source_path).await
    {
      log::warn!(
        "failed to delete copied source {}: {error}",
        source_path.display()
      );
    }
    if let Some(tx) = persisted_tx {
      let _ = tx.send(Ok(()));
    }
    if let Some(completion_tx) = completion_tx {
      self.upload_completions.insert(uuid.clone(), completion_tx);
    }

    #[cfg(test)]
    if let Some(hooks) = &self.test_hooks {
      hooks.entry_received_tx.send(uuid.clone()).await.unwrap();
    }
  }

  async fn write_index(&self) -> anyhow::Result<()> {
    log::debug!("writing index to disk");

    let index = ArtifactUploadIndex {
      artifact: self.index.iter().cloned().collect(),
      ..Default::default()
    };

    let compressed = write_compressed_protobuf(&index)?;
    let index_path = ARTIFACT_UPLOAD_DIRECTORY.join(&*REPORT_INDEX_FILE);
    let staging_path = index_path.with_extension("tmp");
    self
      .file_system
      .write_file(&staging_path, &compressed)
      .await?;
    self
      .file_system
      .rename_file(&staging_path, &index_path)
      .await?;
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
    state_metadata: HashMap<String, Data>,
    feature_flags: Vec<FeatureFlag>,
    command_id: Option<String>,
    payload_encoding: ArtifactPayloadEncoding,
  ) -> Result<()> {
    let path = ARTIFACT_UPLOAD_DIRECTORY.join(&name);
    log::debug!("uploading artifact: {}", path.display());

    // Use exponential backoff to avoid retrying over and over again in case something is going
    // wrong. We put no overall timeout as the device might be offline for a long time and we want
    // to give it whatever time it needs to perform the upload.

    loop {
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
          state_metadata: state_metadata.clone(),
          feature_flags: feature_flags.clone(),
          command_id: command_id.clone(),
          payload_encoding: payload_encoding.into(),
          ..Default::default()
        },
      );

      data_upload_tx
        .send(DataUpload::ArtifactUpload(tracked))
        .await
        .map_err(|_| Error::Shutdown)?;

      match response.await {
        Ok(response) if response.success => {
          log::debug!("upload of artifact: {name} succeeded");
          break;
        },
        Ok(_) if command_id.is_some() => {
          return Err(Error::Unhandled(anyhow::anyhow!(
            "command artifact upload was rejected by the server"
          )));
        },
        Ok(_) | Err(_) => {},
      }

      let delay = retry_policy.next_backoff();

      log::debug!("upload of artifact: {name} failed, retrying in {delay:?}");
      delay.sleep().await;
    }

    Ok(())
  }

  async fn perform_intent_negotiation(
    data_upload_tx: tokio::sync::mpsc::Sender<DataUpload>,
    id: String,
    type_id: String,
    session_id: String,
    timestamp: OffsetDateTime,
    state_metadata: HashMap<String, Data>,
    mut retry_policy: ExponentialBackoff,
  ) -> Result<IntentDecision> {
    loop {
      let upload_uuid = TrackedArtifactIntent::upload_uuid();
      let (tracked, response) = TrackedArtifactIntent::new(
        upload_uuid.clone(),
        UploadArtifactIntentRequest {
          type_id: type_id.clone(),
          artifact_id: id.clone(),
          intent_uuid: upload_uuid.clone(),
          session_id: (!session_id.is_empty()).then(|| session_id.clone()),
          time: timestamp.into_proto(),
          metadata: state_metadata.clone(),
          ..Default::default()
        },
      );

      data_upload_tx
        .send(DataUpload::ArtifactUploadIntent(tracked))
        .await
        .map_err(|_| Error::Shutdown)?;

      if let Ok(response) = response.await {
        break Ok(response.decision);
      }

      let delay = retry_policy.next_backoff();
      log::debug!("intent negotiation for artifact: {id} failed, retrying in {delay:?}");
      delay.sleep().await;
    }
  }
}
