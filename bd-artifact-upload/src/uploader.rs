// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./uploader_test.rs"]
mod tests;

use backoff::ExponentialBackoff;
use backoff::backoff::Backoff;
use bd_api::DataUpload;
use bd_api::upload::{IntentDecision, TrackedArtifactIntent, TrackedArtifactUpload};
use bd_bounded_buffer::SendCounters;
use bd_client_common::error::InvariantError;
use bd_client_common::file::{
  async_write_checksummed_data,
  read_checksummed_data,
  read_compressed_protobuf,
  write_compressed_protobuf,
};
use bd_client_common::file_system::FileSystem;
use bd_client_common::maybe_await;
use bd_client_stats_store::{Collector, Counter, Scope};
use bd_error_reporter::reporter::handle_unexpected;
use bd_log_primitives::LogFields;
use bd_log_primitives::size::MemorySized;
use bd_proto::protos::client::api::{UploadArtifactIntentRequest, UploadArtifactRequest};
use bd_proto::protos::client::artifact::ArtifactUploadIndex;
use bd_proto::protos::client::artifact::artifact_upload_index::Artifact;
use bd_proto::protos::client::feature_flag::FeatureFlag;
use bd_proto::protos::logging::payload::data::Data_type;
use bd_proto::protos::logging::payload::{BinaryData, Data};
use bd_runtime::runtime::{ConfigLoader, DurationWatch, IntWatch, artifact_upload};
use bd_shutdown::ComponentShutdown;
use bd_time::{OffsetDateTimeExt, TimeProvider, TimestampExt};
use mockall::automock;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};
#[cfg(test)]
use tests::TestHooks;
use time::OffsetDateTime;
use time::ext::NumericalStdDuration;
use uuid::Uuid;

/// Root directory for all files used for storage and uploading.
pub static REPORT_DIRECTORY: LazyLock<PathBuf> = LazyLock::new(|| "report_uploads".into());

/// The index file used for tracking all of the individual files.
pub static REPORT_INDEX_FILE: LazyLock<PathBuf> = LazyLock::new(|| "report_index.pb".into());

//
// FeatureFlag
//

#[derive(Debug, Clone)]
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
#[derive(Debug)]
struct NewUpload {
  uuid: Uuid,
  file: std::fs::File,
  state: LogFields,
  timestamp: Option<OffsetDateTime>,
  session_id: String,
  feature_flags: Vec<SnappedFeatureFlag>,
}

// Used for bounded_buffer logs
impl std::fmt::Display for NewUpload {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "NewUpload {{ uuid: {}, file: {:?} }}",
      self.uuid, self.file
    )
  }
}

impl MemorySized for SnappedFeatureFlag {
  fn size(&self) -> usize {
    std::mem::size_of::<OffsetDateTime>()
      + std::mem::size_of::<Option<String>>()
      + self.name.len()
      + self.variant.as_ref().map_or(0, std::string::String::len)
  }
}

impl MemorySized for NewUpload {
  fn size(&self) -> usize {
    std::mem::size_of::<Uuid>()
      + self.state.size()
      + std::mem::size_of::<Option<OffsetDateTime>>()
      + self.session_id.len()
      + self.feature_flags.size()
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

#[automock]
pub trait Client: Send + Sync {
  fn enqueue_upload(
    &self,
    file: std::fs::File,
    state: LogFields,
    timestamp: Option<OffsetDateTime>,
    session_id: String,
    feature_flags: Vec<SnappedFeatureFlag>,
  ) -> anyhow::Result<Uuid>;
}

pub struct UploadClient {
  upload_tx: bd_bounded_buffer::Sender<NewUpload>,
  counter_stats: SendCounters,
}

impl Client for UploadClient {
  /// Dispatches a payload to be uploaded, returning the associated artifact UUID.
  fn enqueue_upload(
    &self,
    file: std::fs::File,
    state: LogFields,
    timestamp: Option<OffsetDateTime>,
    session_id: String,
    feature_flags: Vec<SnappedFeatureFlag>,
  ) -> anyhow::Result<Uuid> {
    let uuid = uuid::Uuid::new_v4();

    let result = self
      .upload_tx
      .try_send(NewUpload {
        uuid,
        file,
        state,
        timestamp,
        session_id,
        feature_flags,
      })
      .inspect_err(|e| log::warn!("failed to enqueue artifact upload: {e:?}"));

    self.counter_stats.record(&result);
    result?;

    Ok(uuid)
  }
}

#[derive(thiserror::Error, Debug)]
enum Error {
  #[error("Task is shutting down")]
  Shutdown,
  #[error("Unhandled error: $1")]
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

  max_entries: IntWatch<bd_runtime::runtime::artifact_upload::MaxPendingEntries>,
  initial_backoff_interval: DurationWatch<bd_runtime::runtime::api::InitialBackoffInterval>,
  max_backoff_interval: DurationWatch<bd_runtime::runtime::api::MaxBackoffInterval>,

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
      max_entries: runtime.register_int_watch(),
      initial_backoff_interval: runtime.register_duration_watch(),
      max_backoff_interval: runtime.register_duration_watch(),
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
            next.time.to_offset_date_time(),
            bd_api::backoff_policy(
              &mut self.initial_backoff_interval,
              &mut self.max_backoff_interval,
            ),
          )));
          continue;
        }

        let file_path = REPORT_DIRECTORY.join(&next.name);
        let Ok(contents) = self.file_system.read_file(&file_path).await else {
          log::debug!(
            "failed to read file for artifact {}, deleting and removing from index",
            next.name
          );
          self.file_system.delete_file(&file_path).await?;
          self.index.pop_front();
          self.write_index().await;

          return Ok(());
        };

        let Ok(contents) = read_checksummed_data(&contents) else {
          log::debug!(
            "failed to validate CRC checksum for artifact {}, deleting and removing from index",
            next.name
          );

          self.file_system.delete_file(&file_path).await?;
          self.index.pop_front();
          self.write_index().await;

          return Ok(());
        };


        log::debug!("starting file upload for {:?}", next.name);
        self.upload_task_handle = Some(tokio::spawn(Self::upload_artifact(
          self.data_upload_tx.clone(),
          contents,
          next.name.clone(),
          next.time.to_offset_date_time(),
          next.session_id.clone(),
          bd_api::backoff_policy(
            &mut self.initial_backoff_interval,
            &mut self.max_backoff_interval,
          ),
          next.state_metadata.clone(),
          next.feature_flags.clone(),
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
            file,
            state,
            timestamp,
            session_id,
            feature_flags,
        }) = self.upload_queued_rx.recv() => {
          log::debug!("tracking artifact: {uuid} for upload");
          self.track_new_upload(uuid, file, state, session_id, timestamp, feature_flags).await;
        }
        intent_decision = maybe_await(&mut self.intent_task_handle) => {
            self.handle_intent_negotiation_decision(intent_decision??).await?;
        }
        result = maybe_await(&mut self.upload_task_handle) => {
            result??;

            #[allow(unused)]
            let name = self.handle_upload_complete().await?;

            #[cfg(test)]
            if let Some(hooks) = &self.test_hooks {
                hooks.upload_complete_tx.send(name).await.unwrap();
            }
        }

      }
    }
  }

  // Initialize the uploader from the index file on disk.
  async fn initialize(&mut self) {
    let path = REPORT_DIRECTORY.join(&*REPORT_INDEX_FILE);
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

        let _ignored = self.file_system.remove_dir(&REPORT_DIRECTORY).await;
        let _ignored = self.file_system.create_dir(&REPORT_DIRECTORY).await;
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
    for entry in self.index.drain(..) {
      let file_path = REPORT_DIRECTORY.join(&entry.name);
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
      filenames.insert(entry.name.clone());
      new_index.push_back(entry);
    }

    self.index = new_index;

    if modified {
      self.write_index().await;
    }

    // Remove any files left in the directory that isn't the index or a file referenced by the
    // index.
    let files = self
      .file_system
      .list_files(&REPORT_DIRECTORY)
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
        let entry = &self.index.pop_front().ok_or(InvariantError::Invariant)?;

        if let Err(e) = self
          .file_system
          .delete_file(&REPORT_DIRECTORY.join(&entry.name))
          .await
        {
          log::warn!("failed to delete artifact {:?}: {}", entry.name, e);
        }

        // Even if we failed to delete the artifact still try to clean up the index. There's a good
        // chance this will fail too but might make it less likely that we try to re-upload this
        // file.

        self.index.pop_front();
        self.write_index().await;
      },
      IntentDecision::UploadImmediately => {
        self.stats.accepted_intent.inc();
        let entry = self.index.front_mut().ok_or(InvariantError::Invariant)?;
        // Mark the file as being ready for uploads and persist this to the index.
        entry.pending_intent_negotiation = false;
        self.write_index().await;
      },
    }
    Ok(())
  }

  async fn handle_upload_complete(&mut self) -> Result<String> {
    self.stats.uploaded.inc();

    let entry = self.index.pop_front().ok_or(InvariantError::Invariant)?;
    let file_path = REPORT_DIRECTORY.join(&entry.name);

    if let Err(e) = self.file_system.delete_file(&file_path).await {
      log::warn!("failed to delete artifact {:?}: {}", entry.name, e);
    }

    self.write_index().await;

    Ok(entry.name)
  }

  fn stop_current_upload(&mut self) {
    if let Some(task) = self.upload_task_handle.take() {
      task.abort();
    }
    if let Some(task) = self.intent_task_handle.take() {
      task.abort();
    }
  }

  async fn track_new_upload(
    &mut self,
    uuid: Uuid,
    file: std::fs::File,
    state: LogFields,
    session_id: String,
    timestamp: Option<OffsetDateTime>,
    feature_flags: Vec<SnappedFeatureFlag>,
  ) {
    // If we've reached our limit of entries, stop the entry currently being uploaded (the oldest
    // one) to make space for the newer one.
    // TODO(snowp): Consider also having a bound on the size of the files persisted to disk.
    if self.index.len() == usize::try_from(*self.max_entries.read()).unwrap_or_default() {
      log::debug!("upload queue is full, dropping current upload");

      self.stats.dropped.inc();
      self.stop_current_upload();
      self.index.pop_front();
    }

    let uuid = uuid.to_string();

    let target_file = match self
      .file_system
      .create_file(&REPORT_DIRECTORY.join(&uuid))
      .await
    {
      Ok(file) => file,
      Err(e) => {
        log::warn!("failed to create file for artifact: {uuid} on disk: {e}");

        #[cfg(test)]
        if let Some(hooks) = &self.test_hooks {
          hooks.entry_received_tx.send(uuid.clone()).await.unwrap();
        }
        return;
      },
    };

    if let Err(e) = async_write_checksummed_data(tokio::fs::File::from_std(file), target_file).await
    {
      log::warn!("failed to write artifact to disk: {uuid} to disk: {e}");

      #[cfg(test)]
      if let Some(hooks) = &self.test_hooks {
        hooks.entry_received_tx.send(uuid.clone()).await.unwrap();
      }
      return;
    }

    // Only write the index after we've written the report file to disk to try to minimze the risk
    // of the file being written without a corresponding entry.
    self.index.push_back(Artifact {
      name: uuid.clone(),
      time: timestamp
        .unwrap_or_else(|| self.time_provider.now())
        .into_proto(),
      session_id,
      pending_intent_negotiation: true,
      state_metadata: state
        .into_iter()
        .map(|(key, value)| {
          (
            key.into(),
            Data {
              data_type: Some(match value {
                bd_log_primitives::StringOrBytes::String(s) => Data_type::StringData(s),
                bd_log_primitives::StringOrBytes::SharedString(s) => {
                  Data_type::StringData((*s).clone())
                },
                bd_log_primitives::StringOrBytes::Bytes(b) => Data_type::BinaryData(BinaryData {
                  payload: b,
                  ..Default::default()
                }),
              }),
              ..Default::default()
            },
          )
        })
        .collect(),
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
      ..Default::default()
    });

    self.write_index().await;


    #[cfg(test)]
    if let Some(hooks) = &self.test_hooks {
      hooks.entry_received_tx.send(uuid.clone()).await.unwrap();
    }
  }

  async fn write_index(&self) {
    log::debug!("writing index to disk");

    let index = ArtifactUploadIndex {
      artifact: self.index.iter().cloned().collect(),
      ..Default::default()
    };


    if let Err(e) = async {
      let compressed = write_compressed_protobuf(&index)?;
      self
        .file_system
        .as_ref()
        .write_file(&REPORT_DIRECTORY.join(&*REPORT_INDEX_FILE), &compressed)
        .await
    }
    .await
    {
      log::debug!("failed to write index: {e}");
    }
  }

  async fn upload_artifact(
    data_upload_tx: tokio::sync::mpsc::Sender<DataUpload>,
    contents: Vec<u8>,
    name: String,
    timestamp: OffsetDateTime,
    session_id: String,
    mut retry_policy: ExponentialBackoff,
    state_metadata: HashMap<String, Data>,
    feature_flags: Vec<FeatureFlag>,
  ) -> Result<()> {
    let path = REPORT_DIRECTORY.join(&name);
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
          type_id: "client_report".to_string(),
          contents: contents.clone(),
          artifact_id: name.clone(),
          time: timestamp.into_proto(),
          session_id: session_id.clone(),
          state_metadata: state_metadata.clone(),
          feature_flags: feature_flags.clone(),
          ..Default::default()
        },
      );

      data_upload_tx
        .send(DataUpload::ArtifactUpload(tracked))
        .await
        .map_err(|_| Error::Shutdown)?;

      if response.await.is_ok_and(|r| r.success) {
        log::debug!("upload of artifact: {name} succeeded");
        break;
      }

      let delay = retry_policy
        .next_backoff()
        .unwrap_or_else(|| 1.std_minutes());

      log::debug!("upload of artifact: {name} failed, retrying in {delay:?}");
      tokio::time::sleep(delay).await;
    }

    Ok(())
  }

  async fn perform_intent_negotiation(
    data_upload_tx: tokio::sync::mpsc::Sender<DataUpload>,
    id: String,
    timestamp: OffsetDateTime,
    mut retry_policy: ExponentialBackoff,
  ) -> Result<IntentDecision> {
    loop {
      let upload_uuid = TrackedArtifactIntent::upload_uuid();
      let (tracked, response) = TrackedArtifactIntent::new(
        upload_uuid.clone(),
        UploadArtifactIntentRequest {
          type_id: "client_report".to_string(),
          artifact_id: id.clone(),
          intent_uuid: upload_uuid.clone(),
          time: timestamp.into_proto(),
          // TODO(snowp): Figure out how to send relevant metadata about the artifact here.
          metadata: vec![],
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

      let delay = retry_policy
        .next_backoff()
        .unwrap_or_else(|| 1.std_minutes());
      log::debug!("intent negotiation for artifact: {id} failed, retrying in {delay:?}");
      tokio::time::sleep(delay).await;
    }
  }
}
