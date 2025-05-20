#[cfg(test)]
#[path = "./uploader_test.rs"]
mod tests;

use bd_api::DataUpload;
use bd_api::upload::{IntentDecision, TrackedArtifactIntent, TrackedArtifactUpload};
use bd_bounded_buffer::MemorySized;
use bd_client_common::error;
use bd_client_common::file::{read_compressed_protobuf, write_compressed_protobuf};
use bd_client_common::file_system::FileSystem;
use bd_proto::protos::client::api::{UploadArtifactIntentRequest, UploadArtifactRequest};
use bd_proto::protos::client::artifact::ArtifactUploadIndex;
use bd_proto::protos::client::artifact::artifact_upload_index::Artifact;
use bd_shutdown::ComponentShutdown;
use bd_time::{OffsetDateTimeExt, TimeProvider};
use mockall::automock;
use std::collections::{HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};
#[cfg(test)]
use tests::TestHooks;
use uuid::Uuid;

/// Root directory for all files used for storage and uploading.
pub static REPORT_DIRECTORY: LazyLock<PathBuf> = LazyLock::new(|| "report_uploads".into());

/// The index file used for tracking all of the individual files.
pub static REPORT_INDEX_FILE: LazyLock<PathBuf> = LazyLock::new(|| "report_index.pb".into());

//
// NewUpload
//

// TODO(snowp): Consider allowing passing an open file handle instead of having to hold the data in
// memory while entry is pending within the channel.
#[derive(Debug)]
struct NewUpload {
  uuid: Uuid,
  contents: Vec<u8>,
}

// Used for bounded_buffer logs
impl std::fmt::Display for NewUpload {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "NewUpload {{ uuid: {}, contents: {} }}",
      self.uuid,
      self.contents.len()
    )
  }
}

impl MemorySized for NewUpload {
  fn size(&self) -> usize {
    size_of::<Uuid>() + size_of::<Vec<u8>>() + self.contents.capacity() * size_of::<u8>()
  }
}

#[automock]
pub trait Client: Send + Sync {
  fn enqueue_upload(&self, contents: Vec<u8>) -> anyhow::Result<Uuid>;
}

pub struct UploadClient {
  upload_tx: bd_bounded_buffer::Sender<NewUpload>,
}

impl Client for UploadClient {
  /// Dispatches a payload to be uploaded, returning the associated artifact UUID.
  fn enqueue_upload(&self, contents: Vec<u8>) -> anyhow::Result<Uuid> {
    let uuid = uuid::Uuid::new_v4();

    self
      .upload_tx
      .try_send(NewUpload { uuid, contents })
      .inspect_err(|e| {
        log::warn!("failed to enqueue artifact upload: {e:?}");
      })?;

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

type Result<T> = std::result::Result<T, Error>;

pub struct Uploader {
  data_upload_tx: tokio::sync::mpsc::Sender<DataUpload>,
  upload_queued_rx: bd_bounded_buffer::Receiver<NewUpload>,
  shutdown: ComponentShutdown,
  time_provider: Arc<dyn TimeProvider>,
  file_system: Arc<dyn FileSystem>,

  index: VecDeque<Artifact>,

  max_entries: usize,

  intent_task_handle: Option<tokio::task::JoinHandle<Result<IntentDecision>>>,
  upload_task_handle: Option<tokio::task::JoinHandle<Result<()>>>,

  #[cfg(test)]
  test_hooks: Option<TestHooks>,
}

impl Uploader {
  pub fn new(
    file_system: Arc<dyn FileSystem>,
    data_upload_tx: tokio::sync::mpsc::Sender<DataUpload>,
    time_provider: Arc<dyn TimeProvider>,
    max_entries: usize,
    buffer_capacity: usize,
    buffer_memory_capacity: usize,
    shutdown: ComponentShutdown,
  ) -> (Self, UploadClient) {
    let (upload_tx, upload_rx) =
      bd_bounded_buffer::channel(buffer_capacity, buffer_memory_capacity);

    let uploader = Self {
      data_upload_tx,
      upload_queued_rx: upload_rx,
      shutdown,
      time_provider,
      file_system,
      index: VecDeque::default(),
      max_entries,
      upload_task_handle: None,
      intent_task_handle: None,
      #[cfg(test)]
      test_hooks: None,
    };

    let client = UploadClient { upload_tx };

    (uploader, client)
  }

  pub async fn run(self) {
    if let Err(Error::Unhandled(e)) = self.run_inner().await {
      error::handle_unexpected(Err::<(), _>(e), "artifact uploader");
    }
  }

  async fn run_inner(mut self) -> Result<()> {
    self.initialize().await;

    // TODO(snowp): Add intent retry policy.
    // TODO(snowp): Add upload retry policy.
    // TODO(snowp): Add safety mechanism to clean up files that are not referenced by index.

    // The state machinery below relies on careful handling of the contents of the index list, as
    // we want to make sure that we don't lose entries due to process shutdown. The pending upload
    // remains at the head of the list during intent negotiation/uploads and is only removed after
    // the upload completes or we decide to not upload the file.
    loop {
      // If we're not currently processing an entry and there are pending work to do, check the
      // next entry in the list and perform the next step.
      if self.intent_task_handle.is_none()
        && self.upload_task_handle.is_none()
        && !self.index.is_empty()
      {
        let next = self.index.front().unwrap().clone();
        if next.pending_intent_negotation {
          log::debug!("starting intent negotiation for {:?}", next.name);
          self.intent_task_handle = Some(tokio::spawn(Self::perform_intent_negotiation(
            self.data_upload_tx.clone(),
            next.name.clone(),
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


        log::debug!("starting file upload for {:?}", next.name);
        self.upload_task_handle = Some(tokio::spawn(Self::upload_artifact(
          self.data_upload_tx.clone(),
          contents,
          next.name.clone(),
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
        Some(NewUpload {uuid, contents}) = self.upload_queued_rx.recv() => {
          log::debug!("tracking artifact: {uuid} for upload");
          self.track_new_upload(uuid, contents).await;
        }
        intent_decision = async {
          self.intent_task_handle.as_mut().unwrap().await?
        }, if self.intent_task_handle.is_some() => {
            self.handle_intent_negotiation_decision(intent_decision?).await;
            self.intent_task_handle = None;
        }
        result = async {
          self.upload_task_handle.as_mut().unwrap().await?
        }, if self.upload_task_handle.is_some() => {
            result?;

            #[allow(unused)]
            let name = self.handle_upload_complete().await;
            self.upload_task_handle = None;

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
    // TODO(snowp): Should we check for crc integrity at this point?

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
      if file == REPORT_INDEX_FILE.to_string_lossy() {
        continue;
      }

      if !filenames.contains(&file) {
        log::debug!("removing artifact {} from disk, not in index", file);
        if let Err(e) = self
          .file_system
          .delete_file(&REPORT_DIRECTORY.join(&file))
          .await
        {
          log::warn!("failed to delete artifact {:?}: {}", file, e);
        }
      }
    }
  }

  async fn handle_intent_negotiation_decision(&mut self, decision: IntentDecision) {
    match decision {
      IntentDecision::Drop => {
        let entry = &self.index.pop_front().unwrap();

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
        let entry = self.index.front_mut().unwrap();
        // Mark the file as being ready for uploads and persist this to the index.
        entry.pending_intent_negotation = false;
        self.write_index().await;
      },
    }
  }

  async fn handle_upload_complete(&mut self) -> String {
    let entry = self.index.pop_front().unwrap();
    let file_path = REPORT_DIRECTORY.join(&entry.name);

    if let Err(e) = self.file_system.delete_file(&file_path).await {
      log::warn!("failed to delete artifact {:?}: {}", entry.name, e);
    }

    self.write_index().await;

    entry.name
  }

  fn stop_current_upload(&mut self) {
    if let Some(task) = self.upload_task_handle.take() {
      task.abort();
    }
    if let Some(task) = self.intent_task_handle.take() {
      task.abort();
    }
  }

  async fn track_new_upload(&mut self, uuid: Uuid, contents: Vec<u8>) {
    // If we've reached our limit of entries, stop the entry currently being uploaded (the oldest
    // one) to make space for the newer one.
    if self.index.len() == self.max_entries {
      log::debug!("upload queue is full, dropping current upload");
      self.stop_current_upload();
      self.index.pop_front();
    }

    let uuid = uuid.to_string();
    if let Err(e) = self
      .file_system
      .write_file(&REPORT_DIRECTORY.join(&uuid), &contents)
      .await
    {
      log::warn!("failed to write artifact to disk: {uuid} to disk: {e}");
      return;
    }

    // Only write the index after we've written the report file to disk to try to minimze the risk
    // of the file being written without a corresponding entry.
    self.index.push_back(Artifact {
      name: uuid.clone(),
      time: self.time_provider.now().into_proto(),
      pending_intent_negotation: true,
      ..Default::default()
    });

    self.write_index().await;


    #[cfg(test)]
    if let Some(hooks) = &self.test_hooks {
      hooks
        .entry_received_tx
        .send(uuid.to_string())
        .await
        .unwrap();
    }
  }

  async fn write_index(&self) {
    let index = ArtifactUploadIndex {
      artifact: self.index.iter().cloned().collect(),
      ..Default::default()
    };

    let compressed = write_compressed_protobuf(&index);
    if let Err(e) = self
      .file_system
      .as_ref()
      .write_file(&REPORT_DIRECTORY.join(&*REPORT_INDEX_FILE), &compressed)
      .await
    {
      log::debug!("failed to write index: {e}");
    }
  }

  async fn upload_artifact(
    data_upload_tx: tokio::sync::mpsc::Sender<DataUpload>,
    contents: Vec<u8>,
    name: String,
  ) -> Result<()> {
    let path = REPORT_DIRECTORY.join(&name);
    log::debug!("uploading artifact: {}", path.display());

    loop {
      let upload_uuid = TrackedArtifactUpload::upload_uuid();
      let (tracked, response) = TrackedArtifactUpload::new(
        upload_uuid.clone(),
        UploadArtifactRequest {
          upload_uuid,
          type_id: "client_report".to_string(),
          contents: contents.clone(),
          artifact_id: name.clone(),
          ..Default::default()
        },
      );

      data_upload_tx
        .send(DataUpload::ArtifactUpload(tracked))
        .await
        .map_err(|_| Error::Shutdown)?;

      match response.await {
        Ok(response) => {
          if response.success {
            log::debug!("upload of artifact: {name} succeeded");
            break;
          }
          log::debug!("upload of artifact: {name} failed, retrying");
        },
        Err(_) => log::debug!("upload of artifact: {name} failed, retrying"),
      }
    }

    Ok(())
  }

  async fn perform_intent_negotiation(
    data_upload_tx: tokio::sync::mpsc::Sender<DataUpload>,
    id: String,
  ) -> Result<IntentDecision> {
    loop {
      let upload_uuid = TrackedArtifactIntent::upload_uuid();
      let (tracked, response) = TrackedArtifactIntent::new(
        upload_uuid.clone(),
        UploadArtifactIntentRequest {
          type_id: "client_report".to_string(),
          artifact_id: id.to_string(),
          intent_uuid: upload_uuid.to_string(),
          // TODO(snowp): Figure out how to send relevant metadata about the artifact here.
          metadata: vec![],
          ..Default::default()
        },
      );

      data_upload_tx
        .send(DataUpload::ArtifactUploadIntent(tracked))
        .await
        .map_err(|_| Error::Shutdown)?;

      match response.await {
        Ok(response) => break Ok(response.decision),
        Err(_) => log::debug!("intent negotiation for artifact: {id} failed, retrying"),
      }
    }
  }
}
