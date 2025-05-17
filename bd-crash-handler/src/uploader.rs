#[cfg(test)]
#[path = "./uploader_test.rs"]
mod tests;

use async_trait::async_trait;
use bd_api::upload::{IntentDecision, TrackedArtifactIntent, TrackedArtifactUpload};
use bd_api::DataUpload;
use bd_client_common::file::{read_compressed_protobuf, write_compressed_protobuf};
use bd_client_common::filesystem::FileSystem;
use bd_proto::protos::client::api::{UploadArtifactIntentRequest, UploadArtifactRequest};
use bd_proto::protos::client::artifact::artifact_upload_index::Artifact;
use bd_proto::protos::client::artifact::ArtifactUploadIndex;
use bd_shutdown::ComponentShutdown;
use bd_time::{OffsetDateTimeExt, TimeProvider};
use mockall::automock;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};
use uuid::Uuid;

/// Root directory for all files used for storage and uploading.
pub static REPORT_DIRECTORY: LazyLock<PathBuf> = LazyLock::new(|| "report_uploads".into());

/// The index file used for tracking all of the individual files.
pub static REPORT_INDEX_FILE: LazyLock<PathBuf> = LazyLock::new(|| "report_index.pb".into());

type NewUpload = (Uuid, Vec<u8>);

#[automock]
#[async_trait]
pub trait Client: Send + Sync {
  async fn enqueue_upload(&self, contents: Vec<u8>) -> anyhow::Result<Uuid>;
}

pub struct UploadClient {
  upload_tx: tokio::sync::mpsc::Sender<NewUpload>,
}

#[async_trait]
impl Client for UploadClient {
  /// Dispatches a payload to be uploaded, returning the associated artifact UUID.
  async fn enqueue_upload(&self, contents: Vec<u8>) -> anyhow::Result<Uuid> {
    let report_uuid = uuid::Uuid::new_v4();

    self
      .upload_tx
      .try_send((report_uuid, contents))
      .inspect_err(|e| {
        log::warn!("failed to enqueue artifact upload: {e}");
      })?;

    Ok(report_uuid)
  }
}

pub struct Uploader {
  data_upload_tx: tokio::sync::mpsc::Sender<DataUpload>,
  upload_queued_rx: tokio::sync::mpsc::Receiver<NewUpload>,
  shutdown: ComponentShutdown,
  time_provider: Arc<dyn TimeProvider>,
  filesystem: Arc<dyn FileSystem>,

  index: VecDeque<Artifact>,
}

impl Uploader {
  pub fn new(
    filesystem: Arc<dyn FileSystem>,
    data_upload_tx: tokio::sync::mpsc::Sender<DataUpload>,
    time_provider: Arc<dyn TimeProvider>,
    shutdown: ComponentShutdown,
  ) -> (Self, UploadClient) {
    let (upload_tx, upload_rx) = tokio::sync::mpsc::channel(2);

    let uploader = Self {
      data_upload_tx,
      upload_queued_rx: upload_rx,
      shutdown,
      time_provider,
      filesystem,
      index: VecDeque::default(),
    };

    let client = UploadClient { upload_tx };

    (uploader, client)
  }

  pub async fn run(mut self) -> anyhow::Result<()> {
    self.initialize().await?;

    // TODO(snowp): Due to the way this is structured we only pop values off the queue of inbound
    // requests while we have no pending uploads.
    // TODO(snowp): Add intent retry policy.
    // TODO(snowp): Add upload retry policy.
    // TODO(snowp): Add bound to number of pending uploads.
    // TODO(snowp): Add safety mechanism to clean up files that are not referenced by index.
    // TODO(snowp): Consider upload/intent parallelism.
    loop {
      // Grab the next artifact for upload, removing it from the in-memory index. We'll persist
      // the change after either we remove the entry or modify the state of the entry.
      let Some(mut next) = self.index.pop_front() else {
        tokio::select! {
          () = self.shutdown.cancelled() => {
            log::debug!("shutting down uploader");
            return Ok(());
          }
          Some((uuid, contents)) = self.upload_queued_rx.recv() => {
            log::debug!("tracking artifact: {uuid} for upload");
            self.track_new_upload(uuid, contents).await?;          }

        }
        continue;
      };

      if next.pending_intent_negotation {
        let Ok(decision) = self.perform_intent_negotiation(&next.name).await else {
          log::debug!("intent negotiation failed due to data channel closing, exiting task");
          return Ok(());
        };

        match decision {
          IntentDecision::Drop => {
            log::debug!("intent negotiated completed, dropping artifact");

            self.filesystem.delete_file(&file_path(&next.name)).await?;
            self.write_index().await?;
          },
          IntentDecision::UploadImmediately => {
            log::debug!("intent negotiated completed, proceeding to upload artifact");

            next.pending_intent_negotation = false;
            self.index.push_front(next);
            self.write_index().await?;
          },
        }

        continue;
      }

      let Ok(contents) = self.filesystem.read_file(&file_path(&next.name)).await else {
        log::debug!(
          "failed to read file for artifact {}, deleting and removing from index",
          next.name
        );
        self.write_index().await?;

        return Ok(());
      };


      if let Err(_) = self.upload_artifact(contents, &next.name).await {
        log::debug!("upload failed due to data channel closing, exiting task");
        return Ok(());
      };

      // At this point the payload has been successfully updated, so remove the file from disk and
      // persist the index.

      self.write_index().await?;
      self.filesystem.delete_file(&file_path(&next.name)).await?;
    }
  }

  // Initialize the uploader from the index file on disk.
  async fn initialize(&mut self) -> anyhow::Result<()> {
    let path = REPORT_DIRECTORY.join(&*REPORT_INDEX_FILE);
    log::debug!("initializing pending aggregation index: {}", path.display());
    self.index = match self
      .filesystem
      .read_file(&path)
      .await
      .and_then(|contents| read_compressed_protobuf::<ArtifactUploadIndex>(&contents))
    {
      Ok(index) => index,
      Err(e) => {
        log::debug!("unable to open pending aggregation index: {e}");
        log::debug!("creating new aggregation index");

        self.filesystem.remove_dir(&REPORT_DIRECTORY).await?;
        self.filesystem.create_dir(&REPORT_DIRECTORY).await?;
        ArtifactUploadIndex::default()
      },
    }
    .artifact
    .into_iter()
    .collect();

    Ok(())
  }

  async fn track_new_upload(&mut self, uuid: Uuid, contents: Vec<u8>) -> anyhow::Result<()> {
    let uuid = uuid.to_string();
    self.index.push_back(Artifact {
      name: uuid.clone(),
      time: self.time_provider.now().into_proto(),
      pending_intent_negotation: true,
      ..Default::default()
    });

    self
      .filesystem
      .write_file(&file_path(&uuid), &contents)
      .await?;

    Ok(())
  }

  async fn write_index(&self) -> anyhow::Result<()> {
    let index = ArtifactUploadIndex {
      artifact: self.index.iter().cloned().collect(),
      ..Default::default()
    };

    let compressed = write_compressed_protobuf(&index);
    self
      .filesystem
      .as_ref()
      .write_file(&REPORT_DIRECTORY.join(&*REPORT_INDEX_FILE), &compressed)
      .await?;

    Ok(())
  }

  async fn upload_artifact(&self, contents: Vec<u8>, name: &str) -> anyhow::Result<()> {
    let path = REPORT_DIRECTORY.join(name);
    log::debug!("uploading artifact: {}", path.display());

    loop {
      let upload_uuid = TrackedArtifactUpload::upload_uuid();
      let (tracked, response) = TrackedArtifactUpload::new(
        upload_uuid.clone(),
        UploadArtifactRequest {
          upload_uuid,
          type_id: "client_report".to_string(),
          contents: contents.clone(),
          artifact_id: name.to_string(),
          ..Default::default()
        },
      );

      self
        .data_upload_tx
        .send(DataUpload::ArtifactUpload(tracked))
        .await?;

      match response.await {
        Ok(response) => {
          if response.success {
            log::debug!("upload of artifact: {name} succeeded");
            return Ok(());
          }
          log::debug!("upload of artifact: {name} failed, retrying");
        },
        Err(_) => log::debug!("upload of artifact: {name} failed, retrying"),
      }
    }
  }

  async fn perform_intent_negotiation(&self, uuid: &str) -> anyhow::Result<IntentDecision> {
    loop {
      let upload_uuid = TrackedArtifactIntent::upload_uuid();
      let (tracked, response) = TrackedArtifactIntent::new(
        upload_uuid.clone(),
        UploadArtifactIntentRequest {
          type_id: "client_report".to_string(),
          artifact_id: uuid.to_string(),
          intent_uuid: upload_uuid.to_string(),
          // TODO(snowp): Figure out how to send relevant metadata about the artifact here.
          metadata: vec![],
          ..Default::default()
        },
      );

      self
        .data_upload_tx
        .send(DataUpload::ArtifactUploadIntent(tracked))
        .await?;

      match response.await {
        Ok(response) => break Ok(response.decision),
        Err(_) => log::debug!("intent negotiation for artifact: {uuid} failed, retrying"),
      }
    }
  }
}

pub fn file_path(name: &str) -> PathBuf {
  REPORT_DIRECTORY.join(name)
}
