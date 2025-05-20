use super::UploadClient;
use crate::uploader::{Client, REPORT_INDEX_FILE};
use assert_matches::assert_matches;
use bd_api::DataUpload;
use bd_api::upload::{IntentResponse, UploadResponse};
use bd_client_common::file::read_compressed_protobuf;
use bd_client_common::file_system::{FileSystem, TestFileSystem};
use bd_proto::protos::client::artifact::ArtifactUploadIndex;
use bd_time::TestTimeProvider;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::task::JoinHandle;

pub struct TestHooks {
  pub upload_complete_tx: tokio::sync::mpsc::Sender<String>,
  pub entry_received_tx: tokio::sync::mpsc::Sender<String>,
}

struct Setup {
  upload_complete_rx: tokio::sync::mpsc::Receiver<String>,
  entry_received_rx: tokio::sync::mpsc::Receiver<String>,
  client: UploadClient,
  filesystem: Arc<TestFileSystem>,
  data_upload_rx: tokio::sync::mpsc::Receiver<DataUpload>,
  data_upload_tx: tokio::sync::mpsc::Sender<DataUpload>,
  shutdown: bd_shutdown::ComponentShutdownTrigger,
  task_handle: JoinHandle<()>,

  max_entries: usize,
}

impl Setup {
  async fn reinitialize(self) -> Self {
    self.shutdown.shutdown().await;
    self.task_handle.await.unwrap();

    let shutdown = bd_shutdown::ComponentShutdownTrigger::default();

    let (mut uploader, client) = super::Uploader::new(
      self.filesystem.clone(),
      self.data_upload_tx.clone(),
      Arc::new(TestTimeProvider::new(OffsetDateTime::now_utc())),
      self.max_entries,
      10,
      2 << 12,
      shutdown.make_shutdown(),
    );

    let (upload_complete_tx, upload_complete_rx) = tokio::sync::mpsc::channel(1);
    let (entry_received_tx, entry_received_rx) = tokio::sync::mpsc::channel(1);
    uploader.test_hooks = Some(TestHooks {
      upload_complete_tx,
      entry_received_tx,
    });

    let task_handle = tokio::spawn(uploader.run());

    Self {
      upload_complete_rx,
      entry_received_rx,
      client,
      filesystem: self.filesystem,
      data_upload_rx: self.data_upload_rx,
      data_upload_tx: self.data_upload_tx.clone(),
      shutdown,
      task_handle,
      max_entries: self.max_entries,
    }
  }
  fn new(max_entries: usize) -> Self {
    let (data_upload_tx, data_upload_rx) = tokio::sync::mpsc::channel(1);
    let shutdown = bd_shutdown::ComponentShutdownTrigger::default();

    let filesystem = Arc::new(TestFileSystem::new());

    let (mut uploader, client) = super::Uploader::new(
      filesystem.clone(),
      data_upload_tx.clone(),
      Arc::new(TestTimeProvider::new(OffsetDateTime::now_utc())),
      max_entries,
      10,
      2 << 12,
      shutdown.make_shutdown(),
    );

    let (upload_complete_tx, upload_complete_rx) = tokio::sync::mpsc::channel(1);
    let (entry_received_tx, entry_received_rx) = tokio::sync::mpsc::channel(1);

    uploader.test_hooks = Some(TestHooks {
      upload_complete_tx,
      entry_received_tx,
    });

    let task_handle = tokio::spawn(uploader.run());

    Self {
      client,
      data_upload_rx,
      data_upload_tx,
      filesystem,
      upload_complete_rx,
      entry_received_rx,
      shutdown,
      task_handle,
      max_entries,
    }
  }
}

#[tokio::test]
async fn basic_flow() {
  let mut setup = Setup::new(10);
  let id = setup.client.enqueue_upload(b"abc".to_vec()).unwrap();

  let upload = setup.data_upload_rx.recv().await.unwrap();
  assert_matches!(upload, DataUpload::ArtifactUploadIntent(intent) => {
      assert_eq!(intent.payload.artifact_id, id.to_string());
      assert_eq!(intent.payload.type_id, "client_report");

      intent.response_tx.send(IntentResponse {
          uuid: intent.uuid,
          decision: bd_api::upload::IntentDecision::UploadImmediately }).unwrap();
  });

  let upload = setup.data_upload_rx.recv().await.unwrap();
  assert_matches!(upload, DataUpload::ArtifactUpload(upload) => {
      assert_eq!(upload.payload.artifact_id, id.to_string());
      assert_eq!(upload.payload.contents, b"abc");
      assert_eq!(upload.payload.type_id, "client_report");

      upload.response_tx.send(UploadResponse { uuid: upload.uuid, success: true}).unwrap();
  });

  setup.upload_complete_rx.recv().await.unwrap();

  let files = setup.filesystem.files();
  let index_file = &files[&super::REPORT_DIRECTORY
    .join(&*super::REPORT_INDEX_FILE)
    .to_str()
    .unwrap()
    .to_string()];
  let index_file: ArtifactUploadIndex = read_compressed_protobuf(index_file).unwrap();
  assert_eq!(index_file, ArtifactUploadIndex::default());
}

#[tokio::test]
async fn pending_upload_limit() {
  let mut setup = Setup::new(2);

  let id1 = setup.client.enqueue_upload(b"1".to_vec()).unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id1.to_string()
  );

  let id2 = setup.client.enqueue_upload(b"2".to_vec()).unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id2.to_string()
  );
  let id3 = setup.client.enqueue_upload(b"3".to_vec()).unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id3.to_string()
  );

  // We still ended up sending one intent upload but since we gave up on the upload the response_tx
  // will have been closed.
  let upload = setup.data_upload_rx.recv().await.unwrap();
  assert_matches!(upload, DataUpload::ArtifactUploadIntent(intent) => {
      assert_eq!(intent.payload.artifact_id, id1.to_string());
      assert_eq!(intent.payload.type_id, "client_report");

      assert!(intent.response_tx.is_closed());
  });

  let upload = setup.data_upload_rx.recv().await.unwrap();
  assert_matches!(upload, DataUpload::ArtifactUploadIntent(intent) => {
      assert_eq!(intent.payload.artifact_id, id2.to_string());
      assert_eq!(intent.payload.type_id, "client_report");

      intent.response_tx.send(IntentResponse {
          uuid: intent.uuid,
          decision: bd_api::upload::IntentDecision::UploadImmediately }).unwrap();
  });

  let upload = setup.data_upload_rx.recv().await.unwrap();
  assert_matches!(upload, DataUpload::ArtifactUpload(upload) => {
      assert_eq!(upload.payload.artifact_id, id2.to_string());
      assert_eq!(upload.payload.contents, b"2");
      assert_eq!(upload.payload.type_id, "client_report");

      upload.response_tx.send(UploadResponse { uuid: upload.uuid, success: true}).unwrap();
  });

  let upload = setup.data_upload_rx.recv().await.unwrap();
  assert_matches!(upload, DataUpload::ArtifactUploadIntent(intent) => {
      assert_eq!(intent.payload.artifact_id, id3.to_string());
      assert_eq!(intent.payload.type_id, "client_report");

      intent.response_tx.send(IntentResponse {
          uuid: intent.uuid,
          decision: bd_api::upload::IntentDecision::UploadImmediately }).unwrap();
  });

  let upload = setup.data_upload_rx.recv().await.unwrap();
  assert_matches!(upload, DataUpload::ArtifactUpload(upload) => {
      assert_eq!(upload.payload.artifact_id, id3.to_string());
      assert_eq!(upload.payload.contents, b"3");
      assert_eq!(upload.payload.type_id, "client_report");

      upload.response_tx.send(UploadResponse { uuid: upload.uuid, success: true}).unwrap();
  });
}

#[tokio::test]
async fn inconsistent_state_missing_file() {
  let mut setup = Setup::new(2);
  let id1 = setup.client.enqueue_upload(b"1".to_vec()).unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id1.to_string()
  );
  let id2 = setup.client.enqueue_upload(b"2".to_vec()).unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id2.to_string()
  );

  setup
    .filesystem
    .delete_file(&super::REPORT_DIRECTORY.join(id1.to_string()))
    .await
    .unwrap();

  let mut setup = setup.reinitialize().await;
  let upload = setup.data_upload_rx.recv().await.unwrap();

  // First we'll see the intent negotiation from the first instance. Since we terminated the task
  // we expect to see the response channel already closed.
  assert_matches!(upload, DataUpload::ArtifactUploadIntent(intent) => {
      assert_eq!(intent.payload.artifact_id, id1.to_string());
      assert_eq!(intent.payload.type_id, "client_report");
      assert!(intent.response_tx.is_closed());
  });
  let upload = setup.data_upload_rx.recv().await.unwrap();
  assert_matches!(upload, DataUpload::ArtifactUploadIntent(intent) => {
        assert_eq!(intent.payload.artifact_id, id2.to_string());
      assert_eq!(intent.payload.type_id, "client_report");

  });
}

#[tokio::test]
async fn inconsistent_state_missing_index() {
  let mut setup = Setup::new(2);
  let id1 = setup.client.enqueue_upload(b"1".to_vec()).unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id1.to_string()
  );

  setup
    .filesystem
    .delete_file(&super::REPORT_DIRECTORY.join(&*REPORT_INDEX_FILE))
    .await
    .unwrap();

  let mut setup = setup.reinitialize().await;

  let id2 = setup.client.enqueue_upload(b"2".to_vec()).unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id2.to_string()
  );

  let upload = setup.data_upload_rx.recv().await.unwrap();

  // First we'll see the intent negotiation from the first instance. Since we terminated the task
  // we expect to see the response channel already closed.
  assert_matches!(upload, DataUpload::ArtifactUploadIntent(intent) => {
      assert_eq!(intent.payload.artifact_id, id1.to_string());
      assert_eq!(intent.payload.type_id, "client_report");
      assert!(intent.response_tx.is_closed());
  });
  let upload = setup.data_upload_rx.recv().await.unwrap();
  assert_matches!(upload, DataUpload::ArtifactUploadIntent(intent) => {
        assert_eq!(intent.payload.artifact_id, id2.to_string());
      assert_eq!(intent.payload.type_id, "client_report");

  });
}
