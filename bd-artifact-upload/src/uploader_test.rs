use super::UploadClient;
use crate::uploader::Client;
use assert_matches::assert_matches;
use bd_api::upload::{IntentResponse, UploadResponse};
use bd_api::DataUpload;
use bd_client_common::file::read_compressed_protobuf;
use bd_client_common::file_system::TestFileSystem;
use bd_proto::protos::client::artifact::ArtifactUploadIndex;
use bd_time::TestTimeProvider;
use std::sync::Arc;
use time::OffsetDateTime;

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
  _shutdown: bd_shutdown::ComponentShutdownTrigger,
}

impl Setup {
  async fn new(max_entries: usize) -> Self {
    let (data_upload_tx, data_upload_rx) = tokio::sync::mpsc::channel(1);
    let shutdown = bd_shutdown::ComponentShutdownTrigger::default();

    let filesystem = Arc::new(TestFileSystem::new());

    let (mut uploader, client) = super::Uploader::new(
      filesystem.clone(),
      data_upload_tx,
      Arc::new(TestTimeProvider::new(OffsetDateTime::now_utc())),
      max_entries,
      shutdown.make_shutdown(),
    );

    let (upload_complete_tx, upload_complete_rx) = tokio::sync::mpsc::channel(1);
    let (entry_received_tx, entry_received_rx) = tokio::sync::mpsc::channel(1);

    uploader.test_hooks = Some(TestHooks {
      upload_complete_tx,
      entry_received_tx,
    });

    tokio::spawn(uploader.run());

    Self {
      client,
      data_upload_rx,
      filesystem,
      upload_complete_rx,
      entry_received_rx,
      _shutdown: shutdown,
    }
  }
}

#[tokio::test]
async fn basic_flow() {
  let mut setup = Setup::new(10).await;
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
  let mut setup = Setup::new(2).await;

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
