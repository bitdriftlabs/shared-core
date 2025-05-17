use crate::uploader::Client;
use assert_matches::assert_matches;
use bd_api::upload::{IntentResponse, UploadResponse};
use bd_api::DataUpload;
use bd_client_common::file::read_compressed_protobuf;
use bd_client_common::filesystem::TestFileSystem;
use bd_proto::protos::client::artifact::ArtifactUploadIndex;
use bd_time::TestTimeProvider;
use std::sync::Arc;
use time::ext::{NumericalDuration, NumericalStdDuration};
use time::OffsetDateTime;

#[macro_export]
macro_rules! wait_for {
  ($condition:expr) => {
    let start = std::time::Instant::now();
    while !$condition {
      tokio::task::yield_now().await;
      if start.elapsed() > 5.seconds() {
        panic!("Timeout waiting for condition");
      }
      std::thread::sleep(10.std_milliseconds());
    }
  };
}

#[tokio::test]
async fn basic_flow() {
  let (data_upload_tx, mut data_upload_rx) = tokio::sync::mpsc::channel(1);
  let shutdown = bd_shutdown::ComponentShutdownTrigger::default();

  let filesystem = Arc::new(TestFileSystem::new());

  let (uploader, client) = super::Uploader::new(
    filesystem.clone(),
    data_upload_tx,
    Arc::new(TestTimeProvider::new(OffsetDateTime::now_utc())),
    shutdown.make_shutdown(),
  );

  tokio::spawn(async {
    assert!(uploader.run().await.is_ok());
  });

  let id = client.enqueue_upload(b"abc".to_vec()).await.unwrap();

  let upload = data_upload_rx.recv().await.unwrap();
  assert_matches!(upload, DataUpload::ArtifactUploadIntent(intent) => {
      assert_eq!(intent.payload.artifact_id, id.to_string());
      assert_eq!(intent.payload.type_id, "client_report");

      intent.response_tx.send(IntentResponse {
          uuid: intent.uuid,
          decision: bd_api::upload::IntentDecision::UploadImmediately }).unwrap();
  });

  let upload = data_upload_rx.recv().await.unwrap();
  assert_matches!(upload, DataUpload::ArtifactUpload(upload) => {
      assert_eq!(upload.payload.artifact_id, id.to_string());
      assert_eq!(upload.payload.contents, b"abc");
      assert_eq!(upload.payload.type_id, "client_report");

      upload.response_tx.send(UploadResponse { uuid: upload.uuid, success: true}).unwrap();
  });

  wait_for!(filesystem.files().len() == 1);

  let files = filesystem.files();
  let index_file = &files[&super::REPORT_DIRECTORY
    .join(&*super::REPORT_INDEX_FILE)
    .to_str()
    .unwrap()
    .to_string()];
  let index_file: ArtifactUploadIndex = read_compressed_protobuf(index_file).unwrap();
  assert_eq!(index_file, ArtifactUploadIndex::default());
}
