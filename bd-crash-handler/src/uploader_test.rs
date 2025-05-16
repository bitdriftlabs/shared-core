use crate::uploader::Client;
use assert_matches::assert_matches;
use bd_api::upload::{IntentResponse, UploadResponse};
use bd_api::DataUpload;
use bd_client_common::filesystem::TestFileSystem;
use bd_time::TestTimeProvider;
use std::sync::Arc;
use time::OffsetDateTime;

#[tokio::test]
async fn basic_flow() {
  let (data_upload_tx, mut data_upload_rx) = tokio::sync::mpsc::channel(1);
  let shutdown = bd_shutdown::ComponentShutdownTrigger::default();

  let (uploader, client) = super::Uploader::new(
    Arc::new(TestFileSystem::new()),
    data_upload_tx,
    Arc::new(TestTimeProvider::new(OffsetDateTime::now_utc())),
    shutdown.make_shutdown(),
  );

  tokio::spawn(uploader.run());

  let id = client.enqueue_upload(b"abc".to_vec()).await.unwrap();

  let upload = data_upload_rx.recv().await.unwrap();
  assert_matches!(upload, DataUpload::ArtifactUploadIntent(intent) => {
      assert_eq!(intent.payload.artifact_id, id.to_string());
      assert_eq!(intent.payload.type_id, "client_report");

      intent.response_tx.send(IntentResponse { uuid: intent.uuid, decision: bd_api::upload::IntentDecision::UploadImmediately }).unwrap();
  });

  let upload = data_upload_rx.recv().await.unwrap();
  assert_matches!(upload, DataUpload::ArtifactUpload(intent) => {
      assert_eq!(intent.payload.artifact_id, id.to_string());
      assert_eq!(intent.payload.contents, b"abc");
      assert_eq!(intent.payload.type_id, "client_report");

      intent.response_tx.send(UploadResponse { uuid: intent.uuid, success: true}).unwrap();
  });
}
