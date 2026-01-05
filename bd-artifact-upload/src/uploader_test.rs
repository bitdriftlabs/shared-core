// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::UploadClient;
use crate::uploader::{Client, REPORT_DIRECTORY, REPORT_INDEX_FILE, SnappedFeatureFlag};
use assert_matches::assert_matches;
use bd_api::DataUpload;
use bd_api::upload::{IntentResponse, UploadResponse};
use bd_client_common::file::read_compressed_protobuf;
use bd_client_common::file_system::FileSystem;
use bd_client_common::test::TestFileSystem;
use bd_client_stats_store::Collector;
use bd_proto::protos::client::artifact::ArtifactUploadIndex;
use bd_proto::protos::client::feature_flag::FeatureFlag;
use bd_proto::protos::logging::payload::Data;
use bd_proto::protos::logging::payload::data::Data_type;
use bd_runtime::runtime::{FeatureFlag as _, artifact_upload};
use bd_runtime::test::TestConfigLoader;
use bd_test_helpers::runtime::ValueKind;
use bd_time::{OffsetDateTimeExt as _, TestTimeProvider};
use std::io::{Seek, Write};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use time::OffsetDateTime;
use time::ext::NumericalStdDuration;
use time::macros::datetime;
use tokio::task::JoinHandle;
use tokio::time::timeout;

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
  runtime: TestConfigLoader,

  tmpdir: tempfile::TempDir,
}

impl Setup {
  async fn reinitialize(self) -> Self {
    self.shutdown.shutdown().await;
    self.task_handle.await.unwrap();

    let shutdown = bd_shutdown::ComponentShutdownTrigger::default();

    let runtime = self.runtime;

    let (mut uploader, client) = super::Uploader::new(
      self.filesystem.clone(),
      self.data_upload_tx.clone(),
      Arc::new(TestTimeProvider::new(OffsetDateTime::now_utc())),
      &runtime,
      &Collector::default(),
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
      runtime,
      tmpdir: self.tmpdir,
    }
  }

  async fn new(max_entries: u32) -> Self {
    let (data_upload_tx, data_upload_rx) = tokio::sync::mpsc::channel(1);
    let shutdown = bd_shutdown::ComponentShutdownTrigger::default();

    let filesystem = Arc::new(TestFileSystem::new());
    let config_loader = TestConfigLoader::new().await;

    config_loader
      .update_snapshot(bd_test_helpers::runtime::make_update(
        vec![(
          artifact_upload::MaxPendingEntries::path(),
          ValueKind::Int(max_entries),
        )],
        "1".to_string(),
      ))
      .await
      .unwrap();

    let (mut uploader, client) = super::Uploader::new(
      filesystem.clone(),
      data_upload_tx.clone(),
      Arc::new(TestTimeProvider::new(OffsetDateTime::now_utc())),
      &config_loader,
      &Collector::default(),
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
      filesystem,
      data_upload_rx,
      data_upload_tx,
      shutdown,
      task_handle,
      runtime: config_loader,
      tmpdir: tempfile::tempdir().unwrap(),
    }
  }

  fn make_file(&self, contents: &[u8]) -> std::fs::File {
    let mut file = tempfile::tempfile_in(self.tmpdir.path()).unwrap();
    file.write_all(contents).unwrap();

    file.seek(std::io::SeekFrom::Start(0)).unwrap();

    file
  }
}

#[tokio::test]
async fn basic_flow() {
  let mut setup = Setup::new(10).await;

  let timestamp = datetime!(2023-10-01 12:00:00 UTC);
  let id = setup
    .client
    .enqueue_upload(
      setup.make_file(b"abc"),
      [("foo".into(), "bar".into())].into(),
      Some(timestamp),
      "session_id".to_string(),
      vec![],
    )
    .unwrap();

  let upload = setup.data_upload_rx.recv().await.unwrap();
  assert_matches!(upload, DataUpload::ArtifactUploadIntent(intent) => {
      assert_eq!(intent.payload.artifact_id, id.to_string());
      assert_eq!(intent.payload.type_id, "client_report");
      assert_eq!(intent.payload.time, timestamp.into_proto());

      intent.response_tx.send(IntentResponse {
          uuid: intent.uuid,
          decision: bd_api::upload::IntentDecision::UploadImmediately }).unwrap();
  });

  let upload = setup.data_upload_rx.recv().await.unwrap();
  assert_matches!(upload, DataUpload::ArtifactUpload(upload) => {
      assert_eq!(upload.payload.artifact_id, id.to_string());
      assert_eq!(upload.payload.contents, b"abc");
      assert_eq!(upload.payload.type_id, "client_report");
      assert_eq!(upload.payload.time, timestamp.into_proto());
      assert_eq!(upload.payload.session_id, "session_id");
      assert_eq!(upload.payload.state_metadata, [("foo".into(), Data {
          data_type: Some(Data_type::StringData("bar".to_string())),
          ..Default::default()
      })].into());

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
async fn feature_flags() {
  let mut setup = Setup::new(10).await;

  let timestamp = datetime!(2023-10-01 12:00:00 UTC);
  let id = setup
    .client
    .enqueue_upload(
      setup.make_file(b"abc"),
      [("foo".into(), "bar".into())].into(),
      Some(timestamp),
      "session_id".to_string(),
      vec![
        SnappedFeatureFlag::new(
          "key".to_string(),
          Some("value".to_string()),
          timestamp - 1.std_seconds(),
        ),
        SnappedFeatureFlag::new("key2".to_string(), None, timestamp - 2.std_seconds()),
      ],
    )
    .unwrap();

  let upload = setup.data_upload_rx.recv().await.unwrap();
  assert_matches!(upload, DataUpload::ArtifactUploadIntent(intent) => {
      assert_eq!(intent.payload.artifact_id, id.to_string());
      assert_eq!(intent.payload.type_id, "client_report");
      assert_eq!(intent.payload.time, timestamp.into_proto());

      intent.response_tx.send(IntentResponse {
          uuid: intent.uuid,
          decision: bd_api::upload::IntentDecision::UploadImmediately }).unwrap();
  });


  let upload = setup.data_upload_rx.recv().await.unwrap();
  assert_matches!(upload, DataUpload::ArtifactUpload(upload) => {
      assert_eq!(upload.payload.artifact_id, id.to_string());
      assert_eq!(upload.payload.contents, b"abc");
      assert_eq!(upload.payload.type_id, "client_report");
      assert_eq!(upload.payload.time, timestamp.into_proto());
      assert_eq!(upload.payload.session_id, "session_id");
      assert_eq!(upload.payload.state_metadata, [("foo".into(), Data {
          data_type: Some(Data_type::StringData("bar".to_string())),
          ..Default::default()
      })].into());
      assert_eq!(upload.payload.feature_flags, vec![
          FeatureFlag {
              name: "key".to_string(),
              variant: Some("value".to_string()),
              last_updated: (timestamp - 1.std_seconds()).into_proto(),
              ..Default::default()
          },
          FeatureFlag {
              name: "key2".to_string(),
              variant: None,
              last_updated: (timestamp - 2.std_seconds()).into_proto(),
              ..Default::default()
          },
      ]);

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

  let id1 = setup
    .client
    .enqueue_upload(
      setup.make_file(b"1"),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
    )
    .unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id1.to_string()
  );

  let id2 = setup
    .client
    .enqueue_upload(
      setup.make_file(b"2"),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
    )
    .unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id2.to_string()
  );
  let id3 = setup
    .client
    .enqueue_upload(
      setup.make_file(b"3"),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
    )
    .unwrap();
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
  let mut setup = Setup::new(2).await;
  let id1 = setup
    .client
    .enqueue_upload(
      setup.make_file(b"1"),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
    )
    .unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id1.to_string()
  );
  let id2 = setup
    .client
    .enqueue_upload(
      setup.make_file(b"2"),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
    )
    .unwrap();
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
async fn inconsistent_state_extra_file() {
  let mut setup = Setup::new(2).await;
  let id1 = setup
    .client
    .enqueue_upload(
      setup.make_file(b"1"),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
    )
    .unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id1.to_string()
  );

  // Add another file that is not in the index.
  setup
    .filesystem
    .write_file(&super::REPORT_DIRECTORY.join("other"), b"1")
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
      assert_eq!(intent.payload.artifact_id, id1.to_string());
      assert_eq!(intent.payload.type_id, "client_report");

      intent.response_tx.send(IntentResponse {
          uuid: intent.uuid,
          decision: bd_api::upload::IntentDecision::UploadImmediately }).unwrap();
  });
  let upload = setup.data_upload_rx.recv().await.unwrap();
  assert_matches!(upload, DataUpload::ArtifactUpload(upload) => {
      assert_eq!(upload.payload.artifact_id, id1.to_string());
      assert_eq!(upload.payload.type_id, "client_report");

      upload.response_tx.send(UploadResponse { uuid: upload.uuid, success: true}).unwrap();
  });
  setup.upload_complete_rx.recv().await.unwrap();

  let files = setup
    .filesystem
    .list_files(&REPORT_DIRECTORY)
    .await
    .unwrap();
  assert_eq!(files.len(), 1);
  assert!(
    files[0].ends_with(
      &super::REPORT_DIRECTORY
        .join(&*REPORT_INDEX_FILE)
        .to_str()
        .unwrap()
    )
  );
}

#[tokio::test]
async fn disk_persistence() {
  let mut setup = Setup::new(2).await;
  let id1 = setup
    .client
    .enqueue_upload(
      setup.make_file(b"1"),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
    )
    .unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id1.to_string()
  );

  let mut setup = setup.reinitialize().await;
  let upload = setup.data_upload_rx.recv().await.unwrap();

  // First we'll see the intent negotiation from the first instance. Since we terminated the task
  // we expect to see the response channel already closed.
  assert_matches!(upload, DataUpload::ArtifactUploadIntent(intent) => {
      assert_eq!(intent.payload.artifact_id, id1.to_string());
      assert_eq!(intent.payload.type_id, "client_report");
      assert!(intent.response_tx.is_closed());
  });

  // The upload should resume after reinitializing due to the disk persisted index.
  let upload = setup.data_upload_rx.recv().await.unwrap();
  assert_matches!(upload, DataUpload::ArtifactUploadIntent(intent) => {
      assert_eq!(intent.payload.artifact_id, id1.to_string());
      assert_eq!(intent.payload.type_id, "client_report");

      intent.response_tx.send(IntentResponse {
          uuid: intent.uuid,
          decision: bd_api::upload::IntentDecision::UploadImmediately }).unwrap();
  });
  let upload = setup.data_upload_rx.recv().await.unwrap();
  assert_matches!(upload, DataUpload::ArtifactUpload(upload) => {
      assert_eq!(upload.payload.artifact_id, id1.to_string());
      assert_eq!(upload.payload.type_id, "client_report");
  });
}

#[tokio::test]
async fn inconsistent_state_missing_index() {
  let mut setup = Setup::new(2).await;
  let id1 = setup
    .client
    .enqueue_upload(
      setup.make_file(b"1"),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
    )
    .unwrap();
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

  let id2 = setup
    .client
    .enqueue_upload(
      setup.make_file(b"2"),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
    )
    .unwrap();
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

  // The id1 file should have been cleaned out due to not being referenced by the index file.
  assert!(
    !setup
      .filesystem
      .exists(&super::REPORT_DIRECTORY.join(id1.to_string()))
      .await
      .unwrap()
  );
}

#[tokio::test]
async fn new_entry_disk_full() {
  let mut setup = Setup::new(2).await;
  setup.filesystem.disk_full.store(true, Ordering::SeqCst);

  let id1 = setup
    .client
    .enqueue_upload(
      setup.make_file(b"1"),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
    )
    .unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id1.to_string()
  );

  assert_eq!(setup.filesystem.files().len(), 0);

  assert!(
    timeout(100.std_milliseconds(), setup.data_upload_rx.recv())
      .await
      .is_err()
  );
}

#[tokio::test]
async fn new_entry_disk_full_after_received() {
  let mut setup = Setup::new(2).await;

  let id1 = setup
    .client
    .enqueue_upload(
      setup.make_file(b"1"),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
    )
    .unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id1.to_string()
  );

  // The disk becomes full aka not writeable at this point. We'll continue to process the upload
  // since even if we were not able to update the index on disk.
  setup.filesystem.disk_full.store(true, Ordering::SeqCst);
  assert_eq!(setup.filesystem.files().len(), 2);

  let upload = setup.data_upload_rx.recv().await.unwrap();
  assert_matches!(upload, DataUpload::ArtifactUploadIntent(intent) => {
      assert_eq!(intent.payload.artifact_id, id1.to_string());
      assert_eq!(intent.payload.type_id, "client_report");
      intent.response_tx.send(IntentResponse {
          uuid: intent.uuid,
          decision: bd_api::upload::IntentDecision::UploadImmediately
      }).unwrap();
  });
  let upload = setup.data_upload_rx.recv().await.unwrap();
  assert_matches!(upload, DataUpload::ArtifactUpload(upload) => {
      assert_eq!(upload.payload.artifact_id, id1.to_string());
      assert_eq!(upload.payload.type_id, "client_report");
  });
}

#[tokio::test]
async fn intent_retries() {
  let mut setup = Setup::new(1).await;

  let id1 = setup
    .client
    .enqueue_upload(
      setup.make_file(b"1"),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
    )
    .unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id1.to_string()
  );

  let upload = setup.data_upload_rx.recv().await.unwrap();
  assert_matches!(upload, DataUpload::ArtifactUploadIntent(intent) => {
      assert_eq!(intent.payload.artifact_id, id1.to_string());
      assert_eq!(intent.payload.type_id, "client_report");

      // Drop the response channel. This mimics a disconnect.
  });

  let upload = setup.data_upload_rx.recv().await.unwrap();
  assert_matches!(upload, DataUpload::ArtifactUploadIntent(intent) => {
      assert_eq!(intent.payload.artifact_id, id1.to_string());
      assert_eq!(intent.payload.type_id, "client_report");
  });
}

#[tokio::test]
async fn intent_drop() {
  let mut setup = Setup::new(1).await;

  let id1 = setup
    .client
    .enqueue_upload(
      setup.make_file(b"1"),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
    )
    .unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id1.to_string()
  );

  let upload = setup.data_upload_rx.recv().await.unwrap();
  assert_matches!(upload, DataUpload::ArtifactUploadIntent(intent) => {
      assert_eq!(intent.payload.artifact_id, id1.to_string());
      assert_eq!(intent.payload.type_id, "client_report");

      intent.response_tx.send(IntentResponse {
          uuid: intent.uuid,
          decision: bd_api::upload::IntentDecision::Drop }).unwrap();
  });

  assert!(
    timeout(100.std_milliseconds(), setup.data_upload_rx.recv())
      .await
      .is_err()
  );
}

#[tokio::test]
async fn upload_retries() {
  let mut setup = Setup::new(1).await;

  let id1 = setup
    .client
    .enqueue_upload(
      setup.make_file(b"1"),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
    )
    .unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id1.to_string()
  );

  let upload = setup.data_upload_rx.recv().await.unwrap();
  assert_matches!(upload, DataUpload::ArtifactUploadIntent(intent) => {
      assert_eq!(intent.payload.artifact_id, id1.to_string());
      assert_eq!(intent.payload.type_id, "client_report");

      intent.response_tx.send(IntentResponse {
          uuid: intent.uuid,
          decision: bd_api::upload::IntentDecision::UploadImmediately }).unwrap();
  });

  let upload = setup.data_upload_rx.recv().await.unwrap();
  assert_matches!(upload, DataUpload::ArtifactUpload(upload) => {
      assert_eq!(upload.payload.artifact_id, id1.to_string());
      assert_eq!(upload.payload.type_id, "client_report");

      // Drop the response channel. This mimics a disconnect.
  });

  let upload = setup.data_upload_rx.recv().await.unwrap();
  assert_matches!(upload, DataUpload::ArtifactUpload(upload) => {
      assert_eq!(upload.payload.artifact_id, id1.to_string());
      assert_eq!(upload.payload.type_id, "client_report");
  });
}
