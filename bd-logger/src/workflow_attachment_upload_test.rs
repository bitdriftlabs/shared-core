// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::*;
use crate::workflow_attachment::{AdmittedAttachment, AttachmentStore, AttachmentStoreHandle};
use bd_api::DataUpload;
use bd_api::upload::{IntentDecision, IntentResponse};
use bd_artifact_upload::{ChunkedFileAdmission, EnqueueError, UploadSource, Uploader};
use bd_client_common::artifact::MAX_CHUNKED_ARTIFACT_LOGICAL_BYTES;
use bd_client_common::file_system::RealFileSystem;
use bd_client_common::test::TestFileSystem;
use bd_client_stats_store::Collector;
use bd_proto::protos::client::api::upload_artifact_response::State;
use bd_proto::protos::client::api::{UploadArtifactRequest, UploadArtifactResponse};
use bd_runtime::runtime::{ConfigLoader, FeatureFlag as _, attachment, chunked_artifact_upload};
use bd_runtime::test::TestConfigLoader;
use bd_shutdown::ComponentShutdownTrigger;
use bd_test_helpers::runtime::{ValueKind, make_update};
use bd_time::{OffsetDateTimeExt as _, TestTimeProvider};
use flate2::read::ZlibDecoder;
use std::io::{self, Read as _};
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::Ordering;
use time::OffsetDateTime;

async fn configure_admission(runtime: &ConfigLoader, extra: Vec<(&str, ValueKind)>) {
  runtime
    .update_snapshot(make_update(extra, Uuid::new_v4().to_string()))
    .await
    .unwrap();
}

async fn admit_file(
  store: &Arc<AttachmentStore>,
  directory: &Path,
  size: u64,
) -> io::Result<AdmittedAttachment> {
  let path = directory.join(format!("capture-{}", Uuid::new_v4()));
  tokio::fs::File::create(&path).await?.set_len(size).await?;
  let source = UploadSource::Path(path);
  let _owned = source.owned_file();
  store
    .admit(source, Some("application/octet-stream".into()))
    .await
}

#[tokio::test]
async fn workflow_file_routing_uses_logical_size_before_retention() {
  for (size, extension) in [
    (1024 * 1024 - 1, "payload"),
    (1024 * 1024, "payload"),
    (1024 * 1024 + 1, "raw"),
  ] {
    let directory = tempfile::tempdir().unwrap();
    let runtime = TestConfigLoader::new_in_directory(directory.path()).await;
    let store = AttachmentStoreHandle::new(directory.path().to_owned(), runtime.clone())
      .get()
      .await
      .unwrap();
    let attachment = admit_file(&store, directory.path(), size).await.unwrap();
    let source = store.upload_source(attachment.id).await.unwrap();
    assert_eq!(source.extension().unwrap(), extension);
    if extension == "raw" {
      assert_eq!(
        tokio::fs::metadata(directory.path().join(source))
          .await
          .unwrap()
          .len(),
        size + 4
      );
    } else {
      let compressed = tokio::fs::read(directory.path().join(source))
        .await
        .unwrap();
      let mut decoded = Vec::new();
      ZlibDecoder::new(compressed.as_slice())
        .read_to_end(&mut decoded)
        .unwrap();
      assert_eq!(decoded, vec![0; usize::try_from(size).unwrap()]);
    }
    store.release(attachment.id).await.unwrap();
    configure_admission(
      &runtime,
      vec![(attachment::MaxBytes::path(), ValueKind::Int(1))],
    )
    .await;
    let large = admit_file(&store, directory.path(), 1024 * 1024 + 1)
      .await
      .unwrap();
    store.release(large.id).await.unwrap();
    assert!(
      store
        .admit(UploadSource::Bytes(vec![0; 2]), None)
        .await
        .is_err()
    );
    assert!(
      tokio::fs::read_dir(directory.path().join("workflow-attachments"))
        .await
        .unwrap()
        .next_entry()
        .await
        .unwrap()
        .is_none()
    );
  }
}

#[tokio::test]
async fn workflow_retained_files_reserve_shared_chunk_slots_and_staging_bytes() {
  for (size, accepted) in [(64 * 1024 * 1024 - 4, true), (64 * 1024 * 1024 - 3, false)] {
    let directory = tempfile::tempdir().unwrap();
    let runtime = TestConfigLoader::new_in_directory(directory.path()).await;
    let retained = directory.path().join("workflow-attachments");
    tokio::fs::create_dir(&retained).await.unwrap();
    tokio::fs::File::create(retained.join(format!("{}.raw", Uuid::new_v4())))
      .await
      .unwrap()
      .set_len(u64::from(MAX_CHUNKED_ARTIFACT_LOGICAL_BYTES) + 4)
      .await
      .unwrap();
    let filesystem = RealFileSystem::new(directory.path().to_owned());
    let admission = ChunkedFileAdmission::new(&runtime);
    assert_eq!(
      admission.admit_workflow(&filesystem, size).await.is_ok(),
      accepted
    );
  }
}

#[tokio::test]
async fn workflow_chunk_logical_limit_is_inclusive_and_hard_bounded() {
  let directory = tempfile::tempdir().unwrap();
  let runtime = TestConfigLoader::new_in_directory(directory.path()).await;
  let store = AttachmentStoreHandle::new(directory.path().to_owned(), runtime.clone())
    .get()
    .await
    .unwrap();
  configure_admission(
    &runtime,
    vec![(
      chunked_artifact_upload::MaxLogicalBytes::path(),
      ValueKind::Int(u32::MAX),
    )],
  )
  .await;
  assert!(admit_file(&store, directory.path(), 0).await.is_err());
  let attachment = admit_file(&store, directory.path(), 1024).await.unwrap();
  store.release(attachment.id).await.unwrap();
  let maximum = u64::from(MAX_CHUNKED_ARTIFACT_LOGICAL_BYTES);
  assert!(
    admit_file(&store, directory.path(), maximum + 1)
      .await
      .is_err()
  );
  let attachment = admit_file(&store, directory.path(), maximum).await.unwrap();
  assert_eq!(
    tokio::fs::metadata(
      directory
        .path()
        .join(store.upload_source(attachment.id).await.unwrap())
    )
    .await
    .unwrap()
    .len(),
    maximum + 4
  );
  store.release(attachment.id).await.unwrap();
}

#[tokio::test]
async fn retained_workflows_and_direct_commands_share_chunk_admission() {
  let directory = tempfile::tempdir().unwrap();
  let runtime = TestConfigLoader::new_in_directory(directory.path()).await;
  configure_admission(&runtime, Vec::new()).await;
  let store = AttachmentStoreHandle::new(directory.path().to_owned(), runtime.clone())
    .get()
    .await
    .unwrap();
  let mut retained = Vec::new();
  for _ in 0 .. 64 {
    retained.push(
      admit_file(&store, directory.path(), 1024 * 1024 + 1)
        .await
        .unwrap(),
    );
  }
  let (tx, _rx) = tokio::sync::mpsc::channel(1);
  let shutdown = ComponentShutdownTrigger::default();
  let (uploader, client) = Uploader::new(
    Arc::new(RealFileSystem::new(directory.path().to_owned())),
    tx,
    Arc::new(TestTimeProvider::new(OffsetDateTime::now_utc())),
    &runtime,
    &Collector::default(),
    HashMap::new(),
    tokio::sync::watch::channel(Arc::default()).1,
    shutdown.make_shutdown(),
  );
  let uploader = tokio::spawn(uploader.run());
  for admitted in [false, true] {
    let id = Uuid::new_v4();
    let path = directory.path().join(format!("command-{id}"));
    tokio::fs::write(&path, b"").await.unwrap();
    let (tx, rx) = oneshot::channel();
    client
      .enqueue_chunked_file(
        path.clone(),
        UploadArtifactRequest {
          artifact_id: id.to_string(),
          command_id: Some(Uuid::new_v4().to_string()),
          type_id: "device_command_attachment".into(),
          session_id: "session".into(),
          time: OffsetDateTime::now_utc().into_proto(),
          ..Default::default()
        },
        tx,
        None,
      )
      .unwrap();
    let result = rx.await.unwrap();
    assert_eq!(result.is_ok(), admitted);
    assert!(!path.exists());
    if admitted {
      assert_eq!(result.unwrap(), 0);
      assert!(
        admit_file(&store, directory.path(), 1024 * 1024 + 1)
          .await
          .is_err()
      );
    } else {
      store.release(retained.pop().unwrap().id).await.unwrap();
    }
  }
  for attachment in retained {
    store.release(attachment.id).await.unwrap();
  }
  shutdown.shutdown().await;
  uploader.await.unwrap();
}

#[tokio::test]
async fn workflow_chunk_failed_persistence_and_restart_keep_retained_ownership() {
  for fault in ["chunk_sync", "published_index_sync", "crc"] {
    let filesystem = Arc::new(TestFileSystem::new());
    let directory = filesystem.directory();
    let runtime = TestConfigLoader::new_in_directory(directory).await;
    configure_admission(&runtime, Vec::new()).await;
    let store = AttachmentStoreHandle::new(directory.to_owned(), runtime.clone())
      .get()
      .await
      .unwrap();
    let attachment = admit_file(&store, directory, 1024 * 1024 + 17)
      .await
      .unwrap();
    let id = attachment.id;
    store
      .record_timestamp(id, OffsetDateTime::from_unix_timestamp(10).unwrap())
      .await
      .unwrap();
    let source = store.upload_source(id).await.unwrap();
    let payload = directory.join(&source);
    let (data_upload_tx, _data_upload_rx) = tokio::sync::mpsc::channel(1);
    let shutdown = ComponentShutdownTrigger::default();
    let (uploader, client) = Uploader::new(
      filesystem.clone(),
      data_upload_tx,
      Arc::new(TestTimeProvider::new(OffsetDateTime::now_utc())),
      &runtime,
      &Collector::default(),
      HashMap::new(),
      tokio::sync::watch::channel(Arc::default()).1,
      shutdown.make_shutdown(),
    );
    let mut revision = client.upload_queue_revision();
    let uploader = tokio::spawn(uploader.run());
    let _ = revision.wait_for(|revision| *revision > 0).await.unwrap();
    match fault {
      "chunk_sync" => filesystem.fail_next_sync.store(true, Ordering::Relaxed),
      "published_index_sync" => {
        filesystem.fail_next_sync_at("report_uploads/report_index.pb".into());
      },
      _ => {
        assert_eq!(fault, "crc");
        let mut bytes = tokio::fs::read(&payload).await.unwrap();
        bytes[0] ^= 1;
        tokio::fs::write(&payload, bytes).await.unwrap();
      },
    }
    let (tx, rx) = oneshot::channel();
    client
      .enqueue_workflow_attachment(
        id,
        source.clone(),
        "session".into(),
        Some("application/octet-stream".into()),
        Some(tx),
        None,
        Some(store.upload_lease().await.unwrap()),
      )
      .unwrap();
    assert!(rx.await.unwrap().is_err());
    assert!(payload.exists());
    assert!(!store.is_uploaded(id).await.unwrap());
    let pending = Uploader::pending_workflow_attachments(directory)
      .await
      .unwrap();
    assert_eq!(pending.contains(&id), fault == "published_index_sync");
    shutdown.shutdown().await;
    uploader.await.unwrap();
    if fault == "crc" {
      assert!(pending.is_empty());
      store.release(id).await.unwrap();
      assert!(!payload.exists());
      continue;
    }
    drop(store);
    configure_admission(
      &runtime,
      vec![(
        chunked_artifact_upload::MaxLogicalBytes::path(),
        ValueKind::Int(1),
      )],
    )
    .await;
    let store = AttachmentStoreHandle::new(directory.to_owned(), runtime.clone())
      .get()
      .await
      .unwrap();
    let (data_upload_tx, mut data_upload_rx) = tokio::sync::mpsc::channel(1);
    let shutdown = ComponentShutdownTrigger::default();
    let (uploader, client) = Uploader::new(
      filesystem.clone(),
      data_upload_tx,
      Arc::new(TestTimeProvider::new(OffsetDateTime::now_utc())),
      &runtime,
      &Collector::default(),
      HashMap::new(),
      tokio::sync::watch::channel(Arc::default()).1,
      shutdown.make_shutdown(),
    );
    let uploader = tokio::spawn(uploader.run());
    let (persisted_tx, persisted_rx) = oneshot::channel();
    let (completed_tx, completed_rx) = oneshot::channel();
    client
      .enqueue_workflow_attachment(
        id,
        source,
        "session".into(),
        Some("application/octet-stream".into()),
        Some(persisted_tx),
        Some(completed_tx),
        Some(store.upload_lease().await.unwrap()),
      )
      .unwrap();
    persisted_rx.await.unwrap().unwrap();
    store.cleanup_all().await.unwrap();
    assert!(payload.exists());
    let Some(DataUpload::ArtifactUpload(upload)) = data_upload_rx.recv().await else {
      panic!("expected recovered workflow Begin");
    };
    assert_eq!(upload.payload.artifact_id, id.to_string());
    assert_eq!(upload.payload.session_id, "session");
    assert_eq!(upload.payload.logical_bytes, Some(1024 * 1024 + 17));
    assert_eq!(upload.payload.chunks.len(), 2);
    upload
      .response_tx
      .send(UploadArtifactResponse {
        upload_uuid: upload.uuid,
        state: State::STATE_FINALIZED.into(),
        received_chunks: vec![3],
        ..Default::default()
      })
      .unwrap();
    completed_rx.await.unwrap().unwrap();
    store.complete_upload(id).await.unwrap();
    assert!(!payload.exists());
    shutdown.shutdown().await;
    uploader.await.unwrap();
  }
}

#[tokio::test]
async fn workflow_chunk_handoff_is_durable_idempotent_and_protected_until_finalized() {
  let filesystem = Arc::new(TestFileSystem::new());
  let directory = filesystem.directory();
  let runtime = TestConfigLoader::new_in_directory(directory).await;
  configure_admission(&runtime, Vec::new()).await;
  let attachment_store = AttachmentStoreHandle::new(directory.to_owned(), runtime.clone());
  let store = attachment_store.get().await.unwrap();
  let attachment = admit_file(&store, directory, 1024 * 1024 + 1)
    .await
    .unwrap();
  let id = attachment.id;
  store
    .record_timestamp(id, OffsetDateTime::from_unix_timestamp(10).unwrap())
    .await
    .unwrap();
  let payload = directory.join(store.upload_source(id).await.unwrap());
  // Admission, not the later flush, fixes policy. A lower limit cannot strand this source.
  configure_admission(
    &runtime,
    vec![(
      chunked_artifact_upload::MaxLogicalBytes::path(),
      ValueKind::Int(1),
    )],
  )
  .await;
  let chunk_sync = filesystem.gate_next_sync(format!("report_uploads/chunk-{id}-0").into());
  let (data_upload_tx, mut data_upload_rx) = tokio::sync::mpsc::channel(1);
  let shutdown = ComponentShutdownTrigger::default();
  let (uploader, client) = Uploader::new(
    filesystem.clone(),
    data_upload_tx,
    Arc::new(TestTimeProvider::new(OffsetDateTime::now_utc())),
    &runtime,
    &Collector::default(),
    HashMap::new(),
    tokio::sync::watch::channel(Arc::default()).1,
    shutdown.make_shutdown(),
  );
  let uploader = tokio::spawn(uploader.run());
  let (completed_tx, completed_rx) = oneshot::channel();
  let (handle, worker) = WorkflowAttachmentUploadHandle::new_with_attachment_store_and_test_hooks(
    Arc::new(client),
    attachment_store,
    Some(Arc::new(UploadCompletionHook {
      completion_tx: Mutex::new(Some(completed_tx)),
    })),
  );
  let handle = Arc::new(handle);
  let worker = tokio::spawn(worker.run());
  let stage_handle = handle.clone();
  let stage = tokio::spawn(async move {
    stage_handle
      .stage(HashMap::from([(id, "session".into())]))
      .await
  });
  let resume_chunk = chunk_sync.await.unwrap();
  assert!(!stage.is_finished());
  let mut cleanup = Box::pin(store.cleanup_all());
  assert!(futures_util::poll!(cleanup.as_mut()).is_pending());
  let index_sync = filesystem.gate_next_sync("report_uploads/report_index.pb".into());
  resume_chunk.send(()).unwrap();
  let resume_index = index_sync.await.unwrap();
  assert!(!stage.is_finished());
  assert_eq!(
    Uploader::pending_workflow_attachments(directory)
      .await
      .unwrap(),
    [id].into()
  );
  assert!(futures_util::poll!(cleanup.as_mut()).is_pending());
  resume_index.send(()).unwrap();
  assert!(stage.await.unwrap().unwrap().is_empty());
  cleanup.await.unwrap();
  assert!(payload.exists());
  assert!(
    handle
      .stage(HashMap::from([(id, "session".into())]))
      .await
      .unwrap()
      .is_empty()
  );
  assert_eq!(
    handle
      .stage(HashMap::from([(id, "other-session".into())]))
      .await
      .unwrap()
      .len(),
    1
  );
  for (chunk, state, receipts) in [
    (None, State::STATE_RECEIVING, 0),
    (Some(0), State::STATE_RECEIVING, 1),
    (Some(1), State::STATE_FINALIZING, 3),
    (None, State::STATE_FINALIZED, 3),
  ] {
    let Some(DataUpload::ArtifactUpload(upload)) = data_upload_rx.recv().await else {
      panic!("expected shared chunk transport, not intent negotiation");
    };
    assert_eq!(upload.payload.artifact_id, id.to_string());
    assert_eq!(upload.payload.command_id, None);
    assert_eq!(upload.payload.logical_bytes, Some(1024 * 1024 + 1));
    assert_eq!(upload.payload.chunk_index, chunk);
    if upload.payload.time.is_some() {
      assert_eq!(upload.payload.type_id, "workflow_attachment");
      assert_eq!(upload.payload.session_id, "session");
      assert_eq!(upload.payload.chunks.len(), 2);
    }
    assert!(payload.exists());
    upload
      .response_tx
      .send(UploadArtifactResponse {
        upload_uuid: upload.uuid,
        state: state.into(),
        received_chunks: vec![receipts],
        ..Default::default()
      })
      .unwrap();
  }
  assert_eq!(completed_rx.await.unwrap(), id);
  assert!(!payload.exists());
  assert!(store.is_uploaded(id).await.unwrap());
  drop(handle);
  worker.await.unwrap();
  shutdown.shutdown().await;
  uploader.await.unwrap();
}

#[tokio::test]
async fn cancelled_staging_retains_real_admission_ownership_through_both_syncs() {
  let filesystem = Arc::new(TestFileSystem::new());
  let directory = filesystem.directory();
  let runtime = TestConfigLoader::new_in_directory(directory).await;
  let attachment_store = AttachmentStoreHandle::new(directory.to_owned(), runtime.clone());
  let store = attachment_store.get().await.unwrap();
  let attachment = store
    .admit(UploadSource::Bytes(b"attachment".to_vec()), None)
    .await
    .unwrap();
  store
    .record_timestamp(
      attachment.id,
      OffsetDateTime::from_unix_timestamp(10).unwrap(),
    )
    .await
    .unwrap();
  let payload = PathBuf::from(format!("workflow-attachments/{}.payload", attachment.id));
  let source_sync = filesystem.gate_next_sync(payload.clone());
  let (data_upload_tx, mut data_upload_rx) = tokio::sync::mpsc::channel(1);
  let shutdown = ComponentShutdownTrigger::default();
  let (uploader, client) = Uploader::new(
    filesystem.clone(),
    data_upload_tx,
    Arc::new(TestTimeProvider::new(OffsetDateTime::now_utc())),
    &runtime,
    &Collector::default(),
    HashMap::new(),
    tokio::sync::watch::channel(Arc::default()).1,
    shutdown.make_shutdown(),
  );
  let mut revision = client.upload_queue_revision();
  let uploader = tokio::spawn(uploader.run());
  let (handle, worker) = WorkflowAttachmentUploadHandle::new_with_attachment_store_and_test_hooks(
    Arc::new(client),
    attachment_store,
    None,
  );
  let worker = tokio::spawn(worker.run());
  let stage = tokio::spawn(async move {
    handle
      .stage(HashMap::from([(attachment.id, "session".into())]))
      .await
  });
  let resume_source = source_sync.await.unwrap();
  stage.abort();
  assert!(matches!(stage.await, Err(error) if error.is_cancelled()));
  worker.await.unwrap();

  assert!(
    Uploader::pending_workflow_attachments(directory)
      .await
      .unwrap()
      .is_empty()
  );
  // The staging worker is gone; only the request being persisted can own this permit.
  {
    let mut lease = Box::pin(store.upload_lease());
    assert!(futures_util::poll!(lease.as_mut()).is_pending());
  }
  let mut cleanup = Box::pin(store.cleanup_all());
  assert!(futures_util::poll!(cleanup.as_mut()).is_pending());
  let index_sync = filesystem.gate_next_sync("report_uploads/report_index.pb".into());
  resume_source.send(()).unwrap();
  let resume_index = index_sync.await.unwrap();
  assert_eq!(
    Uploader::pending_workflow_attachments(directory)
      .await
      .unwrap(),
    [attachment.id].into()
  );
  assert!(futures_util::poll!(cleanup.as_mut()).is_pending());
  resume_index.send(()).unwrap();
  cleanup.await.unwrap();
  assert!(
    tokio::fs::try_exists(directory.join(&payload))
      .await
      .unwrap()
  );
  assert!(
    !tokio::fs::try_exists(directory.join(format!("report_uploads/{}", attachment.id)))
      .await
      .unwrap()
  );

  let Some(DataUpload::ArtifactUploadIntent(intent)) = data_upload_rx.recv().await else {
    panic!("expected workflow intent");
  };
  intent
    .response_tx
    .send(IntentResponse {
      uuid: intent.uuid,
      decision: IntentDecision::UploadImmediately,
    })
    .unwrap();
  let Some(DataUpload::ArtifactUpload(upload)) = data_upload_rx.recv().await else {
    panic!("expected workflow bytes");
  };
  let mut decoded = Vec::new();
  ZlibDecoder::new(upload.payload.contents.as_slice())
    .read_to_end(&mut decoded)
    .unwrap();
  assert_eq!(decoded, b"attachment");
  let previous = *revision.borrow();
  upload
    .response_tx
    .send(UploadArtifactResponse {
      upload_uuid: upload.uuid,
      ..Default::default()
    })
    .unwrap();
  let _ = revision
    .wait_for(|revision| *revision > previous)
    .await
    .unwrap();
  // Cancellation drops the callback; retired bytes must still be collectible once unpinned.
  assert!(!store.is_uploaded(attachment.id).await.unwrap());
  store.cleanup_all().await.unwrap();
  assert!(
    !tokio::fs::try_exists(directory.join(payload))
      .await
      .unwrap()
  );
  shutdown.shutdown().await;
  uploader.await.unwrap();
}

struct UploadCompletionHook {
  completion_tx: Mutex<Option<oneshot::Sender<Uuid>>>,
}

impl TestHooks for UploadCompletionHook {
  fn workflow_attachment_upload_completed(&self, artifact_id: Uuid) {
    if let Some(completion_tx) = self.completion_tx.lock().unwrap().take() {
      let _ignored = completion_tx.send(artifact_id);
    }
  }
}

#[tokio::test]
async fn single_payload_survives_log_retirement_until_real_upload_completes() {
  let directory = tempfile::tempdir().unwrap();
  let runtime = TestConfigLoader::new_in_directory(directory.path()).await;
  let attachment_store = AttachmentStoreHandle::new(directory.path().to_owned(), runtime.clone());
  let store = attachment_store.get().await.unwrap();
  let attachment = store
    .admit(UploadSource::Bytes(b"attachment".to_vec()), None)
    .await
    .unwrap();
  store
    .record_timestamp(
      attachment.id,
      OffsetDateTime::from_unix_timestamp(10).unwrap(),
    )
    .await
    .unwrap();
  let (data_upload_tx, mut data_upload_rx) = tokio::sync::mpsc::channel(1);
  let shutdown = ComponentShutdownTrigger::default();
  let (uploader, client) = Uploader::new(
    Arc::new(RealFileSystem::new(directory.path().to_owned())),
    data_upload_tx,
    Arc::new(TestTimeProvider::new(OffsetDateTime::now_utc())),
    &runtime,
    &Collector::default(),
    HashMap::new(),
    tokio::sync::watch::channel(Arc::default()).1,
    shutdown.make_shutdown(),
  );
  let uploader = tokio::spawn(uploader.run());
  let (completed_tx, completed_rx) = oneshot::channel();
  let (handle, worker) = WorkflowAttachmentUploadHandle::new_with_attachment_store_and_test_hooks(
    Arc::new(client),
    attachment_store,
    Some(Arc::new(UploadCompletionHook {
      completion_tx: Mutex::new(Some(completed_tx)),
    })),
  );
  let worker = tokio::spawn(worker.run());
  assert!(
    handle
      .stage(HashMap::from([(attachment.id, "session".into())]))
      .await
      .unwrap()
      .is_empty()
  );
  assert_eq!(
    Uploader::pending_workflow_attachments(directory.path())
      .await
      .unwrap(),
    [attachment.id].into()
  );
  let payload = directory
    .path()
    .join(format!("workflow-attachments/{}.payload", attachment.id));
  assert!(tokio::fs::try_exists(&payload).await.unwrap());
  assert!(
    !tokio::fs::try_exists(
      directory
        .path()
        .join(format!("report_uploads/{}", attachment.id))
    )
    .await
    .unwrap()
  );
  store.cleanup_all().await.unwrap();
  assert!(tokio::fs::try_exists(&payload).await.unwrap());

  let Some(DataUpload::ArtifactUploadIntent(intent)) = data_upload_rx.recv().await else {
    panic!("expected workflow artifact intent");
  };
  intent
    .response_tx
    .send(IntentResponse {
      uuid: intent.uuid,
      decision: IntentDecision::UploadImmediately,
    })
    .unwrap();
  let Some(DataUpload::ArtifactUpload(upload)) = data_upload_rx.recv().await else {
    panic!("expected workflow artifact bytes");
  };
  let mut decoded = Vec::new();
  ZlibDecoder::new(upload.payload.contents.as_slice())
    .read_to_end(&mut decoded)
    .unwrap();
  assert_eq!(decoded, b"attachment");
  upload
    .response_tx
    .send(UploadArtifactResponse {
      upload_uuid: upload.uuid,
      ..Default::default()
    })
    .unwrap();
  assert_eq!(completed_rx.await.unwrap(), attachment.id);
  assert!(store.is_uploaded(attachment.id).await.unwrap());
  assert!(!tokio::fs::try_exists(&payload).await.unwrap());
  store.cleanup_all().await.unwrap();
  drop(handle);
  worker.await.unwrap();
  shutdown.shutdown().await;
  uploader.await.unwrap();
}

#[tokio::test]
async fn cancelled_staging_keeps_cleanup_excluded_until_the_queue_releases_its_lease() {
  let directory = tempfile::tempdir().unwrap();
  let attachment_store = AttachmentStoreHandle::new(
    directory.path().to_owned(),
    ConfigLoader::new(directory.path()),
  );
  let store = attachment_store.get().await.unwrap();
  let attachment = store
    .admit(UploadSource::Bytes(b"attachment".to_vec()), None)
    .await
    .unwrap();
  store
    .record_timestamp(
      attachment.id,
      OffsetDateTime::from_unix_timestamp(10).unwrap(),
    )
    .await
    .unwrap();
  let (admission_tx, admission_rx) = oneshot::channel();
  let mut admission_tx = Some(admission_tx);
  let mut client = bd_artifact_upload::MockClient::new();
  client
    .expect_enqueue_workflow_attachment()
    .once()
    .returning(move |_, _, _, _, persisted, _, lease| {
      admission_tx
        .take()
        .unwrap()
        .send((persisted.unwrap(), lease.unwrap()))
        .unwrap();
      Ok(())
    });
  let (handle, worker) = WorkflowAttachmentUploadHandle::new_with_attachment_store_and_test_hooks(
    Arc::new(client),
    attachment_store,
    None,
  );
  let worker = tokio::spawn(worker.run());
  let stage = tokio::spawn(async move {
    handle
      .stage(HashMap::from([(attachment.id, "session".into())]))
      .await
  });
  let (ack, lease) = admission_rx.await.unwrap();
  stage.abort();
  assert!(matches!(stage.await, Err(error) if error.is_cancelled()));
  let mut cleanup = Box::pin(store.cleanup_all());
  assert!(futures_util::poll!(cleanup.as_mut()).is_pending());
  let payload = directory
    .path()
    .join(format!("workflow-attachments/{}.payload", attachment.id));
  assert!(tokio::fs::try_exists(&payload).await.unwrap());
  drop(ack);
  drop(lease);
  cleanup.await.unwrap();
  assert!(!tokio::fs::try_exists(payload).await.unwrap());
  worker.await.unwrap();
}

#[tokio::test]
async fn stage_waits_for_indexed_lease_ack() {
  let artifact_id = Uuid::new_v4();
  let expected_source = PathBuf::from(format!("workflow-attachments/{artifact_id}.payload"));
  let (ack_tx, ack_rx) = oneshot::channel();
  let mut mock_client = bd_artifact_upload::MockClient::new();
  let mut ack_tx = Some(ack_tx);
  mock_client
    .expect_enqueue_workflow_attachment()
    .withf(move |id, source, session, _, _, _, _| {
      *id == artifact_id && *source == expected_source && session == "session"
    })
    .once()
    .returning(move |_, _, _, _, persisted, _, _| {
      ack_tx.take().unwrap().send(persisted.unwrap()).unwrap();
      Ok(())
    });
  let (handle, worker) = WorkflowAttachmentUploadHandle::new(Arc::new(mock_client));
  let worker = tokio::spawn(worker.run());
  let stage = tokio::spawn(async move {
    handle
      .stage(HashMap::from([(artifact_id, "session".to_string())]))
      .await
  });

  let indexed_lease_ack = ack_rx.await.unwrap();
  assert!(!stage.is_finished());
  indexed_lease_ack.send(Ok(())).unwrap();
  stage.await.unwrap().unwrap();
  worker.await.unwrap();
}

#[tokio::test]
async fn queue_backpressure_does_not_stall_another_batch() {
  let first_id = Uuid::new_v4();
  let second_id = Uuid::new_v4();
  let (first_tx, first_rx) = oneshot::channel();
  let mut first_tx = Some(first_tx);
  let mut mock_client = bd_artifact_upload::MockClient::new();
  mock_client
    .expect_enqueue_workflow_attachment()
    .withf(move |id, _, _, _, _, _, _| *id == first_id)
    .once()
    .returning(move |_, _, _, _, persisted, _, _| {
      first_tx.take().unwrap().send(()).unwrap();
      persisted
        .unwrap()
        .send(Err(EnqueueError::QueueFull))
        .unwrap();
      Ok(())
    });
  mock_client
    .expect_enqueue_workflow_attachment()
    .withf(move |id, _, _, _, _, _, _| *id == second_id)
    .once()
    .returning(|_, _, _, _, persisted, _, _| {
      persisted.unwrap().send(Ok(())).unwrap();
      Ok(())
    });

  let (handle, worker) = WorkflowAttachmentUploadHandle::new(Arc::new(mock_client));
  let handle = Arc::new(handle);
  let worker = tokio::spawn(worker.run());
  let first_handle = handle.clone();
  let first = tokio::spawn(async move {
    first_handle
      .stage(HashMap::from([(first_id, "first".to_string())]))
      .await
  });
  first_rx.await.unwrap();
  handle
    .stage(HashMap::from([(second_id, "second".to_string())]))
    .await
    .unwrap();
  let failures = first.await.unwrap().unwrap();
  assert_eq!(failures.len(), 1);
  assert_eq!(failures[0].artifact_id, first_id);
  drop(handle);
  worker.await.unwrap();
}

#[tokio::test]
async fn retryable_persistence_failure_is_reported() {
  let artifact_id = Uuid::new_v4();
  let mut mock_client = bd_artifact_upload::MockClient::new();
  mock_client
    .expect_enqueue_workflow_attachment()
    .once()
    .returning(|_, _, _, _, persisted, _, _| {
      persisted
        .unwrap()
        .send(Err(EnqueueError::RetryablePersistence(anyhow::anyhow!(
          "injected sync failure"
        ))))
        .unwrap();
      Ok(())
    });

  let (handle, worker) = WorkflowAttachmentUploadHandle::new(Arc::new(mock_client));
  let worker = tokio::spawn(worker.run());
  let stage = tokio::spawn(async move {
    handle
      .stage(HashMap::from([(artifact_id, "session".to_string())]))
      .await
  });

  let failures = stage.await.unwrap().unwrap();
  assert_eq!(failures.len(), 1);
  assert_eq!(failures[0].artifact_id, artifact_id);
  assert!(failures[0].error.contains("injected sync failure"));
  worker.await.unwrap();
}

#[tokio::test]
async fn permanent_staging_failure_does_not_block_other_attachments() {
  let failed_id = Uuid::new_v4();
  let staged_id = Uuid::new_v4();
  let mut mock_client = bd_artifact_upload::MockClient::new();
  mock_client
    .expect_enqueue_workflow_attachment()
    .times(2)
    .returning(move |id, _, _, _, persisted, _, _| {
      let result = if id == failed_id {
        Err(EnqueueError::Other(anyhow::anyhow!(
          "missing retained payload"
        )))
      } else {
        assert_eq!(id, staged_id);
        Ok(())
      };
      persisted.unwrap().send(result).unwrap();
      Ok(())
    });

  let (handle, worker) = WorkflowAttachmentUploadHandle::new(Arc::new(mock_client));
  let worker = tokio::spawn(worker.run());
  let failures = handle
    .stage(HashMap::from([
      (failed_id, "failed".to_string()),
      (staged_id, "staged".to_string()),
    ]))
    .await
    .unwrap();

  assert_eq!(failures.len(), 1);
  assert_eq!(failures[0].artifact_id, failed_id);
  assert!(failures[0].error.contains("missing retained payload"));
  drop(handle);
  worker.await.unwrap();
}

#[tokio::test]
async fn corrupt_metadata_does_not_block_healthy_attachment_staging() {
  for restart in [false, true] {
    let directory = tempfile::tempdir().unwrap();
    let attachment_store = AttachmentStoreHandle::new(
      directory.path().to_owned(),
      ConfigLoader::new(directory.path()),
    );
    let store = attachment_store.get().await.unwrap();
    let corrupt = store
      .admit(UploadSource::Bytes(b"corrupt".to_vec()), None)
      .await
      .unwrap();
    let healthy = store
      .admit(
        UploadSource::Bytes(b"healthy".to_vec()),
        Some("image/jpeg".into()),
      )
      .await
      .unwrap();
    let path = directory
      .path()
      .join("workflow-attachments")
      .join(format!("{}.metadata", corrupt.id));
    let mut bytes = tokio::fs::read(&path).await.unwrap();
    bytes[0] ^= 1;
    tokio::fs::write(path, bytes).await.unwrap();
    let attachment_store = if restart {
      AttachmentStoreHandle::new(
        directory.path().to_owned(),
        ConfigLoader::new(directory.path()),
      )
    } else {
      attachment_store
    };
    let mut client = bd_artifact_upload::MockClient::new();
    client
      .expect_enqueue_workflow_attachment()
      .withf(move |id, _, session, content_type, _, _, _| {
        *id == healthy.id && session == "healthy" && content_type.as_deref() == Some("image/jpeg")
      })
      .once()
      .returning(|_, _, _, _, persisted, _, _| {
        persisted.unwrap().send(Ok(())).unwrap();
        Ok(())
      });
    let (handle, worker) = WorkflowAttachmentUploadHandle::new_with_attachment_store_and_test_hooks(
      Arc::new(client),
      attachment_store,
      None,
    );
    let worker = tokio::spawn(worker.run());
    let failures = handle
      .stage(HashMap::from([
        (corrupt.id, "corrupt".to_string()),
        (healthy.id, "healthy".to_string()),
      ]))
      .await
      .unwrap();
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].artifact_id, corrupt.id);
    assert!(failures[0].error.contains(
      if restart {
        "missing workflow attachment metadata"
      } else {
        "crc mismatch"
      }
    ));
    drop(handle);
    worker.await.unwrap();
  }
}

#[tokio::test]
async fn successful_upload_releases_the_retained_payload() {
  let directory = tempfile::tempdir().unwrap();
  let runtime = ConfigLoader::new(directory.path());
  let attachment_store = AttachmentStoreHandle::new(directory.path().to_owned(), runtime);
  let store = attachment_store.get().await.unwrap();
  let admitted = store
    .admit(
      UploadSource::Bytes(b"attachment".to_vec()),
      Some("application/vnd.example.capture".to_string()),
    )
    .await
    .unwrap();

  let (completion_tx, completion_rx) = oneshot::channel();
  let mut completion_tx = Some(completion_tx);
  let mut mock_client = bd_artifact_upload::MockClient::new();
  mock_client
    .expect_enqueue_workflow_attachment()
    .withf(move |id, _, session, content_type, _, _, _| {
      *id == admitted.id
        && session == "session"
        && content_type.as_deref() == Some("application/vnd.example.capture")
    })
    .once()
    .returning(move |_, _, _, _, persisted, completion, _| {
      persisted.unwrap().send(Ok(())).unwrap();
      completion_tx
        .take()
        .unwrap()
        .send(completion.unwrap())
        .unwrap();
      Ok(())
    });

  let (upload_completed_tx, upload_completed_rx) = oneshot::channel();
  let (handle, worker) = WorkflowAttachmentUploadHandle::new_with_attachment_store_and_test_hooks(
    Arc::new(mock_client),
    attachment_store,
    Some(Arc::new(UploadCompletionHook {
      completion_tx: Mutex::new(Some(upload_completed_tx)),
    })),
  );
  let worker = tokio::spawn(worker.run());
  handle
    .stage(HashMap::from([(admitted.id, "session".to_string())]))
    .await
    .unwrap();
  completion_rx.await.unwrap().send(Ok(())).unwrap();

  assert_eq!(upload_completed_rx.await.unwrap(), admitted.id);
  assert!(store.is_uploaded(admitted.id).await.unwrap());
  assert!(
    !tokio::fs::try_exists(
      directory
        .path()
        .join("workflow-attachments")
        .join(format!("{}.payload", admitted.id)),
    )
    .await
    .unwrap()
  );
  drop(handle);
  worker.await.unwrap();
}
