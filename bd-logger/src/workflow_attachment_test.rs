// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{AttachmentStore, AttachmentStoreHandle, WorkflowAttachmentCleanupWorker};
use crate::workflow_attachment_upload::WorkflowAttachmentUploadHandle;
use bd_artifact_upload::{MockClient, UploadSource};
use bd_buffer::Buffer as RingBuffer;
use bd_buffer::buffer::NonVolatileFileHeader;
use bd_client_common::file::{read_checksummed_data, write_checksummed_data};
use bd_client_stats_store::Collector;
use bd_log_primitives::{EncodableLog, Log, log_level};
use bd_proto::protos::client::api::RuntimeUpdate;
use bd_proto::protos::client::artifact::WorkflowAttachmentState;
use bd_proto::protos::client::runtime::Runtime;
use bd_proto::protos::client::runtime::runtime::Value;
use bd_proto::protos::client::runtime::runtime::value::Type;
use bd_proto::protos::logging::payload::LogType;
use bd_runtime::runtime::attachment::MaxBytes;
use bd_runtime::runtime::workflow_attachment::{MaxOwnedBytes, MaxOwnedFiles};
use bd_runtime::runtime::{ConfigLoader, FeatureFlag};
use bd_state::{RetentionHandle, RetentionRegistry};
use flate2::read::ZlibDecoder;
use protobuf::Message;
use std::collections::HashMap;
use std::io::{self, Read};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use time::OffsetDateTime;
use tokio::fs;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use uuid::Uuid;

const MIN_READ_LIMIT_BYTES: u64 = 32 * 1024 * 1024;

impl AttachmentStore {
  pub(crate) async fn content_type(&self, id: Uuid) -> io::Result<Option<String>> {
    Ok(
      self
        .metadata(id)
        .await?
        .and_then(|metadata| metadata.content_type),
    )
  }

  pub(crate) async fn is_uploaded(&self, id: Uuid) -> io::Result<bool> {
    Ok(
      self
        .metadata(id)
        .await?
        .is_some_and(|metadata| metadata.uploaded),
    )
  }
}

fn make_log_bytes(timestamp: OffsetDateTime) -> Vec<u8> {
  let mut log = EncodableLog::new(
    Log {
      log_level: log_level::INFO,
      log_type: LogType::NORMAL,
      message: "workflow outcome".into(),
      fields: [].into(),
      matching_fields: [].into(),
      session_id: String::new().into(),
      occurred_at: timestamp,
      capture_session: None,
    },
    u64::MAX,
  );
  let size = usize::try_from(log.compute_size(&[], &[]).unwrap()).unwrap();
  let mut bytes = vec![0; size];
  log.serialize_to_bytes(&[], &[], &mut bytes).unwrap();
  bytes
}

async fn read_checked(store: &AttachmentStore, id: Uuid) -> io::Result<Vec<u8>> {
  let path = store.payload_path(id);
  let mut file = super::open_regular_file_async(&path).await?;
  let metadata = file.metadata().await?;
  let read_limit = u64::from(*store.max_owned_bytes.read()).max(MIN_READ_LIMIT_BYTES);
  if metadata.len() > read_limit {
    return Err(io::Error::new(
      io::ErrorKind::InvalidData,
      "attachment exceeds size limit",
    ));
  }
  let mut compressed =
    Vec::with_capacity(usize::try_from(metadata.len()).map_err(io::Error::other)?);
  file.read_to_end(&mut compressed).await?;
  let mut payload = Vec::new();
  let read = ZlibDecoder::new(compressed.as_slice())
    .take(read_limit.saturating_add(1))
    .read_to_end(&mut payload)?;
  if read as u64 > read_limit {
    return Err(io::Error::new(
      io::ErrorKind::InvalidData,
      "attachment exceeds size limit",
    ));
  }
  Ok(payload)
}

async fn new_store(directory: &tempfile::TempDir) -> AttachmentStore {
  let runtime = ConfigLoader::new(directory.path());
  AttachmentStore::new(directory.path(), &runtime)
    .await
    .unwrap()
}

#[tokio::test]
async fn workflow_attachment_uses_one_metadata_file_through_lifecycle() {
  let directory = tempfile::tempdir().unwrap();
  let store = Arc::new(new_store(&directory).await);
  let admitted = store
    .admit(
      UploadSource::Bytes(b"attachment".to_vec()),
      Some("image/jpeg".into()),
    )
    .await
    .unwrap();
  let metadata_path = store.directory.join(format!("{}.metadata", admitted.id));
  assert!(fs::try_exists(&metadata_path).await.unwrap());
  store
    .record_timestamp(
      admitted.id,
      OffsetDateTime::from_unix_timestamp(10).unwrap(),
    )
    .await
    .unwrap();
  let mut entries = fs::read_dir(&store.directory).await.unwrap();
  let mut files = 0;
  while entries.next_entry().await.unwrap().is_some() {
    files += 1;
  }
  assert_eq!(files, 2);
  store.complete_upload(admitted.id).await.unwrap();
  let restarted = Arc::new(new_store(&directory).await);
  assert!(restarted.is_uploaded(admitted.id).await.unwrap());
  assert_eq!(
    restarted
      .content_type(admitted.id)
      .await
      .unwrap()
      .as_deref(),
    Some("image/jpeg")
  );
  let mut entries = fs::read_dir(&restarted.directory).await.unwrap();
  assert_eq!(
    entries.next_entry().await.unwrap().unwrap().path(),
    metadata_path
  );
  assert!(entries.next_entry().await.unwrap().is_none());
  restarted.cleanup_before(11_000_000).await.unwrap();
  restarted
    .record_timestamp(admitted.id, OffsetDateTime::now_utc())
    .await
    .unwrap();
  restarted.complete_upload(admitted.id).await.unwrap();
  assert!(
    fs::read_dir(&restarted.directory)
      .await
      .unwrap()
      .next_entry()
      .await
      .unwrap()
      .is_none()
  );
}

#[tokio::test]
async fn workflow_attachment_timestamp_preserves_completed_upload() {
  let directory = tempfile::tempdir().unwrap();
  let store = Arc::new(new_store(&directory).await);
  let admitted = store
    .admit(
      UploadSource::Bytes(b"attachment".to_vec()),
      Some("image/jpeg".into()),
    )
    .await
    .unwrap();
  store.complete_upload(admitted.id).await.unwrap();
  store
    .record_timestamp(
      admitted.id,
      OffsetDateTime::from_unix_timestamp(10).unwrap(),
    )
    .await
    .unwrap();

  let restarted = new_store(&directory).await;
  let metadata = super::read_metadata(&restarted.metadata_path(admitted.id))
    .await
    .unwrap();
  assert!(metadata.uploaded);
  assert_eq!(metadata.occurred_at_micros, Some(10_000_000));
  assert_eq!(metadata.content_type.as_deref(), Some("image/jpeg"));
  restarted.cleanup_before(10_000_000).await.unwrap();
  assert!(restarted.is_uploaded(admitted.id).await.unwrap());
  restarted.cleanup_before(11_000_000).await.unwrap();
  assert!(!restarted.is_uploaded(admitted.id).await.unwrap());
}

#[tokio::test]
async fn workflow_attachment_cleanup_preserves_unrecorded_admission() {
  let directory = tempfile::tempdir().unwrap();
  let store = Arc::new(new_store(&directory).await);
  let unrecorded = store
    .admit(UploadSource::Bytes(b"unrecorded".to_vec()), None)
    .await
    .unwrap();
  let recorded = store
    .admit(UploadSource::Bytes(b"recorded".to_vec()), None)
    .await
    .unwrap();
  store
    .record_timestamp(
      recorded.id,
      OffsetDateTime::from_unix_timestamp(10).unwrap(),
    )
    .await
    .unwrap();

  store.cleanup_all().await.unwrap();

  assert_eq!(
    read_checked(&store, unrecorded.id).await.unwrap(),
    b"unrecorded"
  );
  assert!(
    super::read_metadata(&store.metadata_path(unrecorded.id))
      .await
      .unwrap()
      .occurred_at_micros
      .is_none()
  );
  assert!(
    !fs::try_exists(store.metadata_path(recorded.id))
      .await
      .unwrap()
  );
  let restarted = new_store(&directory).await;
  restarted.cleanup_all().await.unwrap();
  assert!(
    !fs::try_exists(restarted.metadata_path(unrecorded.id))
      .await
      .unwrap()
  );
}

#[tokio::test]
async fn workflow_attachment_content_type_checks_are_minimal() {
  let directory = tempfile::tempdir().unwrap();
  let store = Arc::new(new_store(&directory).await);
  for content_type in [
    "a".repeat(256),
    "image/jpeg\r\n".into(),
    "text/\0plain".into(),
    "text/\u{7f}".into(),
  ] {
    let error = store
      .admit(
        UploadSource::Bytes(b"attachment".to_vec()),
        Some(content_type),
      )
      .await
      .err()
      .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(
      fs::read_dir(&store.directory)
        .await
        .unwrap()
        .next_entry()
        .await
        .unwrap()
        .is_none()
    );
  }
  for content_type in [
    None,
    Some(String::new()),
    Some("a".repeat(255)),
    Some("*/*".into()),
    Some("not-a-mime".into()),
  ] {
    let admitted = store
      .admit(
        UploadSource::Bytes(b"attachment".to_vec()),
        content_type.clone(),
      )
      .await
      .unwrap();
    assert_eq!(store.content_type(admitted.id).await.unwrap(), content_type);
    store.release(admitted.id).await.unwrap();
  }
}

#[tokio::test]
async fn workflow_attachment_staging_does_not_acquire_admission_permit() {
  let directory = tempfile::tempdir().unwrap();
  let attachment_store = AttachmentStoreHandle::new(
    directory.path().to_owned(),
    ConfigLoader::new(directory.path()),
  );
  let store = attachment_store.get().await.unwrap();
  let admitted = store
    .admit(
      UploadSource::Bytes(b"attachment".to_vec()),
      Some("image/jpeg".into()),
    )
    .await
    .unwrap();
  let permit = store.admissions.acquire().await.unwrap();
  store.admissions.close();
  let mut client = MockClient::new();
  client
    .expect_enqueue_workflow_attachment()
    .withf(move |id, _, _, content_type, _, _| {
      *id == admitted.id && content_type.as_deref() == Some("image/jpeg")
    })
    .once()
    .returning(|_, _, _, _, persisted, _| {
      persisted.unwrap().send(Ok(())).unwrap();
      Ok(())
    });
  let (handle, worker) = WorkflowAttachmentUploadHandle::new_with_attachment_store_and_test_hooks(
    Arc::new(client),
    attachment_store,
    None,
  );
  let worker = tokio::spawn(worker.run());
  assert!(
    handle
      .stage(HashMap::from([(admitted.id, "session".to_string())]))
      .await
      .unwrap()
      .is_empty()
  );
  drop(permit);
  drop(handle);
  worker.await.unwrap();
}

#[tokio::test]
async fn workflow_attachment_mime_survives_restart_and_release() {
  let directory = tempfile::tempdir().unwrap();
  let store = Arc::new(new_store(&directory).await);
  let admitted = store
    .admit(
      UploadSource::Bytes(b"attachment".to_vec()),
      Some("application/vnd.example.capture".to_string()),
    )
    .await
    .unwrap();
  assert!(
    fs::try_exists(store.metadata_path(admitted.id))
      .await
      .unwrap()
  );
  let restarted = Arc::new(new_store(&directory).await);
  assert_eq!(
    restarted
      .content_type(admitted.id)
      .await
      .unwrap()
      .as_deref(),
    Some("application/vnd.example.capture")
  );
  restarted.release(admitted.id).await.unwrap();
  assert!(
    !fs::try_exists(restarted.metadata_path(admitted.id))
      .await
      .unwrap()
  );
}

#[tokio::test]
async fn workflow_attachment_mime_survives_upload_until_retirement() {
  let directory = tempfile::tempdir().unwrap();
  let store = Arc::new(new_store(&directory).await);
  let admitted = store
    .admit(
      UploadSource::Bytes(b"attachment".to_vec()),
      Some("image/jpeg".into()),
    )
    .await
    .unwrap();
  store
    .record_timestamp(
      admitted.id,
      OffsetDateTime::from_unix_timestamp(10).unwrap(),
    )
    .await
    .unwrap();
  assert!(
    fs::try_exists(store.metadata_path(admitted.id))
      .await
      .unwrap()
  );
  assert_eq!(
    store.content_type(admitted.id).await.unwrap().as_deref(),
    Some("image/jpeg")
  );
  store.complete_upload(admitted.id).await.unwrap();
  store.complete_upload(admitted.id).await.unwrap();
  assert!(
    fs::try_exists(store.metadata_path(admitted.id))
      .await
      .unwrap()
  );
  let restarted = Arc::new(new_store(&directory).await);
  assert_eq!(
    restarted
      .content_type(admitted.id)
      .await
      .unwrap()
      .as_deref(),
    Some("image/jpeg")
  );
  restarted.cleanup_before(11_000_000).await.unwrap();
  assert!(
    !fs::try_exists(restarted.metadata_path(admitted.id))
      .await
      .unwrap()
  );
}

#[tokio::test]
async fn workflow_attachment_rejects_malformed_or_oversized_metadata() {
  for uploaded in [false, true] {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(new_store(&directory).await);
    let admitted = store
      .admit(UploadSource::Bytes(b"attachment".to_vec()), None)
      .await
      .unwrap();
    if uploaded {
      store.complete_upload(admitted.id).await.unwrap();
    }
    let path = store.metadata_path(admitted.id);
    fs::write(&path, write_checksummed_data(&[0xff]))
      .await
      .unwrap();
    assert!(store.content_type(admitted.id).await.is_err());
    fs::write(&path, write_checksummed_data(&vec![0; 1025]))
      .await
      .unwrap();
    assert!(store.content_type(admitted.id).await.is_err());
  }
}

#[tokio::test]
async fn workflow_attachment_rejects_corrupted_lifecycle_markers() {
  for uploaded in [false, true] {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(new_store(&directory).await);
    let admitted = store
      .admit(
        UploadSource::Bytes(b"attachment".to_vec()),
        Some("image/jpeg".to_string()),
      )
      .await
      .unwrap();
    if uploaded {
      store.complete_upload(admitted.id).await.unwrap();
    }
    let path = store.metadata_path(admitted.id);
    let mut bytes = fs::read(&path).await.unwrap();
    let metadata =
      WorkflowAttachmentState::parse_from_bytes(&read_checksummed_data(&bytes).unwrap()).unwrap();
    assert_eq!(metadata.content_type.as_deref(), Some("image/jpeg"));
    bytes[0] ^= 1;
    fs::write(&path, bytes).await.unwrap();
    assert_eq!(
      store
        .content_type(admitted.id)
        .await
        .unwrap_err()
        .to_string(),
      "crc mismatch"
    );
    assert!(store.complete_upload(admitted.id).await.is_err());
    assert!(store.is_uploaded(admitted.id).await.is_err());
    let restarted = new_store(&directory).await;
    assert!(!fs::try_exists(&path).await.unwrap());
    assert!(
      !fs::try_exists(restarted.payload_path(admitted.id))
        .await
        .unwrap()
    );
    assert_eq!(restarted.capacity.lock().files, 0);
    assert_eq!(restarted.capacity.lock().bytes, 0);
  }
}

#[tokio::test]
async fn workflow_attachment_restart_isolates_corrupt_metadata() {
  for corrupt_bytes in [
    vec![1; 4],
    write_checksummed_data(&[0xff]),
    write_checksummed_data(&vec![0; 1025]),
  ] {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(new_store(&directory).await);
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
    store
      .record_timestamp(healthy.id, OffsetDateTime::from_unix_timestamp(10).unwrap())
      .await
      .unwrap();
    fs::write(store.metadata_path(corrupt.id), corrupt_bytes)
      .await
      .unwrap();

    let restarted = Arc::new(new_store(&directory).await);
    assert_eq!(
      read_checked(&restarted, healthy.id).await.unwrap(),
      b"healthy"
    );
    assert_eq!(
      restarted.content_type(healthy.id).await.unwrap().as_deref(),
      Some("image/jpeg")
    );
    assert!(
      !fs::try_exists(restarted.metadata_path(corrupt.id))
        .await
        .unwrap()
    );
    assert!(
      !fs::try_exists(restarted.payload_path(corrupt.id))
        .await
        .unwrap()
    );
    assert_eq!(restarted.capacity.lock().files, 1);
    let payload_bytes = fs::metadata(restarted.payload_path(healthy.id))
      .await
      .unwrap()
      .len();
    assert_eq!(restarted.capacity.lock().bytes, payload_bytes);
    let admitted = restarted
      .admit(UploadSource::Bytes(b"new".to_vec()), None)
      .await
      .unwrap();
    restarted.cleanup_all().await.unwrap();
    assert!(
      !fs::try_exists(restarted.payload_path(healthy.id))
        .await
        .unwrap()
    );
    assert_eq!(read_checked(&restarted, admitted.id).await.unwrap(), b"new");
    assert_eq!(restarted.capacity.lock().files, 1);
  }
}

#[tokio::test]
async fn workflow_attachment_cleanup_isolates_corrupted_retention_timestamp() {
  let directory = tempfile::tempdir().unwrap();
  let store = Arc::new(new_store(&directory).await);
  let admitted = store
    .admit(UploadSource::Bytes(b"attachment".to_vec()), None)
    .await
    .unwrap();
  store
    .record_timestamp(
      admitted.id,
      OffsetDateTime::from_unix_timestamp(10).unwrap(),
    )
    .await
    .unwrap();
  let path = store.metadata_path(admitted.id);
  let mut bytes = fs::read(&path).await.unwrap();
  assert_eq!(
    WorkflowAttachmentState::parse_from_bytes(&read_checksummed_data(&bytes).unwrap())
      .unwrap()
      .occurred_at_micros,
    Some(10_000_000)
  );
  bytes[0] ^= 1;
  fs::write(&path, bytes).await.unwrap();
  assert_eq!(
    super::read_metadata(&path).await.unwrap_err().to_string(),
    "crc mismatch"
  );
  let expired = store
    .admit(UploadSource::Bytes(b"expired".to_vec()), None)
    .await
    .unwrap();
  let retained = store
    .admit(UploadSource::Bytes(b"retained".to_vec()), None)
    .await
    .unwrap();
  store
    .record_timestamp(expired.id, OffsetDateTime::from_unix_timestamp(20).unwrap())
    .await
    .unwrap();
  store
    .record_timestamp(
      retained.id,
      OffsetDateTime::from_unix_timestamp(30).unwrap(),
    )
    .await
    .unwrap();
  store.cleanup_before(25_000_000).await.unwrap();
  assert!(!fs::try_exists(&path).await.unwrap());
  assert!(
    !fs::try_exists(store.payload_path(admitted.id))
      .await
      .unwrap()
  );
  assert!(
    !fs::try_exists(store.payload_path(expired.id))
      .await
      .unwrap()
  );
  assert_eq!(
    read_checked(&store, retained.id).await.unwrap(),
    b"retained"
  );
  assert_eq!(store.capacity.lock().files, 1);
  let payload_bytes = fs::metadata(store.payload_path(retained.id))
    .await
    .unwrap()
    .len();
  assert_eq!(store.capacity.lock().bytes, payload_bytes);
  store.cleanup_all().await.unwrap();
  assert_eq!(store.capacity.lock().files, 0);
  assert_eq!(store.capacity.lock().bytes, 0);
  assert_eq!(new_store(&directory).await.capacity.lock().files, 0);
}

#[tokio::test]
async fn workflow_attachment_restart_reclaims_unpublished_metadata() {
  let directory = tempfile::tempdir().unwrap();
  let store = Arc::new(new_store(&directory).await);
  let admitted = store
    .admit(
      UploadSource::Bytes(b"attachment".to_vec()),
      Some("image/jpeg".to_string()),
    )
    .await
    .unwrap();
  let marker_path = store.metadata_path(admitted.id);
  let staging_path = super::sidecar_staging_path(&marker_path).unwrap();
  fs::rename(&marker_path, &staging_path).await.unwrap();
  fs::remove_file(store.payload_path(admitted.id))
    .await
    .unwrap();

  let restarted = new_store(&directory).await;

  assert!(!fs::try_exists(marker_path).await.unwrap());
  assert!(!fs::try_exists(staging_path).await.unwrap());
  assert_eq!(restarted.content_type(admitted.id).await.unwrap(), None);
  assert_eq!(restarted.capacity.lock().files, 0);
}

async fn new_cleanup_worker(
  directory: &tempfile::TempDir,
  retention_micros: u64,
) -> (
  AttachmentStoreHandle,
  RetentionHandle,
  WorkflowAttachmentCleanupWorker,
) {
  let runtime = ConfigLoader::new(directory.path());
  let store_handle = AttachmentStoreHandle::new(directory.path().to_owned(), runtime);
  let retention_registry = Arc::new(RetentionRegistry::new(
    bd_runtime::runtime::IntWatch::new_for_testing(0),
  ));
  let retention_handle = retention_registry.create_handle().await;
  retention_handle.update_retention_micros(retention_micros);
  let worker = WorkflowAttachmentCleanupWorker::new(
    store_handle.clone(),
    retention_registry,
    Arc::new(AtomicBool::new(true)),
  );
  (store_handle, retention_handle, worker)
}

#[tokio::test]
async fn runtime_limits_apply_to_existing_store() {
  let directory = tempfile::tempdir().unwrap();
  let runtime = ConfigLoader::new(directory.path());
  let store = Arc::new(
    AttachmentStore::new(directory.path(), &runtime)
      .await
      .unwrap(),
  );
  let integer = |value| Value {
    type_: Some(Type::UintValue(value)),
    ..Default::default()
  };
  runtime
    .update_snapshot(RuntimeUpdate {
      version_nonce: "limits".to_string(),
      runtime: Some(Runtime {
        values: [
          (MaxBytes::path().to_string(), integer(3)),
          (MaxOwnedBytes::path().to_string(), integer(64)),
          (MaxOwnedFiles::path().to_string(), integer(2)),
        ]
        .into(),
        ..Default::default()
      })
      .into(),
      ..Default::default()
    })
    .await
    .unwrap();

  assert!(
    store
      .admit(UploadSource::Bytes(vec![0; 4]), None)
      .await
      .is_err()
  );
  let first = store
    .admit(UploadSource::Bytes(vec![0; 3]), None)
    .await
    .unwrap();
  let second = store
    .admit(UploadSource::Bytes(vec![0; 1]), None)
    .await
    .unwrap();
  assert!(
    store
      .admit(UploadSource::Bytes(vec![0; 1]), None)
      .await
      .is_err()
  );
  runtime
    .update_snapshot(RuntimeUpdate {
      version_nonce: "smaller limit".to_string(),
      runtime: Some(Runtime {
        values: [(MaxBytes::path().to_string(), integer(1))].into(),
        ..Default::default()
      })
      .into(),
      ..Default::default()
    })
    .await
    .unwrap();
  assert_eq!(read_checked(&store, first.id).await.unwrap(), vec![0; 3]);
  assert_eq!(read_checked(&store, second.id).await.unwrap(), vec![0]);
  store.release(first.id).await.unwrap();
  store.release(second.id).await.unwrap();
}

#[tokio::test]
async fn admits_bytes_and_path_and_counts_owned_files_after_restart() {
  let directory = tempfile::tempdir().unwrap();
  let store = Arc::new(new_store(&directory).await);
  let admitted = store
    .admit(UploadSource::Bytes(b"first".to_vec()), None)
    .await
    .unwrap();
  assert_eq!(
    store
      .payload_path(admitted.id)
      .file_stem()
      .unwrap()
      .to_str()
      .unwrap(),
    admitted.id.to_string()
  );
  assert_eq!(read_checked(&store, admitted.id).await.unwrap(), b"first");
  let stored_bytes = store.capacity.lock().bytes;
  assert_eq!(
    stored_bytes,
    fs::metadata(store.payload_path(admitted.id))
      .await
      .unwrap()
      .len()
  );

  let input = directory.path().join("input");
  fs::write(&input, b"second").await.unwrap();
  let restarted = Arc::new(new_store(&directory).await);
  let second = restarted
    .admit(UploadSource::Path(input.clone()), None)
    .await
    .unwrap();
  assert_eq!(
    read_checked(&restarted, second.id).await.unwrap(),
    b"second"
  );
  assert!(fs::try_exists(&input).await.unwrap());

  let relative_input = directory.path().join("relative-input");
  fs::write(&relative_input, b"third").await.unwrap();
  let third = restarted
    .admit(UploadSource::Path("relative-input".into()), None)
    .await
    .unwrap();
  assert_eq!(read_checked(&restarted, third.id).await.unwrap(), b"third");
  assert_eq!(restarted.capacity.lock().files, 3);
}

#[tokio::test]
async fn concurrent_admissions_respect_owned_byte_limit() {
  let directory = tempfile::tempdir().unwrap();
  let runtime = ConfigLoader::new(directory.path());
  let store = Arc::new(
    AttachmentStore::new(directory.path(), &runtime)
      .await
      .unwrap(),
  );
  runtime
    .update_snapshot(RuntimeUpdate {
      version_nonce: "concurrent limit".to_string(),
      runtime: Some(Runtime {
        values: [(
          MaxOwnedBytes::path().to_string(),
          Value {
            type_: Some(Type::UintValue(15)),
            ..Default::default()
          },
        )]
        .into(),
        ..Default::default()
      })
      .into(),
      ..Default::default()
    })
    .await
    .unwrap();
  let (first, second) = tokio::join!(
    store.admit(UploadSource::Bytes(vec![1; 3]), None),
    store.admit(UploadSource::Bytes(vec![2; 3]), None)
  );
  assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
  assert!(store.capacity.lock().bytes <= 15);
  assert_eq!(new_store(&directory).await.capacity.lock().files, 1);
}

#[tokio::test]
async fn admits_compressible_attachment_that_fits_owned_byte_limit() {
  let directory = tempfile::tempdir().unwrap();
  let runtime = ConfigLoader::new(directory.path());
  let store = Arc::new(
    AttachmentStore::new(directory.path(), &runtime)
      .await
      .unwrap(),
  );
  let integer = |value| Value {
    type_: Some(Type::UintValue(value)),
    ..Default::default()
  };
  runtime
    .update_snapshot(RuntimeUpdate {
      version_nonce: "compressed capacity".to_string(),
      runtime: Some(Runtime {
        values: [
          (MaxBytes::path().to_string(), integer(1024)),
          (MaxOwnedBytes::path().to_string(), integer(64)),
        ]
        .into(),
        ..Default::default()
      })
      .into(),
      ..Default::default()
    })
    .await
    .unwrap();

  let admitted = store
    .admit(UploadSource::Bytes(vec![0; 1024]), None)
    .await
    .unwrap();
  assert_eq!(
    read_checked(&store, admitted.id).await.unwrap(),
    vec![0; 1024]
  );
  assert!(store.capacity.lock().bytes <= 64);
}

#[tokio::test]
async fn rejects_oversized_and_nonregular_sources_without_publishing() {
  let directory = tempfile::tempdir().unwrap();
  let store = Arc::new(new_store(&directory).await);
  let max_bytes = usize::try_from(*store.max_attachment_bytes.read()).unwrap();
  let oversized_file = directory.path().join("oversized-file");
  fs::write(&oversized_file, vec![0; max_bytes + 1])
    .await
    .unwrap();
  assert!(
    store
      .admit(UploadSource::Bytes(vec![0; max_bytes + 1]), None)
      .await
      .is_err()
  );
  assert!(
    store
      .admit(UploadSource::Path(oversized_file), None)
      .await
      .is_err()
  );
  assert!(
    store
      .admit(UploadSource::Path(directory.path().to_owned()), None)
      .await
      .is_err()
  );
  #[cfg(unix)]
  {
    fs::symlink(directory.path(), directory.path().join("link"))
      .await
      .unwrap();
    assert!(
      store
        .admit(UploadSource::Path(directory.path().join("link")), None)
        .await
        .is_err()
    );
  }
  assert_eq!(store.capacity.lock().files, 0);
}

#[tokio::test]
async fn releasing_an_unreferenced_attachment_frees_capacity() {
  let directory = tempfile::tempdir().unwrap();
  let store = Arc::new(new_store(&directory).await);
  let admitted = store
    .admit(UploadSource::Bytes(vec![1; 64]), None)
    .await
    .unwrap();
  store.release(admitted.id).await.unwrap();
  assert!(
    !fs::try_exists(store.payload_path(admitted.id))
      .await
      .unwrap()
  );
  assert_eq!(store.capacity.lock().bytes, 0);
  assert_eq!(store.capacity.lock().files, 0);
}

#[tokio::test]
async fn uploaded_attachments_keep_metadata_without_the_payload() {
  let directory = tempfile::tempdir().unwrap();
  let store = Arc::new(new_store(&directory).await);
  let admitted = store
    .admit(UploadSource::Bytes(b"owned".to_vec()), None)
    .await
    .unwrap();
  store
    .record_timestamp(
      admitted.id,
      OffsetDateTime::from_unix_timestamp(10).unwrap(),
    )
    .await
    .unwrap();
  store.complete_upload(admitted.id).await.unwrap();

  assert!(
    !fs::try_exists(store.payload_path(admitted.id))
      .await
      .unwrap()
  );
  assert!(store.is_uploaded(admitted.id).await.unwrap());
  assert!(
    fs::try_exists(store.metadata_path(admitted.id))
      .await
      .unwrap()
  );
  assert_eq!(store.capacity.lock().files, 0);
}

#[tokio::test]
async fn upload_completion_does_not_recreate_a_retired_marker() {
  let directory = tempfile::tempdir().unwrap();
  let store = Arc::new(new_store(&directory).await);
  let admitted = store
    .admit(UploadSource::Bytes(b"owned".to_vec()), None)
    .await
    .unwrap();
  store
    .record_timestamp(
      admitted.id,
      OffsetDateTime::from_unix_timestamp(10).unwrap(),
    )
    .await
    .unwrap();
  store.complete_upload(admitted.id).await.unwrap();
  store.cleanup_all().await.unwrap();

  store.complete_upload(admitted.id).await.unwrap();

  assert!(!store.is_uploaded(admitted.id).await.unwrap());
}

#[tokio::test]
async fn cleanup_worker_rechecks_when_timestamp_generation_changes() {
  let directory = tempfile::tempdir().unwrap();
  let (store_handle, _retention_handle, mut worker) =
    new_cleanup_worker(&directory, 20_000_000).await;
  let store = store_handle.get().await.unwrap();

  assert!(worker.cleanup_once().await);
  assert!(!worker.cleanup_once().await);

  let attachment = store
    .admit(UploadSource::Bytes(b"expired".to_vec()), None)
    .await
    .unwrap();
  store
    .record_timestamp(
      attachment.id,
      OffsetDateTime::from_unix_timestamp(10).unwrap(),
    )
    .await
    .unwrap();
  store.complete_upload(attachment.id).await.unwrap();

  assert!(worker.cleanup_once().await);
  assert!(!store.is_uploaded(attachment.id).await.unwrap());
  assert!(
    !fs::try_exists(store.metadata_path(attachment.id))
      .await
      .unwrap()
  );
  assert!(!worker.cleanup_once().await);
}

#[tokio::test]
async fn cleanup_worker_rechecks_when_retention_changes() {
  let directory = tempfile::tempdir().unwrap();
  let (store_handle, retention_handle, mut worker) =
    new_cleanup_worker(&directory, 10_000_000).await;
  let store = store_handle.get().await.unwrap();
  let attachment = store
    .admit(UploadSource::Bytes(b"retained".to_vec()), None)
    .await
    .unwrap();
  store
    .record_timestamp(
      attachment.id,
      OffsetDateTime::from_unix_timestamp(20).unwrap(),
    )
    .await
    .unwrap();
  store.complete_upload(attachment.id).await.unwrap();

  assert!(worker.cleanup_once().await);
  assert!(store.is_uploaded(attachment.id).await.unwrap());
  assert!(!worker.cleanup_once().await);

  retention_handle.update_retention_micros(30_000_000);

  assert!(worker.cleanup_once().await);
  assert!(!store.is_uploaded(attachment.id).await.unwrap());
  assert!(!worker.cleanup_once().await);
}

#[tokio::test]
async fn cleanup_worker_keeps_attachment_referenced_by_fresh_trigger_buffer() {
  let directory = tempfile::tempdir().unwrap();
  let runtime = ConfigLoader::new(directory.path());
  let store_handle = AttachmentStoreHandle::new(directory.path().to_owned(), runtime);
  let retention_registry = Arc::new(RetentionRegistry::new(
    bd_runtime::runtime::IntWatch::new_for_testing(0),
  ));
  let retention_handle = retention_registry.create_handle().await;
  retention_handle.update_retention_micros(RetentionHandle::RETENTION_NONE);
  let buffer = RingBuffer::new(
    "trigger",
    10_000,
    directory.path().join("trigger"),
    20_000,
    true,
    Collector::default().scope("test").counter("write"),
    Collector::default().scope("test").counter("write_failure"),
    Collector::default().scope("test").counter("overwrite"),
    Collector::default().scope("test").counter("corruption"),
    Collector::default().scope("test").counter("data_loss"),
    retention_handle,
  )
  .unwrap()
  .0;
  let timestamp = OffsetDateTime::now_utc();
  buffer
    .new_thread_local_producer()
    .unwrap()
    .write(&make_log_bytes(timestamp))
    .unwrap();

  let store = store_handle.get().await.unwrap();
  let attachment = store
    .admit(UploadSource::Bytes(b"attachment".to_vec()), None)
    .await
    .unwrap();
  store
    .record_timestamp(attachment.id, timestamp)
    .await
    .unwrap();
  let mut worker = WorkflowAttachmentCleanupWorker::new(
    store_handle,
    retention_registry,
    Arc::new(AtomicBool::new(true)),
  );

  assert!(worker.cleanup_once().await);
  assert!(
    fs::try_exists(store.payload_path(attachment.id))
      .await
      .unwrap()
  );
}

#[tokio::test]
async fn cleanup_worker_retires_attachment_after_trigger_disk_overwrite() {
  let directory = tempfile::tempdir().unwrap();
  let runtime = ConfigLoader::new(directory.path());
  let store_handle = AttachmentStoreHandle::new(directory.path().to_owned(), runtime);
  let retention_registry = Arc::new(RetentionRegistry::new(
    bd_runtime::runtime::IntWatch::new_for_testing(0),
  ));
  let retention_handle = retention_registry.create_handle().await;
  let timestamps: Vec<_> = (10 .. 14)
    .map(|seconds| OffsetDateTime::from_unix_timestamp(seconds).unwrap())
    .collect();
  let logs: Vec<_> = timestamps.iter().copied().map(make_log_bytes).collect();
  let record_size = u32::try_from(logs[0].len()).unwrap();
  let buffer = RingBuffer::new(
    "trigger",
    (record_size + 4) * 2,
    directory.path().join("trigger"),
    (record_size + 8) * 3 + u32::try_from(std::mem::size_of::<NonVolatileFileHeader>()).unwrap(),
    true,
    Collector::default().scope("test").counter("write"),
    Collector::default().scope("test").counter("write_failure"),
    Collector::default().scope("test").counter("overwrite"),
    Collector::default().scope("test").counter("corruption"),
    Collector::default().scope("test").counter("data_loss"),
    retention_handle,
  )
  .unwrap()
  .0;
  let store = store_handle.get().await.unwrap();
  let mut attachments = Vec::new();
  for timestamp in timestamps.iter().take(2) {
    let attachment = store
      .admit(UploadSource::Bytes(b"attachment".to_vec()), None)
      .await
      .unwrap();
    store
      .record_timestamp(attachment.id, *timestamp)
      .await
      .unwrap();
    attachments.push(attachment);
  }
  let mut worker = WorkflowAttachmentCleanupWorker::new(
    store_handle,
    retention_registry,
    Arc::new(AtomicBool::new(true)),
  );
  let mut producer = buffer.new_thread_local_producer().unwrap();
  for log in &logs[.. 3] {
    producer.write(log).unwrap();
    buffer.flush();
  }
  assert!(worker.cleanup_once().await);
  assert!(
    fs::try_exists(store.payload_path(attachments[0].id))
      .await
      .unwrap()
  );

  producer.write(&logs[3]).unwrap();
  buffer.flush();
  assert!(worker.cleanup_once().await);
  assert!(
    !fs::try_exists(store.payload_path(attachments[0].id))
      .await
      .unwrap()
  );
  assert!(
    fs::try_exists(store.payload_path(attachments[1].id))
      .await
      .unwrap()
  );
}

#[tokio::test]
async fn cleanup_worker_waits_for_buffer_configuration() {
  let directory = tempfile::tempdir().unwrap();
  let runtime = ConfigLoader::new(directory.path());
  let store_handle = AttachmentStoreHandle::new(directory.path().to_owned(), runtime);
  let cleanup_ready = Arc::new(AtomicBool::new(false));
  let retention_registry = Arc::new(RetentionRegistry::new(
    bd_runtime::runtime::IntWatch::new_for_testing(0),
  ));
  let mut worker = WorkflowAttachmentCleanupWorker::new(
    store_handle.clone(),
    retention_registry,
    cleanup_ready.clone(),
  );
  let store = store_handle.get().await.unwrap();
  let attachment = store
    .admit(UploadSource::Bytes(b"recovered".to_vec()), None)
    .await
    .unwrap();
  store
    .record_timestamp(
      attachment.id,
      OffsetDateTime::from_unix_timestamp(10).unwrap(),
    )
    .await
    .unwrap();
  store.complete_upload(attachment.id).await.unwrap();

  assert!(!worker.cleanup_once().await);
  assert!(store.is_uploaded(attachment.id).await.unwrap());

  cleanup_ready.store(true, Ordering::Release);

  assert!(worker.cleanup_once().await);
  assert!(!store.is_uploaded(attachment.id).await.unwrap());
}

#[tokio::test]
async fn cleanup_worker_retires_attachments_without_retention() {
  let directory = tempfile::tempdir().unwrap();
  let runtime = ConfigLoader::new(directory.path());
  let store_handle = AttachmentStoreHandle::new(directory.path().to_owned(), runtime);
  let retention_registry = Arc::new(RetentionRegistry::new(
    bd_runtime::runtime::IntWatch::new_for_testing(0),
  ));
  let mut worker = WorkflowAttachmentCleanupWorker::new(
    store_handle.clone(),
    retention_registry,
    Arc::new(AtomicBool::new(true)),
  );
  let store = store_handle.get().await.unwrap();
  let attachment = store
    .admit(UploadSource::Bytes(b"unretained".to_vec()), None)
    .await
    .unwrap();
  store
    .record_timestamp(
      attachment.id,
      OffsetDateTime::from_unix_timestamp(10).unwrap(),
    )
    .await
    .unwrap();
  store.complete_upload(attachment.id).await.unwrap();

  assert!(worker.cleanup_once().await);
  assert!(!store.is_uploaded(attachment.id).await.unwrap());
  assert!(
    !fs::try_exists(store.metadata_path(attachment.id))
      .await
      .unwrap()
  );
  assert!(!worker.cleanup_once().await);
}

#[tokio::test]
async fn timestamp_cleanup_retires_only_strictly_older_attachments() {
  let directory = tempfile::tempdir().unwrap();
  let store = Arc::new(new_store(&directory).await);
  let old = store
    .admit(UploadSource::Bytes(b"old".to_vec()), None)
    .await
    .unwrap();
  let retained = store
    .admit(UploadSource::Bytes(b"retained".to_vec()), None)
    .await
    .unwrap();
  store
    .record_timestamp(old.id, OffsetDateTime::from_unix_timestamp(10).unwrap())
    .await
    .unwrap();
  store
    .record_timestamp(
      retained.id,
      OffsetDateTime::from_unix_timestamp(20).unwrap(),
    )
    .await
    .unwrap();
  store.complete_upload(old.id).await.unwrap();

  store.cleanup_before(20_000_000).await.unwrap();

  assert!(!store.is_uploaded(old.id).await.unwrap());
  assert!(!fs::try_exists(store.metadata_path(old.id)).await.unwrap());
  assert!(
    fs::try_exists(store.payload_path(retained.id))
      .await
      .unwrap()
  );
  assert!(
    fs::try_exists(store.metadata_path(retained.id))
      .await
      .unwrap()
  );
}

#[tokio::test]
async fn cleanup_all_retires_timestamped_attachments() {
  let directory = tempfile::tempdir().unwrap();
  let store = Arc::new(new_store(&directory).await);
  let admitted = store
    .admit(UploadSource::Bytes(b"attachment".to_vec()), None)
    .await
    .unwrap();
  store
    .record_timestamp(
      admitted.id,
      OffsetDateTime::from_unix_timestamp(10).unwrap(),
    )
    .await
    .unwrap();
  store.complete_upload(admitted.id).await.unwrap();

  store.cleanup_all().await.unwrap();

  assert!(!store.is_uploaded(admitted.id).await.unwrap());
  assert!(
    !fs::try_exists(store.metadata_path(admitted.id))
      .await
      .unwrap()
  );
}

#[tokio::test]
async fn rejects_tampered_payload_after_restart() {
  let directory = tempfile::tempdir().unwrap();
  let store = Arc::new(new_store(&directory).await);
  let admitted = store
    .admit(UploadSource::Bytes(b"original".to_vec()), None)
    .await
    .unwrap();
  let mut file = fs::OpenOptions::new()
    .write(true)
    .open(store.payload_path(admitted.id))
    .await
    .unwrap();
  file.write_all(b"X").await.unwrap();
  let restarted = Arc::new(new_store(&directory).await);
  assert!(read_checked(&restarted, admitted.id).await.is_err());
  let restarted_bytes = restarted.capacity.lock().bytes;
  assert_eq!(
    restarted_bytes,
    fs::metadata(restarted.payload_path(admitted.id))
      .await
      .unwrap()
      .len()
  );
}

#[cfg(unix)]
#[tokio::test]
async fn rejects_replaced_payload_symlink() {
  let directory = tempfile::tempdir().unwrap();
  let store = Arc::new(new_store(&directory).await);
  let admitted = store
    .admit(UploadSource::Bytes(b"owned".to_vec()), None)
    .await
    .unwrap();
  let external = directory.path().join("external");
  fs::rename(store.payload_path(admitted.id), &external)
    .await
    .unwrap();
  fs::symlink(&external, store.payload_path(admitted.id))
    .await
    .unwrap();
  assert!(read_checked(&store, admitted.id).await.is_err());
}

#[tokio::test]
async fn refuses_to_allocate_for_an_oversized_owned_file() {
  let directory = tempfile::tempdir().unwrap();
  let store = Arc::new(new_store(&directory).await);
  let admitted = store
    .admit(UploadSource::Bytes(vec![1]), None)
    .await
    .unwrap();
  fs::OpenOptions::new()
    .write(true)
    .open(store.payload_path(admitted.id))
    .await
    .unwrap()
    .set_len(MIN_READ_LIMIT_BYTES + 1)
    .await
    .unwrap();
  assert!(read_checked(&store, admitted.id).await.is_err());
}

#[tokio::test]
async fn restart_reclaims_interrupted_admissions() {
  let directory = tempfile::tempdir().unwrap();
  let store = new_store(&directory).await;
  let pending = store.directory.join(format!(".{}.partial", Uuid::new_v4()));
  fs::write(&pending, b"interrupted").await.unwrap();
  let restarted = new_store(&directory).await;
  assert!(!fs::try_exists(&pending).await.unwrap());
  assert_eq!(restarted.capacity.lock().files, 0);
}

#[tokio::test]
async fn restart_timestamps_admitted_attachments_without_outcome_metadata() {
  let directory = tempfile::tempdir().unwrap();
  let store = Arc::new(new_store(&directory).await);
  let admitted = store
    .admit(
      UploadSource::Bytes(b"interrupted".to_vec()),
      Some("image/jpeg".to_string()),
    )
    .await
    .unwrap();

  let restarted = new_store(&directory).await;

  let metadata = super::read_metadata(&restarted.metadata_path(admitted.id))
    .await
    .unwrap();
  assert_eq!(
    metadata.occurred_at_micros,
    Some(metadata.admitted_at_micros)
  );
  assert_eq!(metadata.content_type.as_deref(), Some("image/jpeg"));
  assert!(!metadata.uploaded);
}

#[tokio::test]
async fn restart_timestamps_uploaded_attachment_without_outcome_metadata() {
  let directory = tempfile::tempdir().unwrap();
  let store = Arc::new(new_store(&directory).await);
  let admitted = store
    .admit(
      UploadSource::Bytes(b"interrupted".to_vec()),
      Some("image/jpeg".to_string()),
    )
    .await
    .unwrap();
  store.complete_upload(admitted.id).await.unwrap();

  let restarted = new_store(&directory).await;

  assert!(restarted.is_uploaded(admitted.id).await.unwrap());
  assert_eq!(
    restarted
      .content_type(admitted.id)
      .await
      .unwrap()
      .as_deref(),
    Some("image/jpeg")
  );
  let metadata = super::read_metadata(&restarted.metadata_path(admitted.id))
    .await
    .unwrap();
  assert_eq!(
    metadata.occurred_at_micros,
    Some(metadata.admitted_at_micros)
  );

  restarted.cleanup_all().await.unwrap();

  assert!(!restarted.is_uploaded(admitted.id).await.unwrap());
  assert!(
    !fs::try_exists(restarted.metadata_path(admitted.id))
      .await
      .unwrap()
  );
}

#[tokio::test]
async fn restart_finalizes_interrupted_sidecar_writes() {
  let directory = tempfile::tempdir().unwrap();
  let store = new_store(&directory).await;
  let uploaded_id = Uuid::new_v4();
  let uploaded_staging_path =
    super::sidecar_staging_path(&store.metadata_path(uploaded_id)).unwrap();
  fs::write(
    &uploaded_staging_path,
    write_checksummed_data(
      &WorkflowAttachmentState {
        admitted_at_micros: 10_000_000,
        occurred_at_micros: Some(100),
        uploaded: true,
        content_type: Some("image/jpeg".to_string()),
        ..Default::default()
      }
      .write_to_bytes()
      .unwrap(),
    ),
  )
  .await
  .unwrap();

  let restarted = new_store(&directory).await;

  assert_eq!(
    super::read_metadata(&restarted.metadata_path(uploaded_id))
      .await
      .unwrap()
      .occurred_at_micros,
    Some(100)
  );
  assert!(restarted.is_uploaded(uploaded_id).await.unwrap());
  assert_eq!(
    restarted
      .content_type(uploaded_id)
      .await
      .unwrap()
      .as_deref(),
    Some("image/jpeg")
  );
  assert!(!fs::try_exists(uploaded_staging_path).await.unwrap());

  restarted.cleanup_all().await.unwrap();

  assert!(
    !fs::try_exists(restarted.metadata_path(uploaded_id))
      .await
      .unwrap()
  );
}

#[tokio::test]
async fn restart_discards_uncommitted_metadata_updates() {
  let directory = tempfile::tempdir().unwrap();
  let store = Arc::new(new_store(&directory).await);
  let admitted = store
    .admit(
      UploadSource::Bytes(b"attachment".to_vec()),
      Some("image/jpeg".into()),
    )
    .await
    .unwrap();
  store
    .record_timestamp(
      admitted.id,
      OffsetDateTime::from_unix_timestamp(10).unwrap(),
    )
    .await
    .unwrap();
  let path = store.metadata_path(admitted.id);
  let mut metadata = super::read_metadata(&path).await.unwrap();
  metadata.occurred_at_micros = Some(20_000_000);
  metadata.uploaded = true;
  let staging_path = super::sidecar_staging_path(&path).unwrap();
  fs::write(
    &staging_path,
    write_checksummed_data(&metadata.write_to_bytes().unwrap()),
  )
  .await
  .unwrap();

  let restarted = new_store(&directory).await;

  let metadata = super::read_metadata(&path).await.unwrap();
  assert_eq!(metadata.occurred_at_micros, Some(10_000_000));
  assert!(!metadata.uploaded);
  assert_eq!(metadata.content_type.as_deref(), Some("image/jpeg"));
  assert_eq!(
    read_checked(&restarted, admitted.id).await.unwrap(),
    b"attachment"
  );
  assert!(!fs::try_exists(staging_path).await.unwrap());
}

#[tokio::test]
async fn restart_discards_sidecars_with_incomplete_checksums() {
  let directory = tempfile::tempdir().unwrap();
  let store = new_store(&directory).await;
  let path = store.metadata_path(Uuid::new_v4());
  let staging_path = super::sidecar_staging_path(&path).unwrap();
  fs::write(&staging_path, [0xff]).await.unwrap();

  let restarted = new_store(&directory).await;

  assert_eq!(restarted.capacity.lock().files, 0);
  assert!(!fs::try_exists(path).await.unwrap());
  assert!(!fs::try_exists(staging_path).await.unwrap());
}

#[tokio::test]
async fn restart_preserves_staged_metadata_on_io_errors() {
  let directory = tempfile::tempdir().unwrap();
  let store = Arc::new(new_store(&directory).await);
  let admitted = store
    .admit(
      UploadSource::Bytes(b"attachment".to_vec()),
      Some("image/jpeg".into()),
    )
    .await
    .unwrap();
  let path = store.metadata_path(admitted.id);
  let staging_path = super::sidecar_staging_path(&path).unwrap();
  fs::rename(&path, &staging_path).await.unwrap();
  let original = fs::read(&staging_path).await.unwrap();
  for kind in [
    io::ErrorKind::PermissionDenied,
    io::ErrorKind::Interrupted,
    io::ErrorKind::Other,
  ] {
    let error = super::discard_corrupt_sidecar(&staging_path, io::Error::new(kind, "injected"))
      .await
      .unwrap_err();
    assert_eq!(error.kind(), kind);
    assert_eq!(fs::read(&staging_path).await.unwrap(), original);
  }

  let restarted = new_store(&directory).await;
  assert_eq!(
    read_checked(&restarted, admitted.id).await.unwrap(),
    b"attachment"
  );
  assert_eq!(
    restarted
      .content_type(admitted.id)
      .await
      .unwrap()
      .as_deref(),
    Some("image/jpeg")
  );
  assert_eq!(restarted.capacity.lock().files, 1);
  assert!(!fs::try_exists(staging_path).await.unwrap());
}

#[tokio::test]
async fn restart_isolates_nonregular_owned_entries() {
  for extension in ["payload", "metadata", "metadata.partial"] {
    for is_symlink in [
      false,
      #[cfg(unix)]
      true,
    ] {
      let directory = tempfile::tempdir().unwrap();
      let store = Arc::new(new_store(&directory).await);
      let corrupt = store
        .admit(UploadSource::Bytes(b"corrupt".to_vec()), None)
        .await
        .unwrap();
      let healthy = store
        .admit(UploadSource::Bytes(b"healthy".to_vec()), None)
        .await
        .unwrap();
      let path = store
        .directory
        .join(format!("{}.{}", corrupt.id, extension));
      if extension == "metadata.partial" {
        fs::remove_file(store.metadata_path(corrupt.id))
          .await
          .unwrap();
      } else {
        fs::remove_file(&path).await.unwrap();
      }
      let external = directory.path().join("external");
      fs::create_dir(&external).await.unwrap();
      fs::write(external.join("sentinel"), b"external")
        .await
        .unwrap();
      if is_symlink {
        #[cfg(unix)]
        fs::symlink(&external, &path).await.unwrap();
      } else {
        fs::create_dir(&path).await.unwrap();
        fs::write(path.join("child"), b"corrupt").await.unwrap();
        #[cfg(unix)]
        fs::symlink(&external, path.join("link")).await.unwrap();
      }

      let restarted = Arc::new(new_store(&directory).await);
      assert_eq!(
        read_checked(&restarted, healthy.id).await.unwrap(),
        b"healthy"
      );
      assert_eq!(restarted.capacity.lock().files, 1);
      let remaining_bytes = fs::metadata(restarted.payload_path(healthy.id))
        .await
        .unwrap()
        .len();
      assert_eq!(restarted.capacity.lock().bytes, remaining_bytes);
      assert_eq!(
        fs::symlink_metadata(&path).await.unwrap_err().kind(),
        io::ErrorKind::NotFound
      );
      assert!(
        !fs::try_exists(restarted.payload_path(corrupt.id))
          .await
          .unwrap()
      );
      assert!(
        !fs::try_exists(restarted.metadata_path(corrupt.id))
          .await
          .unwrap()
      );
      assert_eq!(
        fs::read(external.join("sentinel")).await.unwrap(),
        b"external"
      );
      let admitted = restarted
        .admit(UploadSource::Bytes(b"new".to_vec()), None)
        .await
        .unwrap();
      assert_eq!(read_checked(&restarted, admitted.id).await.unwrap(), b"new");
    }
  }
}

#[tokio::test]
async fn restart_reclaims_orphan_and_uploaded_payloads() {
  for state in ["orphan", "corrupt_staged", "uploaded"] {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(new_store(&directory).await);
    let discarded = store
      .admit(UploadSource::Bytes(b"discarded".to_vec()), None)
      .await
      .unwrap();
    let healthy = store
      .admit(UploadSource::Bytes(b"healthy".to_vec()), None)
      .await
      .unwrap();
    let path = store.metadata_path(discarded.id);
    let staging_path = super::sidecar_staging_path(&path).unwrap();
    match state {
      "orphan" => fs::remove_file(&path).await.unwrap(),
      "corrupt_staged" => {
        fs::rename(&path, &staging_path).await.unwrap();
        fs::write(&staging_path, [0xff]).await.unwrap();
      },
      "uploaded" => {
        let mut metadata = super::read_metadata(&path).await.unwrap();
        metadata.uploaded = true;
        super::write_metadata(&path, &metadata).await.unwrap();
      },
      _ => panic!("unexpected recovery state: {state}"),
    }

    let restarted = Arc::new(new_store(&directory).await);
    assert_eq!(
      read_checked(&restarted, healthy.id).await.unwrap(),
      b"healthy"
    );
    assert_eq!(restarted.capacity.lock().files, 1);
    let remaining_bytes = fs::metadata(restarted.payload_path(healthy.id))
      .await
      .unwrap()
      .len();
    assert_eq!(restarted.capacity.lock().bytes, remaining_bytes);
    assert!(
      !fs::try_exists(restarted.payload_path(discarded.id))
        .await
        .unwrap()
    );
    assert!(!fs::try_exists(staging_path).await.unwrap());
    assert_eq!(fs::try_exists(&path).await.unwrap(), state == "uploaded");
    assert_eq!(
      restarted.is_uploaded(discarded.id).await.unwrap(),
      state == "uploaded"
    );
    let admitted = restarted
      .admit(UploadSource::Bytes(b"new".to_vec()), None)
      .await
      .unwrap();
    assert_eq!(read_checked(&restarted, admitted.id).await.unwrap(), b"new");
  }
}

#[test]
fn lifecycle_metadata_reserve_covers_maximum_fields() {
  let mut metadata = WorkflowAttachmentState::default();
  let initial_size = metadata.compute_size();
  metadata.occurred_at_micros = Some(u64::MAX);
  metadata.uploaded = true;
  assert_eq!(
    metadata.compute_size() - initial_size,
    super::MAX_LIFECYCLE_METADATA_BYTES
  );
}

#[tokio::test]
async fn restart_discards_invalid_owned_payload_names() {
  for name in ["invalid".to_string(), Uuid::new_v4().simple().to_string()] {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(new_store(&directory).await);
    let healthy = store
      .admit(UploadSource::Bytes(b"healthy".to_vec()), None)
      .await
      .unwrap();
    let path = store.directory.join(format!("{name}.payload"));
    fs::write(&path, b"orphan").await.unwrap();

    let restarted = new_store(&directory).await;
    assert_eq!(
      read_checked(&restarted, healthy.id).await.unwrap(),
      b"healthy"
    );
    assert_eq!(restarted.capacity.lock().files, 1);
    assert!(!fs::try_exists(path).await.unwrap());
  }
}
