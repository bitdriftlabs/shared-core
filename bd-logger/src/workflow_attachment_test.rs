// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::AttachmentStore;
use bd_artifact_upload::UploadSource;
use bd_proto::protos::client::api::RuntimeUpdate;
use bd_proto::protos::client::runtime::Runtime;
use bd_proto::protos::client::runtime::runtime::Value;
use bd_proto::protos::client::runtime::runtime::value::Type;
use bd_runtime::runtime::workflow_attachment::{MaxAttachmentBytes, MaxOwnedBytes, MaxOwnedFiles};
use bd_runtime::runtime::{ConfigLoader, FeatureFlag};
use flate2::read::ZlibDecoder;
use std::io::{self, Read};
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::fs;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use uuid::Uuid;

const MIN_READ_LIMIT_BYTES: u64 = 32 * 1024 * 1024;

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
          (MaxAttachmentBytes::path().to_string(), integer(3)),
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

  assert!(store.admit(UploadSource::Bytes(vec![0; 4])).await.is_err());
  let first = store.admit(UploadSource::Bytes(vec![0; 3])).await.unwrap();
  let second = store.admit(UploadSource::Bytes(vec![0; 1])).await.unwrap();
  assert!(store.admit(UploadSource::Bytes(vec![0; 1])).await.is_err());
  runtime
    .update_snapshot(RuntimeUpdate {
      version_nonce: "smaller limit".to_string(),
      runtime: Some(Runtime {
        values: [(MaxAttachmentBytes::path().to_string(), integer(1))].into(),
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
    .admit(UploadSource::Bytes(b"first".to_vec()))
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
    .admit(UploadSource::Path(input.clone()))
    .await
    .unwrap();
  assert_eq!(
    read_checked(&restarted, second.id).await.unwrap(),
    b"second"
  );
  assert!(fs::try_exists(&input).await.unwrap());
  assert_eq!(restarted.capacity.lock().files, 2);
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
    store.admit(UploadSource::Bytes(vec![1; 3])),
    store.admit(UploadSource::Bytes(vec![2; 3]))
  );
  assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
  assert!(store.capacity.lock().bytes <= 15);
  assert_eq!(new_store(&directory).await.capacity.lock().files, 1);
}

#[tokio::test]
async fn rejects_oversized_and_nonregular_sources_without_publishing() {
  let directory = tempfile::tempdir().unwrap();
  let store = Arc::new(new_store(&directory).await);
  let max_bytes = usize::try_from(*store.max_attachment_bytes.read()).unwrap();
  assert!(
    store
      .admit(UploadSource::Bytes(vec![0; max_bytes + 1]))
      .await
      .is_err()
  );
  assert!(
    store
      .admit(UploadSource::Path(directory.path().to_owned()))
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
        .admit(UploadSource::Path(directory.path().join("link")))
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
  let admitted = store.admit(UploadSource::Bytes(vec![1; 64])).await.unwrap();
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
async fn uploaded_attachments_keep_a_timestamp_marker_without_the_payload() {
  let directory = tempfile::tempdir().unwrap();
  let store = Arc::new(new_store(&directory).await);
  let admitted = store
    .admit(UploadSource::Bytes(b"owned".to_vec()))
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
    fs::try_exists(store.timestamp_path(admitted.id))
      .await
      .unwrap()
  );
  assert_eq!(store.capacity.lock().files, 0);
}

#[tokio::test]
async fn timestamp_cleanup_retires_only_strictly_older_attachments() {
  let directory = tempfile::tempdir().unwrap();
  let store = Arc::new(new_store(&directory).await);
  let old = store
    .admit(UploadSource::Bytes(b"old".to_vec()))
    .await
    .unwrap();
  let retained = store
    .admit(UploadSource::Bytes(b"retained".to_vec()))
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
  assert!(!fs::try_exists(store.timestamp_path(old.id)).await.unwrap());
  assert!(
    fs::try_exists(store.payload_path(retained.id))
      .await
      .unwrap()
  );
  assert!(
    fs::try_exists(store.timestamp_path(retained.id))
      .await
      .unwrap()
  );
}

#[tokio::test]
async fn cleanup_all_retires_timestamped_attachments() {
  let directory = tempfile::tempdir().unwrap();
  let store = Arc::new(new_store(&directory).await);
  let admitted = store
    .admit(UploadSource::Bytes(b"attachment".to_vec()))
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
    !fs::try_exists(store.timestamp_path(admitted.id))
      .await
      .unwrap()
  );
}

#[tokio::test]
async fn rejects_tampered_payload_after_restart() {
  let directory = tempfile::tempdir().unwrap();
  let store = Arc::new(new_store(&directory).await);
  let admitted = store
    .admit(UploadSource::Bytes(b"original".to_vec()))
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
    .admit(UploadSource::Bytes(b"owned".to_vec()))
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
  let admitted = store.admit(UploadSource::Bytes(vec![1])).await.unwrap();
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
async fn restart_finalizes_interrupted_sidecar_writes() {
  let directory = tempfile::tempdir().unwrap();
  let store = new_store(&directory).await;
  let timestamp_id = Uuid::new_v4();
  let timestamp_staging_path =
    super::sidecar_staging_path(&store.timestamp_path(timestamp_id)).unwrap();
  fs::write(&timestamp_staging_path, b"100").await.unwrap();
  let uploaded_id = Uuid::new_v4();
  let uploaded_staging_path =
    super::sidecar_staging_path(&store.uploaded_path(uploaded_id)).unwrap();
  fs::write(&uploaded_staging_path, b"uploaded")
    .await
    .unwrap();

  let restarted = new_store(&directory).await;

  assert_eq!(
    fs::read_to_string(restarted.timestamp_path(timestamp_id))
      .await
      .unwrap(),
    "100"
  );
  assert!(restarted.is_uploaded(uploaded_id).await.unwrap());
  assert!(!fs::try_exists(timestamp_staging_path).await.unwrap());
  assert!(!fs::try_exists(uploaded_staging_path).await.unwrap());
}
