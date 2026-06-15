// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{
  PersistedTriggerUploadArtifactBatch,
  TRIGGER_UPLOAD_ARTIFACTS_DIRECTORY,
  TriggerUploadArtifactStore,
};
use std::io::ErrorKind;
use std::path::PathBuf;
use tempfile::TempDir;

fn logger_state_directory(temp_dir: &TempDir) -> PathBuf {
  temp_dir.path().join("state").join("logger")
}

fn artifact_directory(temp_dir: &TempDir) -> PathBuf {
  logger_state_directory(temp_dir).join(TRIGGER_UPLOAD_ARTIFACTS_DIRECTORY)
}

fn make_store(temp_dir: &TempDir) -> TriggerUploadArtifactStore {
  TriggerUploadArtifactStore::new(
    logger_state_directory(temp_dir),
    "trigger-upload",
    "buffer-a",
  )
}

fn artifact_directory_is_empty(temp_dir: &TempDir) -> bool {
  match std::fs::read_dir(artifact_directory(temp_dir)) {
    Ok(mut entries) => entries.next().is_none(),
    Err(error) if error.kind() == ErrorKind::NotFound => true,
    Err(error) => panic!("failed to inspect artifact directory: {error}"),
  }
}

fn persisted_batch(logs: &[&[u8]], upload_uuid: &str) -> PersistedTriggerUploadArtifactBatch {
  PersistedTriggerUploadArtifactBatch {
    upload_uuid: upload_uuid.to_string(),
    logs: logs.iter().map(|log| log.to_vec()).collect(),
  }
}

#[tokio::test]
async fn stage_batch_round_trips_queued_batch_from_disk() {
  let temp_dir = TempDir::with_prefix("trigger-upload-artifact").unwrap();
  let store = make_store(&temp_dir);

  let staged = store
    .stage_batch(vec![b"one".to_vec(), b"two".to_vec()])
    .await
    .unwrap();

  assert_eq!(
    store.queued_batch().await.unwrap(),
    Some(PersistedTriggerUploadArtifactBatch {
      upload_uuid: staged.upload_uuid.clone(),
      logs: vec![b"one".to_vec(), b"two".to_vec()],
    })
  );
  assert_eq!(
    make_store(&temp_dir).queued_batch().await.unwrap(),
    Some(staged)
  );
}

#[tokio::test]
async fn promote_moves_queued_batch_to_inflight_and_preserves_uuid() {
  let temp_dir = TempDir::with_prefix("trigger-upload-artifact").unwrap();
  let store = make_store(&temp_dir);

  let staged = store.stage_batch(vec![b"one".to_vec()]).await.unwrap();
  let promoted = store
    .promote_queued_batch_to_inflight()
    .await
    .unwrap()
    .unwrap();

  assert_eq!(promoted, staged);
  assert_eq!(store.queued_batch().await.unwrap(), None);
  assert_eq!(make_store(&temp_dir).queued_batch().await.unwrap(), None);
  assert_eq!(
    make_store(&temp_dir).inflight_batch().await.unwrap(),
    Some(staged)
  );
}

#[tokio::test]
async fn promote_without_queued_batch_is_noop() {
  let temp_dir = TempDir::with_prefix("trigger-upload-artifact").unwrap();
  let store = make_store(&temp_dir);

  assert_eq!(
    store.promote_queued_batch_to_inflight().await.unwrap(),
    None
  );
  assert_eq!(store.queued_batch().await.unwrap(), None);
  assert_eq!(store.inflight_batch().await.unwrap(), None);
  assert!(artifact_directory_is_empty(&temp_dir));
}

#[tokio::test]
async fn stage_batch_overwrites_queued_batch_and_preserves_inflight_batch() {
  let temp_dir = TempDir::with_prefix("trigger-upload-artifact").unwrap();
  let store = make_store(&temp_dir);

  let inflight = store.stage_batch(vec![b"one".to_vec()]).await.unwrap();
  store
    .promote_queued_batch_to_inflight()
    .await
    .unwrap()
    .unwrap();

  let queued = store
    .stage_batch(vec![b"two".to_vec(), b"three".to_vec()])
    .await
    .unwrap();

  assert_eq!(
    make_store(&temp_dir).inflight_batch().await.unwrap(),
    Some(inflight)
  );
  assert_eq!(
    make_store(&temp_dir).queued_batch().await.unwrap(),
    Some(queued)
  );
}

#[tokio::test]
async fn remove_queued_batch_deletes_snapshot_when_no_inflight_batch_remains() {
  let temp_dir = TempDir::with_prefix("trigger-upload-artifact").unwrap();
  let store = make_store(&temp_dir);

  store.stage_batch(vec![b"one".to_vec()]).await.unwrap();
  assert!(!artifact_directory_is_empty(&temp_dir));

  store.remove_queued_batch().await.unwrap();

  assert_eq!(make_store(&temp_dir).queued_batch().await.unwrap(), None);
  assert_eq!(make_store(&temp_dir).inflight_batch().await.unwrap(), None);
  assert!(artifact_directory_is_empty(&temp_dir));
}

#[tokio::test]
async fn clear_inflight_batch_deletes_snapshot_when_no_queued_batch_remains() {
  let temp_dir = TempDir::with_prefix("trigger-upload-artifact").unwrap();
  let store = make_store(&temp_dir);

  let staged = store.stage_batch(vec![b"one".to_vec()]).await.unwrap();
  assert_eq!(
    store.promote_queued_batch_to_inflight().await.unwrap(),
    Some(persisted_batch(&[b"one"], &staged.upload_uuid))
  );
  assert!(!artifact_directory_is_empty(&temp_dir));

  store.clear_inflight_batch().await.unwrap();

  assert_eq!(make_store(&temp_dir).queued_batch().await.unwrap(), None);
  assert_eq!(make_store(&temp_dir).inflight_batch().await.unwrap(), None);
  assert!(artifact_directory_is_empty(&temp_dir));
}
