// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{
  PendingTriggerUploadsStore,
  PersistedTriggerUpload,
  PersistedTriggerUploadBufferLifecycle,
  PersistedTriggerUploadBufferProgress,
  PersistedTriggerUploadLifecycle,
  PersistedTriggerUploadSource,
  PersistedTriggerUploadStreaming,
};
use tempfile::TempDir;

fn make_store(temp_directory: &TempDir) -> PendingTriggerUploadsStore {
  PendingTriggerUploadsStore::new(temp_directory.path())
}

fn buffer_progress(buffer_id: &str) -> PersistedTriggerUploadBufferProgress {
  PersistedTriggerUploadBufferProgress::new(buffer_id.to_string())
}

#[tokio::test]
async fn upsert_replaces_existing_upload_with_same_id() {
  let temp_directory = TempDir::with_prefix("flush-registry").unwrap();
  let store = make_store(&temp_directory);
  store
    .upsert(PersistedTriggerUpload {
      id: "flush-1".to_string(),
      source: PersistedTriggerUploadSource::RemoteCommand("flush-1".to_string()),
      session_id: "session-a".to_string(),
      buffers: vec![buffer_progress("a")],
      streaming: None,
      lifecycle: PersistedTriggerUploadLifecycle::ReadyToUpload,
    })
    .await;
  store
    .upsert(PersistedTriggerUpload {
      id: "flush-1".to_string(),
      source: PersistedTriggerUploadSource::RemoteCommand("flush-1".to_string()),
      session_id: "session-b".to_string(),
      buffers: vec![buffer_progress("b")],
      streaming: Some(PersistedTriggerUploadStreaming {
        destination_buffer_ids: vec!["stream".to_string()],
        max_logs_count: Some(10),
      }),
      lifecycle: PersistedTriggerUploadLifecycle::UploadingFromBuffer,
    })
    .await;

  assert_eq!(
    make_store(&temp_directory).pending_uploads().await,
    vec![PersistedTriggerUpload {
      id: "flush-1".to_string(),
      source: PersistedTriggerUploadSource::RemoteCommand("flush-1".to_string()),
      session_id: "session-b".to_string(),
      buffers: vec![buffer_progress("b")],
      streaming: Some(PersistedTriggerUploadStreaming {
        destination_buffer_ids: vec!["stream".to_string()],
        max_logs_count: Some(10),
      }),
      lifecycle: PersistedTriggerUploadLifecycle::UploadingFromBuffer,
    }]
  );
}

#[tokio::test]
async fn remove_clears_matching_upload() {
  let temp_directory = TempDir::with_prefix("flush-registry").unwrap();
  let store = make_store(&temp_directory);
  store
    .upsert(PersistedTriggerUpload {
      id: "flush-1".to_string(),
      source: PersistedTriggerUploadSource::WorkflowAction("flush-1".to_string()),
      session_id: "workflow-session".to_string(),
      buffers: vec![buffer_progress("trigger")],
      streaming: None,
      lifecycle: PersistedTriggerUploadLifecycle::ReadyToUpload,
    })
    .await;

  store.remove("flush-1").await;

  assert!(
    make_store(&temp_directory)
      .pending_uploads()
      .await
      .is_empty()
  );
}

#[tokio::test]
async fn mark_uploading_updates_lifecycle_without_replacing_other_fields() {
  let temp_directory = TempDir::with_prefix("flush-registry").unwrap();
  let store = make_store(&temp_directory);
  store
    .upsert(PersistedTriggerUpload {
      id: "flush-1".to_string(),
      source: PersistedTriggerUploadSource::RemoteCommand("flush-1".to_string()),
      session_id: "remote-session".to_string(),
      buffers: vec![buffer_progress("trigger")],
      streaming: Some(PersistedTriggerUploadStreaming {
        destination_buffer_ids: vec!["stream".to_string()],
        max_logs_count: Some(10),
      }),
      lifecycle: PersistedTriggerUploadLifecycle::ReadyToUpload,
    })
    .await;

  store.mark_uploading("flush-1", "trigger").await;

  assert_eq!(
    make_store(&temp_directory).pending_uploads().await,
    vec![PersistedTriggerUpload {
      id: "flush-1".to_string(),
      source: PersistedTriggerUploadSource::RemoteCommand("flush-1".to_string()),
      session_id: "remote-session".to_string(),
      buffers: vec![PersistedTriggerUploadBufferProgress {
        buffer_id: "trigger".to_string(),
        lifecycle: PersistedTriggerUploadBufferLifecycle::UploadingFromBuffer,
        uploaded_batches_count: 0,
        uploaded_logs_count: 0,
      }],
      streaming: Some(PersistedTriggerUploadStreaming {
        destination_buffer_ids: vec!["stream".to_string()],
        max_logs_count: Some(10),
      }),
      lifecycle: PersistedTriggerUploadLifecycle::UploadingFromBuffer,
    }]
  );
}

#[tokio::test]
async fn record_uploaded_chunk_updates_per_buffer_progress() {
  let temp_directory = TempDir::with_prefix("flush-registry").unwrap();
  let store = make_store(&temp_directory);
  store
    .upsert(PersistedTriggerUpload {
      id: "flush-1".to_string(),
      source: PersistedTriggerUploadSource::RemoteCommand("flush-1".to_string()),
      session_id: "remote-session".to_string(),
      buffers: vec![buffer_progress("trigger")],
      streaming: None,
      lifecycle: PersistedTriggerUploadLifecycle::ReadyToUpload,
    })
    .await;

  store.record_uploaded_chunk("flush-1", "trigger", 3).await;

  assert_eq!(
    make_store(&temp_directory).pending_uploads().await,
    vec![PersistedTriggerUpload {
      id: "flush-1".to_string(),
      source: PersistedTriggerUploadSource::RemoteCommand("flush-1".to_string()),
      session_id: "remote-session".to_string(),
      buffers: vec![PersistedTriggerUploadBufferProgress {
        buffer_id: "trigger".to_string(),
        lifecycle: PersistedTriggerUploadBufferLifecycle::UploadingFromBuffer,
        uploaded_batches_count: 1,
        uploaded_logs_count: 3,
      }],
      streaming: None,
      lifecycle: PersistedTriggerUploadLifecycle::UploadingFromBuffer,
    }]
  );
}

#[tokio::test]
async fn corrupted_snapshot_is_dropped_and_treated_as_empty() {
  let temp_directory = TempDir::with_prefix("flush-registry").unwrap();
  let snapshot_path = temp_directory
    .path()
    .join("state")
    .join("logger")
    .join("pending_trigger_uploads_snapshot.1.pb");

  tokio::fs::create_dir_all(snapshot_path.parent().unwrap())
    .await
    .unwrap();
  tokio::fs::write(&snapshot_path, b"not-a-valid-protobuf")
    .await
    .unwrap();

  let store = make_store(&temp_directory);
  assert!(store.pending_uploads().await.is_empty());
  assert!(!tokio::fs::try_exists(&snapshot_path).await.unwrap());
}
