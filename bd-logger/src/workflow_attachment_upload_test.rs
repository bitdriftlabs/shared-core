// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::*;
use crate::workflow_attachment::AttachmentStoreHandle;
use bd_artifact_upload::{EnqueueError, UploadSource};
use bd_runtime::runtime::ConfigLoader;
use std::sync::Mutex;

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
async fn stage_waits_for_indexed_lease_ack() {
  let artifact_id = Uuid::new_v4();
  let expected_source = PathBuf::from(format!("workflow-attachments/{artifact_id}.payload"));
  let (ack_tx, ack_rx) = oneshot::channel();
  let mut mock_client = bd_artifact_upload::MockClient::new();
  let mut ack_tx = Some(ack_tx);
  mock_client
    .expect_enqueue_workflow_attachment()
    .withf(move |id, source, session, _, _| {
      *id == artifact_id && *source == expected_source && session == "session"
    })
    .once()
    .returning(move |_, _, _, persisted, _| {
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
    .withf(move |id, _, _, _, _| *id == first_id)
    .once()
    .returning(move |_, _, _, persisted, _| {
      first_tx.take().unwrap().send(()).unwrap();
      persisted
        .unwrap()
        .send(Err(EnqueueError::QueueFull))
        .unwrap();
      Ok(())
    });
  mock_client
    .expect_enqueue_workflow_attachment()
    .withf(move |id, _, _, _, _| *id == second_id)
    .once()
    .returning(|_, _, _, persisted, _| {
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
    .returning(|_, _, _, persisted, _| {
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
    .returning(move |id, _, _, persisted, _| {
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
async fn successful_upload_releases_the_retained_payload() {
  let directory = tempfile::tempdir().unwrap();
  let runtime = ConfigLoader::new(directory.path());
  let attachment_store = AttachmentStoreHandle::new(directory.path().to_owned(), runtime);
  let store = attachment_store.get().await.unwrap();
  let admitted = store
    .admit(UploadSource::Bytes(b"attachment".to_vec()))
    .await
    .unwrap();

  let (completion_tx, completion_rx) = oneshot::channel();
  let mut completion_tx = Some(completion_tx);
  let mut mock_client = bd_artifact_upload::MockClient::new();
  mock_client
    .expect_enqueue_workflow_attachment()
    .withf(move |id, _, session, _, _| *id == admitted.id && session == "session")
    .once()
    .returning(move |_, _, _, persisted, completion| {
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
