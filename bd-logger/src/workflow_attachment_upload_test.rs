use super::*;
use crate::workflow_attachment::AttachmentStoreHandle;
use bd_artifact_upload::{EnqueueError, UploadSource};
use bd_runtime::runtime::ConfigLoader;
use std::time::Duration;
use tokio::time::timeout;

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
  assert!(!first.is_finished());
  first.abort();
  let _ = first.await;
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

  let (handle, worker) = WorkflowAttachmentUploadHandle::new_with_attachment_store(
    Arc::new(mock_client),
    attachment_store,
  );
  let worker = tokio::spawn(worker.run());
  handle
    .stage(HashMap::from([(admitted.id, "session".to_string())]))
    .await
    .unwrap();
  completion_rx.await.unwrap().send(Ok(())).unwrap();

  timeout(Duration::from_secs(1), async {
    loop {
      if store.is_uploaded(admitted.id).await.unwrap()
        && !tokio::fs::try_exists(
          directory
            .path()
            .join("workflow-attachments")
            .join(format!("{}.payload", admitted.id)),
        )
        .await
        .unwrap()
      {
        break;
      }
      tokio::task::yield_now().await;
    }
  })
  .await
  .unwrap();
  drop(handle);
  worker.await.unwrap();
}
