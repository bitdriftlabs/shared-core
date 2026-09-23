use super::*;

#[tokio::test]
async fn persisted_enqueue_confirms_queue_index() {
  let result = enqueue_and_wait_for_persistence(|persisted_tx| {
    persisted_tx.send(Ok(())).unwrap();
    Ok::<_, EnqueueError>(())
  })
  .await;

  assert!(result.is_ok());
}

#[tokio::test]
async fn persisted_enqueue_classifies_backpressure() {
  let result = enqueue_and_wait_for_persistence(|persisted_tx| {
    persisted_tx.send(Err(EnqueueError::QueueFull)).unwrap();
    Ok::<_, EnqueueError>(())
  })
  .await;

  assert!(matches!(result, Err(PersistedEnqueueError::Backpressure)));
}

#[tokio::test]
async fn persisted_enqueue_classifies_dropped_acknowledgement() {
  let result = enqueue_and_wait_for_persistence(|_persisted_tx| Ok::<_, EnqueueError>(())).await;

  assert!(matches!(
    result,
    Err(PersistedEnqueueError::AcknowledgementDropped)
  ));
}
