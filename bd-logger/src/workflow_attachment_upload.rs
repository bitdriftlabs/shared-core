// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./workflow_attachment_upload_test.rs"]
mod tests;

use crate::logger::TestHooks;
use crate::upload_coordination::{
  Coalesced,
  UploadNotifier,
  UploadWake,
  enqueue_and_wait_for_persistence,
};
use crate::workflow_attachment::AttachmentStoreHandle;
use bd_artifact_upload::Client as ArtifactClient;
use futures_util::stream::{FuturesUnordered, StreamExt as _};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use uuid::Uuid;

const MAX_PENDING_BATCHES: usize = 4;

//
// BatchRequest
//

struct BatchRequest {
  ids: HashMap<Uuid, String>,
  result_tx: oneshot::Sender<Vec<WorkflowAttachmentStagingFailure>>,
  _permit: OwnedSemaphorePermit,
}

struct PendingBatches(Vec<BatchRequest>);

impl Coalesced for PendingBatches {
  fn merge(&mut self, mut other: Self) {
    self.0.append(&mut other.0);
  }
}

pub struct WorkflowAttachmentStagingFailure {
  pub artifact_id: Uuid,
  pub error: String,
}

//
// WorkflowAttachmentUploadHandle
//

pub struct WorkflowAttachmentUploadHandle {
  coordination: UploadNotifier<PendingBatches>,
  slots: Arc<Semaphore>,
}

impl WorkflowAttachmentUploadHandle {
  pub fn new(artifact_client: Arc<dyn ArtifactClient>) -> (Self, WorkflowAttachmentUploadWorker) {
    let (coordination, wake) = UploadWake::new();
    (
      Self {
        coordination,
        slots: Arc::new(Semaphore::new(MAX_PENDING_BATCHES)),
      },
      WorkflowAttachmentUploadWorker {
        artifact_client,
        wake,
        attachment_store: None,
        test_hooks: None,
      },
    )
  }

  pub fn new_with_attachment_store_and_test_hooks(
    artifact_client: Arc<dyn ArtifactClient>,
    attachment_store: AttachmentStoreHandle,
    test_hooks: Option<Arc<dyn TestHooks>>,
  ) -> (Self, WorkflowAttachmentUploadWorker) {
    let (handle, mut worker) = Self::new(artifact_client);
    worker.attachment_store = Some(attachment_store);
    worker.test_hooks = test_hooks;
    (handle, worker)
  }

  pub async fn stage(
    &self,
    ids: HashMap<Uuid, String>,
  ) -> anyhow::Result<Vec<WorkflowAttachmentStagingFailure>> {
    if ids.is_empty() {
      return Ok(Vec::new());
    }
    let permit = self.slots.clone().acquire_owned().await?;
    let (result_tx, result_rx) = oneshot::channel();
    self.coordination.notify(PendingBatches(vec![BatchRequest {
      ids,
      result_tx,
      _permit: permit,
    }]));
    Ok(result_rx.await?)
  }
}

//
// WorkflowAttachmentUploadWorker
//

pub struct WorkflowAttachmentUploadWorker {
  artifact_client: Arc<dyn ArtifactClient>,
  wake: UploadWake<PendingBatches>,
  attachment_store: Option<AttachmentStoreHandle>,
  test_hooks: Option<Arc<dyn TestHooks>>,
}

impl WorkflowAttachmentUploadWorker {
  pub async fn run(mut self) {
    let mut pending = None;
    let mut active = FuturesUnordered::new();
    loop {
      tokio::select! {
        wake = self.wake.recv() => {
          if wake.is_none() {
            break;
          }
          self.wake.drain_into(&mut pending);
          if let Some(PendingBatches(batches)) = pending.take() {
            for batch in batches {
              let client = self.artifact_client.clone();
              let attachment_store = self.attachment_store.clone();
              let test_hooks = self.test_hooks.clone();
              let BatchRequest { ids, result_tx, _permit: permit } = batch;
              active.push(async move {
                let result = Self::stage_batch(client, attachment_store, test_hooks, ids).await;
                let _ = result_tx.send(result);
                drop(permit);
              });
            }
          }
        },
        Some(()) = active.next(), if !active.is_empty() => {},
      }
    }
  }

  async fn stage_batch(
    artifact_client: Arc<dyn ArtifactClient>,
    attachment_store: Option<AttachmentStoreHandle>,
    test_hooks: Option<Arc<dyn TestHooks>>,
    ids: HashMap<Uuid, String>,
  ) -> Vec<WorkflowAttachmentStagingFailure> {
    let mut failures = Vec::new();
    for (id, session_id) in ids {
      let already_uploaded = if let Some(store_handle) = &attachment_store {
        let store = match store_handle.get().await {
          Ok(store) => store,
          Err(error) => {
            failures.push(WorkflowAttachmentStagingFailure {
              artifact_id: id,
              error: error.to_string(),
            });
            continue;
          },
        };
        match store.is_uploaded(id).await {
          Ok(already_uploaded) => already_uploaded,
          Err(error) => {
            failures.push(WorkflowAttachmentStagingFailure {
              artifact_id: id,
              error: error.to_string(),
            });
            continue;
          },
        }
      } else {
        false
      };
      if already_uploaded {
        continue;
      }
      let source = PathBuf::from(format!("workflow-attachments/{id}.payload"));
      let (completion_tx, completion_rx) = if attachment_store.is_some() {
        let (completion_tx, completion_rx) = oneshot::channel();
        (Some(completion_tx), Some(completion_rx))
      } else {
        (None, None)
      };
      // TODO: Add a bounded retry policy for explicitly transient staging failures.
      if let Err(error) = enqueue_and_wait_for_persistence(|persisted_tx| {
        artifact_client.enqueue_workflow_attachment(
          id,
          source,
          session_id,
          Some(persisted_tx),
          completion_tx,
        )
      })
      .await
      {
        failures.push(WorkflowAttachmentStagingFailure {
          artifact_id: id,
          error: error.to_string(),
        });
        continue;
      }
      if let (Some(store), Some(completion_rx)) = (attachment_store.clone(), completion_rx) {
        let test_hooks = test_hooks.clone();
        tokio::spawn(async move {
          match completion_rx.await {
            Ok(Ok(())) => match store.get().await {
              Ok(store) => {
                if let Err(error) = store.complete_upload(id).await {
                  log::warn!("failed to release uploaded workflow attachment {id}: {error}");
                } else if let Some(test_hooks) = test_hooks {
                  test_hooks.workflow_attachment_upload_completed(id);
                }
              },
              Err(error) => {
                log::warn!("failed to open workflow attachment store after upload: {error}");
              },
            },
            Ok(Err(error)) => {
              log::debug!("workflow attachment {id} upload did not complete: {error}");
            },
            Err(_) => log::debug!("workflow attachment {id} upload completion was dropped"),
          }
        });
      }
    }
    failures
  }
}
