#[cfg(test)]
#[path = "./upload_coordination_test.rs"]
mod tests;

use bd_artifact_upload::EnqueueError;
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

pub const BACKPRESSURE_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

//
// PersistedEnqueueError
//

#[derive(Debug)]
pub enum PersistedEnqueueError {
  Backpressure,
  Enqueue(EnqueueError),
  AcknowledgementDropped,
}

impl std::fmt::Display for PersistedEnqueueError {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Backpressure => formatter.write_str("artifact upload queue is full"),
      Self::Enqueue(error) => error.fmt(formatter),
      Self::AcknowledgementDropped => {
        formatter.write_str("artifact uploader stopped before persistence acknowledgement")
      },
    }
  }
}

/// Enqueues work and waits until the uploader has durably written its queue index.
///
/// Callers decide whether backpressure should defer a shared range or retry one independent item.
pub async fn enqueue_and_wait_for_persistence<T>(
  enqueue: impl FnOnce(
    oneshot::Sender<std::result::Result<(), EnqueueError>>,
  ) -> Result<T, EnqueueError>,
) -> Result<(), PersistedEnqueueError> {
  let (persisted_tx, persisted_rx) = oneshot::channel();
  match enqueue(persisted_tx) {
    Ok(_) => match persisted_rx.await {
      Ok(Ok(())) => Ok(()),
      Ok(Err(EnqueueError::QueueFull)) => Err(PersistedEnqueueError::Backpressure),
      Ok(Err(error)) => Err(PersistedEnqueueError::Enqueue(error)),
      Err(_) => Err(PersistedEnqueueError::AcknowledgementDropped),
    },
    Err(EnqueueError::QueueFull) => Err(PersistedEnqueueError::Backpressure),
    Err(error) => Err(PersistedEnqueueError::Enqueue(error)),
  }
}

pub trait Coalesced: Send {
  fn merge(&mut self, other: Self);
}

struct Pending<T> {
  value: Option<T>,
  version: u64,
  wake_queued: bool,
}

impl<T> Default for Pending<T> {
  fn default() -> Self {
    Self {
      value: None,
      version: 0,
      wake_queued: false,
    }
  }
}

//
// UploadNotifier
//

pub struct UploadNotifier<T> {
  pending: Arc<Mutex<Pending<T>>>,
  wake_tx: mpsc::Sender<()>,
}

impl<T: Coalesced> UploadNotifier<T> {
  pub fn notify(&self, incoming: T) {
    let should_wake = {
      let mut pending = self.pending.lock();
      if let Some(existing) = &mut pending.value {
        existing.merge(incoming);
      } else {
        pending.value = Some(incoming);
      }
      pending.version = pending.version.wrapping_add(1);
      if pending.wake_queued {
        false
      } else {
        pending.wake_queued = true;
        true
      }
    };
    if should_wake {
      let _ = self.wake_tx.try_send(());
    }
  }

  #[cfg(test)]
  pub fn fill_wake_channel(&self) {
    let _ = self.wake_tx.try_send(());
  }

  #[cfg(test)]
  pub fn pending_value(&self) -> Option<T>
  where
    T: Clone,
  {
    self.pending.lock().value.clone()
  }
}

//
// UploadWake
//

pub struct UploadWake<T> {
  pending: Arc<Mutex<Pending<T>>>,
  wake_rx: mpsc::Receiver<()>,
  seen_version: u64,
}

impl<T: Coalesced> UploadWake<T> {
  pub fn new() -> (UploadNotifier<T>, Self) {
    let (wake_tx, wake_rx) = mpsc::channel(1);
    let pending = Arc::new(Mutex::new(Pending::default()));
    (
      UploadNotifier {
        pending: pending.clone(),
        wake_tx,
      },
      Self {
        pending,
        wake_rx,
        seen_version: 0,
      },
    )
  }

  pub async fn recv(&mut self) -> Option<()> {
    self.wake_rx.recv().await
  }

  pub fn drain_into(&mut self, current: &mut Option<T>) {
    let mut pending = self.pending.lock();
    if let Some(incoming) = pending.value.take() {
      if let Some(existing) = current {
        existing.merge(incoming);
      } else {
        *current = Some(incoming);
      }
    }
    self.seen_version = pending.version;
    pending.wake_queued = false;
  }

  pub fn version_changed(&self) -> bool {
    self.pending.lock().version != self.seen_version
  }
}
