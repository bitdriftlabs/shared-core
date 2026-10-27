// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// Upload drains let logger-cli wait until the backend has acknowledged persisted stats before it
// shuts the SDK down. They exist only with the `logger-cli` feature, so the mobile SDK's upload
// state machine is unchanged.

use super::{Flusher, PendingUploadContext};
use crate::FlushTrigger;
use bd_client_common::maybe_await;
use bd_proto::protos::client::api::stats_upload_request::UploadReason;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Sleep;

// The global upload limiter does not expose when it next admits an upload, so a drain it refuses
// polls at this interval rather than waiting for a periodic upload that may be minutes away.
const THROTTLED_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);

type UploadDrainRequest = oneshot::Sender<anyhow::Result<()>>;

//
// UploadDrainChannel
//

// Carries drain requests from every FlushTrigger clone to the Flusher, which takes the receiver
// when it is constructed. Dropping the Flusher therefore fails any request still in flight.
#[derive(Clone)]
pub struct UploadDrainChannel {
  tx: mpsc::Sender<UploadDrainRequest>,
  rx: Arc<Mutex<Option<mpsc::Receiver<UploadDrainRequest>>>>,
}

impl UploadDrainChannel {
  pub fn new() -> Self {
    let (tx, rx) = mpsc::channel(1);
    Self {
      tx,
      rx: Arc::new(Mutex::new(Some(rx))),
    }
  }
}

impl FlushTrigger {
  /// Persists current stats and waits until the backend acknowledges every snapshot persisted at
  /// that point. Unlike `flush`, this waits for upload ACKs and is not limited by the explicit
  /// flush minimum upload interval, so it suits a final drain before shutdown. The global upload
  /// limiter still applies. A rejected or abandoned upload, or a snapshot discarded before it was
  /// acknowledged, fails the drain; callers should bound the wait.
  pub async fn drain_uploads(&self) -> anyhow::Result<()> {
    let (completion_tx, completion_rx) = oneshot::channel();
    self
      .drain
      .tx
      .send(completion_tx)
      .await
      .map_err(|_| anyhow::anyhow!("stats flusher shut down before upload drain was requested"))?;
    completion_rx
      .await
      .map_err(|_| anyhow::anyhow!("stats flusher shut down before upload drain completed"))?
  }
}

//
// UploadDrains
//

pub(super) enum DrainEvent {
  Request(UploadDrainRequest),
  Retry,
}

// A caller waiting for the backend to acknowledge every snapshot that was persisted when it asked.
// Snapshots written later are uploaded opportunistically but do not hold the caller.
struct UploadDrain {
  unacknowledged_file_ids: HashSet<String>,
  completion_tx: UploadDrainRequest,
}

// Drains complete from successful ACKs of their files, whichever upload carries them. One drain
// upload is in flight at a time; its completion, or that of any other upload, re-evaluates them.
pub(super) struct UploadDrains {
  requests: Option<mpsc::Receiver<UploadDrainRequest>>,
  waiting: Vec<UploadDrain>,
  upload_in_flight: bool,
  retry: Option<Pin<Box<Sleep>>>,
}

impl UploadDrains {
  pub(super) fn new(trigger: &FlushTrigger) -> Self {
    Self {
      requests: trigger.drain.rx.lock().take(),
      waiting: Vec::new(),
      upload_in_flight: false,
      retry: None,
    }
  }

  pub(super) async fn next_event(&mut self) -> DrainEvent {
    let requests = async {
      match &mut self.requests {
        Some(requests) => requests.recv().await,
        None => std::future::pending().await,
      }
    };
    tokio::select! {
      Some(request) = requests => DrainEvent::Request(request),
      () = maybe_await(&mut self.retry) => DrainEvent::Retry,
    }
  }

  fn fail(&mut self, reason: &str) {
    for drain in self.waiting.drain(..) {
      let _ignored = drain.completion_tx.send(Err(anyhow::anyhow!("{reason}")));
    }
  }
}

impl Flusher {
  pub(super) async fn handle_drain_event(&mut self, event: DrainEvent) {
    match event {
      DrainEvent::Request(completion_tx) => self.handle_drain_request(completion_tx).await,
      DrainEvent::Retry => self.advance_drains().await,
    }
  }

  async fn handle_drain_request(&mut self, completion_tx: UploadDrainRequest) {
    // Write directly instead of joining a debounce window: the drain must cover every metric
    // recorded before the request, and a single extra write is bounded by the caller.
    if !self.flush_to_disk().await {
      let _ignored = completion_tx.send(Err(anyhow::anyhow!(
        "stats flush failed before upload drain"
      )));
      return;
    }
    match self.file_manager.snapshot_ids().await {
      Ok(unacknowledged_file_ids) => {
        log::debug!(
          "draining stats uploads for {} persisted snapshots",
          unacknowledged_file_ids.len()
        );
        self.drains.waiting.push(UploadDrain {
          unacknowledged_file_ids,
          completion_tx,
        });
        self.advance_drains().await;
      },
      Err(error) => {
        let _ignored = completion_tx.send(Err(anyhow::anyhow!(
          "listing stats snapshots to drain: {error}"
        )));
      },
    }
  }

  // Must run for every successful ACK before `advance_drains`, so that files deleted by the ACK
  // are not mistaken for files discarded without one.
  pub(super) fn record_drain_ack(&mut self, source_file_ids: &[String]) {
    for drain in &mut self.drains.waiting {
      for file_id in source_file_ids {
        drain.unacknowledged_file_ids.remove(file_id);
      }
    }
  }

  // Releases the drain upload slot. `acknowledged` is None when the upload never received an ACK.
  pub(super) fn finish_drain_upload(&mut self, acknowledged: Option<bool>) {
    self.drains.upload_in_flight = false;
    match acknowledged {
      Some(true) => {},
      Some(false) => self.drains.fail("backend rejected the stats drain upload"),
      None => self
        .drains
        .fail("stats upload stream closed before the drain upload was acknowledged"),
    }
  }

  // Completes drains whose files have all been acknowledged, fails drains whose files were
  // discarded without an ACK, then starts the next drain upload if the retained-upload slot is
  // free.
  pub(super) async fn advance_drains(&mut self) {
    self
      .drains
      .waiting
      .retain(|drain| !drain.completion_tx.is_closed());
    if self.drains.waiting.is_empty() {
      self.drains.retry = None;
      return;
    }
    let persisted_file_ids = match self.file_manager.snapshot_ids().await {
      Ok(persisted_file_ids) => persisted_file_ids,
      Err(error) => {
        self
          .drains
          .fail(&format!("listing stats snapshots to drain: {error}"));
        return;
      },
    };
    for drain in std::mem::take(&mut self.drains.waiting) {
      if drain.unacknowledged_file_ids.is_empty() {
        log::debug!("stats upload drain completed");
        let _ignored = drain.completion_tx.send(Ok(()));
      } else if drain
        .unacknowledged_file_ids
        .iter()
        .any(|file_id| !persisted_file_ids.contains(file_id))
      {
        // Retention rotation and corruption recovery remove snapshots without uploading them.
        let _ignored = drain.completion_tx.send(Err(anyhow::anyhow!(
          "stats snapshots were discarded before the backend acknowledged them"
        )));
      } else {
        self.drains.waiting.push(drain);
      }
    }

    if self.drains.waiting.is_empty()
      || self.drains.upload_in_flight
      || self.pending_data_upload.is_some()
    {
      return;
    }
    // Drained files are the oldest in the index, so the oldest eligible file is one of them unless
    // another upload has claimed them all, in which case its completion re-enters here.
    let Some(prepared_upload) = self
      .upload_from_disk(false, UploadReason::UPLOAD_REASON_EVENT_TRIGGERED)
      .await
    else {
      log::debug!("stats upload drain waiting for in-flight or throttled uploads");
      self.drains.retry = Some(Box::pin(tokio::time::sleep(THROTTLED_RETRY_INTERVAL)));
      return;
    };
    self.drains.upload_in_flight = true;
    // A closed channel abandons the upload, which fails the drains through
    // `abandon_pending_upload`.
    let _ = self
      .dispatch_prepared_upload(prepared_upload, PendingUploadContext::Drain)
      .await;
  }
}
