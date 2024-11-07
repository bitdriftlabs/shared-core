// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::workflow::SankeyPath;
use bd_api::api::Decision;
use bd_api::upload::{TrackedSankeyPathUploadIntentRequest, TrackedSankeyPathUploadRequest};
use bd_api::DataUpload;
use bd_client_stats_store::{Counter, Scope};
use bd_proto::protos::client::api::sankey_path_upload_request::Node;
use bd_proto::protos::client::api::{SankeyIntentRequest, SankeyPathUploadRequest};
use itertools::Itertools;
use tokio::sync::mpsc::{Receiver, Sender};

const PROCESSED_INTENTS_LRU_CACHE_SIZE: usize = 100;

//
// ProcessedIntents
//

#[derive(Clone, Debug, PartialEq)]
struct SeenSankeyPath {
  sankey_id: String,
  path_id: String,
}

// Stores information about a limited number of recent Sankey path upload intent results,
// reducing the need for repeated intent negotiation network requests.
// The underlying vector acts as an LRU cache with the least recently seen paths stored at the
// front.
#[derive(Clone, Debug, Default)]
struct ProcessedIntents(Vec<SeenSankeyPath>);

impl ProcessedIntents {
  fn contains(&mut self, sankey_path: &SankeyPath) -> bool {
    let seen_path = SeenSankeyPath {
      sankey_id: sankey_path.sankey_id.to_string(),
      path_id: sankey_path.path_id.to_string(),
    };

    let Some(index) = self.0.iter().position(|e| e == &seen_path) else {
      return false;
    };

    self.0.remove(index);
    self.0.push(seen_path);
    true
  }

  fn insert(&mut self, sankey_path: SankeyPath) {
    let seen_path = SeenSankeyPath {
      sankey_id: sankey_path.sankey_id,
      path_id: sankey_path.path_id,
    };

    match self.0.iter().position(|e| e == &seen_path) {
      Some(index) => {
        self.0.remove(index);
        self.0.push(seen_path);
      },
      None => {
        self.0.push(seen_path);
      },
    }

    // Enforce the LRU cache size limit.
    if self.0.len() > PROCESSED_INTENTS_LRU_CACHE_SIZE {
      self.0.remove(0);
    }
  }
}

//
// Processor
//

#[derive(Debug)]
pub(crate) struct Processor {
  data_upload_tx: Sender<DataUpload>,
  input_rx: Receiver<SankeyPath>,

  processed_intents: ProcessedIntents,

  stats: Stats,
}

impl Processor {
  pub(crate) fn new(
    input_rx: Receiver<SankeyPath>,
    data_upload_tx: Sender<DataUpload>,
    scope: &Scope,
  ) -> Self {
    Self {
      data_upload_tx,
      input_rx,
      processed_intents: ProcessedIntents::default(),
      stats: Stats::new(scope),
    }
  }

  pub(crate) fn run(mut self) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn(async move {
      loop {
        if let Some(sankey_diagram) = self.input_rx.recv().await {
          self.process_sankey(sankey_diagram).await;
        }
      }
    })
  }

  pub(crate) async fn process_sankey(&mut self, sankey_path: SankeyPath) {
    self.stats.intent_initiations_total.inc();
    log::debug!(
      "processing sankey: sankey id {:?}, path id {:?}",
      sankey_path.sankey_id,
      sankey_path.path_id
    );

    if self.processed_intents.contains(&sankey_path) {
      log::debug!(
        "sankey path upload intent already processed, sankey id: {:?}, path id: {:?}",
        sankey_path.sankey_id,
        sankey_path.path_id
      );
      self.stats.intent_completion_already_processed_total.inc();
      return;
    }

    match self.perform_upload_intent_negotiation(&sankey_path).await {
      Ok((decision, upload_intent_uuid)) => {
        self
          .handle_upload_intent_decision(sankey_path, decision, upload_intent_uuid)
          .await;
      },
      Err(error) => {
        self.stats.intent_request_failures_total.inc();
        log::debug!("failed to negotiate sankey path upload intent: {error}");
      },
    };
  }

  async fn handle_upload_intent_decision(
    &mut self,
    sankey_path: SankeyPath,
    decision: Decision,
    upload_intent_uuid: String,
  ) {
    match decision {
      Decision::UploadImmediately(_) => {
        log::debug!(
          "sankey path upload intent accepted, upload immediately: {upload_intent_uuid:?}"
        );
        self.stats.intent_completion_uploads_total.inc();
        self.processed_intents.insert(sankey_path.clone());
        self.upload_sankey_path(sankey_path).await;
      },
      Decision::Drop(_) => {
        log::debug!("sankey path upload intent rejected, drop: {upload_intent_uuid:?}");
        self.stats.intent_completion_drops_total.inc();
        self.processed_intents.insert(sankey_path);
      },
    }
  }

  async fn upload_sankey_path(&self, sankey_path: SankeyPath) {
    let upload_uuid = TrackedSankeyPathUploadRequest::upload_uuid();

    let upload_request = SankeyPathUploadRequest {
      upload_uuid: upload_uuid.clone(),
      id: sankey_path.sankey_id.clone(),
      path_id: sankey_path.path_id.clone(),
      nodes: sankey_path
        .nodes
        .into_iter()
        .map(|value| Node {
          extracted_value: value,
          ..Default::default()
        })
        .collect_vec(),
      ..Default::default()
    };

    let (upload_request, response) =
      TrackedSankeyPathUploadRequest::new(upload_uuid, upload_request);

    log::debug!(
      "sending sankey upload request with id {:?}",
      upload_request.uuid
    );

    if let Err(e) = self
      .data_upload_tx
      .send(DataUpload::SankeyPathUpload(upload_request))
      .await
    {
      log::debug!("failed to send sankey upload request: {e}");
      self.stats.upload_completion_failure_total.inc();
      return;
    }

    if let Err(error) = response.await {
      log::debug!("failed to wait for sankey upload response: {error}");
      self.stats.upload_completion_failure_total.inc();
      return;
    }

    self.stats.upload_completion_success_total.inc();
  }

  async fn perform_upload_intent_negotiation(
    &self,
    sankey_path: &SankeyPath,
  ) -> anyhow::Result<(Decision, String)> {
    let intent_upload_uuid = TrackedSankeyPathUploadRequest::upload_uuid();

    let intent_request = SankeyIntentRequest {
      intent_uuid: intent_upload_uuid.to_string(),
      sankey_diagram_id: sankey_path.sankey_id.clone(),
      path_id: sankey_path.path_id.clone(),
      ..Default::default()
    };

    let (intent_upload_request, response) =
      TrackedSankeyPathUploadIntentRequest::new(intent_upload_uuid.to_string(), intent_request);

    self
      .data_upload_tx
      .send(DataUpload::SankeyPathUploadIntentRequest(
        intent_upload_request,
      ))
      .await?;

    Ok((response.await?, intent_upload_uuid))
  }
}

//
// Stats
//

#[derive(Debug)]
#[allow(clippy::struct_field_names)]
struct Stats {
  intent_initiations_total: Counter,

  intent_completion_uploads_total: Counter,
  intent_completion_already_processed_total: Counter,
  intent_completion_drops_total: Counter,

  intent_request_failures_total: Counter,

  upload_completion_success_total: Counter,
  upload_completion_failure_total: Counter,
}

impl Stats {
  fn new(scope: &Scope) -> Self {
    let scope = scope.scope("sankeys");
    Self {
      intent_initiations_total: scope.counter("intent_initiations_total"),

      intent_completion_uploads_total: scope.counter("intent_completion_uploads_total"),
      intent_completion_already_processed_total: scope
        .counter("intent_completion_already_processed_total"),
      intent_completion_drops_total: scope.counter("intent_completion_drops_total"),

      intent_request_failures_total: scope.counter("intent_request_failures_total"),

      upload_completion_success_total: scope.counter("upload_completion_success_total"),
      upload_completion_failure_total: scope.counter("upload_completion_failure_total"),
    }
  }
}
