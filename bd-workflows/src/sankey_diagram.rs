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
use bd_proto::protos::client::api::sankey_path_upload_request::Node;
use bd_proto::protos::client::api::{SankeyIntentRequest, SankeyPathUploadRequest};
use itertools::Itertools;
use tokio::sync::mpsc::{Receiver, Sender};

//
// ProcessedIntents
//

#[derive(Debug, PartialEq)]
struct SeenSankeyPath {
  sankey_id: String,
  path_id: String,
}

#[derive(Debug, Default)]
struct ProcessedIntents(Vec<SeenSankeyPath>);

impl ProcessedIntents {
  fn contains(&self, sankey_path: &SankeyPath) -> bool {
    self.0.contains(&SeenSankeyPath {
      sankey_id: sankey_path.sankey_id.to_string(),
      path_id: sankey_path.path_id.to_string(),
    })
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

    if self.0.len() > 100 {
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
}

impl Processor {
  pub(crate) fn new(input_rx: Receiver<SankeyPath>, data_upload_tx: Sender<DataUpload>) -> Self {
    Self {
      data_upload_tx,
      input_rx,
      processed_intents: ProcessedIntents::default(),
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
    if self.processed_intents.contains(&sankey_path) {
      log::debug!(
        "sankey path upload intent already processed, sankey id: {:?}, path id: {:?}",
        sankey_path.sankey_id,
        sankey_path.path_id
      );
      return;
    }

    match self.perform_upload_intent_negotiation(&sankey_path).await {
      Ok((decision, upload_intent_uuid)) => {
        self
          .handle_upload_intent_decision(sankey_path, decision, upload_intent_uuid)
          .await;
      },
      Err(error) => {
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
        self.upload_sankey_path(sankey_path).await;
      },
      Decision::Drop(_) => {
        log::debug!("sankey path upload intent rejected, drop: {upload_intent_uuid:?}");
        self.processed_intents.insert(sankey_path)
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
    }

    if let Err(error) = response.await {
      log::debug!("failed to wait for sankey upload response: {error}");
    }
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
