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
use std::collections::HashMap;
use tokio::sync::mpsc::{Receiver, Sender};

//
// IntentDecision
//

#[derive(Debug)]
struct IntentDecision {
  path_id: String,
  accepted: bool,
}

//
// Processor
//

#[derive(Debug)]
pub(crate) struct Processor {
  data_upload_tx: Sender<DataUpload>,
  input_rx: Receiver<SankeyPath>,

  // Sankey ID to path ID map.
  #[allow(dead_code)]
  rejected_intents: HashMap<String, Vec<IntentDecision>>,
}

impl Processor {
  pub(crate) fn new(input_rx: Receiver<SankeyPath>, data_upload_tx: Sender<DataUpload>) -> Self {
    Self {
      data_upload_tx,
      input_rx,
      rejected_intents: HashMap::new(),
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

  pub(crate) async fn process_sankey(&self, sankey_path: SankeyPath) {

    let decision = self.perform_intent_negotiation(sankey_path).await;

    match decision {
      Ok(Decision::UploadImmediately(_)) => {
        self.upload_sankey_path(sankey_path).await;
      }
      Ok(Decision::Drop(_)) => {
        // TODO(Augustyniak): Add a counter stat in here.
        log::debug!("sankey path rejected");
      }
      Err(error) => {
        // TODO(Augustyniak): Add a counter stat in here.
        log::debug!("failed to negotiate intent: {error}");
      }
    }


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
      // TODO(Augustyniak): Add a counter stat in here.
      log::debug!("failed to send sankey upload request: {e}");
    }

    if let Err(error) = response.await {
      log::debug!("failed to wait for sankey upload response: {error}");
    }
  }

  async fn perform_intent_negotiation(&self, sankey_path: SankeyPath) -> anyhow::Result<Decision> {
    let intent_upload_uuid = TrackedSankeyPathUploadRequest::upload_uuid();
    let intent_request = SankeyIntentRequest {
      intent_uuid: intent_upload_uuid.clone(),
      sankey_diagram_id: sankey_path.sankey_id.clone(),
      path_id: sankey_path.path_id.clone(),
      ..Default::default()
    };

    let (intent_upload_request, response) = TrackedSankeyPathUploadIntentRequest::new(intent_upload_uuid, intent_request);

    self
      .data_upload_tx
      .send(DataUpload::SankeyPathUploadIntentRequest(intent_upload_request)).await?;

    Ok(response.await?)
  }
}
