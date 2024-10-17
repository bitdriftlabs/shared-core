use crate::workflow::SankeyDiagramPath;
use bd_api::upload::TrackedSankeyDiagramUploadRequest;
use bd_api::DataUpload;
use bd_proto::protos::client::api::sankey_diagram_upload_request::Node;
use bd_proto::protos::client::api::SankeyDiagramUploadRequest;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{Receiver, Sender};

//
// SankeyDiagram
//

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd, Ord, Eq)]
pub(crate) struct SankeyDiagram {
  id: String,
  path: SankeyDiagramPath,
}

impl SankeyDiagram {
  pub(crate) const fn new(id: String, path: SankeyDiagramPath) -> Self {
    Self { id, path }
  }
}

//
// Processor
//

#[derive(Debug)]
pub(crate) struct Processor {
  data_upload_tx: Sender<DataUpload>,
  input_rx: Receiver<SankeyDiagram>,
}

impl Processor {
  pub(crate) const fn new(
    input_rx: Receiver<SankeyDiagram>,
    data_upload_tx: Sender<DataUpload>,
  ) -> Self {
    Self {
      data_upload_tx,
      input_rx,
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

  pub(crate) async fn process_sankey(&self, sankey: SankeyDiagram) {
    let upload_request = SankeyDiagramUploadRequest {
      id: sankey.id.clone(),
      path_id: sankey.path.path_id.clone(),
      nodes: sankey
        .path
        .nodes
        .into_iter()
        .map(|n| Node {
          extracted_value: n,
          ..Default::default()
        })
        .collect_vec(),
      ..Default::default()
    };

    let (upload_request, _response) = TrackedSankeyDiagramUploadRequest::new(
      TrackedSankeyDiagramUploadRequest::upload_uuid(),
      upload_request,
    );

    if let Err(e) = self
      .data_upload_tx
      .send(DataUpload::SankeyPathUpload(upload_request))
      .await
    {
      // TODO(Augustyniak): Add a counter stat in here.
      log::debug!("failed to send sankey diagram upload request: {e}");
    }
  }
}
