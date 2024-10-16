use bd_api::upload::TrackedSankeyDiagramUploadRequest;
use bd_api::DataUpload;
use bd_client_stats::DynamicStats;
use bd_proto::protos::client::api::SankeyDiagramUploadRequest;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};

//
// SankeyDiagram
//

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd, Ord, Eq)]
pub(crate) struct SankeyDiagram {
  id: String,
  path: Vec<String>,
  // TODO: add information about whether the limit was hit or not.
}

impl SankeyDiagram {
  pub(crate) const fn new(id: String, path: Vec<String>) -> Self {
    Self { id, path }
  }
}


//
// Processor
//

#[derive(Debug)]
pub(crate) struct Processor {
  dynamic_stats: Arc<DynamicStats>,
  data_upload_tx: Sender<DataUpload>,
  input_rx: Receiver<SankeyDiagram>,
}

impl Processor {
  pub(crate) const fn new(
    input_rx: Receiver<SankeyDiagram>,
    data_upload_tx: Sender<DataUpload>,
    dynamic_stats: Arc<DynamicStats>,
  ) -> Self {
    Self {
      dynamic_stats,
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
    let path_id = "_path_id".to_string();
    let upload_request = SankeyDiagramUploadRequest {
      id: sankey.id.clone(),
      path_id: path_id.clone(),
      nodes: vec![],
      ..Default::default()
    };

    let (upload_request, _response) = TrackedSankeyDiagramUploadRequest::new(
      TrackedSankeyDiagramUploadRequest::upload_uuid(),
      upload_request,
    );

    self
      .data_upload_tx
      .send(DataUpload::SankeyDiagramPathUpload(upload_request))
      .await
      .unwrap();

    let tags = BTreeMap::from([
      ("_id".to_string(), sankey.id),
      ("_path_id".to_string(), path_id),
    ]);

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    self
      .dynamic_stats
      .record_dynamic_counter("workflows_dyn:action", tags, 1);
  }
}
