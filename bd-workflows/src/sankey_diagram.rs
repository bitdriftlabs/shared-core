use bd_api::upload::TrackedSankeyDiagramUploadRequest;
use bd_api::DataUpload;
use bd_client_stats::DynamicStats;
use bd_proto::protos::client::api::SankeyDiagramUploadRequest;
use std::collections::BTreeMap;
use tokio::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;

pub(crate) struct SankeyDiagram {
  id: String,
  path: Vec<String>,
}

struct Processor {
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
      data_upload_tx,
      input_rx,
      dynamic_stats,
    }
  }

  pub(crate) fn run(mut self) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn(async move {
      loop {
        if let Some(sankey_diagram) = self.input_rx.recv().await {
          self.process_sankey_diagram(sankey_diagram).await;
        }
      }
    })
  }

  pub(crate) async fn process_sankey_diagram(&self, sankey_diagram: SankeyDiagram) {

    // for action in actions {
      let upload_request = SankeyDiagramUploadRequest {
        id: sankey_diagram.id.clone(),
        path_id: "path_id".to_string(),
        nodes: vec![],
        ..Default::default()
      };

      let (upload_request, _response) = TrackedSankeyDiagramUploadRequest::new(
        TrackedSankeyDiagramUploadRequest::upload_uuid(),
        upload_request,
      );

      // let (intent, response) = LogUploadIntent::new(intent_uuid.clone(), upload_request.clone());
      self
        .data_upload_tx
        .send(DataUpload::SankeyDiagramPathUpload(upload_request))
        .await
        .unwrap();
    // }

    // for action in actions {
      let tags = BTreeMap::from([("_id".to_string(), sankey_diagram.id)]);

      #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
      self
        .dynamic_stats
        .record_dynamic_counter("workflows_dyn:sankey", tags, 1);
    // }
  }
}
