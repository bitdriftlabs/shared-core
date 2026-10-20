// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{
  PENDING_AGGREGATION_INDEX_FILE,
  STALE_SNAPSHOT_FILE,
  STATS_DIRECTORY,
  seed_stale_snapshot,
};
use crate::test::setup::create_minimal_init_params;
use crate::{LoggerBuilder, TestHooks};
use bd_api::{PlatformNetworkManager, PlatformNetworkStream, StreamEvent, StreamEventSender};
use bd_client_common::file::read_compressed_protobuf;
use bd_client_common::{HANDSHAKE_FLAG_CONFIG_UP_TO_DATE, HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE};
use bd_grpc_codec::{Decoder, Decompression, Encoder, OptimizeFor};
use bd_proto::protos::client::api::api_request::Request_type;
use bd_proto::protos::client::api::api_response::Response_type;
use bd_proto::protos::client::api::handshake_response::StreamSettings;
use bd_proto::protos::client::api::{
  ApiRequest,
  ApiResponse,
  HandshakeResponse,
  StatsUploadResponse,
};
use bd_proto::protos::client::metric::PendingAggregationIndex;
use bd_runtime::runtime::ConfigLoader;
use bd_test_helpers::runtime::{ValueKind, make_simple_update};
use bd_time::ToProtoDuration;
use bd_time::test::TestTicker;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use time::ext::NumericalDuration;
use tokio::sync::{Notify, mpsc};

//
// Network
//

struct Network(mpsc::Sender<TestStream>);

struct TestStream {
  events: StreamEventSender,
  requests: mpsc::Receiver<Vec<u8>>,
}

struct OutboundStream(mpsc::Sender<Vec<u8>>);

#[async_trait::async_trait]
impl PlatformNetworkManager<ConfigLoader> for Network {
  async fn start_stream(
    &self,
    event_tx: StreamEventSender,
    _: &ConfigLoader,
    _: &HashMap<&str, &str>,
  ) -> anyhow::Result<Box<dyn PlatformNetworkStream>> {
    let (request_tx, requests) = mpsc::channel(16);
    self
      .0
      .send(TestStream {
        events: event_tx,
        requests,
      })
      .await?;
    Ok(Box::new(OutboundStream(request_tx)))
  }
}

#[async_trait::async_trait]
impl PlatformNetworkStream for OutboundStream {
  async fn send_data(&mut self, data: &[u8]) -> anyhow::Result<()> {
    self.0.send(data.to_vec()).await?;
    Ok(())
  }
}

impl TestStream {
  async fn next_request(&mut self) -> ApiRequest {
    let mut decoder = Decoder::new(Some(Decompression::StatelessZlib), None, OptimizeFor::Cpu);
    let mut requests = decoder
      .decode_data(&self.requests.recv().await.unwrap())
      .unwrap();
    assert_eq!(requests.len(), 1);
    requests.pop().unwrap()
  }

  async fn respond(&self, response: Response_type) {
    let encoded = Encoder::new(None)
      .encode(&ApiResponse {
        response_type: Some(response),
        ..Default::default()
      })
      .unwrap();
    self
      .events
      .send(StreamEvent::Data(encoded.to_vec()))
      .await
      .unwrap();
  }
}

//
// StatsCompletion
//

#[derive(Default)]
struct StatsCompletion(Notify);

impl TestHooks for StatsCompletion {
  fn startup_stats_upload_completed(&self) {
    self.0.notify_one();
  }
}

#[tokio::test(start_paused = true)]
async fn delayed_stats_ack_retires_persisted_batch_before_reconnect() {
  let directory = tempfile::tempdir_in(".").unwrap();
  seed_stale_snapshot(&directory);
  let runtime = ConfigLoader::new(directory.path());
  runtime
    .update_snapshot(make_simple_update(vec![
      ("api.data_idle_timeout_interval_ms", ValueKind::Int(1500)),
      ("api.min_reconnect_interval_ms", ValueKind::Int(0)),
    ]))
    .await
    .unwrap();
  runtime.mark_safe().await;

  let (stream_tx, mut streams) = mpsc::channel(1);
  let mut params = create_minimal_init_params(directory.path());
  params.network = Box::new(Network(stream_tx));
  let completion = Arc::new(StatsCompletion::default());
  let (_flush_tick_tx, flush_ticker) = TestTicker::new();
  let (_upload_tick_tx, upload_ticker) = TestTicker::new();
  let (logger, _data_tx, driver, _flush_trigger) = LoggerBuilder::new(params)
    .with_client_stats_tickers_for_test(Box::new(flush_ticker), Box::new(upload_ticker))
    .with_test_hooks(Some(completion.clone()))
    .build()
    .unwrap();
  let driver = tokio::spawn(driver);
  let mut stream = streams.recv().await.unwrap();
  let Some(Request_type::Handshake(handshake)) = stream.next_request().await.request_type else {
    panic!("expected handshake");
  };
  let stats = handshake.startup_stats_upload.as_ref().unwrap();
  assert_eq!(stats.upload_uuid, STALE_SNAPSHOT_FILE);

  stream
    .respond(Response_type::Handshake(HandshakeResponse {
      configuration_update_status: HANDSHAKE_FLAG_CONFIG_UP_TO_DATE
        | HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE,
      stream_settings: Some(StreamSettings {
        ping_interval: 1.seconds().into_proto(),
        ..Default::default()
      })
      .into(),
      ..Default::default()
    }))
    .await;
  // A sent ping gates established-stream entry without introducing wall-clock sleeps.
  assert!(matches!(
    stream.next_request().await.request_type,
    Some(Request_type::Ping(_))
  ));
  tokio::time::advance(10.seconds().unsigned_abs()).await;
  let stats_directory = directory.path().join(STATS_DIRECTORY);
  assert!(stats_directory.join(STALE_SNAPSHOT_FILE).exists());

  stream
    .respond(Response_type::StatsUpload(StatsUploadResponse {
      upload_uuid: stats.upload_uuid.clone(),
      ..Default::default()
    }))
    .await;
  completion.0.notified().await;
  assert!(!stats_directory.join(STALE_SNAPSHOT_FILE).exists());
  let index: PendingAggregationIndex = read_compressed_protobuf(
    &fs::read(stats_directory.join(PENDING_AGGREGATION_INDEX_FILE)).unwrap(),
  )
  .unwrap();
  assert_eq!(index.pending_files, []);

  // ACK completion restores the already-expired idle deadline and reconnects.
  let mut reconnected = streams.recv().await.unwrap();
  let Some(Request_type::Handshake(handshake)) = reconnected.next_request().await.request_type
  else {
    panic!("expected reconnect handshake");
  };
  assert!(handshake.startup_stats_upload.is_none());
  logger.shutdown(false);
  driver.await.unwrap().unwrap();
}
