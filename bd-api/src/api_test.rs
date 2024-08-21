// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{Api, PlatformNetworkManager, PlatformNetworkStream};
use crate::api::StreamEvent;
use anyhow::anyhow;
use bd_client_stats_store::test::StatsHelper;
use bd_client_stats_store::Collector;
use bd_grpc_codec::Encoder;
use bd_internal_logging::{LogFields, LogLevel, LogType};
use bd_metadata::{Metadata, Platform};
use bd_proto::protos::client::api::api_response::Response_type;
use bd_proto::protos::client::api::{ApiResponse, ErrorShutdown, HandshakeResponse, RuntimeUpdate};
use bd_runtime::runtime::{ConfigLoader, FeatureFlag};
use bd_shutdown::ComponentShutdownTrigger;
use bd_stats_common::labels;
use bd_time::TimeDurationExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use time::ext::NumericalDuration;
use time::Duration;
use tokio::sync::mpsc::{channel, Receiver, Sender};

struct EmptyMetadata;

impl Metadata for EmptyMetadata {
  fn sdk_version(&self) -> &'static str {
    "test"
  }

  fn platform(&self) -> &Platform {
    &Platform::Other("unknown", "unknown")
  }

  fn collect_inner(&self) -> HashMap<String, String> {
    HashMap::new()
  }
}

#[derive(Clone)]
#[allow(clippy::struct_field_names)]
struct PlatformNetwork {
  start_stream_tx: Sender<()>,
  send_data_tx: Sender<()>,

  current_stream_tx: Arc<Mutex<Option<Sender<StreamEvent>>>>,
}

impl PlatformNetwork {
  const fn new(
    start_stream_tx: Sender<()>,
    send_data_tx: Sender<()>,
    current_stream_tx: Arc<Mutex<Option<Sender<StreamEvent>>>>,
  ) -> Self {
    Self {
      start_stream_tx,
      send_data_tx,
      current_stream_tx,
    }
  }
}

#[async_trait::async_trait]
impl<T> PlatformNetworkManager<T> for PlatformNetwork {
  async fn start_stream(
    &self,
    event_tx: Sender<StreamEvent>,
    _runtime: &T,
    _headers: &HashMap<&str, &str>,
  ) -> anyhow::Result<Box<dyn PlatformNetworkStream>> {
    self
      .start_stream_tx
      .send(())
      .await
      .map_err(|_| anyhow!("start stream"))?;

    *self.current_stream_tx.lock().unwrap() = Some(event_tx.clone());

    Ok(Box::new(Stream {
      _event_tx: event_tx,
      send_data_tx: self.send_data_tx.clone(),
    }))
  }
}

struct Stream {
  _event_tx: Sender<StreamEvent>,

  send_data_tx: Sender<()>,
}

#[async_trait::async_trait]
impl PlatformNetworkStream for Stream {
  async fn send_data(&mut self, _data: &[u8]) -> anyhow::Result<()> {
    self
      .send_data_tx
      .send(())
      .await
      .map_err(|_| anyhow!("start stream"))
  }
}

struct Setup {
  _sdk_directory: tempdir::TempDir,
  send_data_rx: Receiver<()>,
  start_stream_rx: Receiver<()>,
  shutdown_trigger: ComponentShutdownTrigger,
  collector: Collector,

  current_stream_tx: Arc<Mutex<Option<Sender<StreamEvent>>>>,
}

struct TestLog {}

impl bd_internal_logging::Logger for TestLog {
  fn log(&self, _level: LogLevel, _log_type: LogType, _msg: &str, _fields: LogFields) {}
}

impl Setup {
  fn new() -> Self {
    let sdk_directory = tempdir::TempDir::new("sdk").unwrap();

    let (start_stream_tx, start_stream_rx) = channel(1);
    let (send_data_tx, send_data_rx) = channel(1);
    let current_stream_tx = Arc::new(Mutex::new(None));
    let manager = Box::new(PlatformNetwork::new(
      start_stream_tx,
      send_data_tx,
      current_stream_tx.clone(),
    ));
    let shutdown_trigger = ComponentShutdownTrigger::default();
    let (_log_upload_tx, data_rx) = channel(1);
    let (trigger_upload_tx, _trigger_upload_rx) = channel(1);

    let collector = Collector::default();

    let api = Api::new(
      "api-key-test".to_string(),
      manager,
      shutdown_trigger.make_shutdown(),
      data_rx,
      trigger_upload_tx,
      Arc::new(EmptyMetadata),
      ConfigLoader::new(sdk_directory.path()),
      Vec::new(),
      Arc::new(TestLog {}),
      &collector.scope("api"),
    )
    .unwrap();

    tokio::task::spawn(api.start());

    Self {
      current_stream_tx,
      _sdk_directory: sdk_directory,
      start_stream_rx,
      send_data_rx,
      shutdown_trigger,
      collector,
    }
  }

  async fn handshake_response(&self) {
    let response = ApiResponse {
      response_type: Some(Response_type::Handshake(HandshakeResponse {
        stream_settings: None.into(),
        ..Default::default()
      })),
      ..Default::default()
    };

    self.send_response(response).await;
  }

  async fn send_response(&self, response: ApiResponse) {
    let tx = self
      .current_stream_tx
      .lock()
      .unwrap()
      .as_ref()
      .unwrap()
      .clone();

    let mut encoder = Encoder::new(None);
    let encoded = encoder.encode(&response);
    tx.send(StreamEvent::Data(encoded.to_vec())).await.unwrap();
  }

  async fn close_stream(&self) {
    let tx = self
      .current_stream_tx
      .lock()
      .unwrap()
      .as_ref()
      .unwrap()
      .clone();

    tx.send(StreamEvent::StreamClosed("test".to_string()))
      .await
      .unwrap();
  }

  async fn next_stream(&mut self, wait: Duration) -> bool {
    tokio::select! {
      _ = self.start_stream_rx.recv() => {},
      () = wait.sleep() => {
        return false;
      }
    };
    self.send_data_rx.recv().await.unwrap();

    true
  }
}

#[tokio::test(start_paused = true)]
async fn api_retry_stream() {
  let mut setup = Setup::new();

  // Since the backoff uses random values we have no control over, we loop multiple attempts
  // until it takes over a minute before we get a new stream. This demonstrates that the delay
  // grows past our initial delay of 500ms.
  while setup.next_stream(1.minutes()).await {
    setup.close_stream().await;
  }

  // At this point we've verified that we've taken over 60s to get a new stream. Now let a stream
  // finalize the handshake and then verify that it resets the interval.

  5.minutes().advance().await;

  assert!(setup.next_stream(1.seconds()).await);

  setup.handshake_response().await;
  setup.close_stream().await;

  assert!(setup.next_stream(1.seconds()).await);

  setup.shutdown_trigger.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn api_retry_stream_runtime_override() {
  let mut setup = Setup::new();

  assert!(setup.next_stream(1.seconds()).await);
  setup.handshake_response().await;

  setup
    .send_response(ApiResponse {
      response_type: Some(Response_type::RuntimeUpdate(RuntimeUpdate {
        version_nonce: "test".to_string(),
        runtime: Some(bd_test_helpers::runtime::make_proto(vec![(
          bd_runtime::runtime::api::MaxBackoffInterval::path(),
          bd_test_helpers::runtime::ValueKind::Int(1),
        )]))
        .into(),
        ..Default::default()
      })),
      ..Default::default()
    })
    .await;

  // Reconnect 10 times, asserting that it never takes more than 1s to connect. This proves that the
  // backoff never exceeds 1, per the runtime override.
  for _ in 0 .. 10 {
    setup.close_stream().await;
    setup.next_stream(1.seconds()).await;
  }

  setup.shutdown_trigger.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn error_response() {
  let mut setup = Setup::new();

  setup.next_stream(1.seconds()).await;
  setup.handshake_response().await;

  setup
    .send_response(ApiResponse {
      response_type: Some(Response_type::ErrorShutdown(ErrorShutdown {
        grpc_status: 1,
        grpc_message: "some message".to_string(),
        ..Default::default()
      })),
      ..Default::default()
    })
    .await;

  // Processing the error message has no side effects, so we just make sure that we process is to
  // provide code coverage. To do so, we close the stream and wait for the next one. Since the close
  // event is processed via the same channel as the response, we know that the response must have
  // been processed.
  setup.close_stream().await;
  setup.next_stream(1.seconds()).await;

  setup
    .collector
    .assert_counter_eq(1, "api:error_shutdown_total", labels! {});
}

#[tokio::test(start_paused = true)]
async fn error_response_before_handshake() {
  let mut setup = Setup::new();

  setup.next_stream(1.seconds()).await;

  setup
    .send_response(ApiResponse {
      response_type: Some(Response_type::ErrorShutdown(ErrorShutdown {
        grpc_status: 1,
        grpc_message: "some message".to_string(),
        ..Default::default()
      })),
      ..Default::default()
    })
    .await;

  // Processing the error message has no side effects, so we just make sure that we process is to
  // provide code coverage. To do so, we close the stream and wait for the next one. Since the close
  // event is processed via the same channel as the response, we know that the response must have
  // been processed.
  setup.close_stream().await;
  setup.next_stream(1.seconds()).await;

  setup
    .collector
    .assert_counter_eq(1, "api:error_shutdown_total", labels! {});
  setup
    .collector
    .assert_counter_eq(1, "api:remote_connect_failure", labels! {});
}

#[tokio::test(start_paused = true)]
async fn unauthenticated_response_before_handshake() {
  let mut setup = Setup::new();

  setup.next_stream(1.seconds()).await;

  setup
    .send_response(ApiResponse {
      response_type: Some(Response_type::ErrorShutdown(ErrorShutdown {
        grpc_status: 16,
        grpc_message: "some message".to_string(),
        ..Default::default()
      })),
      ..Default::default()
    })
    .await;

  // Processing the error message has no side effects, so we just make sure that we process is to
  // provide code coverage. To do so, we close the stream and wait for the next one. Since the close
  // event is processed via the same channel as the response, we know that the response must have
  // been processed.
  setup.close_stream().await;
  setup.next_stream(1.seconds()).await;

  setup
    .collector
    .assert_counter_eq(0, "api:error_shutdown_total", labels! {});
}
