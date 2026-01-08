// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{ErrorPayload, HyperNetwork};
use assert_matches::assert_matches;
use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use bd_api::PlatformNetworkManager;
use bd_error_reporter::reporter::Reporter;
use bd_shutdown::{ComponentShutdown, ComponentShutdownTrigger};
use http::HeaderMap;
use std::collections::HashMap;
use std::net::TcpListener;

#[tokio::test]
async fn connect_failure() {
  let shutdown = ComponentShutdownTrigger::default();
  let (network, handle) = HyperNetwork::new("http://localhost:1234", shutdown.make_shutdown());

  tokio::spawn(network.start());

  let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(1);
  let mut stream = handle
    .start_stream(event_tx, &(), &HashMap::new())
    .await
    .unwrap();

  stream.send_data(b"hello").await.unwrap();

  assert_matches!(
    event_rx.recv().await,
    Some(bd_api::StreamEvent::StreamClosed(_))
  );
}

#[test]
fn error_reporter() {
  let mut test_server = TestServer::new();

  let (reporter, handle) =
    super::ErrorReporter::new(test_server.address.clone(), "api-key".to_string()).unwrap();

  std::thread::spawn(move || {
    tokio::runtime::Runtime::new()
      .unwrap()
      .block_on(reporter.start());
  });

  handle.report(
    "foo",
    &Some("other".to_string()),
    &[("x-header".into(), "value".into())].into(),
  );

  let reported_error = test_server.rx.blocking_recv().unwrap();

  assert_eq!(reported_error.message, "foo");
  assert_eq!(reported_error.details, Some("other".to_string()));
  assert_eq!(
    reported_error.headers.get("x-header"),
    Some(&"value".to_string())
  );
}

struct ReportedError {
  message: String,
  details: Option<String>,
  headers: HashMap<String, String>,
}

struct TestServer {
  address: String,
  _shutdown: ComponentShutdownTrigger,
  rx: tokio::sync::mpsc::Receiver<ReportedError>,
}

impl TestServer {
  fn new() -> Self {
    // Bind to a random port.
    let listener = TcpListener::bind("localhost:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let shutdown_trigger = ComponentShutdownTrigger::default();

    let (tx, rx) = tokio::sync::mpsc::channel(1);

    let shutdown = shutdown_trigger.make_shutdown();

    std::thread::spawn(move || {
      tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(Self::start_server(listener, tx, shutdown));
    });

    Self {
      address: format!("http://{address}"),
      rx,
      _shutdown: shutdown_trigger,
    }
  }

  async fn start_server(
    listener: TcpListener,
    tx: tokio::sync::mpsc::Sender<ReportedError>,
    mut shutdown: ComponentShutdown,
  ) {
    listener.set_nonblocking(true).unwrap();
    axum::serve(
      tokio::net::TcpListener::from_std(listener).unwrap(),
      Router::new()
        .route("/v1/sdk-errors", post(handler))
        .with_state(tx),
    )
    .with_graceful_shutdown(async move { shutdown.cancelled().await })
    .await
    .unwrap();
  }
}

async fn handler(
  State(tx): State<tokio::sync::mpsc::Sender<ReportedError>>,
  headers: HeaderMap,
  payload: Json<ErrorPayload>,
) {
  assert_eq!(
    headers.get("x-bitdrift-api-key"),
    Some(&"api-key".parse().unwrap())
  );

  tx.send(ReportedError {
    message: payload.0.message,
    details: payload.0.details.clone(),
    headers: headers
      .iter()
      .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap().to_string()))
      .collect(),
  })
  .await
  .unwrap();
}
