// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./hyper_test.rs"]
mod hyper_test;

use bd_api::{PlatformNetworkManager, PlatformNetworkStream};
use bd_error_reporter::reporter::{Reporter, handle_unexpected};
use bd_shutdown::ComponentShutdown;
use bytes::Bytes;
use http::{Method, Request};
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use std::collections::HashMap;
use std::convert::Infallible;
use tokio::sync::mpsc::{self, Sender, channel};
use tokio_stream::wrappers::ReceiverStream;

type BoxBody = http_body_util::combinators::BoxBody<Bytes, Infallible>;
type BodySender = mpsc::Sender<std::result::Result<Frame<Bytes>, Infallible>>;

/// An implementation of `bd_api::PlatformNetworkManager` using hyper, for use in native contexts.
#[derive(Debug)]
pub struct HyperNetwork {
  stream_event_rx: mpsc::Receiver<NewStream>,
  uri: String,
  shutdown: ComponentShutdown,
}

struct NewStream {
  event_tx: Sender<bd_api::StreamEvent>,
  headers: HashMap<String, String>,
  data_rx: mpsc::Receiver<Vec<u8>>,
}

/// The handle used to interface with the active hyper task.
#[derive(Clone)]
pub struct Handle {
  new_stream_tx: mpsc::Sender<NewStream>,
}

impl HyperNetwork {
  /// Runs the network on a dedicated thread, returning a Handle that can be used to interface with
  /// the running network processing loop.
  #[must_use]
  pub fn run_on_thread(address: &str, shutdown: ComponentShutdown) -> Handle {
    let (network, handle) = Self::new(address, shutdown);

    std::thread::spawn(|| {
      handle_unexpected(
        tokio::runtime::Builder::new_current_thread()
          .enable_all()
          .build()
          .unwrap()
          .block_on(network.start()),
        "single threaded hyper loop",
      );
    });

    handle
  }

  #[must_use]
  pub fn new(address: &str, shutdown: ComponentShutdown) -> (Self, Handle) {
    let (stream_event_tx, stream_event_rx) = channel(1);

    let uri = format!("{address}/bitdrift_public.protobuf.client.v1.ApiService/Mux");

    (
      Self {
        stream_event_rx,
        uri,
        shutdown,
      },
      Handle {
        new_stream_tx: stream_event_tx,
      },
    )
  }

  /// Called to start the processing loop.
  pub async fn start(mut self) -> anyhow::Result<()> {
    log::debug!("initializing hyper networking");

    let client = Client::builder(TokioExecutor::new())
      .http2_only(true)
      .build(make_tls_connector());

    loop {
      // Loop until we get a new start stream event. There might be stale data left from an
      // previous stream, so this serves to clear out the channel of events that
      // corresponded to the previous stream.
      log::debug!("awaiting next stream");
      let new_stream = {
        let event = tokio::select! {
          event = self.stream_event_rx.recv() => event,
          () = self.shutdown.cancelled() => return Ok(()),
        };

        match event {
          Some(new_stream) => new_stream,
          // Channel has been closed, so we must be shutting down.
          None => return Ok(()),
        }
      };

      let event_tx = new_stream.event_tx.clone();

      let Some(event) = self.process_stream(&client, new_stream).await else {
        return Ok(());
      };

      if event_tx.send(event).await.is_err() {
        // Channel has shut down on us, we must be shutting down.
        log::debug!("event channel closed, shutting down hyper networking");
        return Ok(());
      }
    }
  }

  async fn process_stream(
    &mut self,
    client: &Client<HttpsConnector<HttpConnector>, BoxBody>,
    mut stream: NewStream,
  ) -> Option<bd_api::StreamEvent> {
    log::debug!("received request for new stream: {}", self.uri.as_str());

    let (body_sender, request) = Self::create_request(self.uri.as_str(), stream.headers);

    let initial_data = tokio::select! {
      data = stream.data_rx.recv() => data,
      () = self.shutdown.cancelled() => {
        log::debug!("received shutdown signal, shutting down hyper networking");
        return None;
      }
    };

    body_sender
      .send(Ok(Frame::data(initial_data?.into())))
      .await
      .ok()?;

    // TODO(snowp): Should this also be gated on the shutdown hook?
    let mut response = match client.request(request).await {
      Ok(response) => response,
      Err(e) => {
        log::debug!("failed to connect to server: {e}");

        return Some(bd_api::StreamEvent::StreamClosed(
          "failed to connect".to_string(),
        ));
      },
    };

    if response.status() != 200 {
      log::debug!(
        "closing stream due to bad response status: {}",
        response.status()
      );

      return Some(bd_api::StreamEvent::StreamClosed(
        "bad response status".to_string(),
      ));
    }

    if let Some(grpc_status) = response.headers().get("grpc-status")
      && grpc_status != "0"
    {
      // TODO(snowp): The grpc-message is url encoded, it would be nice to decode it.
      log::debug!(
        "closing stream due to bad response grpc-status {:?}, messsage: {:?}",
        std::str::from_utf8(grpc_status.as_bytes()).unwrap_or_default(),
        response.headers().get("grpc-message")
      );

      return Some(bd_api::StreamEvent::StreamClosed(
        "bad grpc response code".to_string(),
      ));
    }

    loop {
      // Handle either an inbound message from the server or an outbound message from the api mux.
      let maybe_event = tokio::select! {
        data = stream.data_rx.recv() => {
          // Short circuit if stream.data_rx has been closed - this indicates shutdown or that the
          // stream has been dropped.
          Self::handle_outbound_data(&body_sender, data?).await
        },
        frame = response.body_mut().frame() => {
          Some(Self::handle_inbound_data(frame))
        }
        () = self.shutdown.cancelled() => {
          log::debug!("received shutdown signal, shutting down hyper networking");
          return None;
        }
      };

      // We may need to process a stream event in response to the above call.
      let Some(event) = maybe_event else {
        continue;
      };

      // Forward any data directly via the event_tx so we can continue this stream, if we end up
      // with a StreamClosed then we bubble that up.
      match event {
        bd_api::StreamEvent::Data(data) => {
          if stream
            .event_tx
            .send(bd_api::StreamEvent::Data(data))
            .await
            .is_err()
          {
            return None;
          }
        },
        bd_api::StreamEvent::StreamClosed(closed) => {
          return Some(bd_api::StreamEvent::StreamClosed(closed));
        },
      }
    }
  }

  /// Handles outbound data received over the stream event channel from the API mux. Returns true
  /// if the stream should be closed.
  async fn handle_outbound_data(
    body_sender: &BodySender,
    data: Vec<u8>,
  ) -> Option<bd_api::StreamEvent> {
    if body_sender
      .send(Ok(Frame::data(data.into())))
      .await
      .is_err()
    {
      // We failed to send data over the body channel, the stream must have closed.
      // Signal that the stream closed.
      return Some(bd_api::StreamEvent::StreamClosed(
        "upstream close".to_string(),
      ));
    }

    None
  }

  /// Handles inbound data arriving via the hyper response channel. Returns true if the stream
  /// should be closed.
  fn handle_inbound_data(
    frame: Option<std::result::Result<Frame<Bytes>, hyper::Error>>,
  ) -> bd_api::StreamEvent {
    // Stream is closing, signal close and go back to waiting for a new stream.
    let Some(Ok(frame)) = frame else {
      return bd_api::StreamEvent::StreamClosed("upstream closed stream".to_string());
    };
    if frame.is_trailers() {
      // TODO(mattklein123): Do something with this?
      return bd_api::StreamEvent::StreamClosed("upstream closed stream".to_string());
    }

    bd_api::StreamEvent::Data(frame.into_data().unwrap().into())
  }

  fn create_request(uri: &str, headers: HashMap<String, String>) -> (BodySender, Request<BoxBody>) {
    let (tx, rx) = mpsc::channel(1);
    let mut builder = Request::builder().method(Method::POST).uri(uri);

    for (key, value) in headers {
      builder = builder.header(key, value);
    }
    let result = builder.body(StreamBody::new(ReceiverStream::new(rx)).boxed());

    (tx, result.unwrap())
  }
}

#[async_trait::async_trait]
impl<T> PlatformNetworkManager<T> for Handle {
  async fn start_stream(
    &self,
    event_tx: Sender<bd_api::StreamEvent>,
    _runtime: &T,
    headers: &HashMap<&str, &str>,
  ) -> anyhow::Result<Box<dyn PlatformNetworkStream>> {
    log::debug!("hyper starting new stream");

    let headers = headers
      .iter()
      .map(|(&k, &v)| (k.to_string(), v.to_string()))
      .collect();

    let (data_tx, data_rx) = mpsc::channel(1);

    let new_stream = NewStream {
      event_tx: event_tx.clone(),
      headers,
      data_rx,
    };

    self.new_stream_tx.send(new_stream).await.unwrap();

    Ok(Box::new(StreamHandle { data_tx }))
  }
}

struct StreamHandle {
  data_tx: mpsc::Sender<Vec<u8>>,
}

#[async_trait::async_trait]
impl PlatformNetworkStream for StreamHandle {
  async fn send_data(&mut self, data: &[u8]) -> anyhow::Result<()> {
    let _ignored = self.data_tx.send(data.to_vec()).await;

    Ok(())
  }
}

//
// ErrorPayload
//

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ErrorPayload {
  message: String,
  details: Option<String>,
}

pub type ErrorReport = (ErrorPayload, HashMap<String, String>);

//
// ErrorReporter
//

/// An implementation of `bd_client_common::error::Reporter` that sends errors to the API. In order
/// to provide a sync API, this is split into a handle and the actual implementation. The
/// implementation must be started on a tokio runtime in order to process the reports sent via the
/// handle.
pub struct ErrorReporter {
  api_address: String,
  api_key: String,
  client: Client<HttpsConnector<HttpConnector>, String>,
  rx: tokio::sync::mpsc::Receiver<ErrorReport>,
}

impl ErrorReporter {
  #[must_use]
  pub fn new(api_address: String, api_key: String) -> (Self, ErrorReporterHandle) {
    let client = Client::builder(TokioExecutor::new())
      .http2_only(true)
      .build(make_tls_connector());

    // Give the channel a buffer of 5 so we can send multiple errors without dropping errors.
    let (tx, rx) = tokio::sync::mpsc::channel(5);
    (
      Self {
        api_address,
        api_key,
        client,
        rx,
      },
      ErrorReporterHandle(tx),
    )
  }

  pub async fn start(mut self) {
    loop {
      let Some((payload, fields)) = self.rx.recv().await else {
        log::debug!("error reporter shutting down");
        return;
      };

      if let Err(e) = self.send_error(payload, &fields).await {
        log::error!("failed to send error report: {e:?}");
      }
    }
  }

  async fn send_error(
    &self,
    payload: ErrorPayload,
    headers: &HashMap<String, String>,
  ) -> anyhow::Result<()> {
    let uri = format!("{}/v1/sdk-errors", self.api_address);
    let mut request = Request::builder()
      .method(Method::POST)
      .uri(uri)
      .header("content-type", "application/json")
      .header("x-bitdrift-api-key", &self.api_key);

    for (k, v) in headers {
      request = request.header(k, v);
    }

    self
      .client
      .request(request.body(serde_json::to_string(&payload)?)?)
      .await?;

    Ok(())
  }
}

pub struct ErrorReporterHandle(tokio::sync::mpsc::Sender<(ErrorPayload, HashMap<String, String>)>);

impl Reporter for ErrorReporterHandle {
  fn report(
    &self,
    message: &str,
    details: &Option<String>,
    fields: &HashMap<std::borrow::Cow<'_, str>, std::borrow::Cow<'_, str>>,
  ) {
    let payload = ErrorPayload {
      message: message.to_string(),
      details: details.clone(),
    };

    if let Err(e) = self.0.try_send((
      payload,
      fields
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect(),
    )) {
      log::error!("failed to send error report: {e:?}");
    }
  }
}

fn make_tls_connector() -> HttpsConnector<HttpConnector> {
  let mut connector = HttpConnector::new();
  connector.enforce_http(false);

  HttpsConnectorBuilder::new()
    .with_webpki_roots()
    .https_or_http()
    .enable_http2()
    .wrap_connector(connector)
}

#[cfg(test)]
#[ctor::ctor]
fn init_logger() {
  bd_test_helpers::test_global_init();
}
