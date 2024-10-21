// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::generated::proto::test::{EchoRequest, EchoResponse};
use crate::{
  make_server_streaming_router,
  make_unary_router,
  new_grpc_response,
  Client,
  Code,
  Compression,
  Error,
  Handler,
  Result,
  ServerStreamingHandler,
  ServiceMethod,
  Status,
  StreamStats,
  StreamingApi,
  StreamingApiSender,
};
use assert_matches::assert_matches;
use async_trait::async_trait;
use axum::body::Body;
use axum::extract::Request;
use axum::routing::post;
use axum::Router;
use bd_grpc_codec::stats::DeferredCounter;
use bd_grpc_codec::OptimizeFor;
use bd_server_stats::stats::CounterWrapper;
use bd_server_stats::test::util::stats::{self, Helper};
use bd_time::TimeDurationExt;
use futures::poll;
use http::{Extensions, HeaderMap};
use http_body_util::StreamBody;
use prometheus::labels;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use time::ext::NumericalDuration;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;

#[ctor::ctor]
fn test_global_init() {
  bd_test_helpers::test_global_init();
}

#[test]
fn deferred_counter_stats() {
  let mut stats = DeferredCounter::default();

  stats.inc_by(20);

  let counter = Helper::new().collector().scope("test").counter("test");
  stats.initialize(CounterWrapper::make_dyn(counter.clone()));

  assert_eq!(counter.get(), 20);

  stats.inc_by(5);

  assert_eq!(counter.get(), 25);

  stats.inc();

  assert_eq!(counter.get(), 26);
}

#[test]
fn invalid_response_header() {
  let status = Status::new(Code::InvalidArgument, "\nhello");
  let response = status.into_response();
  assert_eq!(response.headers().get("grpc-message").unwrap(), "%0Ahello");
}

#[test]
fn invalid_address() {
  assert_eq!(
    Client::new_http("github.com:22/foo", 1.milliseconds(), 1024)
      .unwrap_err()
      .to_string(),
    anyhow::anyhow!("extra path parameter not supported in address: /foo").to_string()
  );

  assert_eq!(
    Client::new_http("", 1.milliseconds(), 1024)
      .unwrap_err()
      .to_string(),
    anyhow::anyhow!("invalid format").to_string()
  );

  assert_eq!(
    Client::new_http("/?foo=bar", 1.milliseconds(), 1024)
      .unwrap_err()
      .to_string(),
    anyhow::anyhow!("invalid format").to_string()
  );

  assert_eq!(
    Client::new_http("github.com?foo=bar", 1.milliseconds(), 1024)
      .unwrap_err()
      .to_string(),
    anyhow::anyhow!("extra query parameter not supported in address").to_string()
  );
}

#[tokio::test]
#[ignore]
// TODO(mattklein123): This test is flaky. It is ignored for now until we can figure out why.
async fn connect_timeout() {
  let client =
    Client::new_http("github.com:22".to_string().as_str(), 1.milliseconds(), 1024).unwrap();
  assert_matches!(
    client
      .unary(
        &ServiceMethod::<EchoRequest, EchoResponse>::new("Test", "Echo"),
        None,
        EchoRequest::default(),
        10.seconds(),
        Compression::None
      )
      .await,
    Err(Error::ConnectionTimeout)
  );
}

struct EchoHandler {
  do_sleep: bool,
}

#[async_trait]
impl Handler<EchoRequest, EchoResponse> for EchoHandler {
  async fn handle(
    &self,
    _headers: HeaderMap,
    _extensions: Extensions,
    _request: EchoRequest,
  ) -> Result<EchoResponse> {
    if self.do_sleep {
      10.seconds().sleep().await;
    }

    Ok(EchoResponse::default())
  }
}

#[async_trait]
impl ServerStreamingHandler<EchoResponse, EchoRequest> for EchoHandler {
  async fn stream(
    &self,
    _headers: HeaderMap,
    _extensions: Extensions,
    _request: EchoRequest,
    sender: &mut StreamingApiSender<EchoResponse>,
  ) -> Result<()> {
    if self.do_sleep {
      10.seconds().sleep().await;
    }

    sender.send(EchoResponse::default()).await.unwrap();

    Ok(())
  }
}

struct ErrorHandler {}

#[async_trait]
impl Handler<EchoRequest, EchoResponse> for ErrorHandler {
  async fn handle(
    &self,
    _headers: HeaderMap,
    _extensions: Extensions,
    _request: EchoRequest,
  ) -> Result<EchoResponse> {
    Err(crate::Error::Grpc(crate::Status::new(
      crate::Code::Internal,
      "foo",
    )))
  }
}

#[async_trait]
impl ServerStreamingHandler<EchoResponse, EchoRequest> for ErrorHandler {
  async fn stream(
    &self,
    _headers: HeaderMap,
    _extensions: Extensions,
    _request: EchoRequest,
    _sender: &mut StreamingApiSender<EchoResponse>,
  ) -> Result<()> {
    Err(crate::Error::Grpc(crate::Status::new(
      crate::Code::Internal,
      "foo",
    )))
  }
}

#[tokio::test]
async fn unary_error_handler() {
  let error_counter = prometheus::IntCounter::new("error", "-").unwrap();
  let called = Arc::new(AtomicBool::new(false));
  let called_clone = called.clone();
  let router = make_unary_router(
    &ServiceMethod::<EchoRequest, EchoResponse>::new("Test", "Echo"),
    Arc::new(ErrorHandler {}),
    move |e| {
      assert_matches!(e, crate::Error::Grpc(_));
      called_clone.store(true, Ordering::SeqCst);
    },
    error_counter,
    true,
  );
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let local_address = listener.local_addr().unwrap();
  let server = axum::serve(listener, router.into_make_service());
  tokio::spawn(async { server.await.unwrap() });
  let client = Client::new_http(local_address.to_string().as_str(), 1.minutes(), 1024).unwrap();
  assert_matches!(
    client
      .unary(
        &ServiceMethod::<EchoRequest, EchoResponse>::new("Test", "Echo"),
        None,
        EchoRequest::default(),
        1.seconds(),
        Compression::None,
      )
      .await,
    Err(Error::Grpc(_))
  );
  assert!(called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn read_stop() {
  let (read_stop_tx, read_stop_rx) = watch::channel(false);
  let (api_tx, mut api_rx) = mpsc::channel(1);

  let router = Router::new().route(
    &ServiceMethod::<EchoRequest, EchoResponse>::new("Test", "Echo").full_path,
    post(move |request: Request| async move {
      let (parts, body) = request.into_parts();
      let (response_sender, response_body) = mpsc::channel(1);
      let response = new_grpc_response(
        Body::new(StreamBody::new(ReceiverStream::new(response_body))),
        None,
      );
      let api = StreamingApi::<EchoResponse, EchoRequest>::new(
        response_sender,
        parts.headers,
        body,
        false,
        None,
        OptimizeFor::Memory,
        Some(read_stop_rx),
      );
      api_tx.send(api).await.unwrap();
      response
    }),
  );
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let local_address = listener.local_addr().unwrap();
  let server = axum::serve(listener, router.into_make_service());
  tokio::spawn(async { server.await.unwrap() });
  let client = Client::new_http(local_address.to_string().as_str(), 1.minutes(), 1024).unwrap();
  let mut stream = client
    .streaming(
      &ServiceMethod::<EchoRequest, EchoResponse>::new("Test", "Echo"),
      None,
      true,
      None,
      OptimizeFor::Memory,
    )
    .await
    .unwrap();
  stream.send(EchoRequest::default()).await.unwrap();
  let mut api = api_rx.recv().await.unwrap();
  assert_eq!(
    EchoRequest::default(),
    api.next().await.unwrap().unwrap()[0]
  );

  {
    // Start the next future and poll it once. It should be waiting for a frame.
    let next_future = api.next();
    tokio::pin!(next_future);
    assert!(poll!(&mut next_future).is_pending());
    // Set to read stop and then poll again. It should now be waiting for the read stop to release.
    read_stop_tx.send(true).unwrap();
    assert!(poll!(&mut next_future).is_pending());
    // Send a new message. We should not get anything back.
    stream.send(EchoRequest::default()).await.unwrap();
    assert!(poll!(&mut next_future).is_pending());
    // Clear the read stop and poll again. We should now get the new message.
    read_stop_tx.send(false).unwrap();
    assert_eq!(
      EchoRequest::default(),
      next_future.await.unwrap().unwrap()[0]
    );
  }

  // Write a message and set read stop and make sure that we go into immediate read stop.
  stream.send(EchoRequest::default()).await.unwrap();
  read_stop_tx.send(true).unwrap();
  let next_future = api.next();
  tokio::pin!(next_future);
  assert!(poll!(&mut next_future).is_pending());
}

#[tokio::test]
async fn server_streaming() {
  let stats_helper = stats::Helper::new();
  let stream_stats = StreamStats::new(&stats_helper.collector().scope("streams"), "foo");

  let router = make_server_streaming_router(
    &ServiceMethod::<EchoRequest, EchoResponse>::new("Test", "Echo"),
    Arc::new(EchoHandler { do_sleep: false }),
    |_| {},
    stream_stats,
    true,
    None,
  );
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let local_address = listener.local_addr().unwrap();
  let server = axum::serve(listener, router.into_make_service());
  tokio::spawn(async { server.await.unwrap() });
  let client = Client::new_http(local_address.to_string().as_str(), 1.minutes(), 1024).unwrap();
  let mut stream = client
    .server_streaming(
      &ServiceMethod::<EchoRequest, EchoResponse>::new("Test", "Echo"),
      None,
      EchoRequest::default(),
      false,
      OptimizeFor::Memory,
    )
    .await
    .unwrap();

  assert!(stream.next().await.is_ok());
  assert!(stream.next().await.is_ok());

  stats_helper.assert_counter_eq(1, "streams:foo:stream_initiations_total", &labels! {});
  stats_helper.assert_counter_eq(
    1,
    "streams:foo:stream_completions_total",
    &labels! { "result" => "success" },
  );
  stats_helper.assert_counter_eq(1, "streams:foo:stream_tx_messages_total", &labels! {});
  stats_helper.assert_counter_eq(5, "streams:foo:bandwidth_tx_bytes_total", &labels! {});
  stats_helper.assert_counter_eq(
    5,
    "streams:foo:bandwidth_tx_bytes_uncompressed_total",
    &labels! {},
  );
}

#[tokio::test]
async fn server_streaming_error_handler() {
  let stats_helper = stats::Helper::new();
  let stream_stats = StreamStats::new(&stats_helper.collector().scope("streams"), "foo");

  let called = Arc::new(AtomicBool::new(false));
  let called_clone = called.clone();
  let router = make_server_streaming_router(
    &ServiceMethod::<EchoRequest, EchoResponse>::new("Test", "Echo"),
    Arc::new(ErrorHandler {}),
    move |e| {
      assert_matches!(e, crate::Error::Grpc(_));
      called_clone.store(true, Ordering::SeqCst);
    },
    stream_stats,
    false,
    None,
  );
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let local_address = listener.local_addr().unwrap();
  let server = axum::serve(listener, router.into_make_service());
  tokio::spawn(async { server.await.unwrap() });
  let client = Client::new_http(local_address.to_string().as_str(), 1.minutes(), 1024).unwrap();

  let mut streaming = client
    .server_streaming(
      &ServiceMethod::<EchoRequest, EchoResponse>::new("Test", "Echo"),
      None,
      EchoRequest::default(),
      false,
      OptimizeFor::Cpu,
    )
    .await
    .unwrap();

  assert_matches!(streaming.next().await,
    Err(Error::Grpc(status)) =>
    {
        assert_eq!(status.code, Code::Internal);
        assert_eq!(status.message.as_deref(), Some("foo"));
    }
  );

  assert!(called.load(Ordering::SeqCst));

  stats_helper.assert_counter_eq(1, "streams:foo:stream_initiations_total", &labels! {});
  stats_helper.assert_counter_eq(
    1,
    "streams:foo:stream_completions_total",
    &labels! { "result" => "failure" },
  );
  stats_helper.assert_counter_eq(0, "streams:foo:stream_tx_messages_total", &labels! {});
  stats_helper.assert_counter_eq(0, "streams:foo:bandwidth_tx_bytes_total", &labels! {});
  stats_helper.assert_counter_eq(
    0,
    "streams:foo:bandwidth_tx_bytes_uncompressed_total",
    &labels! {},
  );
}

#[tokio::test]
async fn request_timeout() {
  let error_counter = prometheus::IntCounter::new("error", "-").unwrap();
  let router = make_unary_router(
    &ServiceMethod::<EchoRequest, EchoResponse>::new("Test", "Echo"),
    Arc::new(EchoHandler { do_sleep: true }),
    |_| {},
    error_counter,
    true,
  );
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let local_address = listener.local_addr().unwrap();
  let server = axum::serve(listener, router.into_make_service());
  tokio::spawn(async { server.await.unwrap() });
  let client = Client::new_http(local_address.to_string().as_str(), 1.minutes(), 1024).unwrap();
  assert_matches!(
    client
      .unary(
        &ServiceMethod::<EchoRequest, EchoResponse>::new("Test", "Echo"),
        None,
        EchoRequest::default(),
        1.milliseconds(),
        Compression::None,
      )
      .await,
    Err(Error::RequestTimeout)
  );
}

#[tokio::test]
async fn snappy_compression() {
  let error_counter = prometheus::IntCounter::new("error", "-").unwrap();
  let router = make_unary_router(
    &ServiceMethod::<EchoRequest, EchoResponse>::new("Test", "Echo"),
    Arc::new(EchoHandler { do_sleep: false }),
    |_| {},
    error_counter,
    true,
  );
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let local_address = listener.local_addr().unwrap();
  let server = axum::serve(listener, router.into_make_service());
  tokio::spawn(async { server.await.unwrap() });
  let client = Client::new_http(local_address.to_string().as_str(), 1.minutes(), 1024).unwrap();
  assert_eq!(
    client
      .unary(
        &ServiceMethod::<EchoRequest, EchoResponse>::new("Test", "Echo"),
        None,
        EchoRequest::default(),
        10.seconds(),
        Compression::Snappy,
      )
      .await
      .unwrap(),
    EchoResponse::default()
  );
}
