// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::client::{AddressHelper, Client};
use crate::compression::{Compression, ConnectSafeCompressionLayer};
use crate::connect_protocol::ConnectProtocolType;
use crate::generated::proto::test::{EchoRequest, EchoResponse};
use crate::stats::EndpointStats;
use crate::{
  CONNECT_PROTOCOL_VERSION,
  CONTENT_TYPE,
  CONTENT_TYPE_PROTO,
  Code,
  Error,
  Handler,
  Result,
  ServerStreamingHandler,
  ServiceMethod,
  Status,
  StreamingApi,
  StreamingApiSender,
  make_server_streaming_router,
  make_unary_router,
  new_grpc_response,
};
use assert_matches::assert_matches;
use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::extract::Request;
use axum::routing::post;
use bd_grpc_codec::stats::DeferredCounter;
use bd_grpc_codec::{DecodingResult, OptimizeFor};
use bd_server_stats::stats::CounterWrapper;
use bd_server_stats::test::util::stats::{self, Helper};
use bd_time::TimeDurationExt;
use bytes::Bytes;
use futures::poll;
use http::{Extensions, HeaderMap};
use http_body_util::StreamBody;
use parking_lot::Mutex;
use prometheus::labels;
use protobuf::{Message, MessageFull};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use time::ext::NumericalDuration;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;

#[ctor::ctor]
fn test_global_init() {
  bd_test_helpers::test_global_init();
}

fn service_method() -> ServiceMethod<EchoRequest, EchoResponse> {
  ServiceMethod::<EchoRequest, EchoResponse>::new("Test", "Echo")
}

async fn make_unary_server(
  handler: Arc<dyn Handler<EchoRequest, EchoResponse>>,
  error_handler: impl Fn(&crate::Error) + Clone + Send + Sync + 'static,
  endpoint_stats: Option<&EndpointStats>,
) -> SocketAddr {
  let router = make_unary_router(
    &service_method(),
    handler,
    error_handler,
    endpoint_stats,
    true,
  )
  .layer(ConnectSafeCompressionLayer::new());
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let local_address = listener.local_addr().unwrap();
  let server = axum::serve(listener, router.into_make_service());
  tokio::spawn(async { server.await.unwrap() });
  local_address
}

async fn make_server_streaming_server(
  handler: Arc<dyn ServerStreamingHandler<EchoResponse, EchoRequest> + 'static>,
  error_handler: impl Fn(&crate::Error) + Clone + Send + Sync + 'static,
) -> (SocketAddr, stats::Helper) {
  let stats_helper = stats::Helper::new();
  let endpoint_stats = EndpointStats::new(stats_helper.collector().scope("test"));
  let router = make_server_streaming_router(
    &service_method(),
    handler,
    error_handler,
    Some(&endpoint_stats),
    true,
    None,
  )
  .layer(ConnectSafeCompressionLayer::new());
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let local_address = listener.local_addr().unwrap();
  let server = axum::serve(listener, router.into_make_service());
  tokio::spawn(async { server.await.unwrap() });
  (local_address, stats_helper)
}

//
// ConnectMessageOrEndOfStream
//

enum ConnectMessageOrEndOfStream<MessageType> {
  Message(MessageType),
  ConnectEndOfStream(Bytes),
}

impl<MessageType: MessageFull> DecodingResult for ConnectMessageOrEndOfStream<MessageType> {
  type Message = MessageType;

  fn from_flags_and_bytes(flags: u8, bytes: Bytes) -> bd_grpc_codec::Result<Self> {
    if flags == 0x2 {
      Ok(Self::ConnectEndOfStream(bytes))
    } else {
      Ok(Self::Message(Message::parse_from_bytes(&bytes)?))
    }
  }

  fn message(&self) -> Option<&MessageType> {
    match self {
      Self::Message(message) => Some(message),
      Self::ConnectEndOfStream(_) => None,
    }
  }
}

impl<MessageType: MessageFull> ConnectMessageOrEndOfStream<MessageType> {
  const fn end_of_stream(&self) -> Option<&Bytes> {
    match self {
      Self::ConnectEndOfStream(bytes) => Some(bytes),
      Self::Message(_) => None,
    }
  }
}

//
// EchoHandler
//

#[derive(Default)]
struct EchoHandler {
  do_sleep: bool,
  streaming_event_sender: Mutex<Option<mpsc::Receiver<StreamingTestEvent>>>,
}
enum StreamingTestEvent {
  Message(EchoResponse),
  EndStreamOk,
  EndStreamError(Status),
}

#[async_trait]
impl Handler<EchoRequest, EchoResponse> for EchoHandler {
  async fn handle(
    &self,
    _headers: HeaderMap,
    _extensions: Extensions,
    request: EchoRequest,
  ) -> Result<EchoResponse> {
    if self.do_sleep {
      10.seconds().sleep().await;
    }

    Ok(EchoResponse {
      echo: request.echo,
      ..Default::default()
    })
  }
}

#[async_trait]
impl ServerStreamingHandler<EchoResponse, EchoRequest> for EchoHandler {
  async fn stream(
    &self,
    _headers: HeaderMap,
    _extensions: Extensions,
    request: EchoRequest,
    sender: &mut StreamingApiSender<EchoResponse>,
  ) -> Result<()> {
    if self.do_sleep {
      10.seconds().sleep().await;
    }

    sender
      .send(EchoResponse {
        echo: request.echo,
        ..Default::default()
      })
      .await
      .unwrap();

    if let Some(mut event_rx) = { self.streaming_event_sender.lock().take() } {
      while let Some(event) = event_rx.recv().await {
        match event {
          StreamingTestEvent::Message(message) => {
            sender.send(message).await.unwrap();
          },
          StreamingTestEvent::EndStreamOk => {
            return Ok(());
          },
          StreamingTestEvent::EndStreamError(status) => {
            return Err(crate::Error::Grpc(status));
          },
        }
      }
    }

    Ok(())
  }
}

//
// ErrorHandler
//

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
    anyhow::anyhow!("empty string").to_string()
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
#[ignore = "TODO(mattklein123): This test is flaky. It is ignored for now until we can figure out \
            why."]
async fn connect_timeout() {
  let client =
    Client::new_http("github.com:22".to_string().as_str(), 1.milliseconds(), 1024).unwrap();
  assert_matches!(
    client
      .unary(
        &service_method(),
        None,
        EchoRequest::default(),
        10.seconds(),
        Compression::None
      )
      .await,
    Err(Error::ConnectionTimeout)
  );
}

#[tokio::test]
async fn unary_compression() {
  let stats = Helper::new();
  let endpoints_stats = EndpointStats::new(stats.collector().scope("test"));
  let local_address = make_unary_server(
    Arc::new(EchoHandler::default()),
    |_| {},
    Some(&endpoints_stats),
  )
  .await;
  let client = Client::new_http(local_address.to_string().as_str(), 1.minutes(), 1024).unwrap();
  let response = client
    .unary(
      &service_method(),
      None,
      EchoRequest {
        echo: "a".repeat(1000),
        ..Default::default()
      },
      1.seconds(),
      Compression::GRpc(bd_grpc_codec::Compression::StatelessZlib { level: 3 }),
    )
    .await;
  assert_eq!(response.unwrap().echo, "a".repeat(1000));
  stats.assert_counter_eq(
    1,
    "test:rpc",
    &labels! {
      "service" => "test_Test",
      "endpoint" => "Echo",
      "result" => "success"
    },
  );
}

#[tokio::test]
async fn unary_error_handler() {
  let stats = Helper::new();
  let endpoints_stats = EndpointStats::new(stats.collector().scope("test"));
  let called = Arc::new(AtomicBool::new(false));
  let called_clone = called.clone();
  let local_address = make_unary_server(
    Arc::new(ErrorHandler {}),
    move |e| {
      assert_matches!(e, crate::Error::Grpc(_));
      called_clone.store(true, Ordering::SeqCst);
    },
    Some(&endpoints_stats),
  )
  .await;
  let client = Client::new_http(local_address.to_string().as_str(), 1.minutes(), 1024).unwrap();
  assert_matches!(
    client
      .unary(
        &service_method(),
        None,
        EchoRequest::default(),
        1.seconds(),
        Compression::None,
      )
      .await,
    Err(Error::Grpc(_))
  );
  assert!(called.load(Ordering::SeqCst));
  stats.assert_counter_eq(
    1,
    "test:rpc",
    &labels! {
      "service" => "test_Test",
      "endpoint" => "Echo",
      "result" => "failure"
    },
  );
}

#[tokio::test]
async fn read_stop() {
  let (read_stop_tx, read_stop_rx) = watch::channel(false);
  let (api_tx, mut api_rx) = mpsc::channel(1);

  let router = Router::new().route(
    &service_method().full_path(),
    post(move |request: Request| async move {
      let (parts, body) = request.into_parts();
      let (response_sender, response_body) = mpsc::channel(1);
      let response = new_grpc_response(
        Body::new(StreamBody::new(ReceiverStream::new(response_body))),
        None,
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
      &service_method(),
      None,
      vec![],
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
async fn streaming_with_initial_requests() {
  let (api_tx, mut api_rx) = mpsc::channel(1);

  let router = Router::new().route(
    &service_method().full_path(),
    post(move |request: Request| async move {
      let (parts, body) = request.into_parts();
      let (response_sender, response_body) = mpsc::channel(1);
      let response = new_grpc_response(
        Body::new(StreamBody::new(ReceiverStream::new(response_body))),
        None,
        None,
      );
      let api = StreamingApi::<EchoResponse, EchoRequest>::new(
        response_sender,
        parts.headers,
        body,
        false,
        None,
        OptimizeFor::Memory,
        None,
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

  // Create initial requests to send before waiting for headers.
  let initial_request_1 = EchoRequest {
    echo: "first".to_string(),
    ..Default::default()
  };
  let initial_request_2 = EchoRequest {
    echo: "second".to_string(),
    ..Default::default()
  };

  let mut stream = client
    .streaming(
      &service_method(),
      None,
      vec![initial_request_1.clone(), initial_request_2.clone()],
      true,
      None,
      OptimizeFor::Memory,
    )
    .await
    .unwrap();

  // The server should receive the two initial requests without needing to wait for any additional
  // sends.
  let mut api = api_rx.recv().await.unwrap();

  // Verify the first initial request was received.
  let messages = api.next().await.unwrap().unwrap();
  assert_eq!(1, messages.len());
  assert_eq!(initial_request_1.echo, messages[0].echo);

  // Verify the second initial request was received.
  let messages = api.next().await.unwrap().unwrap();
  assert_eq!(1, messages.len());
  assert_eq!(initial_request_2.echo, messages[0].echo);

  // Now send an additional message after the stream is established.
  let additional_request = EchoRequest {
    echo: "third".to_string(),
    ..Default::default()
  };
  stream.send(additional_request.clone()).await.unwrap();

  // Verify the additional request was received.
  let messages = api.next().await.unwrap().unwrap();
  assert_eq!(1, messages.len());
  assert_eq!(additional_request.echo, messages[0].echo);
}

#[tokio::test]
async fn server_streaming() {
  let (local_address, stats_helper) =
    make_server_streaming_server(Arc::new(EchoHandler::default()), |_| {}).await;
  let client = Client::new_http(local_address.to_string().as_str(), 1.minutes(), 1024).unwrap();
  let mut stream = client
    .server_streaming::<EchoRequest, EchoResponse>(
      &service_method(),
      None,
      EchoRequest {
        echo: "a".repeat(1000),
        ..Default::default()
      },
      false,
      OptimizeFor::Memory,
      None,
    )
    .await
    .unwrap();

  assert!(stream.next().await.is_ok());
  assert!(stream.next().await.is_ok());

  stats_helper.assert_counter_eq(
    1,
    "test:stream_initiations_total",
    &labels! {
      "service" => "test_Test",
      "endpoint" => "Echo",
    },
  );
  stats_helper.assert_counter_eq(
    1,
    "test:rpc",
    &labels! {
      "service" => "test_Test",
      "endpoint" => "Echo",
      "result" => "success"
    },
  );
  stats_helper.assert_counter_eq(
    1,
    "test:stream_tx_messages_total",
    &labels! {
      "service" => "test_Test",
      "endpoint" => "Echo",
    },
  );
  stats_helper.assert_counter_eq(
    1008,
    "test:bandwidth_tx_bytes_total",
    &labels! {
      "service" => "test_Test",
      "endpoint" => "Echo",
    },
  );
  stats_helper.assert_counter_eq(
    1008,
    "test:bandwidth_tx_bytes_uncompressed_total",
    &labels! {
      "service" => "test_Test",
      "endpoint" => "Echo",
    },
  );
}

#[tokio::test]
async fn connect_server_streaming() {
  let (event_tx, event_rx) = mpsc::channel(1);
  let (local_address, _) = make_server_streaming_server(
    Arc::new(EchoHandler {
      do_sleep: false,
      streaming_event_sender: Mutex::new(Some(event_rx)),
    }),
    |_| {},
  )
  .await;
  let client = Client::new_http(local_address.to_string().as_str(), 1.minutes(), 1024).unwrap();
  let mut stream = client
    .server_streaming::<EchoRequest, ConnectMessageOrEndOfStream<EchoResponse>>(
      &service_method(),
      None,
      EchoRequest {
        echo: "a".repeat(1000),
        ..Default::default()
      },
      false,
      OptimizeFor::Memory,
      Some(ConnectProtocolType::Streaming),
    )
    .await
    .unwrap();

  assert_eq!(
    stream.next().await.unwrap().unwrap()[0].message().unwrap(),
    &EchoResponse {
      echo: "a".repeat(1000),
      ..Default::default()
    }
  );
  event_tx
    .send(StreamingTestEvent::Message(EchoResponse::default()))
    .await
    .unwrap();
  assert_eq!(
    stream.next().await.unwrap().unwrap()[0].message().unwrap(),
    &EchoResponse::default()
  );
  event_tx
    .send(StreamingTestEvent::EndStreamOk)
    .await
    .unwrap();
  assert_eq!(
    stream.next().await.unwrap().unwrap()[0]
      .end_of_stream()
      .unwrap(),
    "{}",
  );
  assert!(stream.next().await.unwrap().is_none());
}

#[tokio::test]
async fn connect_server_streaming_error() {
  let (event_tx, event_rx) = mpsc::channel(1);
  let (local_address, _) = make_server_streaming_server(
    Arc::new(EchoHandler {
      do_sleep: false,
      streaming_event_sender: Mutex::new(Some(event_rx)),
    }),
    |_| {},
  )
  .await;
  let client = Client::new_http(local_address.to_string().as_str(), 1.minutes(), 1024).unwrap();
  let mut stream = client
    .server_streaming::<EchoRequest, ConnectMessageOrEndOfStream<EchoResponse>>(
      &service_method(),
      None,
      EchoRequest {
        echo: "a".repeat(1000),
        ..Default::default()
      },
      false,
      OptimizeFor::Memory,
      Some(ConnectProtocolType::Streaming),
    )
    .await
    .unwrap();

  assert_eq!(
    stream.next().await.unwrap().unwrap()[0].message().unwrap(),
    &EchoResponse {
      echo: "a".repeat(1000),
      ..Default::default()
    }
  );
  event_tx
    .send(StreamingTestEvent::EndStreamError(Status::new(
      Code::Internal,
      "foo",
    )))
    .await
    .unwrap();
  assert_eq!(
    stream.next().await.unwrap().unwrap()[0]
      .end_of_stream()
      .unwrap(),
    "{\"error\":{\"code\":\"internal\",\"message\":\"foo\"}}",
  );
  assert!(stream.next().await.unwrap().is_none());
}

#[tokio::test]
async fn server_streaming_error_handler() {
  let called = Arc::new(AtomicBool::new(false));
  let called_clone = called.clone();
  let (local_address, stats_helper) =
    make_server_streaming_server(Arc::new(ErrorHandler {}), move |e| {
      assert_matches!(e, crate::Error::Grpc(_));
      called_clone.store(true, Ordering::SeqCst);
    })
    .await;
  let client = Client::new_http(local_address.to_string().as_str(), 1.minutes(), 1024).unwrap();

  let mut streaming = client
    .server_streaming::<EchoRequest, EchoResponse>(
      &service_method(),
      None,
      EchoRequest {
        echo: "a".repeat(1000),
        ..Default::default()
      },
      false,
      OptimizeFor::Cpu,
      None,
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

  stats_helper.assert_counter_eq(
    1,
    "test:stream_initiations_total",
    &labels! {
      "service" => "test_Test",
      "endpoint" => "Echo",
    },
  );
  stats_helper.assert_counter_eq(
    1,
    "test:rpc",
    &labels! {
      "service" => "test_Test",
      "endpoint" => "Echo",
      "result" => "failure"
    },
  );
  stats_helper.assert_counter_eq(
    0,
    "test:stream_tx_messages_total",
    &labels! {
      "service" => "test_Test",
      "endpoint" => "Echo",
    },
  );
  stats_helper.assert_counter_eq(
    0,
    "test:bandwidth_tx_bytes_total",
    &labels! {
      "service" => "test_Test",
      "endpoint" => "Echo",
    },
  );
  stats_helper.assert_counter_eq(
    0,
    "test:bandwidth_tx_bytes_uncompressed_total",
    &labels! {
      "service" => "test_Test",
      "endpoint" => "Echo",
    },
  );
}

#[tokio::test]
async fn request_timeout() {
  let local_address = make_unary_server(
    Arc::new(EchoHandler {
      do_sleep: true,
      streaming_event_sender: Mutex::default(),
    }),
    |_| {},
    None,
  )
  .await;
  let client = Client::new_http(local_address.to_string().as_str(), 1.minutes(), 1024).unwrap();
  assert_matches!(
    client
      .unary(
        &service_method(),
        None,
        EchoRequest {
          echo: "a".repeat(1000),
          ..Default::default()
        },
        1.milliseconds(),
        Compression::None,
      )
      .await,
    Err(Error::RequestTimeout)
  );
}

#[tokio::test]
async fn snappy_compression() {
  let local_address = make_unary_server(Arc::new(EchoHandler::default()), |_| {}, None).await;
  let client = Client::new_http(local_address.to_string().as_str(), 1.minutes(), 1024).unwrap();
  assert_eq!(
    client
      .unary(
        &service_method(),
        None,
        EchoRequest {
          echo: "a".repeat(1000),
          ..Default::default()
        },
        10.seconds(),
        Compression::Snappy,
      )
      .await
      .unwrap(),
    EchoResponse {
      echo: "a".repeat(1000),
      ..Default::default()
    }
  );
}

#[tokio::test]
async fn connect_unary_error() {
  let local_address = make_unary_server(Arc::new(ErrorHandler {}), |_| {}, None).await;
  let client = reqwest::Client::builder().deflate(false).build().unwrap();
  let address = AddressHelper::new(format!("http://{local_address}")).unwrap();
  let response = client
    .post(address.build(&service_method()).to_string())
    .header(CONTENT_TYPE, CONTENT_TYPE_PROTO)
    .header(CONNECT_PROTOCOL_VERSION, "1")
    .body(
      EchoRequest {
        echo: "a".repeat(1024),
        ..Default::default()
      }
      .write_to_bytes()
      .unwrap(),
    )
    .send()
    .await
    .unwrap();
  assert_eq!(response.status(), 500);
  assert_eq!(
    response.bytes().await.unwrap(),
    "{\"code\":\"internal\",\"message\":\"foo\"}"
  );

  let response = client
    .post(address.build(&service_method()).to_string())
    .header(CONTENT_TYPE, CONTENT_TYPE_PROTO)
    .header(CONNECT_PROTOCOL_VERSION, "1")
    .body(EchoRequest::default().write_to_bytes().unwrap())
    .send()
    .await
    .unwrap();
  assert_eq!(response.status(), 400);
  assert_eq!(
    response.bytes().await.unwrap(),
    "{\"code\":\"invalid_argument\",\"message\":\"Invalid request: A proto validation error \
     occurred: field 'test.EchoRequest.echo' in message 'test.EchoRequest' requires string length \
     >= 1\"}"
  );
}

#[tokio::test]
async fn connect_unary() {
  let local_address = make_unary_server(Arc::new(EchoHandler::default()), |_| {}, None).await;

  // Should not compress.
  let client = reqwest::Client::builder().deflate(false).build().unwrap();
  let address = AddressHelper::new(format!("http://{local_address}")).unwrap();
  let response = client
    .post(address.build(&service_method()).to_string())
    .header(CONTENT_TYPE, CONTENT_TYPE_PROTO)
    .header(CONNECT_PROTOCOL_VERSION, "1")
    .body(
      EchoRequest {
        echo: "a".repeat(1024),
        ..Default::default()
      }
      .write_to_bytes()
      .unwrap(),
    )
    .send()
    .await
    .unwrap();
  assert_eq!(response.status(), 200);
  let response = response.bytes().await.unwrap();
  assert_eq!(
    EchoResponse::parse_from_bytes(&response).unwrap(),
    EchoResponse {
      echo: "a".repeat(1024),
      ..Default::default()
    }
  );

  // Should compress.
  let client = reqwest::Client::builder().deflate(true).build().unwrap();
  let address = AddressHelper::new(format!("http://{local_address}")).unwrap();
  let response = client
    .post(address.build(&service_method()).to_string())
    .header(CONTENT_TYPE, CONTENT_TYPE_PROTO)
    .header(CONNECT_PROTOCOL_VERSION, "1")
    .body(
      EchoRequest {
        echo: "a".repeat(1024),
        ..Default::default()
      }
      .write_to_bytes()
      .unwrap(),
    )
    .send()
    .await
    .unwrap();
  assert_eq!(response.status(), 200);
  let response = response.bytes().await.unwrap();
  assert_eq!(
    EchoResponse::parse_from_bytes(&response).unwrap(),
    EchoResponse {
      echo: "a".repeat(1024),
      ..Default::default()
    }
  );
}
