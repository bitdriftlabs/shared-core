// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::client::{AddressHelper, Client};
use crate::compression::{Compression, ConnectSafeCompressionLayer};
use crate::connect_protocol::ConnectProtocolType;
use crate::generated::proto::test::{
  EchoRepeatedResponse,
  EchoRequest,
  EchoResponse,
  UnsupportedRequest,
};
use crate::stats::EndpointStats;
use crate::{
  CONNECT_PROTOCOL_VERSION,
  CONTENT_TYPE,
  CONTENT_TYPE_JSON,
  CONTENT_TYPE_PROTO,
  CONTENT_TYPE_CONNECT_STREAMING,
  BidiStreamingHandler,
  Code,
  DEFAULT_MAX_UNARY_REQUEST_BYTES,
  Error,
  Handler,
  Result,
  ServerStreamingHandler,
  ServiceMethod,
  Status,
  StreamingApi,
  StreamingApiSender,
  UnaryRequestConfig,
  make_bidi_streaming_router,
  make_server_streaming_router,
  make_unary_router_with_config,
  make_unary_router_with_response_mutator,
  make_unary_router_with_response_mutator_and_route_layer,
  new_grpc_response,
};
use assert_matches::assert_matches;
use async_trait::async_trait;
use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::Request;
use axum::middleware::{Next, from_fn};
use axum::routing::post;
use bd_grpc_codec::stats::DeferredCounter;
use bd_grpc_codec::{DecodingResult, OptimizeFor};
use bd_pgv::proto_validate::{ProtoNameMode, ValidationOptions};
use bd_server_stats::stats::CounterWrapper;
use bd_server_stats::test::util::stats::{self, Helper};
use bd_time::TimeDurationExt;
use bytes::Bytes;
use futures::poll;
use http::{Extensions, HeaderMap, StatusCode};
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

fn repeated_service_method() -> ServiceMethod<EchoRequest, EchoRepeatedResponse> {
  ServiceMethod::<EchoRequest, EchoRepeatedResponse>::new("Test", "EchoRepeated")
}

fn unsupported_service_method() -> ServiceMethod<UnsupportedRequest, EchoResponse> {
  ServiceMethod::<UnsupportedRequest, EchoResponse>::new("Test", "UnsupportedEcho")
}

async fn make_unary_server(
  handler: Arc<dyn Handler<EchoRequest, EchoResponse>>,
  error_handler: impl Fn(&crate::Error) + Clone + Send + Sync + 'static,
  endpoint_stats: Option<&EndpointStats>,
) -> SocketAddr {
  make_unary_server_with_response_mutator(handler, error_handler, endpoint_stats, None).await
}

async fn make_unary_server_with_response_mutator(
  handler: Arc<dyn Handler<EchoRequest, EchoResponse>>,
  error_handler: impl Fn(&crate::Error) + Clone + Send + Sync + 'static,
  endpoint_stats: Option<&EndpointStats>,
  response_mutator: Option<crate::UnaryResponseMutator<EchoResponse>>,
) -> SocketAddr {
  make_unary_server_with_request_config(
    handler,
    error_handler,
    endpoint_stats,
    UnaryRequestConfig::default(),
    response_mutator,
  )
  .await
}

async fn make_unary_server_with_request_config(
  handler: Arc<dyn Handler<EchoRequest, EchoResponse>>,
  error_handler: impl Fn(&crate::Error) + Clone + Send + Sync + 'static,
  endpoint_stats: Option<&EndpointStats>,
  request_config: UnaryRequestConfig,
  response_mutator: Option<crate::UnaryResponseMutator<EchoResponse>>,
) -> SocketAddr {
  let router = make_unary_router_with_response_mutator(
    &service_method(),
    handler,
    endpoint_stats,
    true,
    request_config,
    response_mutator,
    error_handler,
  )
  .unwrap()
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
  .unwrap()
  .layer(ConnectSafeCompressionLayer::new());
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let local_address = listener.local_addr().unwrap();
  let server = axum::serve(listener, router.into_make_service());
  tokio::spawn(async { server.await.unwrap() });
  (local_address, stats_helper)
}

async fn make_bidi_streaming_server(
  handler: Arc<dyn BidiStreamingHandler<EchoResponse, EchoRequest> + 'static>,
  error_handler: impl Fn(&crate::Error) + Clone + Send + Sync + 'static,
) -> SocketAddr {
  let router = make_bidi_streaming_router(
    &service_method(),
    handler,
    error_handler,
    None,
    true,
    None,
  )
  .unwrap()
  .layer(ConnectSafeCompressionLayer::new());
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let local_address = listener.local_addr().unwrap();
  let server = axum::serve(listener, router.into_make_service());
  tokio::spawn(async { server.await.unwrap() });
  local_address
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

#[derive(Default)]
struct EchoRepeatedHandler;

#[derive(Default)]
struct UnsupportedHandler;

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
impl Handler<EchoRequest, EchoRepeatedResponse> for EchoRepeatedHandler {
  async fn handle(
    &self,
    _headers: HeaderMap,
    _extensions: Extensions,
    _request: EchoRequest,
  ) -> Result<EchoRepeatedResponse> {
    Ok(EchoRepeatedResponse::default())
  }
}

#[async_trait]
impl Handler<UnsupportedRequest, EchoResponse> for UnsupportedHandler {
  async fn handle(
    &self,
    _headers: HeaderMap,
    _extensions: Extensions,
    request: UnsupportedRequest,
  ) -> Result<EchoResponse> {
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

#[async_trait]
impl ServerStreamingHandler<EchoResponse, UnsupportedRequest> for UnsupportedHandler {
  async fn stream(
    &self,
    _headers: HeaderMap,
    _extensions: Extensions,
    request: UnsupportedRequest,
    sender: &mut StreamingApiSender<EchoResponse>,
  ) -> Result<()> {
    sender
      .send(EchoResponse {
        echo: request.echo,
        ..Default::default()
      })
      .await
      .unwrap();
    Ok(())
  }
}

#[async_trait]
impl BidiStreamingHandler<EchoResponse, EchoRequest> for BidiEchoHandler {
  async fn stream(
    &self,
    _headers: HeaderMap,
    _extensions: Extensions,
    api: &mut StreamingApi<EchoResponse, EchoRequest>,
  ) -> Result<()> {
    while let Some(requests) = api.next().await? {
      for request in requests {
        api
          .send(EchoResponse {
            echo: request.echo,
            ..Default::default()
          })
          .await?;
      }
    }

    Ok(())
  }
}

//
// ErrorHandler
//

struct ErrorHandler {}

struct BidiEchoHandler;

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

//
// EncodedErrorHandler
//

struct EncodedErrorHandler {}

#[async_trait]
impl Handler<EchoRequest, EchoResponse> for EncodedErrorHandler {
  async fn handle(
    &self,
    _headers: HeaderMap,
    _extensions: Extensions,
    _request: EchoRequest,
  ) -> Result<EchoResponse> {
    Err(crate::Error::Grpc(crate::Status::new(
      crate::Code::InvalidArgument,
      "line 1\nline 2 % value",
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

//
// ExtensionInspectingHandler
//

#[derive(Default)]
struct ExtensionInspectingHandler {
  grpc_method_path: Arc<Mutex<Option<String>>>,
}

#[async_trait]
impl Handler<EchoRequest, EchoResponse> for ExtensionInspectingHandler {
  async fn handle(
    &self,
    _headers: HeaderMap,
    extensions: Extensions,
    request: EchoRequest,
  ) -> Result<EchoResponse> {
    let grpc_method = extensions.get::<crate::GrpcMethod>().cloned();
    *self.grpc_method_path.lock() =
      grpc_method.map(|grpc_method| grpc_method.full_path().to_string());

    Ok(EchoResponse {
      echo: request.echo,
      ..Default::default()
    })
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

  let round_tripped_status = Status::from_headers(response.headers());
  assert_eq!(round_tripped_status.message, Some("\nhello".to_string()));
}

#[tokio::test]
async fn request_aware_status_uses_json_error_shape_for_json_requests() {
  let status = Status::new(Code::PermissionDenied, "nope");
  let headers = HeaderMap::from_iter([(CONTENT_TYPE, CONTENT_TYPE_JSON.parse().unwrap())]);
  let response = status.into_response_for_request(&headers);

  assert_eq!(response.status(), StatusCode::FORBIDDEN);
  assert_eq!(
    response.headers().get(CONTENT_TYPE).unwrap(),
    CONTENT_TYPE_JSON
  );
  let body: serde_json::Value =
    serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
  assert_eq!(
    body,
    serde_json::json!({ "code": "permission_denied", "message": "nope" })
  );
}

#[tokio::test]
async fn request_aware_status_uses_json_error_shape_for_connect_requests() {
  let status = Status::new(Code::ResourceExhausted, "slow down");
  let mut headers = HeaderMap::from_iter([(CONTENT_TYPE, CONTENT_TYPE_PROTO.parse().unwrap())]);
  headers.insert(
    http::header::HeaderName::from_static(CONNECT_PROTOCOL_VERSION),
    http::HeaderValue::from_static("1"),
  );
  let response = status.into_response_for_request(&headers);

  assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
  assert_eq!(
    response.headers().get(CONTENT_TYPE).unwrap(),
    CONTENT_TYPE_JSON
  );
  let body: serde_json::Value =
    serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
  assert_eq!(
    body,
    serde_json::json!({ "code": "resource_exhausted", "message": "slow down" })
  );
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
        EchoRequest {
          echo: "ok".to_string(),
          ..Default::default()
        },
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
async fn unary_routers_insert_grpc_method_metadata_for_handlers() {
  let handler = Arc::new(ExtensionInspectingHandler::default());
  let grpc_method_path = handler.grpc_method_path.clone();
  let router = make_unary_router_with_response_mutator(
    &service_method(),
    handler,
    None,
    true,
    UnaryRequestConfig::default(),
    None,
    |_| {},
  )
  .unwrap()
  .layer(ConnectSafeCompressionLayer::new());
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let local_address = listener.local_addr().unwrap();
  let server = axum::serve(listener, router.into_make_service());
  tokio::spawn(async { server.await.unwrap() });

  let client = Client::new_http(local_address.to_string().as_str(), 1.minutes(), 1024).unwrap();
  let response = client
    .unary(
      &service_method(),
      None,
      EchoRequest {
        echo: "ok".to_string(),
        ..Default::default()
      },
      1.seconds(),
      Compression::None,
    )
    .await
    .unwrap();

  assert_eq!(response.echo, "ok");
  assert_eq!(
    grpc_method_path.lock().clone().unwrap(),
    service_method().full_path()
  );
}

#[tokio::test]
async fn unary_route_layers_can_observe_grpc_request_metadata() {
  let middleware_grpc_method_path = Arc::new(Mutex::new(None));
  let middleware_request_transport = Arc::new(Mutex::new(None));
  let route_layer = {
    let middleware_grpc_method_path = middleware_grpc_method_path.clone();
    let middleware_request_transport = middleware_request_transport.clone();
    from_fn(move |request: Request, next: Next| {
      let middleware_grpc_method_path = middleware_grpc_method_path.clone();
      let middleware_request_transport = middleware_request_transport.clone();
      async move {
        *middleware_grpc_method_path.lock() = request
          .extensions()
          .get::<crate::GrpcMethod>()
          .map(|grpc_method| grpc_method.full_path().to_string());
        *middleware_request_transport.lock() = request
          .extensions()
          .get::<crate::status::RequestTransport>()
          .map(|request_transport| {
            (
              request_transport.connect_protocol().is_some(),
              request_transport.json_transcoding(),
            )
          });
        next.run(request).await
      }
    })
  };
  let router = make_unary_router_with_response_mutator_and_route_layer(
    &service_method(),
    Arc::new(EchoHandler::default()),
    None,
    true,
    UnaryRequestConfig::default(),
    None,
    route_layer,
    |_| {},
  )
  .unwrap()
  .layer(ConnectSafeCompressionLayer::new());
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let local_address = listener.local_addr().unwrap();
  let server = axum::serve(listener, router.into_make_service());
  tokio::spawn(async { server.await.unwrap() });

  let client = Client::new_http(local_address.to_string().as_str(), 1.minutes(), 1024).unwrap();
  let response = client
    .unary(
      &service_method(),
      None,
      EchoRequest {
        echo: "ok".to_string(),
        ..Default::default()
      },
      1.seconds(),
      Compression::None,
    )
    .await
    .unwrap();

  assert_eq!(response.echo, "ok");
  assert_eq!(
    middleware_grpc_method_path.lock().clone().unwrap(),
    service_method().full_path()
  );
  assert_eq!(
    (*middleware_request_transport.lock()).unwrap(),
    (false, false)
  );
}

#[tokio::test]
async fn unary_error_handler_decodes_grpc_message() {
  let local_address = make_unary_server(Arc::new(EncodedErrorHandler {}), |_| {}, None).await;
  let client = Client::new_http(local_address.to_string().as_str(), 1.minutes(), 1024).unwrap();
  assert_matches!(
    client
      .unary(
        &service_method(),
        None,
        EchoRequest {
          echo: "ok".to_string(),
          ..Default::default()
        },
        1.seconds(),
        Compression::None,
      )
      .await,
    Err(Error::Grpc(Status {
      code: Code::InvalidArgument,
      message
    })) if message == Some("line 1\nline 2 % value".to_string())
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
async fn bidi_streaming() {
  let local_address = make_bidi_streaming_server(Arc::new(BidiEchoHandler), |_| {}).await;
  let client = Client::new_http(local_address.to_string().as_str(), 1.minutes(), 1024).unwrap();
  let mut stream = client
    .streaming(
      &service_method(),
      None,
      vec![EchoRequest {
        echo: "hello".to_string(),
        ..Default::default()
      }],
      true,
      None,
      OptimizeFor::Memory,
    )
    .await
    .unwrap();

  assert_eq!(
    stream.next().await.unwrap().unwrap()[0],
    EchoResponse {
      echo: "hello".to_string(),
      ..Default::default()
    }
  );
}

#[tokio::test]
async fn connect_bidi_streaming() {
  let local_address = make_bidi_streaming_server(Arc::new(BidiEchoHandler), |_| {}).await;
  let client = reqwest::Client::builder().deflate(false).build().unwrap();
  let address = AddressHelper::new(format!("http://{local_address}")).unwrap();
  let mut encoder = crate::Encoder::new(None);
  let request_body = encoder
    .encode(&EchoRequest {
      echo: "hello".to_string(),
      ..Default::default()
    })
    .unwrap();

  let response = client
    .post(address.build(&service_method()).to_string())
    .header(CONTENT_TYPE, CONTENT_TYPE_CONNECT_STREAMING)
    .header(CONNECT_PROTOCOL_VERSION, "1")
    .body(request_body)
    .send()
    .await
    .unwrap();
  assert_eq!(response.status(), 200);
  assert_eq!(
    response
      .headers()
      .get(CONTENT_TYPE)
      .unwrap()
      .to_str()
      .unwrap(),
    CONTENT_TYPE_CONNECT_STREAMING
  );

  let response_body = response.bytes().await.unwrap();
  let mut decoder =
    bd_grpc_codec::Decoder::<ConnectMessageOrEndOfStream<EchoResponse>>::new(None, OptimizeFor::Cpu);
  let frames = decoder.decode_data(&response_body).unwrap();

  assert_eq!(frames[0].message().unwrap().echo, "hello");
  assert_eq!(frames[1].end_of_stream().unwrap(), "{}");
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
async fn connect_unary_error_uses_package_relative_proto_names() {
  let local_address = make_unary_server_with_request_config(
    Arc::new(EchoHandler::default()),
    |_| {},
    None,
    UnaryRequestConfig {
      validation_options: ValidationOptions {
        proto_name_mode: ProtoNameMode::PackageRelative,
      },
      ..Default::default()
    },
    None,
  )
  .await;
  let client = reqwest::Client::builder().deflate(false).build().unwrap();
  let address = AddressHelper::new(format!("http://{local_address}")).unwrap();
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
     occurred: field 'EchoRequest.echo' in message 'EchoRequest' requires string length >= 1\"}"
  );
}

#[test]
fn make_unary_router_rejects_unsupported_pgv_validation() {
  assert_matches!(
    make_unary_router_with_response_mutator(
      &unsupported_service_method(),
      Arc::new(UnsupportedHandler),
      None,
      true,
      UnaryRequestConfig::default(),
      None,
      |_| {},
    ),
    Err(Error::ProtoValidation(bd_pgv::error::Error::ProtoValidation(message)))
      if message == "not implemented: string rules max_bytes"
  );
}

#[test]
fn make_unary_router_allows_unsupported_pgv_when_validation_disabled() {
  assert!(
    make_unary_router_with_response_mutator(
      &unsupported_service_method(),
      Arc::new(UnsupportedHandler),
      None,
      false,
      UnaryRequestConfig::default(),
      None,
      |_| {},
    )
    .is_ok()
  );
}

#[test]
fn make_server_streaming_router_rejects_unsupported_pgv_validation() {
  assert_matches!(
    make_server_streaming_router(
      &unsupported_service_method(),
      Arc::new(UnsupportedHandler),
      |_| {},
      None,
      true,
      None,
    ),
    Err(Error::ProtoValidation(bd_pgv::error::Error::ProtoValidation(message)))
      if message == "not implemented: string rules max_bytes"
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

#[tokio::test]
async fn unary_json_transcoding() {
  let local_address = make_unary_server(Arc::new(EchoHandler::default()), |_| {}, None).await;
  let client = reqwest::Client::builder().deflate(false).build().unwrap();
  let address = AddressHelper::new(format!("http://{local_address}")).unwrap();
  let response = client
    .post(address.build(&service_method()).to_string())
    .header(CONTENT_TYPE, CONTENT_TYPE_JSON)
    .body("{\"echo\":\"json_echo\"}")
    .send()
    .await
    .unwrap();
  assert_eq!(response.status(), 200);
  assert_eq!(
    response.headers().get(CONTENT_TYPE).unwrap(),
    CONTENT_TYPE_JSON
  );
  let body: serde_json::Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
  assert_eq!(body, serde_json::json!({ "echo": "json_echo" }));
}

#[tokio::test]
async fn unary_json_transcoding_applies_response_mutator() {
  let local_address = make_unary_server_with_response_mutator(
    Arc::new(EchoHandler::default()),
    |_| {},
    None,
    Some(Arc::new(|response: &mut EchoResponse| {
      response.echo = "mutated".to_string();
      Ok(())
    })),
  )
  .await;
  let client = reqwest::Client::builder().deflate(false).build().unwrap();
  let address = AddressHelper::new(format!("http://{local_address}")).unwrap();
  let response = client
    .post(address.build(&service_method()).to_string())
    .header(CONTENT_TYPE, CONTENT_TYPE_JSON)
    .body("{\"echo\":\"json_echo\"}")
    .send()
    .await
    .unwrap();
  assert_eq!(response.status(), 200);
  assert_eq!(
    response.headers().get(CONTENT_TYPE).unwrap(),
    CONTENT_TYPE_JSON
  );
  let body: serde_json::Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
  assert_eq!(body, serde_json::json!({ "echo": "mutated" }));
}

#[tokio::test]
async fn unary_json_transcoding_omits_empty_repeated_fields() {
  let router = make_unary_router_with_response_mutator(
    &repeated_service_method(),
    Arc::new(EchoRepeatedHandler),
    None,
    true,
    UnaryRequestConfig::default(),
    None,
    |_| {},
  )
  .unwrap()
  .layer(ConnectSafeCompressionLayer::new());
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let local_address = listener.local_addr().unwrap();
  let server = axum::serve(listener, router.into_make_service());
  tokio::spawn(async { server.await.unwrap() });

  let client = reqwest::Client::builder().deflate(false).build().unwrap();
  let address = AddressHelper::new(format!("http://{local_address}")).unwrap();
  let response = client
    .post(address.build(&repeated_service_method()).to_string())
    .header(CONTENT_TYPE, CONTENT_TYPE_JSON)
    .body("{\"echo\":\"json_echo\"}")
    .send()
    .await
    .unwrap();
  assert_eq!(response.status(), 200);
  assert_eq!(
    response.headers().get(CONTENT_TYPE).unwrap(),
    CONTENT_TYPE_JSON
  );
  let body: serde_json::Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
  assert_eq!(body, serde_json::json!({}));
}

#[tokio::test]
async fn unary_json_transcoding_returns_json_error_body() {
  let local_address = make_unary_server(Arc::new(ErrorHandler {}), |_| {}, None).await;
  let client = reqwest::Client::builder().deflate(false).build().unwrap();
  let address = AddressHelper::new(format!("http://{local_address}")).unwrap();
  let response = client
    .post(address.build(&service_method()).to_string())
    .header(CONTENT_TYPE, CONTENT_TYPE_JSON)
    .body("{\"echo\":\"json_echo\"}")
    .send()
    .await
    .unwrap();

  assert_eq!(response.status(), 500);
  assert_eq!(
    response.headers().get(CONTENT_TYPE).unwrap(),
    CONTENT_TYPE_JSON
  );
  let body: serde_json::Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
  assert_eq!(
    body,
    serde_json::json!({ "code": "internal", "message": "foo" })
  );
}

#[tokio::test]
async fn unary_json_transcoding_decode_errors_return_json_error_body() {
  let local_address = make_unary_server(Arc::new(EchoHandler::default()), |_| {}, None).await;
  let client = reqwest::Client::builder().deflate(false).build().unwrap();
  let address = AddressHelper::new(format!("http://{local_address}")).unwrap();
  let response = client
    .post(address.build(&service_method()).to_string())
    .header(CONTENT_TYPE, CONTENT_TYPE_JSON)
    .body("{\"echo\":1}")
    .send()
    .await
    .unwrap();

  assert_eq!(response.status(), 400);
  assert_eq!(
    response.headers().get(CONTENT_TYPE).unwrap(),
    CONTENT_TYPE_JSON
  );
  let body: serde_json::Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
  assert_eq!(body["code"], "invalid_argument");
  assert!(
    body["message"]
      .as_str()
      .unwrap()
      .starts_with("Invalid request:")
  );
}

#[tokio::test]
async fn unary_request_over_limit_returns_resource_exhausted() {
  let router = make_unary_router_with_config(
    &service_method(),
    Arc::new(EchoHandler::default()),
    |_| {},
    None,
    true,
    UnaryRequestConfig::default(),
  )
  .unwrap()
  .layer(ConnectSafeCompressionLayer::new());
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let local_address = listener.local_addr().unwrap();
  let server = axum::serve(listener, router.into_make_service());
  tokio::spawn(async { server.await.unwrap() });

  let client = Client::new_http(&local_address.to_string(), 10.seconds(), 1).unwrap();
  let request = EchoRequest {
    echo: "x".repeat(DEFAULT_MAX_UNARY_REQUEST_BYTES + 1024),
    ..Default::default()
  };

  assert_matches!(
    client
      .unary(
        &service_method(),
        None,
        request,
        10.seconds(),
        Compression::None,
      )
      .await,
    Err(Error::Grpc(Status { code: Code::ResourceExhausted, message: Some(message) }))
      if message == format!(
        "Request body exceeds {DEFAULT_MAX_UNARY_REQUEST_BYTES} byte limit"
      )
  );
}

#[tokio::test]
async fn unary_request_under_limit_succeeds() {
  let router = make_unary_router_with_config(
    &service_method(),
    Arc::new(EchoHandler::default()),
    |_| {},
    None,
    true,
    UnaryRequestConfig::default(),
  )
  .unwrap()
  .layer(ConnectSafeCompressionLayer::new());
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let local_address = listener.local_addr().unwrap();
  let server = axum::serve(listener, router.into_make_service());
  tokio::spawn(async { server.await.unwrap() });

  let client = Client::new_http(&local_address.to_string(), 10.seconds(), 1).unwrap();
  let request = EchoRequest {
    echo: "x".repeat(DEFAULT_MAX_UNARY_REQUEST_BYTES - 1024),
    ..Default::default()
  };

  let response = client
    .unary(
      &service_method(),
      None,
      request.clone(),
      10.seconds(),
      Compression::None,
    )
    .await
    .unwrap();
  assert_eq!(response.echo, request.echo);
}

#[tokio::test]
async fn server_streaming_json_request() {
  let local_address = make_server_streaming_server(Arc::new(EchoHandler::default()), |_| {})
    .await
    .0;
  let client = reqwest::Client::builder().deflate(false).build().unwrap();
  let address = AddressHelper::new(format!("http://{local_address}")).unwrap();
  let response = client
    .post(address.build(&service_method()).to_string())
    .header(CONTENT_TYPE, CONTENT_TYPE_JSON)
    .body("{\"echo\":\"json_echo\"}")
    .send()
    .await
    .unwrap();
  assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn connect_unary_error_stats() {
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
