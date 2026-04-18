// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./grpc_test.rs"]
mod grpc_test;

#[cfg(test)]
mod generated;

pub mod axum_helper;
pub mod client;
pub mod compression;
pub mod connect_protocol;
pub mod error;
pub mod service;
pub mod stats;
pub mod status;

use crate::error::{Error, Result};
use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::HeaderValue;
use axum::middleware::{Next, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use axum::routing::{Route, post};
use axum::{BoxError, Router};
use base64ct::Encoding;
use bd_grpc_codec::code::Code;
use bd_grpc_codec::stats::DeferredCounter;
use bd_grpc_codec::{
  Decoder,
  DecodingResult,
  Encoder,
  GRPC_ACCEPT_ENCODING_HEADER,
  GRPC_ENCODING_DEFLATE,
  GRPC_ENCODING_HEADER,
  LEGACY_GRPC_ENCODING_HEADER,
  OptimizeFor,
};
use bd_log::rate_limit_log::WarnTracker;
use bd_stats_common::DynCounter;
use bytes::{BufMut, Bytes, BytesMut};
use connect_protocol::{ConnectProtocolType, EndOfStreamResponse, ErrorResponse, ToContentType};
use http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use http::{Extensions, HeaderMap};
use http_body::Frame;
use http_body_util::{BodyExt, LengthLimitError, StreamBody};
use protobuf::{Message, MessageFull};
use protobuf_json_mapping::{ParseOptions, PrintOptions};
pub use service::{GrpcMethod, ServiceMethod};
use stats::{BandwidthStatsSummary, EndpointStats, StreamStats};
use status::{RequestTransport, Status, code_to_connect_http_status};
use std::convert::Infallible;
use std::error::Error as StdError;
use std::marker::PhantomData;
use std::sync::Arc;
use time::ext::NumericalDuration;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tower::layer::util::Identity;
use tower::{Layer, Service};

const GRPC_STATUS: &str = "grpc-status";
const GRPC_MESSAGE: &str = "grpc-message";
pub const CONTENT_ENCODING_SNAPPY: &str = "snappy";
pub const CONTENT_TYPE_GRPC: &str = "application/grpc";
const CONTENT_TYPE_PROTO: &str = "application/proto";
const CONTENT_TYPE_CONNECT_STREAMING: &str = "application/connect+proto";
const CONTENT_TYPE_JSON: &str = "application/json";
const TRANSFER_ENCODING_TRAILERS: &str = "trailers";
const CONNECT_PROTOCOL_VERSION: &str = "connect-protocol-version";
pub const DEFAULT_MAX_UNARY_REQUEST_BYTES: usize = 10 * 1024 * 1024;

pub type BodySender = mpsc::Sender<std::result::Result<Frame<Bytes>, BoxError>>;
pub type UnaryResponseMutator<IncomingType> =
  Arc<dyn Fn(&mut IncomingType) -> Result<()> + Send + Sync>;
type UnaryErrorHandler = Arc<dyn Fn(&crate::Error) + Send + Sync>;

/// Inserts gRPC request metadata derived from `ServiceMethod` and request headers into extensions.
///
/// Use this with `axum::middleware::from_fn_with_state(service_method.grpc_method(),
/// grpc_method_extension_middleware)` so top-level Axum route assembly can apply ordinary
/// middleware that depends on method annotations.
pub async fn grpc_method_extension_middleware(
  State(grpc_method): State<GrpcMethod>,
  mut request: Request,
  next: Next,
) -> Response {
  let request_transport = RequestTransport::from_headers(request.headers());
  let extensions = request.extensions_mut();
  extensions.insert(grpc_method);
  extensions.insert(request_transport);
  next.run(request).await
}

//
// UnaryRequestConfig
//

#[derive(Clone, Copy, Debug)]
pub struct UnaryRequestConfig {
  pub max_request_bytes: usize,
  pub validation_options: bd_pgv::proto_validate::ValidationOptions,
}

impl Default for UnaryRequestConfig {
  fn default() -> Self {
    Self {
      max_request_bytes: DEFAULT_MAX_UNARY_REQUEST_BYTES,
      validation_options: bd_pgv::proto_validate::ValidationOptions::default(),
    }
  }
}

//
// UnaryRequestTrace
//

#[derive(Clone)]
pub struct UnaryRequestTrace {
  preview_bytes: usize,
  recorder: Arc<dyn Fn(String) + Send + Sync>,
}

impl UnaryRequestTrace {
  #[must_use]
  pub fn new(preview_bytes: usize, recorder: impl Fn(String) + Send + Sync + 'static) -> Self {
    Self {
      preview_bytes,
      recorder: Arc::new(recorder),
    }
  }

  fn record_preview(&self, preview: &str) {
    if self.preview_bytes == 0 {
      return;
    }

    (self.recorder)(truncate_utf8_preview(preview, self.preview_bytes));
  }
}

//
// UnaryRouterBuilder
//

pub struct UnaryRouterBuilder<
  'a,
  OutgoingType: MessageFull,
  IncomingType: MessageFull,
  L = Identity,
> {
  service_method: &'a ServiceMethod<OutgoingType, IncomingType>,
  handler: Arc<dyn Handler<OutgoingType, IncomingType>>,
  endpoint_stats: Option<&'a EndpointStats>,
  validate_request: bool,
  request_config: UnaryRequestConfig,
  response_mutator: Option<UnaryResponseMutator<IncomingType>>,
  route_layer: L,
  error_handler: UnaryErrorHandler,
  custom_json_print_options: Option<PrintOptions>,
  path: Option<String>,
}

impl<'a, OutgoingType: MessageFull, IncomingType: MessageFull>
  UnaryRouterBuilder<'a, OutgoingType, IncomingType>
{
  #[must_use]
  pub fn new(
    service_method: &'a ServiceMethod<OutgoingType, IncomingType>,
    handler: Arc<dyn Handler<OutgoingType, IncomingType>>,
  ) -> Self {
    Self {
      service_method,
      handler,
      endpoint_stats: None,
      validate_request: false,
      request_config: UnaryRequestConfig::default(),
      response_mutator: None,
      route_layer: Identity::new(),
      error_handler: Arc::new(noop_unary_error_handler),
      custom_json_print_options: None,
      path: None,
    }
  }
}

impl<'a, OutgoingType: MessageFull, IncomingType: MessageFull, L>
  UnaryRouterBuilder<'a, OutgoingType, IncomingType, L>
{
  #[must_use]
  pub fn path(mut self, path: impl Into<String>) -> Self {
    self.path = Some(path.into());
    self
  }

  #[must_use]
  pub fn endpoint_stats(mut self, endpoint_stats: &'a EndpointStats) -> Self {
    self.endpoint_stats = Some(endpoint_stats);
    self
  }

  #[must_use]
  pub fn validate_request(mut self, validate_request: bool) -> Self {
    self.validate_request = validate_request;
    self
  }

  #[must_use]
  pub fn request_config(mut self, request_config: UnaryRequestConfig) -> Self {
    self.request_config = request_config;
    self
  }

  #[must_use]
  pub fn response_mutator(mut self, response_mutator: UnaryResponseMutator<IncomingType>) -> Self {
    self.response_mutator = Some(response_mutator);
    self
  }

  #[must_use]
  pub fn error_handler(
    mut self,
    error_handler: impl Fn(&crate::Error) + Send + Sync + 'static,
  ) -> Self {
    self.error_handler = Arc::new(error_handler);
    self
  }

  #[must_use]
  pub fn json_print_options(mut self, custom_json_print_options: PrintOptions) -> Self {
    self.custom_json_print_options = Some(custom_json_print_options);
    self
  }

  #[must_use]
  pub fn route_layer<L2>(
    self,
    route_layer: L2,
  ) -> UnaryRouterBuilder<'a, OutgoingType, IncomingType, L2> {
    UnaryRouterBuilder {
      service_method: self.service_method,
      handler: self.handler,
      endpoint_stats: self.endpoint_stats,
      validate_request: self.validate_request,
      request_config: self.request_config,
      response_mutator: self.response_mutator,
      route_layer,
      error_handler: self.error_handler,
      custom_json_print_options: self.custom_json_print_options,
      path: self.path,
    }
  }
}

impl<OutgoingType: MessageFull, IncomingType: MessageFull, L>
  UnaryRouterBuilder<'_, OutgoingType, IncomingType, L>
where
  L: Layer<Route> + Clone + Send + Sync + 'static,
  L::Service: Service<Request> + Clone + Send + Sync + 'static,
  <L::Service as Service<Request>>::Response: IntoResponse + 'static,
  <L::Service as Service<Request>>::Error: Into<Infallible> + 'static,
  <L::Service as Service<Request>>::Future: Send + 'static,
{
  pub fn build(self) -> Result<Router> {
    verify_request_support::<OutgoingType>(self.validate_request, self.request_config)?;

    let warn_tracker = Arc::new(WarnTracker::default());
    let full_path = Arc::new(self.path.unwrap_or_else(|| self.service_method.full_path()));
    let resolved_stats = self
      .endpoint_stats
      .map(|stats| stats.resolve::<OutgoingType, IncomingType>(self.service_method));
    let grpc_method = self.service_method.grpc_method();
    let handler = self.handler;
    let response_mutator = self.response_mutator;
    let error_handler = self.error_handler;
    let custom_json_print_options = self.custom_json_print_options;
    let route_layer = self.route_layer;
    let validate_request = self.validate_request;
    let request_config = self.request_config;

    Ok(
      Router::new()
        .route(
          full_path.clone().as_ref(),
          post(move |request: Request| async move {
            let request_transport = RequestTransport::from_extensions(request.extensions());
            let result = unary_handler::<OutgoingType, IncomingType>(
              request,
              handler,
              validate_request,
              request_config,
              response_mutator.clone(),
              custom_json_print_options.clone(),
            )
            .await;

            if let Err(e) = &result {
              if let Some(warning) = e.warn_every_message()
                && warn_tracker.should_warn(15.seconds())
              {
                log::warn!("{full_path} failed: {warning}");
              }

              error_handler(e);
              if let Some(resolved_stats) = &resolved_stats {
                resolved_stats.failure.inc();
              }
            } else if let Some(resolved_stats) = &resolved_stats {
              resolved_stats.success.inc();
            }

            result.map_err(|e| e.to_response(request_transport))
          }),
        )
        .route_layer(route_layer)
        .route_layer(from_fn_with_state(
          grpc_method,
          grpc_method_extension_middleware,
        )),
    )
  }
}

fn noop_unary_error_handler(_error: &crate::Error) {}

//
// StreamingApi
//

// Handle around a bidirectional streaming API. Allows for both sending outgoing messages and
// receiving response messages.
pub struct StreamingApi<OutgoingType: Message, IncomingType: MessageFull> {
  sender: StreamingApiSender<OutgoingType>,
  receiver: StreamingApiReceiver<IncomingType>,
}

impl<OutgoingType: Message, IncomingType: MessageFull> StreamingApi<OutgoingType, IncomingType> {
  // Create a new streaming API handler.
  #[must_use]
  pub fn new(
    tx: BodySender,
    headers: HeaderMap,
    body: Body,
    validate: bool,
    compression: Option<bd_grpc_codec::Compression>,
    optimize_for: OptimizeFor,
    read_stop: Option<watch::Receiver<bool>>,
  ) -> Self {
    let sender = StreamingApiSender::new(
      tx,
      compression,
      matches!(
        ConnectProtocolType::from_headers(&headers),
        Some(ConnectProtocolType::Streaming)
      ),
    );
    Self {
      sender,
      receiver: StreamingApiReceiver::new(headers, body, validate, optimize_for, read_stop),
    }
  }

  // Send a message on the stream.
  pub async fn send(&mut self, message: OutgoingType) -> Result<()> {
    self.sender.send(message).await
  }

  // Send a message on the stream.
  pub async fn send_raw(&mut self, bytes: Bytes) -> Result<()> {
    self.sender.send_raw(bytes).await
  }

  #[must_use]
  pub const fn bandwidth_stats(&self) -> BandwidthStatsSummary {
    let (rx, rx_decompressed) = self.receiver.decoder.bandwidth_stats();
    let (tx, tx_uncompressed) = self.sender.encoder.bandwidth_stats();
    BandwidthStatsSummary {
      rx,
      rx_decompressed,
      tx,
      tx_uncompressed,
    }
  }

  pub fn initialize_stats(
    &mut self,
    tx_messages_total: DynCounter,
    tx_bytes_uncompressed: DynCounter,
    tx_bytes: DynCounter,
    rx_bytes: DynCounter,
    rx_bytes_decompressed: DynCounter,
  ) {
    self
      .receiver
      .initialize_bandwidth_stats(rx_bytes, rx_bytes_decompressed);
    self
      .sender
      .initialize_stats(tx_messages_total, tx_bytes, tx_bytes_uncompressed);
  }

  pub async fn next(&mut self) -> Result<Option<Vec<IncomingType>>> {
    self.receiver.next().await
  }

  #[must_use]
  pub const fn received_headers(&self) -> &HeaderMap {
    self.receiver.received_headers()
  }
}

//
// StreamingApiReceiver
//

// Handle around an API stream receiving messages.
pub struct StreamingApiReceiver<IncomingType: DecodingResult> {
  headers: HeaderMap,
  body: Body,
  decoder: Decoder<IncomingType>,
  validate: bool,
  read_stop: Option<watch::Receiver<bool>>,
}

impl<IncomingType: DecodingResult> StreamingApiReceiver<IncomingType> {
  // Create a new streaming API handler.
  #[must_use]
  pub fn new(
    headers: HeaderMap,
    body: Body,
    validate: bool,
    optimize_for: OptimizeFor,
    read_stop: Option<watch::Receiver<bool>>,
  ) -> Self {
    let decompression = if headers
      .get(GRPC_ENCODING_HEADER)
      .as_ref()
      .is_some_and(|v| *v == GRPC_ENCODING_DEFLATE)
    {
      Some(bd_grpc_codec::Decompression::StatelessZlib)
    } else if headers
      .get(LEGACY_GRPC_ENCODING_HEADER)
      .as_ref()
      .is_some_and(|v| *v == GRPC_ENCODING_DEFLATE)
    {
      // This is not to spec but is kept around for legacy clients.
      // TODO(mattklein123): Add a metric for this.
      Some(bd_grpc_codec::Decompression::StatefulZlib)
    } else {
      None
    };

    Self {
      headers,
      body,
      decoder: Decoder::new(decompression, optimize_for),
      validate,
      read_stop,
    }
  }

  // Receive a message on the stream. An error indicates either a network or protobuf error. None
  // indicates the stream is complete.
  pub async fn next(&mut self) -> Result<Option<Vec<IncomingType>>> {
    loop {
      if let Some(read_stop) = &mut self.read_stop
        && *read_stop.borrow_and_update()
      {
        log::trace!("read stop triggered");
        if let Err(e) = read_stop.changed().await {
          // This is a programming error and reasonably might only happen during shutdown.
          // Issue a debug assert and end the stream.
          debug_assert!(false, "read stop watch error: {e}");
          return Ok(None);
        }
        log::trace!("read stop cleared");
        continue;
      }

      let frame = tokio::select! {
        frame = self.body.frame() => frame,
        _ = async {
              self.read_stop.as_mut().unwrap().changed().await
            }, if self.read_stop.is_some() => {
          continue;
        },
      };

      if let Some(frame) = frame {
        let frame = frame.map_err(|e| Error::BodyStream(e.into()))?;
        if frame.is_data() {
          let messages = self.decoder.decode_data(frame.data_ref().unwrap())?;
          if self.validate {
            for message in &messages {
              if let Some(message) = message.message() {
                bd_pgv::proto_validate::validate(message)
                  .inspect_err(|e| log::debug!("validation failure: {e}"))?;
              }
            }
          }
          if !messages.is_empty() {
            return Ok(Some(messages));
          }
        } else if let Some(trailers) = frame.trailers_ref() {
          let (grpc_status, grpc_message) = trailers.iter().fold((None, None), |acc, (k, v)| {
            if k == GRPC_STATUS {
              (Some(v), acc.1)
            } else if k == GRPC_MESSAGE {
              (acc.0, Some(v))
            } else {
              acc
            }
          });

          if let Some(grpc_status) = grpc_status {
            let code = Code::from_str(grpc_status.to_str().unwrap_or_default());
            if code == Code::Ok {
              return Ok(None);
            }

            let status = Status::from_wire(
              code,
              grpc_message
                .map(|value| Status::decode_grpc_message(value.to_str().unwrap_or_default())),
            );
            return Err(Error::Grpc(status));
          }

          return Ok(None);
        }
      } else {
        return Ok(None);
      }
    }
  }

  // Get the received request/response headers for the API call, depending on the direction.
  #[must_use]
  pub const fn received_headers(&self) -> &HeaderMap {
    &self.headers
  }

  pub fn initialize_bandwidth_stats(&mut self, rx: DynCounter, rx_decompressed: DynCounter) {
    self.decoder.initialize_stats(rx, rx_decompressed);
  }
}

// Handle around a streaming sender. Allows for sending outgoing messages.
pub struct StreamingApiSender<ResponseType: Message> {
  encoder: Encoder<ResponseType>,
  tx: BodySender,
  tx_messages_total: DeferredCounter,
  // https://connectrpc.com/docs/protocol
  connect_protocol: bool,
  _type: PhantomData<ResponseType>,
}

impl<ResponseType: Message> StreamingApiSender<ResponseType> {
  #[must_use]
  pub fn new(
    tx: BodySender,
    compression: Option<bd_grpc_codec::Compression>,
    connect_protocol: bool,
  ) -> Self {
    Self {
      encoder: Encoder::new(compression),
      tx,
      tx_messages_total: DeferredCounter::default(),
      connect_protocol,
      _type: PhantomData,
    }
  }

  // Send a message on the stream.
  pub async fn send(&mut self, message: ResponseType) -> Result<()> {
    let encoded = self.encoder.encode(&message)?;
    self.send_raw_inner(encoded).await
  }

  // Send raw bytes on the stream. This can be used for pre-cached frames that are sent over
  // and over again.
  pub async fn send_raw(&mut self, bytes: Bytes) -> Result<()> {
    // Right now we assume that send raw is never used for compressed frames (which is currently
    // the case).
    self.encoder.inc_stats(bytes.len(), bytes.len());
    self.send_raw_inner(bytes).await
  }

  async fn send_raw_inner(&mut self, bytes: Bytes) -> Result<()> {
    self.tx_messages_total.inc();

    self
      .tx
      .send(Ok(Frame::data(bytes)))
      .await
      .map_err(|_| Error::Closed)?;
    Ok(())
  }

  async fn send_connect_end_of_stream(&mut self, end_of_stream: EndOfStreamResponse) -> Result<()> {
    let end_of_stream = serde_json::to_vec(&end_of_stream).unwrap();
    let mut end_of_stream_bytes = BytesMut::new();
    end_of_stream_bytes.put_u8(0x2); // end of stream
    end_of_stream_bytes.put_u32(end_of_stream.len().try_into().unwrap());
    end_of_stream_bytes.put_slice(&end_of_stream);
    self.send_raw(end_of_stream_bytes.freeze()).await
  }

  async fn send_error(&mut self, status: Status) -> Result<()> {
    log::trace!("sending error trailers for stream");

    if self.connect_protocol {
      return self
        .send_connect_end_of_stream(EndOfStreamResponse {
          error: Some(ErrorResponse::new(&status)),
        })
        .await;
    }

    let mut trailers = HeaderMap::new();
    trailers.insert(
      GRPC_STATUS,
      HeaderValue::from_str(&status.code().to_int().to_string()).unwrap(),
    );
    if let Some(message) = status.message() {
      trailers.insert(GRPC_MESSAGE, HeaderValue::from_str(message).unwrap());
    }

    self.send_trailers(trailers).await
  }

  async fn send_ok_trailers(&mut self) -> Result<()> {
    log::trace!("sending ok trailers for stream");

    if self.connect_protocol {
      return self
        .send_connect_end_of_stream(EndOfStreamResponse { error: None })
        .await;
    }

    let mut trailers = HeaderMap::new();
    trailers.insert(GRPC_STATUS, HeaderValue::from_str("0").unwrap());

    self.send_trailers(trailers).await
  }

  async fn send_trailers(&self, trailers: HeaderMap) -> Result<()> {
    self
      .tx
      .send(Ok(Frame::trailers(trailers)))
      .await
      .map_err(|_| Error::Closed)?;
    Ok(())
  }

  pub fn initialize_stats(
    &mut self,
    tx_messages_total: DynCounter,
    tx_bytes: DynCounter,
    tx_bytes_uncompressed: DynCounter,
  ) {
    self
      .encoder
      .initialize_stats(tx_bytes, tx_bytes_uncompressed);
    self.tx_messages_total.initialize(tx_messages_total);
  }
}

// Create a new successful axum gRPC response with a given body.
#[must_use]
pub fn new_grpc_response(
  body: Body,
  compression: Option<bd_grpc_codec::Compression>,
  connect_protocol: Option<ConnectProtocolType>,
) -> Response {
  let mut builder = Response::builder().header(CONTENT_TYPE, connect_protocol.to_content_type());

  match compression {
    None => {},
    Some(bd_grpc_codec::Compression::StatefulZlib { .. }) => {
      // Verified in response compression selection.
      unreachable!()
    },
    Some(bd_grpc_codec::Compression::StatelessZlib { .. }) => {
      builder = builder.header(GRPC_ENCODING_HEADER, GRPC_ENCODING_DEFLATE);
    },
  }

  builder.body(body).unwrap()
}

// Handler for a unary API. Allows for mocking.
#[cfg_attr(feature = "mock", mockall::automock)]
#[async_trait::async_trait]
pub trait Handler<OutgoingType: Message, IncomingType: Message>: Send + Sync {
  async fn handle(
    &self,
    headers: HeaderMap,
    extensions: Extensions,
    request: OutgoingType,
  ) -> Result<IncomingType>;
}

// Handler for a Streaming API. Allows for mocking.
// TODO(msarvar): Unify this trait with bidi streaming handler(StreamingApi).
#[cfg_attr(feature = "mock", mockall::automock)]
#[async_trait::async_trait]
pub trait ServerStreamingHandler<ResponseType: Message, RequestType: Message>: Send + Sync {
  async fn stream(
    &self,
    headers: HeaderMap,
    extensions: Extensions,
    request: RequestType,
    sender: &mut StreamingApiSender<ResponseType>,
  ) -> Result<()>;
}

#[cfg_attr(feature = "mock", mockall::automock)]
#[async_trait::async_trait]
pub trait BidiStreamingHandler<ResponseType: Message, RequestType: MessageFull>:
  Send + Sync
{
  async fn stream(
    &self,
    headers: HeaderMap,
    extensions: Extensions,
    api: &mut StreamingApi<ResponseType, RequestType>,
  ) -> Result<()>;
}

async fn decode_request<Message: MessageFull>(
  request: Request,
  validate_request: bool,
  connect_protocol_type: Option<ConnectProtocolType>,
  request_config: UnaryRequestConfig,
  request_trace: Option<&UnaryRequestTrace>,
) -> Result<(HeaderMap<HeaderValue>, Extensions, Message)> {
  let (parts, body) = request.into_parts();
  let body_bytes = to_bytes(body, request_config.max_request_bytes)
    .await
    .map_err(|e| {
      if StdError::source(&e).is_some_and(<dyn std::error::Error>::is::<LengthLimitError>) {
        Status::new(
          Code::ResourceExhausted,
          format!(
            "Request body exceeds {} byte limit",
            request_config.max_request_bytes
          ),
          None,
        )
        .into()
      } else {
        Error::BodyStream(e.into())
      }
    })?;
  let body_bytes = if parts
    .headers
    .get(CONTENT_ENCODING)
    .is_some_and(|v| v.as_bytes() == CONTENT_ENCODING_SNAPPY.as_bytes())
  {
    let decompressed = snap::raw::Decoder::new().decompress_vec(&body_bytes)?;
    log::trace!(
      "snappy decompressed {} -> {}",
      body_bytes.len(),
      decompressed.len()
    );
    decompressed.into()
  } else {
    body_bytes
  };

  let message = if matches!(connect_protocol_type, Some(ConnectProtocolType::Unary)) {
    let message = Message::parse_from_tokio_bytes(&body_bytes)
      .map_err(|e| Status::new(Code::InvalidArgument, format!("Invalid request: {e}"), None))?;
    record_message_request_trace(request_trace, &message);
    message
  } else if is_json_request_content_type(&parts.headers) {
    let body_str = std::str::from_utf8(&body_bytes)
      .map_err(|e| Status::new(Code::InvalidArgument, format!("Invalid request: {e}"), None))?;
    if let Some(request_trace) = request_trace {
      request_trace.record_preview(body_str);
    }
    protobuf_json_mapping::parse_from_str_with_options(body_str, &json_parse_options())
      .map_err(|e| Status::new(Code::InvalidArgument, format!("Invalid request: {e}"), None))?
  } else {
    let mut grpc_decoder =
      Decoder::<Message>::new(finalize_decompression(&parts.headers), OptimizeFor::Cpu);
    let mut messages = grpc_decoder.decode_data(&body_bytes)?;
    if messages.len() != 1 {
      return Err(Status::new(Code::InvalidArgument, "Invalid request body", None).into());
    }
    let message = messages.remove(0);
    record_message_request_trace(request_trace, &message);
    message
  };

  if validate_request {
    bd_pgv::proto_validate::validate_with_options(&message, request_config.validation_options)
      .map_err(|e| Status::new(Code::InvalidArgument, format!("Invalid request: {e}"), None))?;
  }

  Ok((parts.headers, parts.extensions, message))
}

pub(crate) fn is_json_request_content_type(headers: &HeaderMap) -> bool {
  headers
    .get(CONTENT_TYPE)
    .and_then(|value| value.to_str().ok())
    .and_then(|value| value.split(';').next())
    .is_some_and(|value| value.trim().eq_ignore_ascii_case(CONTENT_TYPE_JSON))
}

fn json_parse_options() -> ParseOptions {
  ParseOptions::default()
}

fn response_json_print_options() -> PrintOptions {
  PrintOptions {
    proto_field_name: true,
    ..Default::default()
  }
}

fn request_trace_json_print_options() -> PrintOptions {
  PrintOptions {
    proto_field_name: true,
    always_output_default_values: false,
    ..Default::default()
  }
}

fn record_message_request_trace<Message: MessageFull>(
  request_trace: Option<&UnaryRequestTrace>,
  message: &Message,
) {
  let Some(request_trace) = request_trace else {
    return;
  };

  let options = request_trace_json_print_options();
  match protobuf_json_mapping::print_to_string_with_options(message, &options) {
    Ok(json) => request_trace.record_preview(&json),
    Err(error) => log::warn!("failed to render unary request trace preview: {error}"),
  }
}

fn truncate_utf8_preview(value: &str, max_bytes: usize) -> String {
  const TRUNCATION_SUFFIX: &str = "...";

  if value.len() <= max_bytes {
    return value.to_string();
  }

  if max_bytes <= TRUNCATION_SUFFIX.len() {
    return value
      .chars()
      .scan(0usize, |total_bytes, ch| {
        let ch_bytes = ch.len_utf8();
        if *total_bytes + ch_bytes > max_bytes {
          return None;
        }

        *total_bytes += ch_bytes;
        Some(ch)
      })
      .collect();
  }

  let preview_bytes = max_bytes - TRUNCATION_SUFFIX.len();
  let mut preview = String::with_capacity(max_bytes);
  let mut total_bytes = 0;
  for ch in value.chars() {
    let ch_bytes = ch.len_utf8();
    if total_bytes + ch_bytes > preview_bytes {
      break;
    }

    total_bytes += ch_bytes;
    preview.push(ch);
  }
  preview.push_str(TRUNCATION_SUFFIX);
  preview
}

async fn unary_connect_handler<OutgoingType: MessageFull, IncomingType: MessageFull>(
  headers: HeaderMap,
  extensions: Extensions,
  message: OutgoingType,
  handler: Arc<dyn Handler<OutgoingType, IncomingType>>,
  response_mutator: Option<UnaryResponseMutator<IncomingType>>,
) -> Result<Response> {
  let mut response = handler.handle(headers, extensions, message).await?;
  if let Some(response_mutator) = response_mutator {
    response_mutator(&mut response)?;
  }
  Ok(new_grpc_response(
    response.write_to_bytes().unwrap().into(),
    None,
    Some(ConnectProtocolType::Unary),
  ))
}

// Axum handler for a unary API.
pub async fn unary_handler<OutgoingType: MessageFull, IncomingType: MessageFull>(
  request: Request,
  handler: Arc<dyn Handler<OutgoingType, IncomingType>>,
  validate_request: bool,
  request_config: UnaryRequestConfig,
  response_mutator: Option<UnaryResponseMutator<IncomingType>>,
  custom_json_print_options: Option<PrintOptions>,
) -> Result<Response> {
  let request_transport = RequestTransport::from_extensions(request.extensions());
  let connect_protocol_type = request_transport.connect_protocol();
  let request_trace = request.extensions().get::<UnaryRequestTrace>().cloned();
  let (headers, extensions, message) = decode_request::<OutgoingType>(
    request,
    validate_request,
    connect_protocol_type,
    request_config,
    request_trace.as_ref(),
  )
  .await?;

  if matches!(connect_protocol_type, Some(ConnectProtocolType::Unary)) {
    return unary_connect_handler(headers, extensions, message, handler, response_mutator).await;
  }

  let compression = finalize_response_compression(
    Some(bd_grpc_codec::Compression::StatelessZlib { level: 3 }),
    &headers,
  );

  let mut response = handler.handle(headers, extensions, message).await?;
  if let Some(response_mutator) = response_mutator {
    response_mutator(&mut response)?;
  }

  if request_transport.json_transcoding() {
    let options = custom_json_print_options.unwrap_or_else(response_json_print_options);
    let json =
      protobuf_json_mapping::print_to_string_with_options(&response, &options).map_err(|e| {
        Status::new(
          Code::Internal,
          format!("Failed to encode response: {e}"),
          None,
        )
      })?;
    return Ok(
      Response::builder()
        .status(code_to_connect_http_status(Code::Ok))
        .header(CONTENT_TYPE, CONTENT_TYPE_JSON)
        .body(json.into())
        .unwrap(),
    );
  }

  let (tx, rx) = mpsc::channel::<std::result::Result<_, Infallible>>(2);

  let mut encoder = Encoder::new(compression);
  let encoded_data = encoder.encode(&response)?;

  tx.send(Ok(Frame::data(encoded_data))).await.unwrap();

  let mut trailers = HeaderMap::new();
  trailers.insert(GRPC_STATUS, HeaderValue::from_str("0").unwrap());
  tx.send(Ok(Frame::trailers(trailers))).await.unwrap();

  Ok(new_grpc_response(
    Body::new(StreamBody::new(ReceiverStream::new(rx))),
    compression,
    None,
  ))
}

#[must_use]
pub fn finalize_response_compression(
  compression: Option<bd_grpc_codec::Compression>,
  headers: &HeaderMap,
) -> Option<bd_grpc_codec::Compression> {
  match compression {
    None => None,
    Some(bd_grpc_codec::Compression::StatefulZlib { .. }) => {
      // Currently for memory usage reasons we do not support this.
      debug_assert!(
        false,
        "StatefulZlib is not supported for response compression"
      );
      None
    },
    Some(bd_grpc_codec::Compression::StatelessZlib { level }) => headers
      .get(GRPC_ACCEPT_ENCODING_HEADER)
      .filter(|v| *v == GRPC_ENCODING_DEFLATE)
      .map(|_| bd_grpc_codec::Compression::StatelessZlib { level }),
  }
}

fn finalize_decompression(headers: &HeaderMap) -> Option<bd_grpc_codec::Decompression> {
  headers
    .get(GRPC_ENCODING_HEADER)
    .filter(|v| *v == GRPC_ENCODING_DEFLATE)
    .map(|_| bd_grpc_codec::Decompression::StatelessZlib)
}

async fn server_streaming_handler<ResponseType: MessageFull, RequestType: MessageFull>(
  handler: Arc<dyn ServerStreamingHandler<ResponseType, RequestType>>,
  request: Request,
  error_handler: impl Fn(&crate::Error) + Clone + Send + 'static,
  stream_stats: Option<StreamStats>,
  validate_request: bool,
  warn_tracker: Arc<WarnTracker>,
  // This indicates if response compression is desired. It will still be gated on whether the
  // client sets the compression header.
  compression: Option<bd_grpc_codec::Compression>,
) -> Result<Response> {
  let request_transport = RequestTransport::from_extensions(request.extensions());
  let connect_protocol_type = request_transport.connect_protocol();
  if let Some(stream_stats) = &stream_stats {
    stream_stats.stream_initiations_total.inc();
  }

  let path = request.uri().path().to_string();

  let (tx, rx) = mpsc::channel(1);
  let (headers, extensions, message) = decode_request::<RequestType>(
    request,
    validate_request,
    connect_protocol_type,
    UnaryRequestConfig::default(),
    None,
  )
  .await
  .inspect_err(|_| {
    if let Some(stream_stats) = &stream_stats {
      stream_stats.rpc.failure.inc();
    }
  })?;

  let compression = finalize_response_compression(compression, &headers);
  tokio::spawn(async move {
    let sender = &mut StreamingApiSender::new(
      tx,
      compression,
      matches!(connect_protocol_type, Some(ConnectProtocolType::Streaming)),
    );
    if let Some(stream_stats) = &stream_stats {
      sender.initialize_stats(
        stream_stats.tx_messages_total.clone(),
        stream_stats.tx_bytes_total.clone(),
        stream_stats.tx_bytes_uncompressed_total.clone(),
      );
    }

    match handler.stream(headers, extensions, message, sender).await {
      Ok(()) => {
        if let Some(stream_stats) = &stream_stats {
          stream_stats.rpc.success.inc();
        }

        // Make sure we send grpc-status: 0 to indicate success if we stop without error.
        // This can fail if the client has disconnected. We ignore the error here since there is
        // nothing more to do.
        let _ignored = sender.send_ok_trailers().await;
      },
      Err(e) => {
        if let Some(warning) = e.warn_every_message()
          && warn_tracker.should_warn(15.seconds())
        {
          log::warn!("{path} failed: {warning}");
        }

        if let Some(stream_stats) = &stream_stats {
          stream_stats.rpc.failure.inc();
        }
        error_handler(&e);

        let status = match e {
          Error::Grpc(status) => status,
          e => Status::new(Code::Internal, format!("{e}"), None),
        };

        log::debug!("Stream {path} failed: {status}");

        // This can fail if the client has disconnected. We ignore the error here since there is
        // nothing more to do.
        let _ignored = sender.send_error(status).await;
      },
    }
  });

  Ok(new_grpc_response(
    Body::new(StreamBody::new(ReceiverStream::new(rx))),
    compression,
    connect_protocol_type,
  ))
}

fn bidi_streaming_handler<ResponseType: MessageFull, RequestType: MessageFull>(
  handler: Arc<dyn BidiStreamingHandler<ResponseType, RequestType>>,
  request: Request,
  error_handler: impl Fn(&crate::Error) + Clone + Send + 'static,
  stream_stats: Option<StreamStats>,
  validate_request: bool,
  warn_tracker: Arc<WarnTracker>,
  compression: Option<bd_grpc_codec::Compression>,
) -> Result<Response> {
  let request_transport = RequestTransport::from_extensions(request.extensions());
  let connect_protocol_type = request_transport.connect_protocol();
  if matches!(request_transport, RequestTransport::JsonTranscoding)
    || matches!(connect_protocol_type, Some(ConnectProtocolType::Unary))
  {
    return Err(
      Status::new(
        Code::FailedPrecondition,
        "bidirectional streaming only supports gRPC and Connect streaming",
        None,
      )
      .into(),
    );
  }

  if let Some(stream_stats) = &stream_stats {
    stream_stats.stream_initiations_total.inc();
  }

  let path = request.uri().path().to_string();
  let (tx, rx) = mpsc::channel(1);
  let (parts, body) = request.into_parts();
  let headers = parts.headers.clone();
  let extensions = parts.extensions;
  let compression = finalize_response_compression(compression, &headers);

  tokio::spawn(async move {
    let mut api = StreamingApi::<ResponseType, RequestType>::new(
      tx,
      parts.headers,
      Body::new(body),
      validate_request,
      compression,
      OptimizeFor::Cpu,
      None,
    );
    if let Some(stream_stats) = &stream_stats {
      api.sender.initialize_stats(
        stream_stats.tx_messages_total.clone(),
        stream_stats.tx_bytes_total.clone(),
        stream_stats.tx_bytes_uncompressed_total.clone(),
      );
    }

    match handler.stream(headers, extensions, &mut api).await {
      Ok(()) => {
        if let Some(stream_stats) = &stream_stats {
          stream_stats.rpc.success.inc();
        }
        let _ignored = api.sender.send_ok_trailers().await;
      },
      Err(e) => {
        if let Some(warning) = e.warn_every_message()
          && warn_tracker.should_warn(15.seconds())
        {
          log::warn!("{path} failed: {warning}");
        }

        if let Some(stream_stats) = &stream_stats {
          stream_stats.rpc.failure.inc();
        }
        error_handler(&e);

        let status = match e {
          Error::Grpc(status) => status,
          e => Status::new(Code::Internal, format!("{e}"), None),
        };

        log::debug!("Stream {path} failed: {status}");

        let _ignored = api.sender.send_error(status).await;
      },
    }
  });

  Ok(new_grpc_response(
    Body::new(StreamBody::new(ReceiverStream::new(rx))),
    compression,
    connect_protocol_type,
  ))
}


// Create an axum router for a one directional streaming handler.
pub fn make_server_streaming_router<ResponseType: MessageFull, RequestType: MessageFull>(
  service_method: &ServiceMethod<RequestType, ResponseType>,
  handler: Arc<dyn ServerStreamingHandler<ResponseType, RequestType>>,
  error_handler: impl Fn(&crate::Error) + Clone + Send + Sync + 'static,
  stream_stats: Option<&EndpointStats>,
  validate_request: bool,
  compression: Option<bd_grpc_codec::Compression>,
) -> Result<Router> {
  make_server_streaming_router_with_route_layer(
    service_method,
    handler,
    error_handler,
    stream_stats,
    validate_request,
    compression,
    Identity::new(),
  )
}

pub fn make_bidi_streaming_router<ResponseType: MessageFull, RequestType: MessageFull>(
  service_method: &ServiceMethod<RequestType, ResponseType>,
  handler: Arc<dyn BidiStreamingHandler<ResponseType, RequestType>>,
  error_handler: impl Fn(&crate::Error) + Clone + Send + Sync + 'static,
  stream_stats: Option<&EndpointStats>,
  validate_request: bool,
  compression: Option<bd_grpc_codec::Compression>,
) -> Result<Router> {
  make_bidi_streaming_router_with_route_layer(
    service_method,
    handler,
    error_handler,
    stream_stats,
    validate_request,
    compression,
    Identity::new(),
  )
}

pub fn make_bidi_streaming_router_with_route_layer<
  ResponseType: MessageFull,
  RequestType: MessageFull,
  L,
>(
  service_method: &ServiceMethod<RequestType, ResponseType>,
  handler: Arc<dyn BidiStreamingHandler<ResponseType, RequestType>>,
  error_handler: impl Fn(&crate::Error) + Clone + Send + Sync + 'static,
  stream_stats: Option<&EndpointStats>,
  validate_request: bool,
  compression: Option<bd_grpc_codec::Compression>,
  route_layer: L,
) -> Result<Router>
where
  L: Layer<Route> + Clone + Send + Sync + 'static,
  L::Service: Service<Request> + Clone + Send + Sync + 'static,
  <L::Service as Service<Request>>::Response: IntoResponse + 'static,
  <L::Service as Service<Request>>::Error: Into<Infallible> + 'static,
  <L::Service as Service<Request>>::Future: Send + 'static,
{
  verify_request_support::<RequestType>(validate_request, UnaryRequestConfig::default())?;
  let warn_tracker = Arc::new(WarnTracker::default());
  let stream_stats = stream_stats.map(|stats| stats.resolve_streaming(service_method));
  let grpc_method = service_method.grpc_method();
  Ok(
    Router::new()
      .route(
        &service_method.full_path(),
        post(move |request: Request| async move {
          let request_transport = RequestTransport::from_extensions(request.extensions());
          bidi_streaming_handler(
            handler,
            request,
            error_handler,
            stream_stats,
            validate_request,
            warn_tracker.clone(),
            compression,
          )
          .map_err(|e| e.to_response(request_transport))
        }),
      )
      .route_layer(route_layer)
      .route_layer(from_fn_with_state(
        grpc_method,
        grpc_method_extension_middleware,
      )),
  )
}

/// Creates a server-streaming router and applies an additional per-route Axum layer inside
/// `bd-grpc`.
///
/// The supplied `route_layer` runs after `bd-grpc` has inserted `GrpcMethod` and
/// `status::RequestTransport` into the request extensions and before the gRPC handler executes.
/// This makes it the right seam for middleware that needs descriptor-derived route metadata and the
/// negotiated response transport.
///
/// The route layer operates on the raw `axum::extract::Request`; the protobuf request body has not
/// been decoded yet, so typed request messages are not available here. Middleware can rely on
/// `GrpcMethod` and `status::RequestTransport` being present in request extensions, along with any
/// extensions added by outer Axum layers that have already run.
pub fn make_server_streaming_router_with_route_layer<
  ResponseType: MessageFull,
  RequestType: MessageFull,
  L,
>(
  service_method: &ServiceMethod<RequestType, ResponseType>,
  handler: Arc<dyn ServerStreamingHandler<ResponseType, RequestType>>,
  error_handler: impl Fn(&crate::Error) + Clone + Send + Sync + 'static,
  stream_stats: Option<&EndpointStats>,
  validate_request: bool,
  compression: Option<bd_grpc_codec::Compression>,
  route_layer: L,
) -> Result<Router>
where
  L: Layer<Route> + Clone + Send + Sync + 'static,
  L::Service: Service<Request> + Clone + Send + Sync + 'static,
  <L::Service as Service<Request>>::Response: IntoResponse + 'static,
  <L::Service as Service<Request>>::Error: Into<Infallible> + 'static,
  <L::Service as Service<Request>>::Future: Send + 'static,
{
  verify_request_support::<RequestType>(validate_request, UnaryRequestConfig::default())?;
  let warn_tracker = Arc::new(WarnTracker::default());
  let stream_stats = stream_stats.map(|stats| stats.resolve_streaming(service_method));
  let grpc_method = service_method.grpc_method();
  Ok(
    Router::new()
      .route(
        &service_method.full_path(),
        post(move |request: Request| async move {
          let request_transport = RequestTransport::from_extensions(request.extensions());
          server_streaming_handler(
            handler,
            request,
            error_handler,
            stream_stats,
            validate_request,
            warn_tracker.clone(),
            compression,
          )
          .await
          .map_err(|e| e.to_response(request_transport))
        }),
      )
      .route_layer(route_layer)
      .route_layer(from_fn_with_state(
        grpc_method,
        grpc_method_extension_middleware,
      )),
  )
}

fn verify_request_support<RequestType: MessageFull>(
  validate_request: bool,
  request_config: UnaryRequestConfig,
) -> Result<()> {
  if validate_request {
    bd_pgv::proto_validate::verify_descriptor_support_with_options(
      &RequestType::descriptor(),
      request_config.validation_options,
    )?;
  }

  Ok(())
}

//
// BinaryHeaderValue
//

// Wrapper around a gRPC binary header that handles base64 encoding.
pub struct BinaryHeaderValue {
  header_value: HeaderValue,
}

impl BinaryHeaderValue {
  #[must_use]
  pub fn new(data: &[u8]) -> Self {
    Self {
      header_value: base64ct::Base64::encode_string(data).try_into().unwrap(),
    }
  }

  pub fn to_header_value(&self) -> HeaderValue {
    self.header_value.clone()
  }
}
