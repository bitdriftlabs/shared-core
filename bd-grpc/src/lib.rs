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

use axum::body::{to_bytes, Body};
use axum::extract::Request;
use axum::http::HeaderValue;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{BoxError, Router};
use base64ct::Encoding;
use bd_grpc_codec::stats::DeferredCounter;
use bd_grpc_codec::{Decoder, Encoder, GRPC_ENCODING_DEFLATE, GRPC_ENCODING_HEADER};
use bd_log::rate_limit_log::WarnTracker;
use bd_server_stats::stats::{CounterWrapper, Scope};
use bd_stats_common::DynCounter;
use bd_time::TimeDurationExt;
use bytes::Bytes;
use http::{Extensions, HeaderMap, StatusCode, Uri};
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Incoming;
use hyper_util::client::legacy::connect::{Connect, HttpConnector};
use hyper_util::rt::TokioExecutor;
use prometheus::IntCounter;
use protobuf::reflect::FileDescriptor;
use protobuf::{Message, MessageFull};
use std::convert::Infallible;
use std::error::Error as StdError;
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::sync::Arc;
use time::ext::NumericalDuration;
use time::Duration;
use tokio::sync::{mpsc, Semaphore};
use tokio_stream::wrappers::ReceiverStream;

const GRPC_STATUS: &str = "grpc-status";
const GRPC_MESSAGE: &str = "grpc-message";
pub const CONTENT_ENCODING: &str = "content-encoding";
pub const CONTENT_ENCODING_SNAPPY: &str = "snappy";
pub const CONTENT_TYPE: &str = "content-type";
pub const CONTENT_TYPE_GRPC: &str = "application/grpc";
const TRANSFER_ENCODING: &str = "te";
const TRANSFER_ENCODING_TRAILERS: &str = "trailers";

pub type BodySender = mpsc::Sender<std::result::Result<Frame<Bytes>, BoxError>>;

//
// Error
//

#[derive(Debug, thiserror::Error)]
pub enum Error {
  #[error("Body stream error ocurred: {0}")]
  BodyStream(BoxError),
  #[error("Stream has closed")]
  Closed,
  #[error("A codec error occurred: {0}")]
  Codec(#[from] bd_grpc_codec::Error),
  #[error("A connection timeout occurred")]
  ConnectionTimeout,
  #[error("A gRPC error occurred: {0}")]
  Grpc(#[from] Status),
  #[error("A hyper client error occurred: {0}")]
  HyperClient(#[from] hyper_util::client::legacy::Error),
  #[error("A proto validation error occurred: {0}")]
  ProtoValidation(#[from] bd_pgv::error::Error),
  #[error("A request timeout occurred")]
  RequestTimeout,
  #[error("A snap decode error occurred: {0}")]
  Snap(#[from] snap::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

impl IntoResponse for Error {
  fn into_response(self) -> Response {
    match self {
      Self::ConnectionTimeout | Self::RequestTimeout => {
        Status::new(Code::Internal, "upstream timeout").into_response()
      },
      Self::Grpc(status) => status.into_response(),
      Self::Codec(_) | Self::ProtoValidation(_) | Self::Snap(_) => {
        StatusCode::BAD_REQUEST.into_response()
      },
      Self::BodyStream(_) | Self::Closed | Self::HyperClient(_) => {
        StatusCode::INTERNAL_SERVER_ERROR.into_response()
      },
    }
  }
}

impl Error {
  fn warn_every_message(&self) -> Option<String> {
    match self {
      Self::ConnectionTimeout | Self::RequestTimeout => Some("upstream timeout".to_string()),
      Self::Grpc(status) => {
        if status.code == Code::Internal {
          Some(format!(
            "gRPC internal error ({})",
            status.message.as_ref().map_or_else(|| "", |s| s.as_str())
          ))
        } else {
          None
        }
      },
      Self::Codec(_) | Self::ProtoValidation(_) | Self::Snap(_) | Self::Closed => None,
      Self::BodyStream(e) => Some(format!("body stream error: {e}")),
      Self::HyperClient(e) => Some(format!("hyper client error: {e}")),
    }
  }
}

//
// Code
//

// Wrapper for supported gRPC status codes. Unknown is a synthetic code if mapping is not possible.
#[derive(PartialEq, Eq, Debug)]
pub enum Code {
  Ok,
  Unknown,
  InvalidArgument,
  FailedPrecondition,
  Internal,
  Unavailable,
  Unauthenticated,
  NotFound,
}

impl Code {
  // Convert to an int via https://grpc.github.io/grpc/core/md_doc_statuscodes.html.
  #[must_use]
  pub const fn to_int(&self) -> i32 {
    match self {
      Self::Ok => 0,
      Self::Unknown => 2,
      Self::InvalidArgument => 3,
      Self::FailedPrecondition => 9,
      Self::NotFound => 5,
      Self::Internal => 13,
      Self::Unavailable => 14,
      Self::Unauthenticated => 16,
    }
  }

  // Convert from a string via https://grpc.github.io/grpc/core/md_doc_statuscodes.html.
  #[must_use]
  pub fn from_string(status: &str) -> Self {
    match status {
      "0" => Self::Ok,
      "3" => Self::InvalidArgument,
      "5" => Self::NotFound,
      "9" => Self::FailedPrecondition,
      "13" => Self::Internal,
      "14" => Self::Unavailable,
      "16" => Self::Unauthenticated,
      _ => Self::Unknown,
    }
  }
}

//
// Compression
//

// What compression type to use for gRPC requests.
pub enum Compression {
  // No compression.
  None,
  // Snappy raw compression. This is meant for unary requests only as it does not use the snappy
  // frame format and persist state for the duration of the stream. We can add this later if
  // needed.
  Snappy,
}

//
// Status
//

// Wrapper for a gRPC status including a code and optional message.
#[derive(PartialEq, Eq, Debug)]
pub struct Status {
  pub code: Code,
  pub message: Option<String>,
}

impl Status {
  // Create a new status.
  #[must_use]
  pub fn new(code: Code, message: &str) -> Self {
    Self {
      code,
      message: Some(message.to_string()),
    }
  }

  // Create a status from headers. The grpc-status header is assumed to exist and this function
  // will panic otherwise.
  #[must_use]
  pub fn from_headers(headers: &HeaderMap) -> Self {
    Self {
      code: Code::from_string(
        headers
          .get(GRPC_STATUS)
          .expect("caller should verify grpc-status exists")
          .to_str()
          .unwrap_or_default(),
      ),
      message: headers
        .get(GRPC_MESSAGE)
        .and_then(|value| value.to_str().ok().map(ToString::to_string)),
    }
  }

  // Convert a status into a response compatible with axum.
  #[must_use]
  pub fn into_response(self) -> Response {
    self.into_response_with_body(().into())
  }

  // Convert a status into a response compatible with axum.
  #[must_use]
  pub fn into_response_with_body(self, body: Body) -> Response {
    let mut builder = Response::builder()
      .header(CONTENT_TYPE, CONTENT_TYPE_GRPC)
      .header(GRPC_STATUS, self.code.to_int());

    if self.message.is_some() {
      // We need to make sure the message is a valid header so we URL encode it to be sure.
      let encoded = urlencoding::encode(self.message.as_ref().unwrap());
      let header_value = HeaderValue::from_str(&encoded).unwrap();

      builder = builder.header(GRPC_MESSAGE, header_value);
    }

    builder.body(body).unwrap()
  }
}

impl std::fmt::Display for Status {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "code: {}, message: {}",
      self.code.to_int(),
      self.message.as_ref().unwrap_or(&"<none>".to_string())
    )
  }
}

impl std::error::Error for Status {}

//
// Client
//

// A simple gRPC client wrapper that allows for both unary and streaming requests.
#[derive(Debug)]
pub struct Client<C> {
  client: hyper_util::client::legacy::Client<C, Body>,
  address: Uri,
  concurrency: Semaphore,
}

impl Client<HttpConnector> {
  // Creates a new client against a target address using HTTP over a TCP socket.
  pub fn new_http(
    address: &str,
    connect_timeout: Duration,
    max_request_concurrency: u64,
  ) -> anyhow::Result<Self> {
    let mut connector = HttpConnector::new();
    connector.set_nodelay(true);
    connector.set_connect_timeout(Some(connect_timeout.unsigned_abs()));

    Self::new_with_client(
      format!("http://{address}"),
      hyper_util::client::legacy::Client::builder(TokioExecutor::new())
        .http2_only(true)
        .build(connector),
      max_request_concurrency,
    )
  }
}

impl<C: Connect + Clone + Send + Sync + 'static> Client<C> {
  // Create a new client against a target address.
  pub fn new_with_client<E: Send + Sync + std::error::Error + 'static>(
    address: impl TryInto<Uri, Error = E>,
    client: hyper_util::client::legacy::Client<C, Body>,
    max_request_concurrency: u64,
  ) -> anyhow::Result<Self> {
    let address: Uri = address.try_into()?;

    // These are unwrapped later on to construct the full URI, so bail early if they are not set.
    if address.scheme().is_none() {
      anyhow::bail!("missing scheme in address");
    }

    if address.authority().is_none() {
      anyhow::bail!("missing authority in address");
    }

    // These are dropped when constructing the final URI, so providing them likely indicates a bug.
    if address.path() != "/" {
      anyhow::bail!(
        "extra path parameter not supported in address: {}",
        address.path()
      );
    }

    if address.query().is_some() {
      anyhow::bail!("extra query parameter not supported in address");
    }

    Ok(Self {
      client,
      address,
      concurrency: Semaphore::new(max_request_concurrency.try_into().unwrap()),
    })
  }

  // Common request generation for both unary and streaming requests.
  async fn common_request<OutgoingType: MessageFull, IncomingType: MessageFull>(
    &self,
    service_method: &ServiceMethod<OutgoingType, IncomingType>,
    extra_headers: Option<HeaderMap>,
    body: Body,
  ) -> Result<Response<Incoming>> {
    // TODO(mattklein123): Potentially implement load shed if we have too many waiters.
    let _permit = self.concurrency.acquire().await.unwrap();

    let uri = Uri::builder()
      .scheme(self.address.scheme().unwrap().clone())
      .authority(self.address.authority().unwrap().as_str())
      .path_and_query(service_method.full_path.as_str())
      .build()
      .unwrap();

    let mut request = hyper::Request::builder()
      .method(hyper::Method::POST)
      .uri(uri)
      .header(CONTENT_TYPE, CONTENT_TYPE_GRPC)
      .header(TRANSFER_ENCODING, TRANSFER_ENCODING_TRAILERS)
      .body(body)
      .unwrap();
    if let Some(extra_headers) = extra_headers {
      request.headers_mut().extend(extra_headers);
    }

    let response = match self.client.request(request).await {
      Ok(response) => response,
      Err(e) => {
        // This is absolutely horrendous but I can't figure out any other way of doing this
        // more cleanly.
        if e
          .source()
          .and_then(std::error::Error::source)
          .and_then(|e| e.downcast_ref::<std::io::Error>())
          .map_or(false, |e| e.kind() == ErrorKind::TimedOut)
        {
          return Err(Error::ConnectionTimeout);
        }

        return Err(e.into());
      },
    };
    if !response.status().is_success() {
      return Err(
        Status::new(
          Code::Internal,
          &format!("Non-200 response code: {}", response.status()),
        )
        .into(),
      );
    }

    // We treat any trailer only response as an error, even with the response status is OK. This
    // seems fine for now.
    if response.headers().contains_key(GRPC_STATUS) {
      return Err(Status::from_headers(response.headers()).into());
    }

    Ok(response)
  }

  // Perform a unary request.
  pub async fn unary<OutgoingType: MessageFull, IncomingType: MessageFull>(
    &self,
    service_method: &ServiceMethod<OutgoingType, IncomingType>,
    extra_headers: Option<HeaderMap>,
    request: OutgoingType,
    request_timeout: Duration,
    compression: Compression,
  ) -> Result<IncomingType> {
    let (extra_headers, body) = match compression {
      Compression::None => {
        let mut encoder = Encoder::new(None);
        (extra_headers, encoder.encode(&request).into())
      },
      Compression::Snappy => {
        // Note: This is not compliant to the gRPC spec. It just compresses the entire payload
        // including the message length. We can consider making this better later but this is simple
        // and works for the basic unary use case where we control both sides.
        let mut encoder = Encoder::new(None);
        let proto_encoded = encoder.encode(&request);
        let body = snap::raw::Encoder::new()
          .compress_vec(&proto_encoded)
          .unwrap();
        log::trace!(
          "snappy compressed {} -> {}",
          proto_encoded.len(),
          body.len()
        );
        let mut extra_headers = extra_headers.unwrap_or_default();
        extra_headers.append(
          CONTENT_ENCODING,
          CONTENT_ENCODING_SNAPPY.try_into().unwrap(),
        );
        (Some(extra_headers), body.into())
      },
    };

    let response = match request_timeout
      .timeout(self.common_request(service_method, extra_headers, body))
      .await
    {
      Ok(response) => response?,
      Err(_) => return Err(Error::RequestTimeout),
    };
    let mut decoder = Decoder::default();
    let body = response
      .into_body()
      .collect()
      .await
      .map_err(|e| Error::BodyStream(e.into()))?
      .to_bytes();
    let mut messages = decoder.decode_data(&body)?;

    if messages.len() != 1 {
      return Err(Status::new(Code::Internal, "Invalid response body").into());
    }

    Ok(messages.remove(0))
  }

  // Perform a bi-di streaming request.
  // TODO(mattklein123): Allow an initial vector of request messages to be sent along with the
  //                     request.
  pub async fn streaming<OutgoingType: MessageFull, IncomingType: MessageFull>(
    &self,
    service_method: &ServiceMethod<OutgoingType, IncomingType>,
    mut extra_headers: Option<HeaderMap>,
    validate: bool,
    compression: Option<bd_grpc_codec::Compression>,
  ) -> Result<StreamingApi<OutgoingType, IncomingType>> {
    let (tx, rx) = mpsc::channel(1);
    let body = StreamBody::new(ReceiverStream::new(rx));

    match compression {
      None => {},
      Some(bd_grpc_codec::Compression::Zlib { .. }) => {
        extra_headers.get_or_insert_with(HeaderMap::default).insert(
          GRPC_ENCODING_HEADER,
          GRPC_ENCODING_DEFLATE.try_into().unwrap(),
        );
      },
    }

    let response = self
      .common_request(service_method, extra_headers, Body::new(body))
      .await?;
    let (parts, body) = response.into_parts();

    Ok(StreamingApi::new(
      tx,
      parts.headers,
      Body::new(body),
      validate,
      compression,
    ))
  }

  // Perform a unary streaming request.
  pub async fn server_streaming<OutgoingType: MessageFull, IncomingType: MessageFull>(
    &self,
    service_method: &ServiceMethod<OutgoingType, IncomingType>,
    extra_headers: Option<HeaderMap>,
    request: OutgoingType,
    validate: bool,
  ) -> Result<ServerStreamingApi<OutgoingType, IncomingType>> {
    let mut encoder = Encoder::new(None);
    let response = self
      .common_request(
        service_method,
        extra_headers,
        encoder.encode(&request).into(),
      )
      .await?;
    let (parts, body) = response.into_parts();
    Ok(ServerStreamingApi::new(
      parts.headers,
      Body::new(body),
      validate,
    ))
  }
}

//
// BandwidthStatsSummary
//

pub struct BandwidthStatsSummary {
  pub rx: u64,
  pub rx_decompressed: u64,
  pub tx: u64,
  pub tx_uncompressed: u64,
}

//
// StreamingApi
//

// Handle around a bidirectional streaming API. Allows for both sending outgoing messages and
// receiving response messages.
#[derive(Debug)]
pub struct StreamingApi<OutgoingType: Message, IncomingType: Message> {
  sender: StreamingApiSender<OutgoingType>,

  // A bidirectional API is just the sender + the server streaming API.
  streaming_api: ServerStreamingApi<OutgoingType, IncomingType>,
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
  ) -> Self {
    let sender = StreamingApiSender::new(tx, compression);
    Self {
      sender,
      streaming_api: ServerStreamingApi::new(headers, body, validate),
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
    let (rx, rx_decompressed) = self.streaming_api.decoder.bandwidth_stats();
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
      .streaming_api
      .initialize_bandwidth_stats(rx_bytes, rx_bytes_decompressed);
    self
      .sender
      .initialize_stats(tx_messages_total, tx_bytes, tx_bytes_uncompressed);
  }

  pub async fn next(&mut self) -> Result<Option<Vec<IncomingType>>> {
    self.streaming_api.next().await
  }

  #[must_use]
  pub const fn received_headers(&self) -> &HeaderMap {
    self.streaming_api.received_headers()
  }
}

//
// ServerStreamingApi
//

// Handle around an API stream where the server is streaming responses.
#[derive(Debug)]
pub struct ServerStreamingApi<OutgoingType: Message, IncomingType: Message> {
  headers: HeaderMap,
  body: Body,
  decoder: Decoder<IncomingType>,
  validate: bool,
  _type: PhantomData<(OutgoingType, IncomingType)>,
}

impl<OutgoingType: Message, IncomingType: MessageFull>
  ServerStreamingApi<OutgoingType, IncomingType>
{
  // Create a new streaming API handler.
  #[must_use]
  pub fn new(headers: HeaderMap, body: Body, validate: bool) -> Self {
    Self {
      headers,
      body,
      decoder: Decoder::default(),
      validate,
      _type: PhantomData,
    }
  }
  // Receive a message on the stream. An error indicates either a network or protobuf error. None
  // indicates the stream is complete.
  pub async fn next(&mut self) -> Result<Option<Vec<IncomingType>>> {
    loop {
      if let Some(frame) = self.body.frame().await {
        let frame = frame.map_err(|e| Error::BodyStream(e.into()))?;
        if frame.is_data() {
          let messages = self.decoder.decode_data(frame.data_ref().unwrap())?;
          if self.validate {
            for message in &messages {
              bd_pgv::proto_validate::validate(message)
                .inspect_err(|e| log::debug!("validation failure: {e}"))?;
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
            let code = Code::from_string(grpc_status.to_str().unwrap_or_default());
            if code == Code::Ok {
              return Ok(None);
            }

            let status = Status {
              code,
              message: grpc_message.map(|v| v.to_str().unwrap_or_default().to_string()),
            };
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
#[derive(Debug)]
pub struct StreamingApiSender<ResponseType: Message> {
  encoder: Encoder<ResponseType>,
  tx: BodySender,
  tx_messages_total: DeferredCounter,
  _type: PhantomData<ResponseType>,
}

impl<ResponseType: Message> StreamingApiSender<ResponseType> {
  #[must_use]
  pub fn new(tx: BodySender, compression: Option<bd_grpc_codec::Compression>) -> Self {
    Self {
      encoder: Encoder::new(compression),
      tx,
      tx_messages_total: DeferredCounter::default(),
      _type: PhantomData,
    }
  }

  // Send a message on the stream.
  pub async fn send(&mut self, message: ResponseType) -> Result<()> {
    let encoded = self.encoder.encode(&message);
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

  async fn send_error(&self, status: Status) -> Result<()> {
    log::trace!("sending error trailers for stream");

    let mut trailers = HeaderMap::new();
    trailers.insert(
      GRPC_STATUS,
      HeaderValue::from_str(&status.code.to_int().to_string()).unwrap(),
    );
    if let Some(message) = status.message {
      trailers.insert(GRPC_MESSAGE, HeaderValue::from_str(&message).unwrap());
    }

    self.send_trailers(trailers).await
  }

  async fn send_ok_trailers(&self) -> Result<()> {
    log::trace!("sending ok trailers for stream");
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
pub fn new_grpc_response(body: Body) -> Response {
  Response::builder()
    .header(CONTENT_TYPE, CONTENT_TYPE_GRPC)
    .body(body)
    .unwrap()
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

async fn decode_request<Message: MessageFull>(
  request: Request,
  validate_request: bool,
) -> Result<(HeaderMap<HeaderValue>, Extensions, Message)> {
  let (parts, body) = request.into_parts();
  let mut grpc_decoder = Decoder::default();
  let body_bytes = to_bytes(body, usize::MAX)
    .await
    .map_err(|e| Error::BodyStream(e.into()))?;
  let body_bytes = if parts.headers.get(CONTENT_ENCODING).map_or(false, |v| {
    v.as_bytes() == CONTENT_ENCODING_SNAPPY.as_bytes()
  }) {
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

  let mut messages = grpc_decoder.decode_data(&body_bytes)?;
  if messages.len() != 1 {
    return Err(Status::new(Code::InvalidArgument, "Invalid request body").into());
  }

  if validate_request {
    if let Err(err) = bd_pgv::proto_validate::validate(&messages[0]) {
      return Err(Status::new(Code::InvalidArgument, &format!("Invalid request: {err:?}")).into());
    }
  }

  Ok((parts.headers, parts.extensions, messages.remove(0)))
}

// Axum handler for a unary API.
pub async fn unary_handler<OutgoingType: MessageFull, IncomingType: MessageFull>(
  request: Request,
  handler: Arc<dyn Handler<OutgoingType, IncomingType>>,
  validate_request: bool,
) -> Result<Response> {
  let (headers, extensions, message) =
    decode_request::<OutgoingType>(request, validate_request).await?;

  let response = handler.handle(headers, extensions, message).await?;

  let (tx, rx) = mpsc::channel::<std::result::Result<_, Infallible>>(2);

  let mut encoder = Encoder::new(None);
  let encoded_data = encoder.encode(&response);

  tx.send(Ok(Frame::data(encoded_data))).await.unwrap();

  let mut trailers = HeaderMap::new();
  trailers.insert(GRPC_STATUS, HeaderValue::from_str("0").unwrap());
  tx.send(Ok(Frame::trailers(trailers))).await.unwrap();

  Ok(new_grpc_response(Body::new(StreamBody::new(
    ReceiverStream::new(rx),
  ))))
}

#[must_use]
pub fn finalize_compression(
  compression: Option<bd_grpc_codec::Compression>,
  headers: &HeaderMap,
) -> Option<bd_grpc_codec::Compression> {
  match compression {
    None => None,
    Some(bd_grpc_codec::Compression::Zlib { level }) => headers
      .get(GRPC_ENCODING_HEADER)
      .filter(|v| *v == GRPC_ENCODING_DEFLATE)
      .map(|_| bd_grpc_codec::Compression::Zlib { level }),
  }
}

async fn server_streaming_handler<ResponseType: MessageFull, RequestType: MessageFull>(
  handler: Arc<dyn ServerStreamingHandler<ResponseType, RequestType>>,
  request: Request,
  error_handler: impl Fn(&crate::Error) + Clone + Send + 'static,
  stream_stats: StreamStats,
  validate_request: bool,
  warn_tracker: Arc<WarnTracker>,
  // This indicates if response compression is desired. It will still be gated on whether the
  // client sets the compression header.
  compression: Option<bd_grpc_codec::Compression>,
) -> Result<Response> {
  stream_stats.stream_initiations_total.inc();

  let path = request.uri().path().to_string();

  let (tx, rx) = mpsc::channel(1);
  let (headers, extensions, message) = decode_request::<RequestType>(request, validate_request)
    .await
    .inspect_err(|_| {
      stream_stats.stream_completion_failures_total.inc();
    })?;

  tokio::spawn(async move {
    let sender = &mut StreamingApiSender::new(tx, finalize_compression(compression, &headers));
    sender.initialize_stats(
      stream_stats.tx_messages_total,
      stream_stats.tx_bytes_total,
      stream_stats.tx_bytes_uncompressed_total,
    );

    match handler.stream(headers, extensions, message, sender).await {
      Ok(()) => {
        stream_stats.stream_completion_successes_total.inc();

        // Make sure we send grpc-status: 0 to indicate success if we stop without error.
        // This can fail if the client has disconnected. We ignore the error here since there is
        // nothing more to do.
        let _ignored = sender.send_ok_trailers().await;
      },
      Err(e) => {
        if let Some(warning) = e.warn_every_message() {
          if warn_tracker.should_warn(15.seconds()) {
            log::warn!("{} failed: {warning}", path);
          }
        }

        stream_stats.stream_completion_failures_total.inc();
        error_handler(&e);

        let status = match e {
          Error::Grpc(status) => status,
          e => Status::new(Code::Internal, &format!("{e}")),
        };

        log::debug!("Stream {path} failed: {status}");

        // This can fail if the client has disconnected. We ignore the error here since there is
        // nothing more to do.
        let _ignored = sender.send_error(status).await;
      },
    };
  });

  Ok(new_grpc_response(Body::new(StreamBody::new(
    ReceiverStream::new(rx),
  ))))
}

// Create an axum router for a one directional streaming handler.
pub fn make_server_streaming_router<ResponseType: MessageFull, RequestType: MessageFull>(
  service_method: &ServiceMethod<RequestType, ResponseType>,
  handler: Arc<dyn ServerStreamingHandler<ResponseType, RequestType>>,
  error_handler: impl Fn(&crate::Error) + Clone + Send + 'static,
  stream_stats: StreamStats,
  validate_request: bool,
  compression: Option<bd_grpc_codec::Compression>,
) -> Router {
  let warn_tracker = Arc::new(WarnTracker::default());
  Router::new().route(
    &service_method.full_path,
    post(move |request: Request| async move {
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
    }),
  )
}

// Create an axum router for a unary request and a handler.
pub fn make_unary_router<OutgoingType: MessageFull, IncomingType: MessageFull>(
  service_method: &ServiceMethod<OutgoingType, IncomingType>,
  handler: Arc<dyn Handler<OutgoingType, IncomingType>>,
  error_handler: impl Fn(&crate::Error) + Clone + Send + 'static,
  error_counter: IntCounter,
  validate_request: bool,
) -> Router {
  let warn_tracker = Arc::new(WarnTracker::default());
  let full_path = Arc::new(service_method.full_path.clone());
  Router::new().route(
    &service_method.full_path,
    post(move |request| {
      let cloned_warn_tracker = warn_tracker.clone();
      let cloned_full_path = full_path.clone();
      async move {
        let result =
          unary_handler::<OutgoingType, IncomingType>(request, handler, validate_request).await;

        if let Err(e) = &result {
          if let Some(warning) = e.warn_every_message() {
            if cloned_warn_tracker.should_warn(15.seconds()) {
              log::warn!("{} failed: {warning}", cloned_full_path);
            }
          }

          error_handler(e);
          error_counter.inc();
        }

        result
      }
    }),
  )
}

//
// ServiceMethod
//

// Wraps a gRPC service method after confirming the path matches the proto file.
pub struct ServiceMethod<OutgoingType: MessageFull, IncomingType: MessageFull> {
  full_path: String,
  outgoing_type: PhantomData<OutgoingType>,
  incoming_type: PhantomData<IncomingType>,
}

impl<OutgoingType: MessageFull, IncomingType: MessageFull>
  ServiceMethod<OutgoingType, IncomingType>
{
  // Create a new service method given the service name and the method name.
  #[must_use]
  pub fn new(service_name: &str, method_name: &str) -> Self {
    let message_descriptor = OutgoingType::descriptor();
    let file_descriptor = message_descriptor.file_descriptor();

    Self::new_with_fd(service_name, method_name, file_descriptor)
  }

  // Create a new service method given the service name and the method name. Useful when we cannot
  // infer the file descriptor of the service via the request/response types.
  #[must_use]
  pub fn new_with_fd(
    service_name: &str,
    method_name: &str,
    file_descriptor: &FileDescriptor,
  ) -> Self {
    let mut service_descriptor = None;
    let mut method_descriptor = None;
    for service in file_descriptor.services() {
      if service.proto().name() != service_name {
        continue;
      }

      service_descriptor = Some(service);
      for method in service_descriptor.as_ref().unwrap().methods() {
        if method.proto().name() == method_name {
          method_descriptor = Some(method);
          break;
        }
      }

      if method_descriptor.is_some() {
        break;
      }
    }

    let service_descriptor =
      service_descriptor.unwrap_or_else(|| panic!("could not find service: {service_name}"));
    let method_descriptor =
      method_descriptor.unwrap_or_else(|| panic!("could not find method: {method_name}"));
    assert!(
      method_descriptor.input_type().full_name() == OutgoingType::descriptor().full_name(),
      "service method outgoing type mismatch: {} != {}",
      method_descriptor.input_type().full_name(),
      OutgoingType::descriptor().full_name()
    );
    assert!(
      method_descriptor.output_type().full_name() == IncomingType::descriptor().full_name(),
      "service method incoming type mismatch: {} != {}",
      method_descriptor.output_type().full_name(),
      IncomingType::descriptor().full_name()
    );

    Self {
      full_path: format!(
        "/{}.{}/{}",
        file_descriptor.package(),
        service_descriptor.proto().name(),
        method_descriptor.proto().name(),
      ),
      outgoing_type: PhantomData,
      incoming_type: PhantomData,
    }
  }
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

//
// StreamStats
//

/// gRPC streaming request stats.
#[derive(Clone, Debug)]
pub struct StreamStats {
  // The number of initiated streaming requests.
  stream_initiations_total: IntCounter,
  // The number of successfully completed streaming requests. These streams completed cleanly,
  // without errors.
  stream_completion_successes_total: IntCounter,
  // The number of streaming requests completed due to an error.
  stream_completion_failures_total: IntCounter,

  // The number of messages sent across a stream that was opened in response to a streaming
  // request.
  tx_messages_total: DynCounter,

  tx_bytes_total: DynCounter,
  tx_bytes_uncompressed_total: DynCounter,
}

impl StreamStats {
  #[must_use]
  pub fn new(scope: &Scope, stream_name: &str) -> Self {
    let scope = scope.scope(stream_name);

    let stream_initiations_total = scope.counter("stream_initiations_total");

    let stream_completions_total = scope.counter_vec("stream_completions_total", &["result"]);
    let stream_completion_successes_total = stream_completions_total
      .get_metric_with_label_values(&["success"])
      .unwrap();
    let stream_completion_failures_total = stream_completions_total
      .get_metric_with_label_values(&["failure"])
      .unwrap();

    let tx_messages_total = CounterWrapper::make_dyn(scope.counter("stream_tx_messages_total"));
    let tx_bytes_total = CounterWrapper::make_dyn(scope.counter("bandwidth_tx_bytes_total"));
    let tx_bytes_uncompressed_total =
      CounterWrapper::make_dyn(scope.counter("bandwidth_tx_bytes_uncompressed_total"));

    Self {
      stream_initiations_total,
      stream_completion_successes_total,
      stream_completion_failures_total,

      tx_messages_total,

      tx_bytes_total,
      tx_bytes_uncompressed_total,
    }
  }
}
