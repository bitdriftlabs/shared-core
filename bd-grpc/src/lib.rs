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
use axum::extract::Request;
use axum::http::HeaderValue;
use axum::response::Response;
use axum::routing::post;
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
use http_body_util::{BodyExt, StreamBody};
use protobuf::{Message, MessageFull};
use service::ServiceMethod;
use stats::{BandwidthStatsSummary, EndpointStats, StreamStats};
use status::Status;
use std::convert::Infallible;
use std::marker::PhantomData;
use std::sync::Arc;
use time::ext::NumericalDuration;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;

const GRPC_STATUS: &str = "grpc-status";
const GRPC_MESSAGE: &str = "grpc-message";
pub const CONTENT_ENCODING_SNAPPY: &str = "snappy";
pub const CONTENT_TYPE_GRPC: &str = "application/grpc";
const CONTENT_TYPE_PROTO: &str = "application/proto";
const CONTENT_TYPE_CONNECT_STREAMING: &str = "application/connect+proto";
const CONTENT_TYPE_JSON: &str = "application/json";
const TRANSFER_ENCODING_TRAILERS: &str = "trailers";
const CONNECT_PROTOCOL_VERSION: &str = "connect-protocol-version";

pub type BodySender = mpsc::Sender<std::result::Result<Frame<Bytes>, BoxError>>;

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
    // TODO(mattklein123): Support connect protocol for bidirectional streaming. We have no
    // current use case for this right now.
    let sender = StreamingApiSender::new(tx, compression, false);
    Self {
      sender,
      receiver: StreamingApiReceiver::new(headers, body, validate, optimize_for, read_stop),
    }
  }

  // Create a new streaming API handler from an existing sender. This is useful when initial
  // requests need to be sent before the HTTP request completes.
  #[must_use]
  pub fn from_sender(
    sender: StreamingApiSender<OutgoingType>,
    headers: HeaderMap,
    body: Body,
    validate: bool,
    optimize_for: OptimizeFor,
    read_stop: Option<watch::Receiver<bool>>,
  ) -> Self {
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
      .filter(|v| *v == GRPC_ENCODING_DEFLATE)
      .is_some()
    {
      Some(bd_grpc_codec::Decompression::StatelessZlib)
    } else if headers
      .get(LEGACY_GRPC_ENCODING_HEADER)
      .filter(|v| *v == GRPC_ENCODING_DEFLATE)
      .is_some()
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
          error: Some(ErrorResponse::new(status)),
        })
        .await;
    }

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

async fn decode_request<Message: MessageFull>(
  request: Request,
  validate_request: bool,
  connect_protocol_type: Option<ConnectProtocolType>,
) -> Result<(HeaderMap<HeaderValue>, Extensions, Message)> {
  let (parts, body) = request.into_parts();
  let body_bytes = to_bytes(body, usize::MAX)
    .await
    .map_err(|e| Error::BodyStream(e.into()))?;
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
    Message::parse_from_tokio_bytes(&body_bytes)
      .map_err(|e| Status::new(Code::InvalidArgument, format!("Invalid request: {e}")))?
  } else {
    let mut grpc_decoder =
      Decoder::<Message>::new(finalize_decompression(&parts.headers), OptimizeFor::Cpu);
    let mut messages = grpc_decoder.decode_data(&body_bytes)?;
    if messages.len() != 1 {
      return Err(Status::new(Code::InvalidArgument, "Invalid request body").into());
    }
    messages.remove(0)
  };

  if validate_request {
    bd_pgv::proto_validate::validate(&message)
      .map_err(|e| Status::new(Code::InvalidArgument, format!("Invalid request: {e}")))?;
  }

  Ok((parts.headers, parts.extensions, message))
}

async fn unary_connect_handler<OutgoingType: MessageFull, IncomingType: MessageFull>(
  headers: HeaderMap,
  extensions: Extensions,
  message: OutgoingType,
  handler: Arc<dyn Handler<OutgoingType, IncomingType>>,
) -> Result<Response> {
  let response = handler.handle(headers, extensions, message).await?;
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
  connect_protocol_type: Option<ConnectProtocolType>,
) -> Result<Response> {
  let (headers, extensions, message) =
    decode_request::<OutgoingType>(request, validate_request, connect_protocol_type).await?;
  if matches!(connect_protocol_type, Some(ConnectProtocolType::Unary)) {
    return Ok(
      match unary_connect_handler(headers, extensions, message, handler).await {
        Ok(response) => response,
        Err(e) => e.to_connect_error_response(),
      },
    );
  }

  let compression = finalize_response_compression(
    Some(bd_grpc_codec::Compression::StatelessZlib { level: 3 }),
    &headers,
  );

  let response = handler.handle(headers, extensions, message).await?;

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
  connect_protocol_type: Option<ConnectProtocolType>,
) -> Result<Response> {
  if let Some(stream_stats) = &stream_stats {
    stream_stats.stream_initiations_total.inc();
  }

  let path = request.uri().path().to_string();

  let (tx, rx) = mpsc::channel(1);
  let (headers, extensions, message) =
    decode_request::<RequestType>(request, validate_request, connect_protocol_type)
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
          e => Status::new(Code::Internal, format!("{e}")),
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

// Create an axum router for a one directional streaming handler.
pub fn make_server_streaming_router<ResponseType: MessageFull, RequestType: MessageFull>(
  service_method: &ServiceMethod<RequestType, ResponseType>,
  handler: Arc<dyn ServerStreamingHandler<ResponseType, RequestType>>,
  error_handler: impl Fn(&crate::Error) + Clone + Send + Sync + 'static,
  stream_stats: Option<&EndpointStats>,
  validate_request: bool,
  compression: Option<bd_grpc_codec::Compression>,
) -> Router {
  let warn_tracker = Arc::new(WarnTracker::default());
  let stream_stats = stream_stats.map(|stats| stats.resolve_streaming(service_method));
  Router::new().route(
    &service_method.full_path(),
    post(move |request: Request| async move {
      let connect_protocol_type = ConnectProtocolType::from_headers(request.headers());
      server_streaming_handler(
        handler,
        request,
        error_handler,
        stream_stats,
        validate_request,
        warn_tracker.clone(),
        compression,
        connect_protocol_type,
      )
      .await
      .map_err(|e| e.to_response(connect_protocol_type))
    }),
  )
}

// Create an axum router for a unary request and a handler.
pub fn make_unary_router<OutgoingType: MessageFull, IncomingType: MessageFull>(
  service_method: &ServiceMethod<OutgoingType, IncomingType>,
  handler: Arc<dyn Handler<OutgoingType, IncomingType>>,
  error_handler: impl Fn(&crate::Error) + Clone + Send + Sync + 'static,
  endpoint_stats: Option<&EndpointStats>,
  validate_request: bool,
) -> Router {
  let warn_tracker = Arc::new(WarnTracker::default());
  let full_path = Arc::new(service_method.full_path());
  let resolved_stats = endpoint_stats
    .as_ref()
    .map(|stats| stats.resolve::<OutgoingType, IncomingType>(service_method));
  Router::new().route(
    &service_method.full_path(),
    post(move |request: Request| async move {
      let connect_protocol_type = ConnectProtocolType::from_headers(request.headers());
      let result = unary_handler::<OutgoingType, IncomingType>(
        request,
        handler,
        validate_request,
        connect_protocol_type,
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

      result.map_err(|e| e.to_response(connect_protocol_type))
    }),
  )
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
