// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::compression::Compression;
use crate::connect_protocol::{ConnectProtocolType, ToContentType};
use crate::error::{Error, Result};
use crate::service::ServiceMethod;
use crate::status::{Code, Status};
use crate::{
  CONNECT_PROTOCOL_VERSION,
  CONTENT_ENCODING_SNAPPY,
  GRPC_STATUS,
  StreamingApi,
  StreamingApiReceiver,
  TRANSFER_ENCODING_TRAILERS,
  finalize_decompression,
};
use assert_matches::debug_assert_matches;
use axum::body::Body;
use axum::response::Response;
use bd_grpc_codec::{
  Decoder,
  DecodingResult,
  Encoder,
  GRPC_ACCEPT_ENCODING_HEADER,
  GRPC_ENCODING_DEFLATE,
  GRPC_ENCODING_HEADER,
  OptimizeFor,
};
use bd_time::TimeDurationExt;
use http::header::{CONTENT_ENCODING, CONTENT_TYPE, TRANSFER_ENCODING};
use http::{HeaderMap, Uri};
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Incoming;
use hyper_util::client::legacy::connect::{Connect, HttpConnector};
use hyper_util::rt::TokioExecutor;
use protobuf::MessageFull;
use std::error::Error as StdError;
use std::io::ErrorKind;
use time::Duration;
use tokio::sync::{Semaphore, mpsc};
use tokio_stream::wrappers::ReceiverStream;

//
// AddressHelper
//

#[derive(Debug)]
pub struct AddressHelper {
  address: Uri,
}

impl AddressHelper {
  pub fn new<E: Send + Sync + std::error::Error + 'static>(
    address: impl TryInto<Uri, Error = E>,
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

    Ok(Self { address })
  }

  pub fn build<OutgoingType: MessageFull, IncomingType: MessageFull>(
    &self,
    service_method: &ServiceMethod<OutgoingType, IncomingType>,
  ) -> Uri {
    Uri::builder()
      .scheme(self.address.scheme().unwrap().clone())
      .authority(self.address.authority().unwrap().as_str())
      .path_and_query(service_method.full_path.as_str())
      .build()
      .unwrap()
  }
}

//
// Client
//

// A simple gRPC client wrapper that allows for both unary and streaming requests.
#[derive(Debug)]
pub struct Client<C> {
  client: hyper_util::client::legacy::Client<C, Body>,
  address: AddressHelper,
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
    Ok(Self {
      client,
      address: AddressHelper::new(address)?,
      concurrency: Semaphore::new(max_request_concurrency.try_into().unwrap()),
    })
  }

  // Common request generation for both unary and streaming requests.
  async fn common_request<OutgoingType: MessageFull, IncomingType: MessageFull>(
    &self,
    service_method: &ServiceMethod<OutgoingType, IncomingType>,
    extra_headers: Option<HeaderMap>,
    body: Body,
    connect_protocol: Option<ConnectProtocolType>,
  ) -> Result<Response<Incoming>> {
    // TODO(mattklein123): Potentially implement load shed if we have too many waiters.
    let _permit = self.concurrency.acquire().await.unwrap();

    let uri = self.address.build(service_method);
    let mut request = hyper::Request::builder()
      .method(hyper::Method::POST)
      .uri(uri)
      .header(CONTENT_TYPE, connect_protocol.to_content_type());
    if connect_protocol.is_some() {
      request = request.header(CONNECT_PROTOCOL_VERSION, "1");
    } else {
      request = request.header(TRANSFER_ENCODING, TRANSFER_ENCODING_TRAILERS);
    }
    let mut request = request.body(body).unwrap();
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
          .is_some_and(|e| e.kind() == ErrorKind::TimedOut)
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
          format!("Non-200 response code: {}", response.status()),
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
      Compression::GRpc(compression) => {
        debug_assert_matches!(
          compression,
          bd_grpc_codec::Compression::StatelessZlib { .. }
        );
        let mut encoder = Encoder::new(Some(compression));
        let mut extra_headers = extra_headers.unwrap_or_default();
        extra_headers.insert(
          GRPC_ENCODING_HEADER,
          GRPC_ENCODING_DEFLATE.try_into().unwrap(),
        );
        extra_headers.insert(
          GRPC_ACCEPT_ENCODING_HEADER,
          GRPC_ENCODING_DEFLATE.try_into().unwrap(),
        );
        (Some(extra_headers), encoder.encode(&request).into())
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

    // We don't support Connect for unary as we only need it for test and it's easier to test with
    // reqwest for compression.
    let response = match request_timeout
      .timeout(self.common_request(service_method, extra_headers, body, None))
      .await
    {
      Ok(response) => response?,
      Err(_) => return Err(Error::RequestTimeout),
    };
    let mut decoder =
      Decoder::<IncomingType>::new(finalize_decompression(response.headers()), OptimizeFor::Cpu);
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
    optimize_for: OptimizeFor,
  ) -> Result<StreamingApi<OutgoingType, IncomingType>> {
    let (tx, rx) = mpsc::channel(1);
    let body = StreamBody::new(ReceiverStream::new(rx));

    match compression {
      None => {},
      Some(bd_grpc_codec::Compression::StatefulZlib { .. }) => {
        // This is kept around for legacy clients where we only support server side. Tests
        // are covered in codec tests.
        unimplemented!()
      },
      Some(bd_grpc_codec::Compression::StatelessZlib { .. }) => {
        let headers = extra_headers.get_or_insert_with(HeaderMap::default);
        headers.insert(
          GRPC_ENCODING_HEADER,
          GRPC_ENCODING_DEFLATE.try_into().unwrap(),
        );
        headers.insert(
          GRPC_ACCEPT_ENCODING_HEADER,
          GRPC_ENCODING_DEFLATE.try_into().unwrap(),
        );
      },
    }

    // No bi-di Connect support currently.
    let response = self
      .common_request(service_method, extra_headers, Body::new(body), None)
      .await?;
    let (parts, body) = response.into_parts();

    Ok(StreamingApi::new(
      tx,
      parts.headers,
      Body::new(body),
      validate,
      compression,
      optimize_for,
      None,
    ))
  }

  // Perform a unary streaming request.
  // TODO(mattklein123): Support compression.
  pub async fn server_streaming<OutgoingType: MessageFull, IncomingType: DecodingResult>(
    &self,
    service_method: &ServiceMethod<OutgoingType, IncomingType::Message>,
    extra_headers: Option<HeaderMap>,
    request: OutgoingType,
    validate: bool,
    optimize_for: OptimizeFor,
    connect_protocol: Option<ConnectProtocolType>,
  ) -> Result<StreamingApiReceiver<IncomingType>> {
    let mut encoder = Encoder::new(None);
    let response = self
      .common_request(
        service_method,
        extra_headers,
        encoder.encode(&request).into(),
        connect_protocol,
      )
      .await?;
    let (parts, body) = response.into_parts();
    Ok(StreamingApiReceiver::new(
      parts.headers,
      Body::new(body),
      validate,
      optimize_for,
      None,
    ))
  }
}
