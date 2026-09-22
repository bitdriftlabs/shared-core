//! Transport policy shared by descriptor-generated unary clients.

use crate::client::Client;
use crate::compression::Compression;
use crate::error::Error;
use crate::service::ServiceMethod;
use async_trait::async_trait;
use http::HeaderMap;
use hyper_util::client::legacy::connect::Connect;
use protobuf::MessageFull;
use time::Duration;

//
// UnaryTransport
//

#[async_trait]
pub trait UnaryTransport: Send + Sync {
  type Error: Send + Sync + 'static;

  async fn unary<Request, Response>(
    &self,
    method: &ServiceMethod<Request, Response>,
    request: Request,
  ) -> Result<Response, Self::Error>
  where
    Request: MessageFull + Clone + Send + Sync + 'static,
    Response: MessageFull + Send + Sync + 'static;
}

//
// GrpcUnaryTransport
//

pub struct GrpcUnaryTransport<C> {
  client: Client<C>,
  headers: HeaderMap,
  timeout: Duration,
  compression: Compression,
}

impl<C> GrpcUnaryTransport<C> {
  #[must_use]
  pub const fn new(
    client: Client<C>,
    headers: HeaderMap,
    timeout: Duration,
    compression: Compression,
  ) -> Self {
    Self {
      client,
      headers,
      timeout,
      compression,
    }
  }
}

#[async_trait]
impl<C: Connect + Clone + Send + Sync + 'static> UnaryTransport for GrpcUnaryTransport<C> {
  type Error = Error;

  async fn unary<Request, Response>(
    &self,
    method: &ServiceMethod<Request, Response>,
    request: Request,
  ) -> Result<Response, Self::Error>
  where
    Request: MessageFull + Clone + Send + Sync + 'static,
    Response: MessageFull + Send + Sync + 'static,
  {
    self
      .client
      .unary(
        method,
        Some(self.headers.clone()),
        request,
        self.timeout,
        self.compression.clone(),
      )
      .await
  }
}
