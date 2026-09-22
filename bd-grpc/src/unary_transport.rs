//! Transport policy shared by descriptor-generated unary clients.

use crate::service::ServiceMethod;
use async_trait::async_trait;
use protobuf::MessageFull;

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
