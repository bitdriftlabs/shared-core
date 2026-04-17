// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::status::{RequestTransport, Status};
use axum::BoxError;
use axum::response::Response;
use bd_grpc_codec::code::Code;

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

impl Error {
  fn into_status(self) -> Status {
    match self {
      Self::Grpc(status) => status,
      Self::Codec(_) | Self::ProtoValidation(_) | Self::Snap(_) => {
        let error_message = self.to_string();
        Status::new(
          Code::InvalidArgument,
          error_message.clone(),
          Some(error_message),
        )
      },
      Self::ConnectionTimeout
      | Self::RequestTimeout
      | Self::BodyStream(_)
      | Self::Closed
      | Self::HyperClient(_) => {
        let error_message = self.to_string();
        Status::new(Code::Internal, error_message.clone(), Some(error_message))
      },
    }
  }

  #[must_use]
  pub fn warn_every_message(&self) -> Option<String> {
    match self {
      Self::ConnectionTimeout | Self::RequestTimeout => Some("upstream timeout".to_string()),
      Self::Grpc(status) => {
        if status.code() == Code::Internal {
          Some(format!(
            "gRPC internal error ({})",
            status.message().unwrap_or("")
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

  #[must_use]
  pub fn to_response(self, transport: RequestTransport) -> Response {
    self.into_status().into_response_for_transport(transport)
  }
}
