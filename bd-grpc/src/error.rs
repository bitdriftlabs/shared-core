// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::CONTENT_TYPE_JSON;
use crate::connect_protocol::{ConnectProtocolType, ErrorResponse};
use crate::status::{Status, code_to_connect_http_status};
use axum::BoxError;
use axum::response::Response;
use bd_grpc_codec::code::Code;
use http::header::CONTENT_TYPE;

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
        Status::new(Code::InvalidArgument, self.to_string())
      },
      Self::ConnectionTimeout
      | Self::RequestTimeout
      | Self::BodyStream(_)
      | Self::Closed
      | Self::HyperClient(_) => Status::new(Code::Internal, self.to_string()),
    }
  }

  #[must_use]
  pub fn warn_every_message(&self) -> Option<String> {
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

  pub fn to_connect_error_response(self) -> Response {
    let status = self.into_status();
    Response::builder()
      .status(code_to_connect_http_status(status.code))
      .header(CONTENT_TYPE, CONTENT_TYPE_JSON)
      .body(
        serde_json::to_vec(&ErrorResponse::new(status))
          .unwrap()
          .into(),
      )
      .unwrap()
  }

  #[must_use]
  pub fn to_response(self, connect_protocol: Option<ConnectProtocolType>) -> Response {
    if connect_protocol.is_some() {
      self.to_connect_error_response()
    } else {
      self.into_status().into_response()
    }
  }
}
