// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use crate::status::{RequestTransport, Status};
use axum::BoxError;
use axum::response::Response;
use bd_grpc_codec::code::Code;
use std::error::Error as StdError;
use std::io::{Error as IoError, ErrorKind};

//
// Error
//

#[derive(Debug, thiserror::Error)]
pub enum Error {
  #[error("Body stream error ocurred: {0}")]
  BodyStream(BoxError),
  #[error("Client disconnected while reading the request body: {0}")]
  ClientDisconnected(BoxError),
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

//
// ClientDisconnect
//

#[derive(Clone, Copy, Debug)]
struct ClientDisconnect;

impl Error {
  pub(super) fn classify_unary_request_error(self) -> Self {
    match self {
      Self::BodyStream(error) if is_peer_disconnect(error.as_ref()) => {
        log::debug!("client disconnected while reading unary request body: {error}");
        Self::ClientDisconnected(error)
      },
      error => error,
    }
  }

  #[must_use]
  pub fn grpc_code(&self) -> Code {
    match self {
      Self::ClientDisconnected(_) => Code::Cancelled,
      Self::Grpc(status) => status.code(),
      Self::Codec(bd_grpc_codec::Error::MessageTooLarge { .. }) => Code::ResourceExhausted,
      Self::Codec(_) | Self::ProtoValidation(_) | Self::Snap(_) => Code::InvalidArgument,
      Self::ConnectionTimeout
      | Self::RequestTimeout
      | Self::BodyStream(_)
      | Self::Closed
      | Self::HyperClient(_) => Code::Internal,
    }
  }

  fn into_status(self) -> Status {
    match self {
      Self::ClientDisconnected(_) => Status::new(Code::Cancelled, "Client disconnected", None),
      Self::Grpc(status) => status,
      Self::Codec(bd_grpc_codec::Error::MessageTooLarge {
        message_bytes,
        max_bytes,
      }) => {
        let error_message = format!("gRPC message exceeds {max_bytes} byte limit: {message_bytes}");
        Status::new(
          Code::ResourceExhausted,
          error_message.clone(),
          Some(error_message),
        )
      },
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
      Self::ClientDisconnected(_)
      | Self::Codec(_)
      | Self::ProtoValidation(_)
      | Self::Snap(_)
      | Self::Closed => None,
      Self::BodyStream(e) => Some(format!("body stream error: {e}")),
      Self::HyperClient(e) => Some(format!("hyper client error: {e}")),
    }
  }

  #[must_use]
  pub fn is_client_disconnected_response<ResponseBody>(response: &Response<ResponseBody>) -> bool {
    response.extensions().get::<ClientDisconnect>().is_some()
  }

  #[must_use]
  pub fn to_response(self, transport: RequestTransport) -> Response {
    let client_disconnected = matches!(&self, Self::ClientDisconnected(_));
    let mut response = self.into_status().into_response_for_transport(transport);
    if client_disconnected {
      response.extensions_mut().insert(ClientDisconnect);
    }
    response
  }
}

fn is_peer_disconnect(mut error: &(dyn StdError + 'static)) -> bool {
  loop {
    if let Some(error) = error.downcast_ref::<hyper::Error>()
      && (error.is_canceled() || error.is_closed() || error.is_incomplete_message())
    {
      return true;
    }
    if let Some(error) = error.downcast_ref::<h2::Error>()
      && matches!(
        error.reason(),
        Some(h2::Reason::CANCEL | h2::Reason::NO_ERROR)
      )
    {
      return true;
    }
    let next = if let Some(error) = error.downcast_ref::<IoError>() {
      if matches!(
        error.kind(),
        ErrorKind::ConnectionReset
          | ErrorKind::ConnectionAborted
          | ErrorKind::BrokenPipe
          | ErrorKind::UnexpectedEof
          | ErrorKind::NotConnected
      ) {
        return true;
      }
      error
        .get_ref()
        .map(|inner| inner as &(dyn StdError + 'static))
        .or_else(|| error.source())
    } else {
      error.source()
    };
    let Some(next) = next else {
      return false;
    };
    error = next;
  }
}
