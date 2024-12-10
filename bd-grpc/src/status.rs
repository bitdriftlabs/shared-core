// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{CONTENT_TYPE_GRPC, GRPC_MESSAGE, GRPC_STATUS};
use axum::body::Body;
use axum::response::Response;
use http::header::CONTENT_TYPE;
use http::{HeaderMap, HeaderValue, StatusCode};

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

  // https://connectrpc.com/docs/protocol#error-codes
  #[must_use]
  pub const fn to_connect_http_status(&self) -> StatusCode {
    match self {
      Self::Ok => StatusCode::OK,
      Self::Unknown | Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
      Self::InvalidArgument | Self::FailedPrecondition => StatusCode::BAD_REQUEST,
      Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
      Self::Unauthenticated => StatusCode::UNAUTHORIZED,
      Self::NotFound => StatusCode::NOT_FOUND,
    }
  }

  // https://connectrpc.com/docs/protocol#error-codes
  #[must_use]
  pub const fn to_connect_code_string(&self) -> &'static str {
    match self {
      Self::Ok => "ok",
      Self::Unknown => "unknown",
      Self::InvalidArgument => "invalid_argument",
      Self::FailedPrecondition => "failed_precondition",
      Self::Internal => "internal",
      Self::Unavailable => "unavailable",
      Self::Unauthenticated => "unauthenticated",
      Self::NotFound => "not_found",
    }
  }
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
  pub fn new(code: Code, message: impl Into<String>) -> Self {
    Self {
      code,
      message: Some(message.into()),
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
      self.message.as_ref().map_or("<none>", |s| s.as_str())
    )
  }
}

impl std::error::Error for Status {}
