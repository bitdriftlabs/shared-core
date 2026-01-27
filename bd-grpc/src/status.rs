// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{CONTENT_TYPE_GRPC, GRPC_MESSAGE, GRPC_STATUS};
use axum::body::Body;
use axum::response::Response;
use bd_grpc_codec::code::Code;
use http::header::CONTENT_TYPE;
use http::{HeaderMap, HeaderValue, StatusCode};

// https://connectrpc.com/docs/protocol#error-codes
#[must_use]
pub const fn code_to_connect_http_status(code: Code) -> StatusCode {
  match code {
    Code::Ok => StatusCode::OK,
    Code::Unknown | Code::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    Code::InvalidArgument | Code::FailedPrecondition => StatusCode::BAD_REQUEST,
    Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
    Code::Unauthenticated => StatusCode::UNAUTHORIZED,
    Code::NotFound => StatusCode::NOT_FOUND,
    Code::PermissionDenied => StatusCode::FORBIDDEN,
    Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
  }
}

// https://connectrpc.com/docs/protocol#error-codes
#[must_use]
pub const fn code_to_connect_code_string(code: Code) -> &'static str {
  match code {
    Code::Ok => "ok",
    Code::Unknown => "unknown",
    Code::InvalidArgument => "invalid_argument",
    Code::FailedPrecondition => "failed_precondition",
    Code::Internal => "internal",
    Code::Unavailable => "unavailable",
    Code::Unauthenticated => "unauthenticated",
    Code::NotFound => "not_found",
    Code::PermissionDenied => "permission_denied",
    Code::ResourceExhausted => "resource_exhausted",
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
      code: Code::from_str(
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

    if let Some(message) = self.message.as_ref() {
      // We need to make sure the message is a valid header so we URL encode it to be sure.
      let encoded = urlencoding::encode(message);
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
