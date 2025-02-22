// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::status::Status;
use crate::{
  CONNECT_PROTOCOL_VERSION,
  CONTENT_TYPE_CONNECT_STREAMING,
  CONTENT_TYPE_GRPC,
  CONTENT_TYPE_PROTO,
};
use http::HeaderMap;
use http::header::CONTENT_TYPE;
use serde::Serialize;

pub trait ToContentType {
  fn to_content_type(&self) -> &'static str;
}

//
// ConnectProtocolType
//

#[derive(Clone, Copy)]
pub enum ConnectProtocolType {
  Unary,
  Streaming,
}

impl ConnectProtocolType {
  pub fn from_headers(headers: &HeaderMap) -> Option<Self> {
    if headers
      .get(CONNECT_PROTOCOL_VERSION)
      .is_none_or(|v| v != "1")
    {
      return None;
    }

    let content_type = headers.get(CONTENT_TYPE);
    if content_type.is_some_and(|v| v == CONTENT_TYPE_PROTO) {
      Some(Self::Unary)
    } else if content_type.is_some_and(|v| v == CONTENT_TYPE_CONNECT_STREAMING) {
      Some(Self::Streaming)
    } else {
      None
    }
  }
}

impl ToContentType for Option<ConnectProtocolType> {
  fn to_content_type(&self) -> &'static str {
    match self {
      Some(ConnectProtocolType::Unary) => CONTENT_TYPE_PROTO,
      Some(ConnectProtocolType::Streaming) => CONTENT_TYPE_CONNECT_STREAMING,
      None => CONTENT_TYPE_GRPC,
    }
  }
}

//
// ErrorResponse
//

#[derive(Serialize)]
pub struct ErrorResponse {
  pub code: String,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub message: Option<String>,
}

impl ErrorResponse {
  #[must_use]
  pub fn new(status: Status) -> Self {
    Self {
      code: status.code.to_connect_code_string().to_string(),
      message: status.message,
    }
  }
}

//
// EndOfStreamResponse
//

#[derive(Serialize)]
pub struct EndOfStreamResponse {
  #[serde(skip_serializing_if = "Option::is_none")]
  pub error: Option<ErrorResponse>,
}
