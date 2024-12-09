use crate::{CONNECT_PROTOCOL_VERSION, CONTENT_TYPE_CONNECT_STREAMING, CONTENT_TYPE_PROTO};
use http::header::CONTENT_TYPE;
use http::HeaderMap;
use serde::Serialize;

pub fn is_unary_connect(headers: &HeaderMap) -> bool {
  headers
    .get(CONTENT_TYPE)
    .map_or(false, |v| v == CONTENT_TYPE_PROTO)
    && headers
      .get(CONNECT_PROTOCOL_VERSION)
      .map_or(false, |v| v == "1")
}

pub fn is_streaming_connect(headers: &HeaderMap) -> bool {
  headers
    .get(CONTENT_TYPE)
    .map_or(false, |v| v == CONTENT_TYPE_CONNECT_STREAMING)
    && headers
      .get(CONNECT_PROTOCOL_VERSION)
      .map_or(false, |v| v == "1")
}


//
// ErrorResponse
//

#[derive(Serialize)]
pub struct ErrorResponse {
  pub error: String,
  pub message: String,
}
