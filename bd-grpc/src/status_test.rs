// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use super::{Status, code_to_connect_code_string, code_to_connect_http_status};
use axum::body::Body;
use axum::response::Response;
use bd_grpc_codec::code::Code;
use http::header::HeaderName;
use http::{HeaderMap, HeaderValue, StatusCode};

#[test]
fn set_trace_error_message_attaches_message_to_response() {
  let mut response = Response::builder()
    .status(500)
    .body(Body::from("internal server error"))
    .unwrap();

  Status::set_trace_error_message(&mut response, "original error");

  assert_eq!(
    Status::trace_error_message_from_response(&response),
    Some("original error")
  );
}

#[test]
fn response_context_preserves_status_and_headers() {
  let response_headers = HeaderMap::from_iter([(
    HeaderName::from_static("x-request-id"),
    HeaderValue::from_static("request-123"),
  )]);
  let status = Status::new(Code::Internal, "upstream error", None)
    .with_response_context(StatusCode::BAD_GATEWAY, response_headers);

  assert_eq!(status.response_status(), Some(StatusCode::BAD_GATEWAY));
  assert_eq!(
    status
      .response_headers()
      .and_then(|headers| headers.get("x-request-id")),
    Some(&HeaderValue::from_static("request-123"))
  );
}

#[test]
fn all_grpc_codes_have_connect_mappings() {
  let mappings = [
    (Code::Ok, "ok", 200),
    (Code::Cancelled, "canceled", 499),
    (Code::Unknown, "unknown", 500),
    (Code::InvalidArgument, "invalid_argument", 400),
    (Code::DeadlineExceeded, "deadline_exceeded", 504),
    (Code::NotFound, "not_found", 404),
    (Code::AlreadyExists, "already_exists", 409),
    (Code::PermissionDenied, "permission_denied", 403),
    (Code::ResourceExhausted, "resource_exhausted", 429),
    (Code::FailedPrecondition, "failed_precondition", 400),
    (Code::Aborted, "aborted", 409),
    (Code::OutOfRange, "out_of_range", 400),
    (Code::Unimplemented, "unimplemented", 501),
    (Code::Internal, "internal", 500),
    (Code::Unavailable, "unavailable", 503),
    (Code::DataLoss, "data_loss", 500),
    (Code::Unauthenticated, "unauthenticated", 401),
  ];

  for (code, expected_connect_code, expected_http_status) in mappings {
    assert_eq!(code_to_connect_code_string(code), expected_connect_code);
    assert_eq!(
      code_to_connect_http_status(code).as_u16(),
      expected_http_status
    );
  }
}
