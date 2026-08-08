// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use super::Status;
use axum::body::Body;
use axum::response::Response;

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
