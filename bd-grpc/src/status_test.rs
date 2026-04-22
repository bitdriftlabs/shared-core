// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

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
