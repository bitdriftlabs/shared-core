// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::SerializedOffloadRequest;
use bytes::Bytes;
use http::{HeaderMap, HeaderValue};
use std::sync::Arc;

#[test]
fn serializes_request_payload_and_headers() {
  let mut headers = HeaderMap::new();
  headers.insert("x-test", HeaderValue::from_static("value"));
  let mut request = SerializedOffloadRequest::new(
    &Bytes::from_static(b"payload"),
    Some(Arc::new(headers)),
    2,
    1_700_000_000,
  );

  assert_eq!(
    request.compressed_write_request(),
    Bytes::from_static(b"payload")
  );
  assert_eq!(request.extra_headers().unwrap()["x-test"], "value");
  assert_eq!(request.num_metrics(), 2);
  assert_eq!(request.created_at().unix_timestamp(), 1_700_000_000);
  assert_eq!(request.retry_attempts(), 0);
  request.inc_retry_attempts();
  assert_eq!(request.retry_attempts(), 1);
}
