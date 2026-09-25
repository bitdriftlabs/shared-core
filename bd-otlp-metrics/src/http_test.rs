// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{HttpRemoteWriteError, should_retry};
use http::{HeaderMap, StatusCode};

#[test]
fn retries_transient_http_errors() {
  assert!(should_retry(&HttpRemoteWriteError::Timeout));
  assert!(should_retry(&HttpRemoteWriteError::Response(
    StatusCode::TOO_MANY_REQUESTS,
    String::new(),
    HeaderMap::new(),
  )));
  assert!(should_retry(&HttpRemoteWriteError::Response(
    StatusCode::INTERNAL_SERVER_ERROR,
    String::new(),
    HeaderMap::new(),
  )));
}

#[test]
fn does_not_retry_client_http_errors() {
  assert!(!should_retry(&HttpRemoteWriteError::Response(
    StatusCode::BAD_REQUEST,
    String::new(),
    HeaderMap::new(),
  )));
}
