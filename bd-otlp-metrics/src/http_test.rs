// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{HttpRemoteWriteError, should_retry};
use crate::http::HttpRetryPolicy;
use http::{HeaderMap, StatusCode};
use std::iter::once;
use std::time::Duration;
use time::OffsetDateTime;

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

#[test]
fn otlp_retry_policy_is_status_specific() {
  for status in 100 .. 600 {
    let error = HttpRemoteWriteError::Response(
      StatusCode::from_u16(status).unwrap(),
      String::new(),
      HeaderMap::new(),
    );
    assert_eq!(
      HttpRetryPolicy::Otlp.should_retry(&error),
      matches!(status, 429 | 502 | 503 | 504)
    );
    assert_eq!(
      HttpRetryPolicy::RemoteWrite.should_retry(&error),
      status == 429 || status >= 500
    );
  }
  assert!(HttpRetryPolicy::Otlp.should_retry(&HttpRemoteWriteError::Timeout));
  for error in [
    HttpRemoteWriteError::Permanent("TLS validation failed".to_string()),
    HttpRemoteWriteError::PartialSuccess {
      rejected_data_points: 2,
      error_message: "rejected".to_string(),
    },
    HttpRemoteWriteError::PartialSuccess {
      rejected_data_points: 0,
      error_message: "warning".to_string(),
    },
  ] {
    assert!(!HttpRetryPolicy::Otlp.should_retry(&error));
    assert!(!HttpRetryPolicy::RemoteWrite.should_retry(&error));
  }
}

#[test]
fn retry_after_seconds_and_dates_are_opt_in() {
  let now = OffsetDateTime::from_unix_timestamp(1_791_374_400)
    .unwrap()
    .into();
  for (value, expected) in [
    ("5", Some(Duration::from_secs(5))),
    (
      "Wed, 07 Oct 2026 12:00:05 GMT",
      Some(Duration::from_secs(5)),
    ),
    ("Wed, 07 Oct 2026 11:59:59 GMT", Some(Duration::ZERO)),
    ("0", Some(Duration::ZERO)),
    ("-1", None),
    ("invalid", None),
    ("18446744073709551616", None),
  ] {
    let error = HttpRemoteWriteError::Response(
      StatusCode::SERVICE_UNAVAILABLE,
      String::new(),
      once((http::header::RETRY_AFTER, value.parse().unwrap())).collect(),
    );
    assert_eq!(
      HttpRetryPolicy::Otlp.retry_after(&error, now),
      expected,
      "{value}"
    );
    assert_eq!(HttpRetryPolicy::RemoteWrite.retry_after(&error, now), None);
  }
}
