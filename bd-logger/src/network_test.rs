// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{NetworkQualityInterceptor, TimeProvider};
use crate::network::HTTPTrafficDataUsageTracker;
use bd_api::api::SimpleNetworkQualityProvider;
use bd_log_metadata::AnnotatedLogFields;
use bd_log_primitives::{
  log_level,
  AnnotatedLogField,
  LogField,
  LogFieldKind,
  LogInterceptor,
  LogMessage,
  LogType,
  StringOrBytes,
};
use bd_network_quality::NetworkQuality;
use pretty_assertions::assert_eq;
use std::sync::Arc;

//
// MockTimeProvider
//

struct MockTimeProvider {
  now: parking_lot::Mutex<std::time::Instant>,
}

impl Default for MockTimeProvider {
  fn default() -> Self {
    Self {
      now: parking_lot::Mutex::new(std::time::Instant::now()),
    }
  }
}

impl TimeProvider for MockTimeProvider {
  fn now(&self) -> std::time::Instant {
    *self.now.lock()
  }
}

impl MockTimeProvider {
  fn advance_by(&self, duration: std::time::Duration) {
    let mut guard = self.now.lock();
    *guard = guard.checked_add(duration).unwrap();
  }
}

#[test]
fn network_quality() {
  let network_quality_provider = Arc::new(SimpleNetworkQualityProvider::default());
  let mut interceptor = NetworkQualityInterceptor::new(network_quality_provider.clone());
  {
    let mut fields = vec![];
    interceptor.process(log_level::DEBUG, LogType::Normal, &"".into(), &mut fields);
    assert!(fields.is_empty());
    network_quality_provider.set_for_test(NetworkQuality::Online);
    interceptor.process(log_level::DEBUG, LogType::Normal, &"".into(), &mut fields);
    assert!(fields.is_empty());
    network_quality_provider.set_for_test(NetworkQuality::Offline);
    interceptor.process(log_level::DEBUG, LogType::Normal, &"".into(), &mut fields);
    assert_eq!(
      fields[0],
      AnnotatedLogField {
        field: LogField {
          key: "_network_quality".to_string(),
          value: StringOrBytes::String("offline".to_string()),
        },
        kind: LogFieldKind::Ootb,
      }
    );
  }
  {
    let mut fields = vec![];
    interceptor.process(log_level::DEBUG, LogType::Replay, &"".into(), &mut fields);
    assert!(fields.is_empty());
  }
}

#[tokio::test]
async fn collects_bandwidth_sample() {
  let time_provider = Arc::new(MockTimeProvider::default());
  let mut tracker = HTTPTrafficDataUsageTracker::new_with_time_provider(time_provider.clone());

  // The tracker reports bandwidth usage on per minute basis.
  // Since we inform the tracker that the rate at which we ask it for fields is equal to 3 seconds,
  // it needs 20 ticks before it accumulates 1 minutes (3 second * 20 ticks) of data and returns
  // samples summary.
  for _ in 0 .. 20 {
    time_provider.advance_by(std::time::Duration::from_secs(3));

    let mut fields = vec![];
    tracker.process(log_level::DEBUG, LogType::Resource, &"".into(), &mut fields);
    assert!(fields.is_empty());
  }

  tracker.process(
    log_level::DEBUG,
    LogType::Span,
    &LogMessage::String("HTTPResponse".to_string()),
    &mut vec![
      create_int_field("_request_body_bytes_sent_count", 100),
      create_int_field("_request_headers_bytes_count", 200),
      create_int_field("_response_body_bytes_received_count", 300),
      create_int_field("_response_headers_bytes_count", 400),
    ],
  );

  for _ in 0 .. 20 {
    time_provider.advance_by(std::time::Duration::from_secs(3));

    let mut fields = vec![];
    tracker.process(log_level::DEBUG, LogType::Resource, &"".into(), &mut fields);
    assert!(!fields.is_empty());

    assert_eq!(
      Some(100),
      get_int_field_value(&fields, "_request_body_bytes_per_min_count")
    );
    assert_eq!(
      Some(200),
      get_int_field_value(&fields, "_request_headers_bytes_per_min_count")
    );
    assert_eq!(
      Some(300),
      get_int_field_value(&fields, "_request_bytes_per_min_count")
    );

    assert_eq!(
      Some(300),
      get_int_field_value(&fields, "_response_body_bytes_per_min_count")
    );
    assert_eq!(
      Some(400),
      get_int_field_value(&fields, "_response_headers_bytes_per_min_count")
    );
    assert_eq!(
      Some(700),
      get_int_field_value(&fields, "_response_bytes_per_min_count")
    );
  }


  // Advancing by 3s removes the only measured bytes from the rolling window that's used to report
  // downloaded/uploaded bytes per minute => reported field values are equal to 0.
  time_provider.advance_by(std::time::Duration::from_secs(3));

  let mut fields = vec![];
  tracker.process(log_level::DEBUG, LogType::Resource, &"".into(), &mut fields);

  assert_eq!(
    Some(0),
    get_int_field_value(&fields, "_request_body_bytes_per_min_count")
  );
  assert_eq!(
    Some(0),
    get_int_field_value(&fields, "_request_headers_bytes_per_min_count")
  );
  assert_eq!(
    Some(0),
    get_int_field_value(&fields, "_request_bytes_per_min_count")
  );

  assert_eq!(
    Some(0),
    get_int_field_value(&fields, "_response_body_bytes_per_min_count")
  );
  assert_eq!(
    Some(0),
    get_int_field_value(&fields, "_response_headers_bytes_per_min_count")
  );
  assert_eq!(
    Some(0),
    get_int_field_value(&fields, "_response_bytes_per_min_count")
  );
}

/// Retrieves an integer value of a field with the specified key from the provided list of the
/// fields.
fn get_int_field_value(fields: &AnnotatedLogFields, field_key: &str) -> Option<u64> {
  let mut result: Option<u64> = None;

  for field in fields {
    if field.field.key != field_key {
      continue;
    }

    let string_value = match &field.field.value {
      StringOrBytes::String(value) => value,
      StringOrBytes::Bytes(_) => break,
    };

    if let Ok(value) = string_value.parse::<u64>() {
      result = Some(value);
      break;
    }
  }

  result
}

/// Creates a string field using a provided key and integer value.
fn create_int_field(key: &str, value: u64) -> AnnotatedLogField {
  AnnotatedLogField {
    field: LogField {
      key: key.to_string(),
      value: StringOrBytes::String(value.to_string()),
    },
    kind: LogFieldKind::Ootb,
  }
}
