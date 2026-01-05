// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{NetworkQualityInterceptor, TimeProvider};
use crate::network::HTTPTrafficDataUsageTracker;
use bd_api::SimpleNetworkQualityProvider;
use bd_log_primitives::{
  AnnotatedLogField,
  AnnotatedLogFields,
  LogInterceptor,
  LogMessage,
  StringOrBytes,
  log_level,
};
use bd_network_quality::{NetworkQuality, NetworkQualityMonitor as _, NetworkQualityResolver as _};
use bd_proto::protos::logging::payload::LogType;
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
  let interceptor = NetworkQualityInterceptor::new(network_quality_provider.clone());
  {
    let mut fields = [].into();
    interceptor.process(
      log_level::DEBUG,
      LogType::NORMAL,
      &"".into(),
      &mut fields,
      &mut [].into(),
    );
    assert!(fields.is_empty());
    network_quality_provider.set_network_quality(NetworkQuality::Online);
    interceptor.process(
      log_level::DEBUG,
      LogType::NORMAL,
      &"".into(),
      &mut fields,
      &mut [].into(),
    );
    assert!(fields.is_empty());
    network_quality_provider.set_network_quality(NetworkQuality::Offline);
    interceptor.process(
      log_level::DEBUG,
      LogType::NORMAL,
      &"".into(),
      &mut fields,
      &mut [].into(),
    );
    assert_eq!(
      fields["_network_quality"],
      AnnotatedLogField::new_ootb("offline")
    );
  }
  {
    let mut fields = [].into();
    interceptor.process(
      log_level::DEBUG,
      LogType::REPLAY,
      &"".into(),
      &mut fields,
      &mut [].into(),
    );
    assert!(fields.is_empty());
  }
}

#[tokio::test]
async fn collects_bandwidth_sample() {
  let time_provider = Arc::new(MockTimeProvider::default());
  let network_quality_provider = Arc::new(SimpleNetworkQualityProvider::default());
  let tracker =
    HTTPTrafficDataUsageTracker::new(time_provider.clone(), network_quality_provider.clone());

  // The tracker reports bandwidth usage on per minute basis.
  // Since we inform the tracker that the rate at which we ask it for fields is equal to 3 seconds,
  // it needs 20 ticks before it accumulates 1 minutes (3 second * 20 ticks) of data and returns
  // samples summary.
  for _ in 0 .. 20 {
    time_provider.advance_by(std::time::Duration::from_secs(3));

    let mut fields = [].into();
    tracker.process(
      log_level::DEBUG,
      LogType::RESOURCE,
      &"".into(),
      &mut fields,
      &mut [].into(),
    );
    assert!(fields.is_empty());
  }

  tracker.process(
    log_level::DEBUG,
    LogType::SPAN,
    &LogMessage::String("HTTPResponse".to_string()),
    &mut [
      ("_status_code".into(), create_int_field(200)),
      (
        "_request_body_bytes_sent_count".into(),
        create_int_field(100),
      ),
      ("_request_headers_bytes_count".into(), create_int_field(200)),
      (
        "_response_body_bytes_received_count".into(),
        create_int_field(300),
      ),
      (
        "_response_headers_bytes_count".into(),
        create_int_field(400),
      ),
    ]
    .into(),
    &mut [].into(),
  );

  assert_eq!(
    NetworkQuality::Online,
    network_quality_provider.get_network_quality()
  );

  for _ in 0 .. 20 {
    time_provider.advance_by(std::time::Duration::from_secs(3));

    let mut fields = [].into();
    tracker.process(
      log_level::DEBUG,
      LogType::RESOURCE,
      &"".into(),
      &mut fields,
      &mut [].into(),
    );
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

  let mut fields = [].into();
  tracker.process(
    log_level::DEBUG,
    LogType::RESOURCE,
    &"".into(),
    &mut fields,
    &mut [].into(),
  );

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
  fields.get(field_key)?.value.as_str()?.parse::<u64>().ok()
}

/// Creates a string field using a provided key and integer value.
fn create_int_field(value: u64) -> AnnotatedLogField {
  AnnotatedLogField::new_ootb(StringOrBytes::String(value.to_string()))
}
