// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{MetricId, MetricType, ParseError, TagValue, prom_stale_marker, unwrap_timestamp};
use bytes::Bytes;

#[test]
fn metric_id_sorts_tags() {
  let metric_id = MetricId::new(
    "requests".into(),
    Some(MetricType::Gauge),
    vec![
      TagValue {
        tag: "zone".into(),
        value: "west".into(),
      },
      TagValue {
        tag: "host".into(),
        value: "api".into(),
      },
    ],
    false,
  )
  .unwrap();

  assert_eq!(metric_id.tags()[0].tag, Bytes::from_static(b"host"));
  assert_eq!(metric_id.tags()[1].tag, Bytes::from_static(b"zone"));
}

#[test]
fn metric_id_rejects_large_components() {
  let error = MetricId::new(
    Bytes::from(vec![0; usize::from(u16::MAX) + 1]),
    None,
    vec![],
    false,
  )
  .unwrap_err();

  assert_eq!(error, ParseError::TooLarge);
}

#[test]
fn normalizes_millisecond_timestamps() {
  assert_eq!(unwrap_timestamp(Some(1_700_000_000)), 1_700_000_000);
  assert_eq!(unwrap_timestamp(Some(1_700_000_000_000)), 1_700_000_000);
}

#[test]
fn preserves_prometheus_stale_marker_bits() {
  assert!(prom_stale_marker().is_nan());
  assert_eq!(prom_stale_marker().to_bits(), 0x7ff0_0000_0000_0002);
}
