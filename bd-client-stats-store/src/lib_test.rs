// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{Collector, MetricData, NameType};
use bd_stats_common::labels;

#[test]
fn retain() {
  let collector = Collector::default();
  let c = collector.scope("").counter("c");
  let h = collector.scope("").histogram("h");
  assert!(
    collector
      .find_counter(&NameType::Global("c".to_string()), &labels!())
      .is_some()
  );
  assert!(
    collector
      .find_histogram(&NameType::Global("h".to_string()), &labels!())
      .is_some()
  );

  // Dropping c and h should leave them in the map until we retain.
  drop(c);
  drop(h);
  assert!(
    collector
      .find_counter(&NameType::Global("c".to_string()), &labels!())
      .is_some()
  );
  assert!(
    collector
      .find_histogram(&NameType::Global("h".to_string()), &labels!())
      .is_some()
  );

  collector.retain(|_, _, metric| match metric {
    MetricData::Counter(c) => c.multiple_references(),
    MetricData::Histogram(h) => h.multiple_references(),
  });

  assert!(
    collector
      .find_counter(&NameType::Global("c".to_string()), &labels!())
      .is_none()
  );
  assert!(
    collector
      .find_histogram(&NameType::Global("h".to_string()), &labels!())
      .is_none()
  );
}
