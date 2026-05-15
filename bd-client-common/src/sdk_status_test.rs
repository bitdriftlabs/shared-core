// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::*;

#[test]
fn new_tracker_starts_as_loaded() {
  let tracker = SdkStatusTracker::new();
  let status = tracker.get();
  assert_eq!(status.initialization_state, InitializationState::Loaded);
  assert!(status.last_handshake_time.is_none());
  assert!(status.last_config_delivery_time.is_none());
}

#[test]
fn record_running_transitions_to_running() {
  let tracker = SdkStatusTracker::new();
  tracker.record_running();
  assert_eq!(tracker.get().initialization_state, InitializationState::Running);
}

#[test]
fn record_handshake_sets_timestamp() {
  let tracker = SdkStatusTracker::new();
  tracker.record_running();
  let now = OffsetDateTime::now_utc();
  tracker.record_handshake(now);

  let status = tracker.get();
  assert_eq!(status.last_handshake_time, Some(now));
}

#[test]
fn record_config_delivery_updates_timestamp() {
  let tracker = SdkStatusTracker::new();
  tracker.record_running();
  let now = OffsetDateTime::now_utc();
  tracker.record_config_delivery(now);

  let status = tracker.get();
  assert_eq!(status.last_config_delivery_time, Some(now));
}

#[test]
fn multiple_handshakes_keep_latest_time() {
  let tracker = SdkStatusTracker::new();
  tracker.record_running();
  let t1 = OffsetDateTime::now_utc();
  let t2 = t1 + time::Duration::seconds(10);

  tracker.record_handshake(t1);
  tracker.record_handshake(t2);

  assert_eq!(tracker.get().last_handshake_time, Some(t2));
}

#[test]
fn full_lifecycle() {
  let tracker = SdkStatusTracker::new();

  // Loaded.
  assert_eq!(tracker.get().initialization_state, InitializationState::Loaded);

  // Running.
  tracker.record_running();
  assert_eq!(tracker.get().initialization_state, InitializationState::Running);

  // Handshake.
  let t1 = OffsetDateTime::now_utc();
  tracker.record_handshake(t1);
  assert_eq!(tracker.get().last_handshake_time, Some(t1));

  // Another handshake — timestamp updated.
  let t2 = t1 + time::Duration::seconds(5);
  tracker.record_handshake(t2);
  assert_eq!(tracker.get().last_handshake_time, Some(t2));
}

#[test]
fn timestamps_ignored_before_running() {
  let tracker = SdkStatusTracker::new();

  // These should be no-ops since the SDK is only Loaded, not Running.
  tracker.record_handshake(OffsetDateTime::now_utc());
  tracker.record_config_delivery(OffsetDateTime::now_utc());

  let status = tracker.get();
  assert_eq!(status.initialization_state, InitializationState::Loaded);
  assert!(status.last_handshake_time.is_none());
  assert!(status.last_config_delivery_time.is_none());
}
