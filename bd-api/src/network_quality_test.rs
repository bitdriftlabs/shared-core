// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::network_quality::{
  AggregatedNetworkQualityProvider,
  SimpleNetworkQualityProvider,
  TimedNetworkQualityProvider,
};
use bd_network_quality::{NetworkQuality, NetworkQualityProvider};
use bd_runtime::runtime::{FeatureFlag, Watch};
use bd_time::TestTimeProvider;
use std::sync::Arc;
use time::OffsetDateTime;

// Define a test feature flag for duration
struct TestDurationFlag;

impl FeatureFlag<time::Duration> for TestDurationFlag {
  fn path() -> &'static str {
    "test.duration"
  }

  fn default() -> time::Duration {
    time::Duration::seconds(60)
  }
}

#[test]
fn simple_network_quality_provider() {
  let provider = SimpleNetworkQualityProvider::default();

  // Default quality should be Unknown
  assert_eq!(provider.get_network_quality(), NetworkQuality::Unknown);

  // Set to Online and verify
  provider.set_network_quality(NetworkQuality::Online);
  assert_eq!(provider.get_network_quality(), NetworkQuality::Online);

  // Set to Offline and verify
  provider.set_network_quality(NetworkQuality::Offline);
  assert_eq!(provider.get_network_quality(), NetworkQuality::Offline);

  // Set back to Unknown and verify
  provider.set_network_quality(NetworkQuality::Unknown);
  assert_eq!(provider.get_network_quality(), NetworkQuality::Unknown);
}

#[test]
fn timed_network_quality_provider() {
  let now = OffsetDateTime::now_utc();
  let time_provider = Arc::new(TestTimeProvider::new(now));

  // Create a watch with timeout of 60 seconds
  let watch =
    Watch::<time::Duration, TestDurationFlag>::new_for_testing(time::Duration::seconds(60));

  let provider = TimedNetworkQualityProvider::new(time_provider.clone(), watch);

  // Default quality should be Unknown
  assert_eq!(provider.get_network_quality(), NetworkQuality::Unknown);

  // Set to Online and verify
  provider.set_network_quality(NetworkQuality::Online);
  assert_eq!(provider.get_network_quality(), NetworkQuality::Online);

  // Set to Offline and verify
  provider.set_network_quality(NetworkQuality::Offline);
  assert_eq!(provider.get_network_quality(), NetworkQuality::Offline);

  // Advance time by 30 seconds (still within timeout)
  time_provider.advance(time::Duration::seconds(30));
  assert_eq!(provider.get_network_quality(), NetworkQuality::Offline);

  // Advance time by another 31 seconds (past timeout)
  time_provider.advance(time::Duration::seconds(31));
  // Quality should revert to Unknown after timeout
  assert_eq!(provider.get_network_quality(), NetworkQuality::Unknown);

  // Set quality again and verify it works
  provider.set_network_quality(NetworkQuality::Online);
  assert_eq!(provider.get_network_quality(), NetworkQuality::Online);
}

#[test]
fn aggregated_network_quality_provider() {
  // Create two simple providers
  let provider1 = Arc::new(SimpleNetworkQualityProvider::default());
  let provider2 = Arc::new(SimpleNetworkQualityProvider::default());

  // Create aggregated provider with the two simple providers
  let aggregated_provider =
    AggregatedNetworkQualityProvider::new(vec![provider1.clone(), provider2.clone()]);

  // All providers start with Unknown, so aggregated should be Unknown
  assert_eq!(
    aggregated_provider.get_network_quality(),
    NetworkQuality::Unknown
  );

  // Set first provider to Online
  provider1.set_network_quality(NetworkQuality::Online);
  // Aggregated should return the first non-Unknown quality (Online)
  assert_eq!(
    aggregated_provider.get_network_quality(),
    NetworkQuality::Online
  );

  // Set second provider to Offline
  provider2.set_network_quality(NetworkQuality::Offline);
  // Aggregated should still return the first non-Unknown quality (Online from provider1)
  assert_eq!(
    aggregated_provider.get_network_quality(),
    NetworkQuality::Online
  );

  // Set first provider back to Unknown
  provider1.set_network_quality(NetworkQuality::Unknown);
  // Aggregated should now return the quality from provider2 (Offline)
  assert_eq!(
    aggregated_provider.get_network_quality(),
    NetworkQuality::Offline
  );

  // Set second provider to Unknown as well
  provider2.set_network_quality(NetworkQuality::Unknown);
  // All providers are Unknown, so aggregated should be Unknown
  assert_eq!(
    aggregated_provider.get_network_quality(),
    NetworkQuality::Unknown
  );

  // Test setting quality via the aggregated provider
  aggregated_provider.set_network_quality(NetworkQuality::Online);
  // This should set all underlying providers to Online
  assert_eq!(provider1.get_network_quality(), NetworkQuality::Online);
  assert_eq!(provider2.get_network_quality(), NetworkQuality::Online);
  assert_eq!(
    aggregated_provider.get_network_quality(),
    NetworkQuality::Online
  );
}

#[test]
fn aggregated_provider_with_mixed_providers() {
  // Create a simple provider
  let simple_provider = Arc::new(SimpleNetworkQualityProvider::default());

  // Create a timed provider
  let now = OffsetDateTime::now_utc();
  let time_provider = Arc::new(TestTimeProvider::new(now));
  let watch =
    Watch::<time::Duration, TestDurationFlag>::new_for_testing(time::Duration::seconds(60));
  let timed_provider = Arc::new(TimedNetworkQualityProvider::new(
    time_provider.clone(),
    watch,
  ));

  // Create aggregated provider with both types of providers
  let aggregated_provider =
    AggregatedNetworkQualityProvider::new(vec![simple_provider.clone(), timed_provider.clone()]);

  // Test initial state
  assert_eq!(
    aggregated_provider.get_network_quality(),
    NetworkQuality::Unknown
  );

  // Set timed provider to Online
  timed_provider.set_network_quality(NetworkQuality::Online);
  assert_eq!(
    aggregated_provider.get_network_quality(),
    NetworkQuality::Online
  );

  // Advance time past the timeout to make timed provider return Unknown
  time_provider.advance(time::Duration::seconds(61));
  assert_eq!(
    timed_provider.get_network_quality(),
    NetworkQuality::Unknown
  );

  // Aggregated provider should still return Unknown since both providers are Unknown
  assert_eq!(
    aggregated_provider.get_network_quality(),
    NetworkQuality::Unknown
  );

  // Set simple provider to Offline
  simple_provider.set_network_quality(NetworkQuality::Offline);
  // Aggregated provider should now return Offline
  assert_eq!(
    aggregated_provider.get_network_quality(),
    NetworkQuality::Offline
  );
}
