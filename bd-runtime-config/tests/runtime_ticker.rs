// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_runtime_config::loader::Loader;
use bd_runtime_config::runtime_ticker::RuntimeFlagTicker;
use bd_test_helpers::feature_flags::{DefaultFeatureFlags, FakeLoader};
use bd_time::{TestTimeProvider, Ticker, TimeProvider};
use std::sync::Arc;
use time::macros::datetime;

fn make_feature_flags(integers: &[(&str, u64)]) -> DefaultFeatureFlags {
  integers
    .iter()
    .fold(DefaultFeatureFlags::default(), |flags, (name, value)| {
      flags.with_integer_flag(name, *value)
    })
}

fn feature_flags_loader(integers: &[(&str, u64)]) -> FakeLoader<DefaultFeatureFlags> {
  FakeLoader::new(Arc::new(make_feature_flags(integers)))
}

#[tokio::test]
async fn runtime_flag_ticker_reloads_each_tick() {
  let feature_flags = feature_flags_loader(&[("poll_interval_s", 10)]);
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2026-01-01 00:00:00 UTC)));
  let mut ticker = RuntimeFlagTicker::new(feature_flags.snapshot_watch(), "poll_interval_s", 60)
    .with_time_provider(time_provider.clone());

  let first_now = time_provider.now();
  ticker.tick().await;
  assert_eq!(time_provider.now(), first_now + time::Duration::seconds(10));

  feature_flags.update(Arc::new(make_feature_flags(&[("poll_interval_s", 1)])));
  let second_now = time_provider.now();
  ticker.tick().await;
  assert_eq!(time_provider.now(), second_now + time::Duration::seconds(1));
}

#[tokio::test]
async fn runtime_flag_ticker_reloads_milliseconds_each_tick() {
  let feature_flags = feature_flags_loader(&[("poll_interval_ms", 250)]);
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2026-01-01 00:00:00 UTC)));
  let mut ticker =
    RuntimeFlagTicker::new_milliseconds(feature_flags.snapshot_watch(), "poll_interval_ms", 60_000)
      .with_time_provider(time_provider.clone());

  let first_now = time_provider.now();
  ticker.tick().await;
  assert_eq!(
    time_provider.now(),
    first_now + time::Duration::milliseconds(250)
  );

  feature_flags.update(Arc::new(make_feature_flags(&[("poll_interval_ms", 1_500)])));
  let second_now = time_provider.now();
  ticker.tick().await;
  assert_eq!(
    time_provider.now(),
    second_now + time::Duration::milliseconds(1_500)
  );
}

#[tokio::test]
async fn runtime_flag_ticker_zero_disables_interval_tick() {
  let feature_flags = feature_flags_loader(&[("poll_interval_s", 0)]);
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2026-01-01 00:00:00 UTC)));
  let mut ticker = RuntimeFlagTicker::new(feature_flags.snapshot_watch(), "poll_interval_s", 60)
    .with_time_provider(time_provider.clone())
    .with_disabled_recheck_interval(time::Duration::seconds(1));

  let start = time_provider.now();
  let tick = tokio::spawn(async move { ticker.tick().await });
  tokio::task::yield_now().await;
  feature_flags.update(Arc::new(make_feature_flags(&[("poll_interval_s", 1)])));
  tokio::time::timeout(std::time::Duration::from_millis(50), tick)
    .await
    .unwrap()
    .unwrap();
  assert!(time_provider.now() >= start + time::Duration::seconds(2));
}

#[tokio::test]
async fn runtime_flag_ticker_uses_default_when_flags_missing() {
  let feature_flags = feature_flags_loader(&[]);
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2026-01-01 00:00:00 UTC)));
  let mut ticker = RuntimeFlagTicker::new(feature_flags.snapshot_watch(), "poll_interval_s", 3)
    .with_time_provider(time_provider.clone());

  let start = time_provider.now();
  ticker.tick().await;
  assert_eq!(time_provider.now(), start + time::Duration::seconds(3));
}

#[tokio::test]
async fn runtime_flag_ticker_recovers_after_disable() {
  let feature_flags = feature_flags_loader(&[("poll_interval_s", 0)]);
  let time_provider = Arc::new(TestTimeProvider::new(datetime!(2026-01-01 00:00:00 UTC)));
  let mut ticker = RuntimeFlagTicker::new(feature_flags.snapshot_watch(), "poll_interval_s", 60)
    .with_time_provider(time_provider.clone())
    .with_disabled_recheck_interval(time::Duration::seconds(1));

  let tick = tokio::spawn(async move {
    ticker.tick().await;
    ticker
  });
  tokio::task::yield_now().await;
  feature_flags.update(Arc::new(make_feature_flags(&[("poll_interval_s", 2)])));
  let mut ticker = tokio::time::timeout(std::time::Duration::from_millis(50), tick)
    .await
    .unwrap()
    .unwrap();

  let second_now = time_provider.now();
  ticker.tick().await;
  assert_eq!(time_provider.now(), second_now + time::Duration::seconds(2));
}
