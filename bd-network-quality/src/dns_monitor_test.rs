// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::*;
use std::time::Duration;

#[tokio::test]
async fn creates_monitor_with_default_config() {
  let resolver = Arc::new(TokioDnsResolver);
  let monitor = DnsNetworkMonitor::new(DnsMonitorConfig::default(), resolver);
  assert_eq!(monitor.get_quality(), NetworkQuality::Unknown);
}

#[tokio::test]
async fn creates_monitor_with_custom_config() {
  let config = DnsMonitorConfig {
    hostname: "example.com:80".to_string(),
    check_interval: Duration::from_secs(10),
    lookup_timeout: Duration::from_secs(2),
  };

  let resolver = Arc::new(TokioDnsResolver);
  let monitor = DnsNetworkMonitor::new(config, resolver);
  assert_eq!(monitor.config.hostname, "example.com:80");
  assert_eq!(monitor.config.check_interval, Duration::from_secs(10));
  assert_eq!(monitor.config.lookup_timeout, Duration::from_secs(2));
}

#[tokio::test]
async fn implements_network_quality_monitor_trait() {
  let resolver = Arc::new(TokioDnsResolver);
  let monitor = DnsNetworkMonitor::new(DnsMonitorConfig::default(), resolver);

  monitor.set_network_quality(NetworkQuality::Online);
  assert_eq!(monitor.get_quality(), NetworkQuality::Online);

  monitor.set_network_quality(NetworkQuality::Offline);
  assert_eq!(monitor.get_quality(), NetworkQuality::Offline);
}

#[tokio::test]
async fn starts_and_stops_monitoring() {
  let resolver = Arc::new(TokioDnsResolver);
  let mut monitor = DnsNetworkMonitor::new(
    DnsMonitorConfig {
      hostname: "dns.google.com:443".to_string(),
      check_interval: Duration::from_millis(100),
      lookup_timeout: Duration::from_secs(2),
    },
    resolver,
  );

  let result = monitor.start();
  assert!(result.is_ok());

  // Wait for at least one check to complete
  tokio::time::sleep(Duration::from_millis(200)).await;

  // Quality should be either Online or Offline (not Unknown) after a check
  let quality = monitor.get_quality();
  assert!(quality == NetworkQuality::Online || quality == NetworkQuality::Offline);

  monitor.stop();
}

#[tokio::test]
async fn prevents_double_start() {
  let resolver = Arc::new(TokioDnsResolver);
  let mut monitor = DnsNetworkMonitor::new(DnsMonitorConfig::default(), resolver);

  let result1 = monitor.start();
  assert!(result1.is_ok());

  let result2 = monitor.start();
  assert!(result2.is_err());

  monitor.stop();
}

#[tokio::test]
async fn resolves_valid_hostname() {
  let config = DnsMonitorConfig {
    hostname: "dns.google.com:443".to_string(),
    check_interval: Duration::from_secs(30),
    lookup_timeout: Duration::from_secs(5),
  };

  let resolver = TokioDnsResolver;
  let quality = DnsNetworkMonitor::check_dns(&config, &resolver).await;
  assert_eq!(quality, NetworkQuality::Online);
}

#[tokio::test]
async fn handles_invalid_hostname() {
  let config = DnsMonitorConfig {
    hostname: "this-hostname-definitely-does-not-exist-12345.invalid:80".to_string(),
    check_interval: Duration::from_secs(30),
    lookup_timeout: Duration::from_secs(2),
  };

  let resolver = TokioDnsResolver;
  let quality = DnsNetworkMonitor::check_dns(&config, &resolver).await;
  assert_eq!(quality, NetworkQuality::Offline);
}

#[tokio::test]
async fn handles_timeout() {
  let config = DnsMonitorConfig {
    hostname: "dns.google.com:443".to_string(),
    check_interval: Duration::from_secs(30),
    lookup_timeout: Duration::from_nanos(1), // Extremely short timeout
  };

  let resolver = TokioDnsResolver;
  let quality = DnsNetworkMonitor::check_dns(&config, &resolver).await;
  assert_eq!(quality, NetworkQuality::Offline);
}

#[tokio::test]
async fn stops_on_drop() {
  let resolver = Arc::new(TokioDnsResolver);
  let mut monitor = DnsNetworkMonitor::new(
    DnsMonitorConfig {
      hostname: "dns.google.com:443".to_string(),
      check_interval: Duration::from_millis(100),
      lookup_timeout: Duration::from_secs(2),
    },
    resolver,
  );

  monitor.start().ok();
  tokio::time::sleep(Duration::from_millis(50)).await;

  // Drop the monitor
  drop(monitor);

  // If we get here without hanging, the monitor stopped successfully
}

// Test that custom resolvers work correctly

struct MockResolver {
  should_succeed: bool,
}

#[async_trait::async_trait]
impl PlatformDnsResolver for MockResolver {
  async fn resolve(&self, _hostname: &str) -> anyhow::Result<bool> {
    Ok(self.should_succeed)
  }
}

#[tokio::test]
async fn uses_custom_resolver_success() {
  let config = DnsMonitorConfig {
    hostname: "example.com:80".to_string(),
    check_interval: Duration::from_secs(30),
    lookup_timeout: Duration::from_secs(5),
  };

  let resolver = MockResolver {
    should_succeed: true,
  };
  let quality = DnsNetworkMonitor::check_dns(&config, &resolver).await;
  assert_eq!(quality, NetworkQuality::Online);
}

#[tokio::test]
async fn uses_custom_resolver_failure() {
  let config = DnsMonitorConfig {
    hostname: "example.com:80".to_string(),
    check_interval: Duration::from_secs(30),
    lookup_timeout: Duration::from_secs(5),
  };

  let resolver = MockResolver {
    should_succeed: false,
  };
  let quality = DnsNetworkMonitor::check_dns(&config, &resolver).await;
  assert_eq!(quality, NetworkQuality::Offline);
}
