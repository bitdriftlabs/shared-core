// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{NetworkQuality, NetworkQualityMonitor};
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;

#[cfg(test)]
#[path = "./dns_monitor_test.rs"]
mod tests;

//
// PlatformDnsResolver
//

/// A trait for platform-specific DNS resolution implementations.
/// This allows different platforms to provide their own DNS resolution logic.
#[async_trait::async_trait]
pub trait PlatformDnsResolver: Send + Sync {
  /// Attempts to resolve the given hostname.
  /// Returns `Ok(true)` if resolution succeeds, `Ok(false)` if it fails,
  /// or `Err` if there's an error performing the check.
  async fn resolve(&self, hostname: &str) -> anyhow::Result<bool>;
}

//
// TokioDnsResolver
//

/// A default DNS resolver implementation using tokio's built-in DNS lookup.
pub struct TokioDnsResolver;

#[async_trait::async_trait]
impl PlatformDnsResolver for TokioDnsResolver {
  async fn resolve(&self, hostname: &str) -> anyhow::Result<bool> {
    Ok(
      tokio::net::lookup_host(hostname)
        .await
        .map_or_else(|_| false, |mut addrs| addrs.next().is_some()),
    )
  }
}

//
// DnsMonitorConfig
//

/// Configuration for the DNS-based network quality monitor.
#[derive(Debug, Clone)]
pub struct DnsMonitorConfig {
  /// The hostname to perform DNS lookups on. Defaults to "dns.google.com:443".
  pub hostname: String,
  /// How often to check DNS resolution. Defaults to 30 seconds.
  pub check_interval: Duration,
  /// Timeout for each DNS lookup attempt. Defaults to 5 seconds.
  pub lookup_timeout: Duration,
}

impl Default for DnsMonitorConfig {
  fn default() -> Self {
    Self {
      hostname: "dns.google.com:443".to_string(),
      check_interval: Duration::from_secs(30),
      lookup_timeout: Duration::from_secs(5),
    }
  }
}

//
// DnsNetworkMonitor
//

/// A network quality monitor that periodically checks DNS resolution to determine network status.
/// This implementation delegates DNS resolution to a platform-specific resolver.
pub struct DnsNetworkMonitor {
  config: DnsMonitorConfig,
  resolver: Arc<dyn PlatformDnsResolver>,
  quality: Arc<RwLock<NetworkQuality>>,
  shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl DnsNetworkMonitor {
  /// Creates a new DNS network monitor with the given configuration and resolver.
  #[must_use]
  pub fn new(config: DnsMonitorConfig, resolver: Arc<dyn PlatformDnsResolver>) -> Self {
    Self {
      config,
      resolver,
      quality: Arc::new(RwLock::new(NetworkQuality::Unknown)),
      shutdown_tx: None,
    }
  }

  /// Starts the monitoring loop in the background.
  /// Returns an error if the monitor has already been started.
  pub fn start(&mut self) -> anyhow::Result<()> {
    if self.shutdown_tx.is_some() {
      anyhow::bail!("DNS network monitor already started");
    }

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    self.shutdown_tx = Some(shutdown_tx);

    let config = self.config.clone();
    let resolver = self.resolver.clone();
    let quality = self.quality.clone();

    tokio::spawn(async move {
      let mut interval = tokio::time::interval(config.check_interval);
      interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

      loop {
        tokio::select! {
          _ = interval.tick() => {
            let new_quality = Self::check_dns(&config, resolver.as_ref()).await;
            let mut current_quality = quality.write();
            if *current_quality != new_quality {
              *current_quality = new_quality;
            }
          }
          _ = &mut shutdown_rx => {
            break;
          }
        }
      }
    });

    Ok(())
  }

  /// Stops the monitoring loop.
  pub fn stop(&mut self) {
    if let Some(shutdown_tx) = self.shutdown_tx.take() {
      let _ = shutdown_tx.send(());
    }
  }

  /// Gets the current network quality.
  #[must_use]
  pub fn get_quality(&self) -> NetworkQuality {
    *self.quality.read()
  }

  /// Performs a DNS lookup to check network connectivity using the provided resolver.
  async fn check_dns(
    config: &DnsMonitorConfig,
    resolver: &dyn PlatformDnsResolver,
  ) -> NetworkQuality {
    let result =
      tokio::time::timeout(config.lookup_timeout, resolver.resolve(&config.hostname)).await;

    match result {
      Ok(Ok(true)) => NetworkQuality::Online,
      Ok(Ok(false) | Err(_)) | Err(_) => NetworkQuality::Offline,
    }
  }
}

impl NetworkQualityMonitor for DnsNetworkMonitor {
  fn set_network_quality(&self, quality: NetworkQuality) {
    *self.quality.write() = quality;
  }
}

impl Drop for DnsNetworkMonitor {
  fn drop(&mut self) {
    self.stop();
  }
}
