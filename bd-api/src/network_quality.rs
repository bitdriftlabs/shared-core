#[cfg(test)]
#[path = "./network_quality_test.rs"]
mod tests;

use bd_network_quality::{NetworkQuality, NetworkQualityProvider};
use bd_runtime::runtime::DurationWatch;
use parking_lot::RwLock;
use std::sync::Arc;
use time::OffsetDateTime;

// The amount of time the API has to be in the disconnected state before network quality will be
// switched to "offline". This offline grace period also governs when cached configuration will
// be marked as safe to use if we can't contact the server. This prevents cached configuration
// from being deleted during perpetually offline states if the process has been up for long
// enough without crashing.
pub const DISCONNECTED_OFFLINE_GRACE_PERIOD: std::time::Duration =
  std::time::Duration::from_secs(15);

//
// SimpleNetworkQualityProvider
//

pub struct SimpleNetworkQualityProvider {
  network_quality: RwLock<NetworkQuality>,
}

impl Default for SimpleNetworkQualityProvider {
  fn default() -> Self {
    Self {
      network_quality: RwLock::new(NetworkQuality::Unknown),
    }
  }
}

impl NetworkQualityProvider for SimpleNetworkQualityProvider {
  fn get_network_quality(&self) -> NetworkQuality {
    *self.network_quality.read()
  }

  fn set_network_quality(&self, quality: NetworkQuality) {
    *self.network_quality.write() = quality;
  }
}

pub struct TimedNetworkQualityProvider<T: bd_runtime::runtime::FeatureFlag<time::Duration>> {
  rw_network_quality: RwLock<(NetworkQuality, OffsetDateTime)>,
  time_provider: Arc<dyn bd_time::TimeProvider>,
  timeout: DurationWatch<T>,
}

impl<T: bd_runtime::runtime::FeatureFlag<time::Duration>> TimedNetworkQualityProvider<T> {
  pub fn new(time_provider: Arc<dyn bd_time::TimeProvider>, timeout: DurationWatch<T>) -> Self {
    Self {
      rw_network_quality: RwLock::new((NetworkQuality::Unknown, OffsetDateTime::now_utc())),
      time_provider,
      timeout,
    }
  }
}

impl<T: bd_runtime::runtime::FeatureFlag<time::Duration> + Send + Sync> NetworkQualityProvider
  for TimedNetworkQualityProvider<T>
{
  fn get_network_quality(&self) -> NetworkQuality {
    let (quality, timestamp) = *self.rw_network_quality.read();

    if timestamp + *self.timeout.read() < self.time_provider.now() {
      return NetworkQuality::Unknown;
    }

    quality
  }

  fn set_network_quality(&self, quality: NetworkQuality) {
    let now = self.time_provider.now();
    *self.rw_network_quality.write() = (quality, now);
  }
}

pub struct AggregatedNetworkQualityProvider {
  providers: Vec<Arc<dyn NetworkQualityProvider>>,
}

impl AggregatedNetworkQualityProvider {
  #[must_use]
  pub fn new(providers: Vec<Arc<dyn NetworkQualityProvider>>) -> Self {
    Self { providers }
  }
}

impl NetworkQualityProvider for AggregatedNetworkQualityProvider {
  fn get_network_quality(&self) -> NetworkQuality {
    for provider in &self.providers {
      let quality = provider.get_network_quality();
      if quality != NetworkQuality::Unknown {
        return quality;
      }
    }
    NetworkQuality::Unknown
  }

  fn set_network_quality(&self, quality: NetworkQuality) {
    for provider in &self.providers {
      provider.set_network_quality(quality);
    }
  }
}
