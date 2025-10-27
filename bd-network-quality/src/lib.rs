// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![deny(
  clippy::expect_used,
  clippy::panic,
  clippy::todo,
  clippy::unimplemented,
  clippy::unreachable,
  clippy::unwrap_used
)]

pub mod dns_monitor;

//
// NetworkQuality
//

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkQuality {
  Unknown,
  Offline,
  Online,
}

//
// NetworkQualityMonitor
//

pub trait NetworkQualityMonitor: Send + Sync {
  fn set_network_quality(&self, quality: NetworkQuality);
}

// NetworkQualityResolver

pub trait NetworkQualityResolver: Send + Sync {
  fn get_network_quality(&self) -> NetworkQuality;
}
