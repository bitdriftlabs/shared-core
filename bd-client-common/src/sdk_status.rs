// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./sdk_status_test.rs"]
mod tests;

use parking_lot::RwLock;
use std::sync::Arc;
use time::OffsetDateTime;

/// The SDK's initialization state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SdkState {
  /// The SDK has not been started yet.
  NotStarted = 0,
  /// The SDK library has been loaded and the logger is being constructed,
  /// but log processing has not yet begun.
  Loaded = 1,
  /// The SDK is fully running and processing logs.
  Running = 2,
}

/// A point-in-time snapshot of the SDK's operational status.
#[derive(Debug, Clone)]
pub struct SdkStatus {
  /// The current initialization state of the SDK.
  pub sdk_state: SdkState,

  /// The wall-clock time of the last successful handshake, if any.
  pub last_handshake_time: Option<OffsetDateTime>,

  /// The wall-clock time of the last successful config delivery from the backend, if any.
  pub last_config_delivery_time: Option<OffsetDateTime>,
}

/// A thread-safe tracker that subsystems update as events occur.
/// Callers can snapshot the current state at any time via [`get()`](Self::get).
#[derive(Clone)]
pub struct SdkStatusTracker {
  inner: Arc<RwLock<SdkStatus>>,
}

impl Default for SdkStatusTracker {
  fn default() -> Self {
    Self::new()
  }
}

impl SdkStatusTracker {
  #[must_use]
  pub fn new() -> Self {
    Self {
      inner: Arc::new(RwLock::new(SdkStatus {
        sdk_state: SdkState::Loaded,
        last_handshake_time: None,
        last_config_delivery_time: None,
      })),
    }
  }

  /// Returns a snapshot of the current SDK status.
  #[must_use]
  pub fn get(&self) -> SdkStatus {
    self.inner.read().clone()
  }

  /// Called when the SDK is fully running (log processing started).
  pub fn record_running(&self) {
    let mut status = self.inner.write();
    status.sdk_state = SdkState::Running;
  }

  /// Called when a handshake with the backend completes successfully.
  pub fn record_handshake(&self, time: OffsetDateTime) {
    let mut status = self.inner.write();
    if status.sdk_state == SdkState::Running {
      status.last_handshake_time = Some(time);
    }
  }

  /// Called when a configuration update is successfully applied from the backend
  /// (not from cache).
  pub fn record_config_delivery(&self, time: OffsetDateTime) {
    let mut status = self.inner.write();
    if status.sdk_state == SdkState::Running {
      status.last_config_delivery_time = Some(time);
    }
  }
}
