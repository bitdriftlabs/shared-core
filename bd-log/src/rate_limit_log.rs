// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./rate_limit_log_test.rs"]
mod rate_limit_log_test;

use parking_lot::Mutex;
use time::Duration;
use tokio::time::Instant;

#[derive(Default)]
pub struct WarnTracker {
  last_used_time: Mutex<Option<Instant>>,
}

impl WarnTracker {
  pub fn should_warn(&self, duration: Duration) -> bool {
    let now = Instant::now();
    let mut last_used_time = self.last_used_time.lock();
    let warn = last_used_time.is_none_or(|last_used_time| now - last_used_time > duration);
    if warn {
      *last_used_time = Some(now);
      return true;
    }
    false
  }
}

#[macro_export]
macro_rules! warn_every_debug_assert {
  ($duration:expr, $condition:expr) => {
    debug_assert!($condition);
    if !$condition {
      $crate::warn_every!(
        $duration,
        "debug assertion failed: {}",
        stringify!($condition)
      );
    }
  };
}

#[macro_export]
macro_rules! warn_every_debug_panic {
  ($duration:expr, $message:expr) => {
    debug_assert!(false, $message);
    $crate::warn_every!($duration, "debug panic: {}", $message);
  };
}

#[macro_export]
macro_rules! warn_every {
  ($duration:expr, $first:tt) => {
    $crate::log_every!(log::Level::Warn, $duration, "{}", $first);
  };
  ($duration:expr, $first:tt, $($arg:tt)+) => {
    $crate::log_every!(log::Level::Warn, $duration, $first, $($arg)+);
  };
}

#[macro_export]
macro_rules! error_every {
  ($duration:expr, $first:tt, $($arg:tt)+) => {
    $crate::log_every!(log::Level::Error, $duration, $first, $($arg)+);
  }
}

#[macro_export]
macro_rules! log_every {
  ($level:expr, $duration:expr, $first:tt, $($arg:tt)+) => {
    {
      use $crate::rate_limit_log::WarnTracker;
      use std::sync::OnceLock;

      static TRACKER: OnceLock<WarnTracker> = OnceLock::new();

      let tracker = TRACKER.get_or_init(WarnTracker::default);
      if tracker.should_warn($duration) {
        log::log!($level, $first, $($arg)+);
      } else {
        log::debug!($first, $($arg)+);
      }
    }
  };
}

pub use {error_every, warn_every, warn_every_debug_assert, warn_every_debug_panic};
