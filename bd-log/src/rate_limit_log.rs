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
use std::fmt;
use std::sync::OnceLock;
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

pub fn log_with_rate_limit(
  tracker: &OnceLock<WarnTracker>,
  level: log::Level,
  duration: Duration,
  args: fmt::Arguments<'_>,
  target: &'static str,
  module_path: &'static str,
  file: &'static str,
  line: u32,
) {
  let tracker = tracker.get_or_init(WarnTracker::default);
  let level = if tracker.should_warn(duration) {
    level
  } else {
    log::Level::Debug
  };

  if level > log::max_level() {
    return;
  }

  // The macro captures call-site metadata and hands it to this helper so the emitted record still
  // points at the caller while the tracker logic stays shared.
  let mut record = log::Record::builder();
  record
    .args(args)
    .level(level)
    .target(target)
    .module_path_static(Some(module_path))
    .file_static(Some(file))
    .line(Some(line));

  log::logger().log(&record.build());
}

#[macro_export]
#[clippy::format_args]
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
#[clippy::format_args]
macro_rules! warn_every_debug_panic {
  ($duration:expr, $message:expr) => {
    debug_assert!(false, $message);
    $crate::warn_every!($duration, "debug panic: {}", $message);
  };
}

#[macro_export]
#[clippy::format_args]
macro_rules! warn_every {
  ($duration:expr, $($arg:tt)+) => {{
    static TRACKER: std::sync::OnceLock<$crate::rate_limit_log::WarnTracker> =
      std::sync::OnceLock::new();

    $crate::rate_limit_log::log_with_rate_limit(
      &TRACKER,
      log::Level::Warn,
      $duration,
      format_args!($($arg)+),
      module_path!(),
      module_path!(),
      file!(),
      line!(),
    );
  }};
}

#[macro_export]
#[clippy::format_args]
macro_rules! error_every {
  ($duration:expr, $($arg:tt)+) => {{
    static TRACKER: std::sync::OnceLock<$crate::rate_limit_log::WarnTracker> =
      std::sync::OnceLock::new();

    $crate::rate_limit_log::log_with_rate_limit(
      &TRACKER,
      log::Level::Error,
      $duration,
      format_args!($($arg)+),
      module_path!(),
      module_path!(),
      file!(),
      line!(),
    );
  }};
}

#[macro_export]
#[clippy::format_args]
macro_rules! log_every {
  ($level:expr, $duration:expr, $($arg:tt)+) => {{
    static TRACKER: std::sync::OnceLock<$crate::rate_limit_log::WarnTracker> =
      std::sync::OnceLock::new();

    $crate::rate_limit_log::log_with_rate_limit(
      &TRACKER,
      $level,
      $duration,
      format_args!($($arg)+),
      module_path!(),
      module_path!(),
      file!(),
      line!(),
    );
  }};
}

pub use error_every;
pub use warn_every;
pub use warn_every_debug_assert;
pub use warn_every_debug_panic;
