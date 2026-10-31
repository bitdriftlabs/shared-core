// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/licenses/strict/1.0.0

#![deny(
  clippy::expect_used,
  clippy::panic,
  clippy::todo,
  clippy::unimplemented,
  clippy::unreachable,
  clippy::unwrap_used
)]

#[cfg(test)]
#[path = "./lib_test.rs"]
mod tests;

use std::collections::HashMap;

// The configuration version allows the server to send different configurations to different
// clients. Native SDK release minor versions must match this number. Increment it whenever newer
// clients support configurations that older clients would reject.
// Version 27: Added double matching support.
// Version 28: Added support for generate log action and client report uploads.
// Version 29: Added support for out of the band artifact uploads.
// Version 30: Added support for workflow timeout transitions.
// Version 31: Added support for the debug mux API.
// Version 32: Added support for use the log body for tag extraction.
// Version 33: Added support for opaque client state in the mux handshake.
// Version 34: Added support for data idle timeouts and reconnect delays.
// Version 35: Added support for workflow matching and tag extraction against feature flags.
// Version 36: Added support for state to the workflow engine.
// Version 37: Added support for map types and JSON matching.
// Version 38: Added support for workflow parallel execution and message/regex transition
//             extractions.
// Version 39: Added support for start tracing action and sampled log matcher.
// Version 40: Added multi-extract support on metrics.
// Version 41: Added support for client driven state updates.
// Version 42: Added support for server driven state updates.
// Version 43: Added support for sending trigger UUIDs in log uploads to allow the server to
//             associate logs with the flush that generated them.
// Version 44: Added support for remote device commands.
// Version 45: Added support for workflow commands.
// Version 46: Added support for typed workflow command arguments.
// Version 47: Added support for named command arguments.
// Version 48: Added JSON path extraction for workflow fields.
// Version 49: Added SDK-instance-scoped device commands with reconnect-safe update replay and
//             explicit acknowledgement dispositions.
// Version 50: Android stack trace built-in command support.
pub const CONFIGURATION_VERSION: &str = "50";

/// The platform we're currently running as.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Platform {
  Android,
  Apple,
}

impl Platform {
  #[must_use]
  pub const fn as_str(self) -> &'static str {
    match self {
      Self::Android => "android",
      Self::Apple => "apple",
    }
  }
}

pub trait Metadata: Send {
  fn sdk_version(&self) -> &'static str;

  fn platform(&self) -> &Platform;

  fn os(&self) -> String;

  fn device_id(&self) -> String;

  fn collect_inner(&self) -> HashMap<String, String>;

  /// SDK-owned identity for new command artifacts. Platforms add their app build identifier.
  fn command_artifact_metadata(&self) -> HashMap<String, String> {
    let mut identity = self.collect_inner();
    identity.retain(|key, _| matches!(key.as_str(), "app_id" | "app_version"));
    identity.insert("platform".to_string(), self.platform().as_str().to_string());
    identity
  }

  fn collect(&self) -> HashMap<String, String> {
    let mut inner = self.collect_inner();

    inner.insert(
      "config_version".to_string(),
      CONFIGURATION_VERSION.to_string(),
    );

    inner.insert("os".to_string(), self.os());
    inner.insert("platform".to_string(), self.platform().as_str().to_string());
    inner.insert("sdk_version".to_string(), self.sdk_version().to_string());
    inner.insert("device_id".to_string(), self.device_id());

    inner
  }
}
