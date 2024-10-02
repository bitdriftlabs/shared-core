// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use std::collections::HashMap;

// The configuration version of the client is a version number maintained independently from the
// release version of SDK which is used to allow the server to send different configurations to
// different clients. This should be incremented whenever we want to be able to send different
// configuration to a client, e.g. if newer clients support config that older clients would reject.
const CONFIGURATION_VERSION: &str = "21";

/// The platform we're currently running as.
pub enum Platform {
  /// Android mobile device.
  Android,
  /// iOS mobile device.
  Ios,
  /// Pulse platform.
  Pulse,

  /// Catch-all for other platforms, e.g. for test. Allows overriding the `os` and `kind` metadata.
  Other(&'static str, &'static str),
}

impl Platform {
  const fn kind(&self) -> &str {
    match self {
      Self::Android | Self::Ios => "mobile",
      Self::Pulse => "pulse",
      Self::Other(_, kind) => kind,
    }
  }

  const fn os(&self) -> &str {
    match self {
      Self::Android => "android",
      Self::Ios => "ios",
      Self::Pulse => "pulse",
      Self::Other(s, _) => s,
    }
  }
}

pub trait Metadata: Send {
  fn sdk_version(&self) -> &'static str;

  fn platform(&self) -> &Platform;

  fn collect_inner(&self) -> HashMap<String, String>;

  fn collect(&self) -> HashMap<String, String> {
    let mut inner = self.collect_inner();

    inner.insert(
      "config_version".to_string(),
      CONFIGURATION_VERSION.to_string(),
    );

    let platform = self.platform();
    inner.insert("os".to_string(), platform.os().to_string());
    inner.insert("kind".to_string(), platform.kind().to_string());

    inner.insert("sdk_version".to_string(), self.sdk_version().to_string());

    inner
  }
}
