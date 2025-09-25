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

use std::collections::HashMap;

// The configuration version of the client is a version number maintained independently from the
// release version of SDK which is used to allow the server to send different configurations to
// different clients. This should be incremented whenever we want to be able to send different
// configuration to a client, e.g. if newer clients support config that older clients would reject.
// Version 27: Added double matching support.
// Version 28: Added support for generate log action and client report uploads.
// Version 29: Added support for out of the band artifact uploads.
// Version 30: Added support for workflow timeout transitions.
// Version 31: Added support for the debug mux API.
const CONFIGURATION_VERSION: &str = "31";

/// The platform we're currently running as.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Platform {
  Android,
  Apple,
  Electron,
}

impl Platform {
  #[must_use]
  pub const fn as_str(self) -> &'static str {
    match self {
      Self::Android => "android",
      Self::Apple => "apple",
      Self::Electron => "electron",
    }
  }
}

pub trait Metadata: Send {
  fn sdk_version(&self) -> &'static str;

  fn platform(&self) -> &Platform;

  fn os(&self) -> String;

  fn device_id(&self) -> String;

  fn collect_inner(&self) -> HashMap<String, String>;

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
