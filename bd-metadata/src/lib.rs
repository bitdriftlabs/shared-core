// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use std::collections::HashMap;
use std::fmt::Display;

// The configuration version of the client is a version number maintained independently from the
// release version of SDK which is used to allow the server to send different configurations to
// different clients. This should be incremented whenever we want to be able to send different
// configuration to a client, e.g. if newer clients support config that older clients would reject.
// Version 27: Added double matching support.
const CONFIGURATION_VERSION: &str = "27";

/// The platform we're currently running as.
#[derive(Clone, Copy)]
pub enum Platform {
  Android,
  Apple,
  Electron,
}

impl Display for Platform {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Android => write!(f, "android"),
      Self::Apple => write!(f, "apple"),
      Self::Electron => write!(f, "electron"),
    }
  }
}

pub trait Metadata: Send {
  fn sdk_version(&self) -> &'static str;

  fn platform(&self) -> &Platform;

  fn os(&self) -> String;

  fn collect_inner(&self) -> HashMap<String, String>;

  fn collect(&self) -> HashMap<String, String> {
    let mut inner = self.collect_inner();

    inner.insert(
      "config_version".to_string(),
      CONFIGURATION_VERSION.to_string(),
    );

    inner.insert("os".to_string(), self.os());
    inner.insert("platform".to_string(), self.platform().to_string());
    inner.insert("sdk_version".to_string(), self.sdk_version().to_string());

    inner
  }
}
