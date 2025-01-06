// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_metadata::{Metadata, Platform};
use std::collections::HashMap;

//
// EmptyMetadata
//

/// A metadata type that doesn't collect any metadata.
pub struct EmptyMetadata;

impl Metadata for EmptyMetadata {
  fn sdk_version(&self) -> &'static str {
    "0.0.0"
  }

  fn platform(&self) -> &Platform {
    &Platform::Apple
  }

  fn os(&self) -> String {
    "ios".to_string()
  }

  #[must_use]
  fn collect_inner(&self) -> HashMap<String, String> {
    HashMap::new()
  }
}
