// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// Declares public modules with feature-gated source info support. When `with-source-info` is
// enabled, each module is loaded from an alternate file path containing proto descriptors with
// embedded source comments.
macro_rules! source_info_gated_mod {
  ($($name:ident => $path:literal),+ $(,)?) => {
    $(
      #[cfg(not(feature = "with-source-info"))]
      pub mod $name;
      #[cfg(feature = "with-source-info")]
      #[path = $path]
      pub mod $name;
    )+
  };
}

pub mod bdtail;
pub mod client;
pub mod config;
pub mod filter;
pub mod google;
pub mod insight;
pub mod log_matcher;
pub mod logging;
pub mod mme;
pub mod prometheus;
pub mod state;
pub mod value_matcher;
pub mod workflow;

#[cfg(all(feature = "public-api", not(feature = "with-source-info")))]
pub mod public_api;

#[cfg(feature = "with-source-info")]
#[path = "public_api_with_source/mod.rs"]
pub mod public_api;
