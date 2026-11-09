// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_runtime_config::feature_flags::FeatureFlags;
use std::sync::Arc;

// Mock for external FeatureFlags trait.
mockall::mock! {
  #[derive(Debug)]
  pub FeatureFlags {}
  impl FeatureFlags for FeatureFlags {
    fn feature_enabled(&self, name: &str, default: bool) -> bool;
    fn get_bool(&self, name: &str, default: bool) -> bool;
    fn get_integer(&self, name: &str, default: u64) -> u64;
    fn get_string(&self, name: &str, default: &Arc<String>) -> Arc<String>;
  }
}
