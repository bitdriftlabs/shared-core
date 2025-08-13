// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use std::collections::HashMap;
use std::sync::Arc;

pub struct Metadata {
  pub app_id: Option<String>,
  pub app_version: Option<String>,
  pub platform: bd_api::Platform,
  pub device: Arc<bd_logger::Device>,
  pub model: String,
}

impl bd_api::Metadata for Metadata {
  fn sdk_version(&self) -> &'static str {
    "1.0.0"
  }

  fn platform(&self) -> &bd_api::Platform {
    &self.platform
  }

  fn os(&self) -> String {
    match self.platform {
      bd_api::Platform::Apple => "ios",
      bd_api::Platform::Android => "android",
      bd_api::Platform::Electron => "electron",
    }
    .into()
  }

  fn device_id(&self) -> String {
    self.device.id()
  }

  fn collect_inner(&self) -> std::collections::HashMap<String, String> {
    let mut metadata_map = HashMap::new();

    if let Some(app_id) = self.app_id.as_ref() {
      metadata_map.insert("app_id".to_string(), app_id.to_string());
    }

    if let Some(app_version) = self.app_version.as_ref() {
      metadata_map.insert("app_version".to_string(), app_version.to_string());
    }

    metadata_map.insert("model".to_string(), self.model.clone());

    metadata_map
  }
}
