// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{CONFIGURATION_VERSION, Metadata, Platform, valid_app_environment_name};
use std::collections::HashMap;

#[test]
fn app_environment_names_are_bounded_ascii_without_normalization() {
  for name in ["Production", "production", "QA-1._", &"a".repeat(64)] {
    assert!(valid_app_environment_name(name), "{name:?}");
  }
  for name in [
    "",
    " production",
    "production ",
    "\tproduction",
    "production\t",
    "has space",
    "has\ttab",
    "has/slash",
    "é",
    &"a".repeat(65),
  ] {
    assert!(!valid_app_environment_name(name), "{name:?}");
  }
}

//
// TestMetadata
//

struct TestMetadata {
  platform: Platform,
  app_environment: Option<String>,
}

impl Metadata for TestMetadata {
  fn sdk_version(&self) -> &'static str {
    "1.0"
  }

  fn platform(&self) -> &Platform {
    &self.platform
  }

  fn os(&self) -> String {
    "os".to_string()
  }

  fn device_id(&self) -> String {
    "device".to_string()
  }

  fn collect_inner(&self) -> HashMap<String, String> {
    let mut metadata = HashMap::from([
      ("app_id".to_string(), "io.bitdrift.test".to_string()),
      ("app_version".to_string(), "01.2".to_string()),
      ("platform".to_string(), "caller-platform".to_string()),
      ("filename".to_string(), "caller-name".to_string()),
      ("custom".to_string(), "value".to_string()),
    ]);
    if let Some(environment) = &self.app_environment {
      metadata.insert("app_environment".to_string(), environment.clone());
    }
    metadata
  }
}

#[test]
fn command_identity_excludes_unrelated_metadata() {
  for platform in [Platform::Android, Platform::Apple] {
    let metadata = TestMetadata {
      platform,
      app_environment: None,
    };
    assert_eq!(
      metadata.command_artifact_metadata(),
      HashMap::from([
        ("platform".to_string(), platform.as_str().to_string()),
        ("app_id".to_string(), "io.bitdrift.test".to_string()),
        ("app_version".to_string(), "01.2".to_string()),
      ])
    );
    assert_eq!(metadata.collect().get("custom"), Some(&"value".to_string()));
    assert_eq!(
      metadata.collect().get("config_version"),
      Some(&CONFIGURATION_VERSION.to_string())
    );
  }
}

#[test]
fn command_identity_excludes_legacy_environment_metadata() {
  for environment in [
    None,
    Some(""),
    Some("Production"),
    Some("not validated locally"),
  ] {
    let metadata = TestMetadata {
      platform: Platform::Apple,
      app_environment: environment.map(str::to_string),
    };
    assert_eq!(
      metadata
        .command_artifact_metadata()
        .get("app_environment")
        .map(String::as_str),
      None,
    );
  }
}
