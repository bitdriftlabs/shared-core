use super::{CONFIGURATION_VERSION, Metadata, Platform};
use std::collections::HashMap;

//
// TestMetadata
//

struct TestMetadata {
  platform: Platform,
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
    HashMap::from([
      ("app_id".to_string(), "io.bitdrift.test".to_string()),
      ("app_version".to_string(), "01.2".to_string()),
      ("platform".to_string(), "caller-platform".to_string()),
      ("filename".to_string(), "caller-name".to_string()),
      ("custom".to_string(), "value".to_string()),
    ])
  }
}

#[test]
fn command_identity_excludes_unrelated_metadata() {
  for platform in [Platform::Android, Platform::Apple] {
    let metadata = TestMetadata { platform };
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
