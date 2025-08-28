// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::config_writer::ConfigWriter;
use bd_runtime::test::TestConfigLoader;
use bd_shutdown::ComponentShutdownTrigger;
use bd_test_helpers::runtime::{ValueKind, make_simple_update};
use std::sync::Arc;
use tempfile::TempDir;

struct Setup {
  directory: TempDir,
  runtime: Arc<TestConfigLoader>,
  writer: ConfigWriter,
  _shutdown: ComponentShutdownTrigger,
}

impl Setup {
  async fn new(crash_reporting_enabled: bool) -> Self {
    let directory = TempDir::new().unwrap();
    let runtime_dir = directory.path().join("runtime");
    std::fs::create_dir_all(&runtime_dir).unwrap();
    let runtime = TestConfigLoader::new_in_directory(&runtime_dir).await;

    if crash_reporting_enabled {
      runtime
        .update_snapshot(&make_simple_update(vec![(
          "crash_reporting.enabled",
          ValueKind::Bool(crash_reporting_enabled),
        )]))
        .await;
    }

    let shutdown = ComponentShutdownTrigger::default();
    let writer = ConfigWriter::new(&runtime, directory.path(), shutdown.make_shutdown());

    Self {
      directory,
      runtime: Arc::new(runtime),
      writer,
      _shutdown: shutdown,
    }
  }

  async fn configure_runtime_flag(&self, value: bool) {
    self
      .runtime
      .update_snapshot(&make_simple_update(vec![(
        "crash_reporting.enabled",
        ValueKind::Bool(value),
      )]))
      .await;
  }

  fn read_config_file(&self) -> String {
    let config_file = self.directory.path().join("reports/config.csv");
    log::info!("Reading config file: {config_file:?}");
    std::fs::read_to_string(&config_file).unwrap()
  }
}

#[tokio::test]
async fn test_config_file_written() {
  let setup = Setup::new(true).await;

  setup.writer.write_config_file().await;
  assert_eq!("crash_reporting.enabled,true", setup.read_config_file());

  setup.configure_runtime_flag(false).await;
  setup.writer.write_config_file().await;
  assert_eq!("crash_reporting.enabled,false", setup.read_config_file());
}
