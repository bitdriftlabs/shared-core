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
  shutdown: ComponentShutdownTrigger,
}

impl Setup {
  async fn new() -> Self {
    let directory = TempDir::new().unwrap();
    let runtime_dir = directory.path().join("runtime");
    std::fs::create_dir_all(&runtime_dir).unwrap();
    let runtime = TestConfigLoader::new_in_directory(&runtime_dir).await;

    let shutdown = ComponentShutdownTrigger::default();
    let writer = ConfigWriter::new(&runtime, directory.path(), shutdown.make_shutdown());

    Self {
      directory,
      runtime: Arc::new(runtime),
      writer,
      shutdown,
    }
  }

  async fn configure_runtime_flag(&self, value: bool) {
    self
      .runtime
      .update_snapshot(make_simple_update(vec![(
        "crash_reporting.enabled",
        ValueKind::Bool(value),
      )]))
      .await
      .unwrap();
  }

  async fn configure_use_bd_crash_reporter_flag(&self, value: bool) {
    self
      .runtime
      .update_snapshot(make_simple_update(vec![(
        "client_feature.ios.use_bd_crash_reporter",
        ValueKind::Bool(value),
      )]))
      .await
      .unwrap();
  }

  fn read_config_file(&self) -> String {
    let config_file = self.directory.path().join("reports/config.csv");
    log::info!("Reading config file: {config_file:?}");
    std::fs::read_to_string(&config_file).unwrap()
  }

  fn write_raw_config_file(&self, contents: &str) {
    let reports_dir = self.directory.path().join("reports");
    std::fs::create_dir_all(&reports_dir).unwrap();
    std::fs::write(reports_dir.join("config.csv"), contents).unwrap();
  }

  // Runs the writer's `run()` loop to completion (triggering shutdown right away) and returns
  // the resulting contents of the config file.
  async fn run_until_shutdown(self) -> String {
    let mut writer = self.writer;
    let handle = tokio::spawn(async move {
      writer.run().await;
    });

    self.shutdown.shutdown().await;
    handle.await.unwrap();

    let config_file = self.directory.path().join("reports/config.csv");
    std::fs::read_to_string(&config_file).unwrap()
  }
}

#[tokio::test]
async fn test_config_file_written() {
  let setup = Setup::new().await;

  setup.writer.write_config_file().await;
  assert_eq!(
    "crash_reporting.enabled,true\nclient_feature.ios.use_bd_crash_reporter,true",
    setup.read_config_file()
  );

  setup.configure_runtime_flag(false).await;
  setup.writer.write_config_file().await;
  assert_eq!(
    "crash_reporting.enabled,false\nclient_feature.ios.use_bd_crash_reporter,true",
    setup.read_config_file()
  );
}

#[tokio::test]
async fn test_config_file_written_use_bd_crash_reporter() {
  let setup = Setup::new().await;

  setup.configure_use_bd_crash_reporter_flag(false).await;
  setup.writer.write_config_file().await;
  assert_eq!(
    "crash_reporting.enabled,true\nclient_feature.ios.use_bd_crash_reporter,false",
    setup.read_config_file()
  );
}

#[tokio::test]
async fn test_run_writes_config_file_when_missing() {
  let setup = Setup::new().await;

  let contents = setup.run_until_shutdown().await;

  assert_eq!(
    "crash_reporting.enabled,true\nclient_feature.ios.use_bd_crash_reporter,true",
    contents
  );
}

#[tokio::test]
async fn test_run_rewrites_legacy_only_config_file() {
  let setup = Setup::new().await;
  setup.write_raw_config_file("crash_reporting.enabled,true");

  let contents = setup.run_until_shutdown().await;

  assert_eq!(
    "crash_reporting.enabled,true\nclient_feature.ios.use_bd_crash_reporter,true",
    contents
  );
}

#[tokio::test]
async fn test_run_does_not_rewrite_up_to_date_config_file() {
  let setup = Setup::new().await;
  setup.write_raw_config_file(
    "crash_reporting.enabled,false\nclient_feature.ios.use_bd_crash_reporter,false",
  );

  let contents = setup.run_until_shutdown().await;

  assert_eq!(
    "crash_reporting.enabled,false\nclient_feature.ios.use_bd_crash_reporter,false",
    contents
  );
}
