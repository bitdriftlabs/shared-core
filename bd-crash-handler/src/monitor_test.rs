// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::Monitor;
use bd_runtime::runtime::crash_handling::CrashDirectories;
use bd_runtime::runtime::{ConfigLoader, FeatureFlag as _};
use bd_shutdown::ComponentShutdownTrigger;
use bd_test_helpers::runtime::{ValueKind, make_simple_update};
use std::sync::Arc;
use tempfile::TempDir;

struct Setup {
  directory: TempDir,
  monitor: Monitor,
  runtime: Arc<ConfigLoader>,
  _shutdown: ComponentShutdownTrigger,
}

impl Setup {
  async fn new() -> Self {
    let directory = TempDir::new().unwrap();
    std::fs::create_dir_all(directory.path().join("runtime")).unwrap();
    let runtime = ConfigLoader::new(&directory.path().join("runtime"));
    let shutdown = ComponentShutdownTrigger::default();

    let monitor = Monitor::new(&runtime, directory.path(), shutdown.make_shutdown());

    Self {
      directory,
      monitor,
      runtime,
      _shutdown: shutdown,
    }
  }

  fn make_crash(&self, name: &str, data: &[u8]) {
    let crash_directory = self.directory.path().join("reports/new");
    std::fs::create_dir_all(&crash_directory).unwrap();
    std::fs::write(crash_directory.join(name), data).unwrap();
  }

  fn configure_runtime_flag(&self, value: &str) {
    self.runtime.update_snapshot(&make_simple_update(vec![(
      CrashDirectories::path(),
      ValueKind::String(value.to_string()),
    )]));
  }

  fn read_config_file(&self) -> String {
    let config_file = self.directory.path().join("reports/directories");
    log::info!("Reading config file: {:?}", config_file);

    std::fs::read_to_string(&config_file).unwrap()
  }

  fn config_file_exists(&self) -> bool {
    std::fs::exists(self.directory.path().join("reports/directories")).unwrap_or_default()
  }
}

#[tokio::test]
async fn crash_handling() {
  let setup = Setup::new().await;

  setup.monitor.try_ensure_directories_exist().await;

  setup.make_crash("crash1", b"crash1");
  setup.make_crash("crash2", b"crash2");

  let logs = setup.monitor.process_new_reports().await;
  assert_eq!(2, logs.len());
  assert_eq!(
    b"crash1",
    &logs[0]
      .fields
      .iter()
      .find(|field| field.key == "_crash_artifact")
      .unwrap()
      .value
      .as_bytes()
      .unwrap()
  );
  assert_eq!(
    b"crash2",
    &logs[1]
      .fields
      .iter()
      .find(|field| field.key == "_crash_artifact")
      .unwrap()
      .value
      .as_bytes()
      .unwrap()
  );
}

#[tokio::test]
async fn config_file() {
  let setup = Setup::new().await;

  setup.configure_runtime_flag("a");

  setup.monitor.write_config_file("a").await;
  assert_eq!("a", setup.read_config_file());

  setup.monitor.write_config_file("a:b").await;
  assert_eq!("a:b", setup.read_config_file());

  setup.monitor.write_config_file("").await;
  assert!(!setup.config_file_exists());
}
