// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{
  global_state,
  Monitor,
  DETAILS_INFERENCE_CONFIG_FILE,
  INGESTION_CONFIG_FILE,
  REASON_INFERENCE_CONFIG_FILE,
};
use bd_device::Store;
use bd_log_primitives::LogFields;
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::{
  Error,
  Report,
  ReportArgs,
};
use bd_runtime::runtime::crash_handling::CrashDirectories;
use bd_runtime::runtime::{ConfigLoader, FeatureFlag as _};
use bd_shutdown::ComponentShutdownTrigger;
use bd_test_helpers::runtime::{make_simple_update, ValueKind};
use bd_test_helpers::session::InMemoryStorage;
use flatbuffers::{FlatBufferBuilder, ForwardsUOffset};
use itertools::Itertools;
use std::sync::Arc;
use tempfile::TempDir;

struct Setup {
  directory: TempDir,
  monitor: Monitor,
  runtime: Arc<ConfigLoader>,
  store: Arc<Store>,
  _shutdown: ComponentShutdownTrigger,
}

impl Setup {
  fn new() -> Self {
    let directory = TempDir::new().unwrap();
    std::fs::create_dir_all(directory.path().join("runtime")).unwrap();
    let runtime = ConfigLoader::new(&directory.path().join("runtime"));
    let shutdown = ComponentShutdownTrigger::default();
    let store = Arc::new(Store::new(Box::<InMemoryStorage>::default()));

    let monitor = Monitor::new(
      &runtime,
      directory.path(),
      store.clone(),
      shutdown.make_shutdown(),
    );

    Self {
      directory,
      monitor,
      runtime,
      store,
      _shutdown: shutdown,
    }
  }

  fn make_crash(&self, name: &str, data: &[u8]) {
    let crash_directory = self.directory.path().join("reports/new");
    std::fs::create_dir_all(&crash_directory).unwrap();
    std::fs::write(crash_directory.join(name), data).unwrap();
  }

  async fn configure_ingestion_runtime_flag(&self, value: &str) {
    self
      .runtime
      .update_snapshot(&make_simple_update(vec![(
        CrashDirectories::path(),
        ValueKind::String(value.to_string()),
      )]))
      .await;
  }

  async fn write_config_file(&self, name: &str, contents: &str) {
    let config_file = self.directory.path().join("reports").join(name);
    log::info!("Writing config file: {config_file:?}");

    self
      .monitor
      .write_config_file(&self.directory.path().join("reports").join(name), contents)
      .await;
  }

  fn read_config_file(&self, name: &str) -> String {
    let config_file = self.directory.path().join("reports").join(name);
    log::info!("Reading config file: {config_file:?}");

    std::fs::read_to_string(&config_file).unwrap()
  }

  fn config_file_exists(&self, name: &str) -> bool {
    std::fs::exists(self.directory.path().join("reports").join(name)).unwrap_or_default()
  }

  async fn process_new_reports(&self) -> Vec<LogFields> {
    // Convert to a HashMap<String, String> for easier testing
    // Sort the logs by the first field to make the test deterministic - otherwise this depends on
    // the order of files traversed in the directory.

    self
      .monitor
      .process_new_reports()
      .await
      .into_iter()
      .sorted_by_key(|log| {
        log.fields["_crash_artifact"]
          .as_bytes()
          .unwrap_or_default()
          .to_vec()
      })
      .map(|log| log.fields)
      .collect()
  }
}

#[tokio::test]
async fn crash_reason_inference() {
  let setup = Setup::new();

  let mut tracker = global_state::Tracker::new(setup.store.clone());

  tracker.maybe_update_global_state(&[("state".into(), "foo".into())].into());

  setup.monitor.try_ensure_directories_exist().await;

  setup
    .write_config_file(REASON_INFERENCE_CONFIG_FILE, "reason,crash.reason")
    .await;
  setup
    .write_config_file(DETAILS_INFERENCE_CONFIG_FILE, "details[0].cause")
    .await;

  let artifact1 = b"{\"reason\":\"foo\",\"details\": [{\"cause\": \"kaboom\"}]}";
  let artifact2 = b"{\"crash\":{\"reason\": \"bar\"}}";
  let artifact3 = b"{}\n{\"crash\":{\"reason\": \"bar\"}}\n{\"crash\":{\"reason\": \"bar\"}}";
  setup.make_crash("crash1", artifact1);
  setup.make_crash("crash2", artifact2);
  setup.make_crash("crash3", artifact3);

  let logs = setup.process_new_reports().await;
  assert_eq!(3, logs.len());
  let log1 = &logs[0];
  let log2 = &logs[1];
  let log3 = &logs[2];

  assert_eq!(artifact2, &log1["_crash_artifact"].as_bytes().unwrap());
  assert_eq!("bar", log1["_crash_reason"].as_str().unwrap());
  assert_eq!("unknown", log1["_crash_details"].as_str().unwrap());
  assert_eq!("foo", log1["state"].as_str().unwrap());

  assert_eq!(artifact1, &log2["_crash_artifact"].as_bytes().unwrap());
  assert_eq!("foo", log2["_crash_reason"].as_str().unwrap());
  assert_eq!("kaboom", log2["_crash_details"].as_str().unwrap());
  assert_eq!("foo", log1["state"].as_str().unwrap());

  assert_eq!(artifact3, &log3["_crash_artifact"].as_bytes().unwrap());
  assert_eq!("bar", log3["_crash_reason"].as_str().unwrap());
  assert_eq!("unknown", log3["_crash_details"].as_str().unwrap());
  assert_eq!("foo", log1["state"].as_str().unwrap());
}

#[tokio::test]
async fn crash_handling_missing_reason() {
  let setup = Setup::new();

  setup.monitor.try_ensure_directories_exist().await;

  setup
    .write_config_file(REASON_INFERENCE_CONFIG_FILE, "reason,crash.reason")
    .await;
  setup
    .write_config_file(DETAILS_INFERENCE_CONFIG_FILE, "details[0].cause")
    .await;

  setup.make_crash("crash1", b"crash1");
  setup.make_crash("crash2", b"crash2");
  setup.make_crash("crash3", b"{\"crash\":{\"reason\": \"bar\"}}");

  let logs = setup.process_new_reports().await;
  assert_eq!(1, logs.len());
  assert_eq!(
    b"{\"crash\":{\"reason\": \"bar\"}}",
    &logs[0]["_crash_artifact"].as_bytes().unwrap()
  );
}

#[tokio::test]
async fn config_file() {
  let setup = Setup::new();

  setup.configure_ingestion_runtime_flag("a").await;

  setup.write_config_file(INGESTION_CONFIG_FILE, "a").await;
  assert_eq!("a", setup.read_config_file(INGESTION_CONFIG_FILE));

  setup.write_config_file(INGESTION_CONFIG_FILE, "a:b").await;
  assert_eq!("a:b", setup.read_config_file(INGESTION_CONFIG_FILE));

  setup.write_config_file(INGESTION_CONFIG_FILE, "").await;
  assert!(!setup.config_file_exists(INGESTION_CONFIG_FILE));
}

#[test]
fn crash_reason_from_empty_errors_vector() {
  let mut builder = FlatBufferBuilder::new();
  let errors = Some(builder.create_vector::<ForwardsUOffset<Error<'_>>>(&[]));
  let report = Report::create(
    &mut builder,
    &ReportArgs {
      errors,
      ..Default::default()
    },
  );
  builder.finish(report, None);
  let data = builder.finished_data();
  let (reason, detail) = Monitor::guess_crash_details(data, &[], &[]);
  assert_eq!(None, reason);
  assert_eq!(None, detail);
}
