// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{
  DETAILS_INFERENCE_CONFIG_FILE,
  INGESTION_CONFIG_FILE,
  Monitor,
  Path,
  REASON_INFERENCE_CONFIG_FILE,
  get_fatal_issue_metadata,
  global_state,
};
use bd_device::Store;
use bd_log_primitives::LogFields;
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::{
  AppBuildNumber,
  AppBuildNumberArgs,
  AppMetrics,
  AppMetricsArgs,
  DeviceMetrics,
  DeviceMetricsArgs,
  Error,
  ErrorArgs,
  ErrorRelation,
  OSBuild,
  OSBuildArgs,
  Platform,
  Report,
  ReportArgs,
  Timestamp,
};
use bd_proto_util::ToFlatBufferString;
use bd_runtime::runtime::crash_handling::CrashDirectories;
use bd_runtime::runtime::{self, FeatureFlag as _};
use bd_runtime::test::TestConfigLoader;
use bd_shutdown::ComponentShutdownTrigger;
use bd_test_helpers::make_mut;
use bd_test_helpers::runtime::{ValueKind, make_simple_update};
use bd_test_helpers::session::InMemoryStorage;
use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, WIPOffset};
use itertools::Itertools;
use mockall::predicate::eq;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use time::OffsetDateTime;
use time::macros::datetime;
use uuid::Uuid;

struct Setup {
  directory: TempDir,
  monitor: Monitor,
  runtime: TestConfigLoader,
  store: Arc<Store>,
  upload_client: Arc<bd_artifact_upload::MockClient>,
  _shutdown: ComponentShutdownTrigger,
}

impl Setup {
  async fn new(artifact_upload: bool) -> Self {
    let directory = TempDir::new().unwrap();
    std::fs::create_dir_all(directory.path().join("runtime")).unwrap();
    let runtime = TestConfigLoader::new_in_directory(&directory.path().join("runtime")).await;

    if artifact_upload {
      runtime
        .update_snapshot(&make_simple_update(vec![(
          runtime::artifact_upload::Enabled::path(),
          ValueKind::Bool(artifact_upload),
        )]))
        .await;
    }


    let shutdown = ComponentShutdownTrigger::default();
    let store = Arc::new(Store::new(Box::<InMemoryStorage>::default()));
    let upload_client = Arc::new(bd_artifact_upload::MockClient::default());

    let monitor = Monitor::new(
      &runtime,
      directory.path(),
      store.clone(),
      upload_client.clone(),
      "previous_session_id".to_string(),
      shutdown.make_shutdown(),
    );

    Self {
      directory,
      monitor,
      runtime,
      store,
      upload_client,
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
        log
          .fields
          .get("_crash_artifact_id")
          .unwrap()
          .as_str()
          .unwrap()
          .to_string()
      })
      .map(|log| log.fields)
      .collect()
  }

  fn expect_artifact_upload(
    &mut self,
    content: &[u8],
    uuid: Uuid,
    state: LogFields,
    timestamp: Option<OffsetDateTime>,
  ) {
    make_mut(&mut self.upload_client)
      .expect_enqueue_upload()
      .with(
        eq(content.to_vec()),
        eq(state),
        eq(timestamp),
        eq("previous_session_id".to_string()),
      )
      .returning(move |_, _, _, _| Ok(uuid));
  }
}
#[tokio::test]
async fn timestamp_propagation() {
  let mut setup = Setup::new(true).await;

  setup.monitor.try_ensure_directories_exist().await;

  setup
    .write_config_file(REASON_INFERENCE_CONFIG_FILE, "reason,crash.reason")
    .await;
  setup
    .write_config_file(DETAILS_INFERENCE_CONFIG_FILE, "details[0].cause")
    .await;

  let timestamp = datetime!(2024-01-01 12:00:00 UTC);
  let artifact1 = b"{\"reason\":\"foo\",\"details\": [{\"cause\": \"kaboom\"}]}";
  setup.make_crash(
    &format!("{}_crash1.envelope", timestamp.unix_timestamp() * 1000),
    artifact1,
  );

  let uuid1 = "12345678-1234-5678-1234-567812345671".parse().unwrap();
  setup.expect_artifact_upload(artifact1, uuid1, [].into(), Some(timestamp));
}

#[tokio::test]
async fn crash_reason_inference() {
  let mut setup = Setup::new(true).await;

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
  setup.make_crash("crash1.envelope", artifact1);
  setup.make_crash("crash2.envelope", artifact2);
  setup.make_crash("crash3.envelope", artifact3);

  let uuid1 = "12345678-1234-5678-1234-567812345671".parse().unwrap();
  let uuid2 = "12345678-1234-5678-1234-567812345672".parse().unwrap();
  let uuid3 = "12345678-1234-5678-1234-567812345673".parse().unwrap();
  setup.expect_artifact_upload(
    artifact1,
    uuid1,
    [("state".into(), "foo".into())].into(),
    None,
  );
  setup.expect_artifact_upload(
    artifact2,
    uuid2,
    [("state".into(), "foo".into())].into(),
    None,
  );
  setup.expect_artifact_upload(
    artifact3,
    uuid3,
    [("state".into(), "foo".into())].into(),
    None,
  );

  let logs = setup.process_new_reports().await;
  assert_eq!(3, logs.len());
  let log1 = &logs[0];
  let log2 = &logs[1];
  let log3 = &logs[2];

  assert_eq!(
    uuid1.to_string(),
    log1["_crash_artifact_id"].as_str().unwrap()
  );
  assert_eq!("foo", log1["_crash_reason"].as_str().unwrap());
  assert_eq!("kaboom", log1["_crash_details"].as_str().unwrap());
  assert_eq!("foo", log1["state"].as_str().unwrap());

  assert_eq!(
    uuid2.to_string(),
    log2["_crash_artifact_id"].as_str().unwrap()
  );
  assert_eq!("bar", log2["_crash_reason"].as_str().unwrap());
  assert_eq!("unknown", log2["_crash_details"].as_str().unwrap());
  assert_eq!("foo", log2["state"].as_str().unwrap());

  assert_eq!(
    uuid3.to_string(),
    log3["_crash_artifact_id"].as_str().unwrap()
  );
  assert_eq!("bar", log3["_crash_reason"].as_str().unwrap());
  assert_eq!("unknown", log3["_crash_details"].as_str().unwrap());
  assert_eq!("foo", log1["state"].as_str().unwrap());
}

#[tokio::test]
async fn crash_handling_missing_reason() {
  let mut setup = Setup::new(true).await;

  setup.monitor.try_ensure_directories_exist().await;

  setup
    .write_config_file(REASON_INFERENCE_CONFIG_FILE, "reason,crash.reason")
    .await;
  setup
    .write_config_file(DETAILS_INFERENCE_CONFIG_FILE, "details[0].cause")
    .await;

  setup.make_crash("crash1.envelope", b"crash1");
  setup.make_crash("crash2.envelope", b"crash2");
  setup.make_crash("crash3.envelope", b"{\"crash\":{\"reason\": \"bar\"}}");

  let uuid = "12345678-1234-5678-1234-567812345671".parse().unwrap();
  setup.expect_artifact_upload(b"{\"crash\":{\"reason\": \"bar\"}}", uuid, [].into(), None);

  let logs = setup.process_new_reports().await;
  assert_eq!(1, logs.len());
  assert_eq!(
    uuid.to_string(),
    logs[0]["_crash_artifact_id"].as_str().unwrap()
  );
}

#[tokio::test]
async fn config_file() {
  let setup = Setup::new(false).await;

  setup.configure_ingestion_runtime_flag("a").await;

  setup.write_config_file(INGESTION_CONFIG_FILE, "a").await;
  assert_eq!("a", setup.read_config_file(INGESTION_CONFIG_FILE));

  setup.write_config_file(INGESTION_CONFIG_FILE, "a:b").await;
  assert_eq!("a:b", setup.read_config_file(INGESTION_CONFIG_FILE));

  setup.write_config_file(INGESTION_CONFIG_FILE, "").await;
  assert!(!setup.config_file_exists(INGESTION_CONFIG_FILE));
}

#[tokio::test]
async fn test_log_report_fields() {
  let mut builder = FlatBufferBuilder::new();
  let name = "BigProb".to_fb(&mut builder);
  let reason = "missing meta".to_fb(&mut builder);
  let error = Error::create(
    &mut builder,
    &ErrorArgs {
      name,
      reason,
      stack_trace: None,
      relation_to_next: ErrorRelation::CausedBy,
    },
  );
  let errors = Some(builder.create_vector::<WIPOffset<Error<'_>>>(&[error]));
  let app_id = "com.example.foo.widget".to_fb(&mut builder);
  let version = "4.15".to_fb(&mut builder);
  let cf_bundle_version = "5".to_fb(&mut builder);
  let build_number = Some(AppBuildNumber::create(
    &mut builder,
    &AppBuildNumberArgs {
      cf_bundle_version,
      ..Default::default()
    },
  ));
  let app_metrics = Some(AppMetrics::create(
    &mut builder,
    &AppMetricsArgs {
      app_id,
      build_number,
      version,
      ..Default::default()
    },
  ));
  let os_version = "3.1".to_fb(&mut builder);
  let os_build = Some(OSBuild::create(
    &mut builder,
    &OSBuildArgs {
      version: os_version,
      ..Default::default()
    },
  ));
  let unix_timestamp: i64 = 1_752_839_953;
  let timestamp = Timestamp::new(unix_timestamp.try_into().unwrap(), 0);
  let device_metrics = Some(DeviceMetrics::create(
    &mut builder,
    &DeviceMetricsArgs {
      time: Some(&timestamp),
      os_build,
      platform: Platform::iOS,
      ..Default::default()
    },
  ));
  let report = Report::create(
    &mut builder,
    &ReportArgs {
      errors,
      app_metrics,
      device_metrics,
      ..Default::default()
    },
  );
  builder.finish(report, None);
  let data = builder.finished_data();

  let mut setup = Setup::new(true).await;
  let mut tracker = global_state::Tracker::new(setup.store.clone());
  tracker.maybe_update_global_state(
    &[
      ("os_version".into(), "6".into()),
      ("app_version".into(), "4.16".into()),
      ("app_id".into(), "com.example.foo".into()),
      ("other_stuff".into(), "foo".into()),
    ]
    .into(),
  );
  setup.make_crash("report.cap", data);

  let uuid = "12345678-1234-5678-1234-5678123456aa".parse().unwrap();
  setup.expect_artifact_upload(
    data,
    uuid,
    [
      ("other_stuff".into(), "foo".into()),
      ("app_version".into(), "4.15".into()),
      ("os_version".into(), "3.1".into()),
      ("_build_number".into(), "5".into()),
      ("app_id".into(), "com.example.foo.widget".into()),
    ]
    .into(),
    OffsetDateTime::from_unix_timestamp(unix_timestamp).ok(),
  );

  let logs = setup.process_new_reports().await;
  assert_eq!(1, logs.len());
  assert_eq!(
    uuid.to_string(),
    logs[0]["_crash_artifact_id"].as_str().unwrap()
  );
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
  let (reason, detail, report2) = Monitor::read_report_contents(data, &[], &[]);
  assert_eq!(None, reason);
  assert_eq!(None, detail);
  assert!(report2.is_some());
}

#[test]
fn get_fatal_issue_mechanism_from_envelope_must_return_integration() {
  let envelope_file = Path::new("foo.envelope");

  let metadata = get_fatal_issue_metadata(envelope_file).unwrap();
  assert_eq!(metadata.details_key, "_crash_details");
  assert_eq!(metadata.reason_key, "_crash_reason");
  assert_eq!(metadata.message_value, "App crashed".into());
  assert_eq!(metadata.mechanism_type_value, "INTEGRATION");
}

#[test]
fn get_fatal_issue_mechanism_from_json_must_return_integration() {
  let json_file = Path::new("foo.json");

  let metadata = get_fatal_issue_metadata(json_file).unwrap();
  assert_eq!(metadata.details_key, "_crash_details");
  assert_eq!(metadata.reason_key, "_crash_reason");
  assert_eq!(metadata.message_value, "App crashed".into());
  assert_eq!(metadata.mechanism_type_value, "INTEGRATION");
  assert_eq!(metadata.report_type_value, "Unknown".into());
}

#[test]
fn get_fatal_issue_mechanism_from_cap_must_return_built_in() {
  let cap_file = Path::new("foo_crash.cap");

  let crash_metadata = get_fatal_issue_metadata(cap_file).unwrap();
  assert_eq!(crash_metadata.details_key, "_app_exit_details");
  assert_eq!(crash_metadata.reason_key, "_app_exit_info");
  assert_eq!(crash_metadata.message_value, "AppExit".into());
  assert_eq!(crash_metadata.mechanism_type_value, "BUILT_IN");
  assert_eq!(crash_metadata.report_type_value, "Crash".into());

  let cap_file = Path::new("foo_anr.cap");
  let anr_metadata = get_fatal_issue_metadata(cap_file).unwrap();
  assert_eq!(anr_metadata.report_type_value, "ANR".into());

  let cap_file = Path::new("foo_native_crash.cap");
  let anr_metadata = get_fatal_issue_metadata(cap_file).unwrap();
  assert_eq!(anr_metadata.report_type_value, "Native Crash".into());
}

#[test]
fn get_fatal_issue_mechanism_without_extension_must_return_error() {
  let file_without_extension = Path::new("no_extension");

  let metadata = get_fatal_issue_metadata(file_without_extension);
  assert!(metadata.is_err());

  let error_message = metadata.unwrap_err().to_string();
  assert!(error_message.contains("Unknown file extension"));
}

#[test]
fn timestamp_from_malformed_filename() {
  let path = PathBuf::from("/path/to/x_crash_somethingsomething.cap");
  let timestamp = Monitor::timestamp_from_filepath(&path);
  assert_eq!(None, timestamp);
}

#[test]
fn timestamp_from_filename_millis() {
  let offset: i128 = 808_142_220_336;
  let path = PathBuf::from(format!("/path/to/{offset}_crash_somethingsomething.cap"));
  let timestamp = Monitor::timestamp_from_filepath(&path);
  assert!(timestamp.is_some());
  assert_eq!(
    OffsetDateTime::from_unix_timestamp_nanos(offset * 1_000_000).ok(),
    timestamp,
  );
}

#[test]
fn timestamp_from_filename_fractional_millis() {
  let offset: f64 = 808_142_220_346.003_3;
  let path = PathBuf::from(format!("/path/to/{offset:?}_crash_somethingsomething.cap"));
  let timestamp = Monitor::timestamp_from_filepath(&path);
  assert!(timestamp.is_some());

  #[allow(clippy::cast_possible_truncation)]
  let nanos = (offset * 1_000_000.0) as i128;
  assert_eq!(
    OffsetDateTime::from_unix_timestamp_nanos(nanos).ok(),
    timestamp,
  );
}
