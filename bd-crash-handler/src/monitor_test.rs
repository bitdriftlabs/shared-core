// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{Monitor, global_state};
use bd_client_common::init_lifecycle::InitLifecycleState;
use bd_feature_flags::FeatureFlagsBuilder;
use bd_log_primitives::{AnnotatedLogFields, LogFields};
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
  ReportType,
  Timestamp,
};
use bd_proto_util::ToFlatBufferString;
use bd_runtime::runtime::{self};
use bd_session::fixed::{self, UUIDCallbacks};
use bd_shutdown::ComponentShutdownTrigger;
use bd_test_helpers::make_mut;
use bd_test_helpers::session::in_memory_store;
use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, WIPOffset};
use itertools::Itertools;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use time::OffsetDateTime;
use time::ext::{NumericalDuration, NumericalStdDuration};
use time::macros::datetime;
use uuid::Uuid;

struct StaticSession(String);

impl fixed::Callbacks for StaticSession {
  fn generate_session_id(&self) -> anyhow::Result<String> {
    Ok(self.0.clone())
  }
}

struct Setup {
  directory: TempDir,
  monitor: Monitor,
  upload_client: Arc<bd_artifact_upload::MockClient>,
  emit_log_rx: tokio::sync::mpsc::Receiver<crate::CrashLog>,
  _shutdown: ComponentShutdownTrigger,
}

impl Setup {
  fn new(maybe_global_state: Option<LogFields>, enable_file_watcher: bool) -> Self {
    let directory = TempDir::new().unwrap();

    let shutdown = ComponentShutdownTrigger::default();
    let store = in_memory_store();
    let upload_client = Arc::new(bd_artifact_upload::MockClient::default());

    if let Some(global_state) = maybe_global_state {
      let mut tracker =
        global_state::Tracker::new(store.clone(), runtime::Watch::new_for_testing(10.seconds()));
      tracker.maybe_update_global_state(&global_state);
    }

    let feature_flags_builder =
      FeatureFlagsBuilder::new(&directory.path().join("feature_flags"), 1024, 0.8);
    // Set up the session to return a fixed previous session ID, making it obvious that we are
    // using the previous session ID for uploads.
    let mut session = bd_session::Strategy::Fixed(bd_session::fixed::Strategy::new(
      store.clone(),
      Arc::new(StaticSession("previous_session_id".into())),
    ));
    assert_eq!(session.session_id(), "previous_session_id");

    session = bd_session::Strategy::Fixed(bd_session::fixed::Strategy::new(
      store.clone(),
      Arc::new(UUIDCallbacks),
    ));

    let (tx, rx) = tokio::sync::mpsc::channel(10);
    let emit_log =
      move |crash_log: crate::CrashLog| tx.try_send(crash_log).map_err(|e| anyhow::anyhow!("{e}"));

    if enable_file_watcher {
      // Create watcher directory structure to enable file watching
      let watcher_dir = directory.path().join("reports/watcher");
      let _ = std::fs::create_dir_all(&watcher_dir);
    }

    let monitor = Monitor::new(
      directory.path(),
      store,
      upload_client.clone(),
      Arc::new(session),
      &InitLifecycleState::new(),
      feature_flags_builder,
      emit_log,
    );

    Self {
      directory,
      monitor,
      upload_client,
      emit_log_rx: rx,
      _shutdown: shutdown,
    }
  }

  fn current_session_directory(&self) -> PathBuf {
    self
      .directory
      .path()
      .join("reports/watcher/current_session")
  }

  fn previous_session_directory(&self) -> PathBuf {
    self
      .directory
      .path()
      .join("reports/watcher/previous_session")
  }

  fn make_crash(&self, name: &str, data: &[u8]) {
    let crash_directory = self.directory.path().join("reports/new");
    std::fs::create_dir_all(&crash_directory).unwrap();
    std::fs::write(crash_directory.join(name), data).unwrap();
  }

  async fn process_all_pending_reports(&self) -> Vec<AnnotatedLogFields> {
    // Convert to a HashMap<String, String> for easier testing
    // Sort the logs by the first field to make the test deterministic - otherwise this depends on
    // the order of files traversed in the directory.

    self
      .monitor
      .process_all_pending_reports()
      .await
      .into_iter()
      .sorted_by_key(|log| {
        log
          .fields
          .get("_crash_artifact_id")
          .map(|f| f.value.clone())
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
    session_id: &str,
  ) {
    let content = content.to_vec();
    let session_id = session_id.to_string();
    make_mut(&mut self.upload_client)
      .expect_enqueue_upload()
      .withf(move |mut file, fstate, ftimestamp, fsession_id, _| {
        let mut output = vec![];
        file.read_to_end(&mut output).unwrap();
        output == content
          && &state == fstate
          && &timestamp == ftimestamp
          && session_id == *fsession_id
      })
      .returning(move |_, _, _, _, _| Ok(uuid));
  }
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
  let crash_timestamp = datetime!(2024-01-15 12:34:56 UTC);
  let unix_timestamp = crash_timestamp.unix_timestamp();
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
      type_: ReportType::NativeCrash,
      errors,
      app_metrics,
      device_metrics,
      ..Default::default()
    },
  );
  builder.finish(report, None);
  let data = builder.finished_data();

  let mut setup = Setup::new(
    Some(
      [
        ("os_version".into(), "6".into()),
        ("app_version".into(), "4.16".into()),
        ("app_id".into(), "com.example.foo".into()),
        ("other_stuff".into(), "foo".into()),
      ]
      .into(),
    ),
    false,
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
    crash_timestamp.into(),
    "previous_session_id",
  );

  let logs = setup.process_all_pending_reports().await;
  assert_eq!(1, logs.len());
  assert_eq!(
    uuid.to_string(),
    logs[0]["_crash_artifact_id"].value.as_str().unwrap()
  );
  assert_eq!("BigProb", logs[0]["_app_exit_info"].value.as_str().unwrap());
  assert_eq!(
    "missing meta",
    logs[0]["_app_exit_details"].value.as_str().unwrap()
  );
  assert_eq!(
    "Native Crash",
    logs[0]["_app_exit_reason"].value.as_str().unwrap()
  );
  assert_eq!(
    "BUILT_IN",
    logs[0]["_fatal_issue_mechanism"].value.as_str().unwrap()
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
  let (reason, detail, report2) = Monitor::read_report_contents(data);
  assert_eq!(None, reason);
  assert_eq!(None, detail);
  assert!(report2.is_some());
}

#[test]
fn test_report_type_to_reason() {
  assert_eq!(
    "ANR",
    Monitor::report_type_to_reason(ReportType::AppNotResponding)
  );
  assert_eq!(
    "Native Crash",
    Monitor::report_type_to_reason(ReportType::NativeCrash)
  );
  assert_eq!(
    "Crash",
    Monitor::report_type_to_reason(ReportType::JVMCrash)
  );
  assert_eq!(
    "Strict Mode Violation",
    Monitor::report_type_to_reason(ReportType::StrictModeViolation)
  );
  assert_eq!(
    "Memory Termination",
    Monitor::report_type_to_reason(ReportType::MemoryTermination)
  );
  assert_eq!(
    "Fatal JavaScript Error",
    Monitor::report_type_to_reason(ReportType::JavaScriptFatalError)
  );
  assert_eq!(
    "Non-Fatal JavaScript Error",
    Monitor::report_type_to_reason(ReportType::JavaScriptNonFatalError)
  );
  assert_eq!(
    "Handled Error",
    Monitor::report_type_to_reason(ReportType::HandledError)
  );
}

#[tokio::test]
async fn file_watcher_enabled_when_watcher_directory_exists() {
  let temp = TempDir::new().unwrap();
  let report_directory = temp.path().join("reports");

  // Watcher should not be enabled when the watcher directory doesn't exist
  assert!(!Monitor::is_reports_watcher_enabled(&report_directory));

  // Create the watcher directory
  std::fs::create_dir_all(report_directory.join("watcher")).unwrap();

  // Watcher should now be enabled
  assert!(Monitor::is_reports_watcher_enabled(&report_directory));
}

#[tokio::test]
async fn file_watcher_detects_current_session_report() {
  let mut builder = FlatBufferBuilder::new();
  let name = "CurrentSessionCrash".to_fb(&mut builder);
  let reason = "null pointer".to_fb(&mut builder);
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
  let app_id = "com.example.app".to_fb(&mut builder);
  let version = "1.0".to_fb(&mut builder);
  let app_metrics = Some(AppMetrics::create(
    &mut builder,
    &AppMetricsArgs {
      app_id,
      version,
      ..Default::default()
    },
  ));
  let crash_timestamp = datetime!(2024-01-15 12:34:56 UTC);
  let unix_timestamp = crash_timestamp.unix_timestamp();
  let timestamp = Timestamp::new(unix_timestamp.try_into().unwrap(), 0);
  let device_metrics = Some(DeviceMetrics::create(
    &mut builder,
    &DeviceMetricsArgs {
      time: Some(&timestamp),
      ..Default::default()
    },
  ));
  let report = Report::create(
    &mut builder,
    &ReportArgs {
      type_: ReportType::NativeCrash,
      errors,
      app_metrics,
      device_metrics,
      ..Default::default()
    },
  );
  builder.finish(report, None);
  let data = builder.finished_data();

  let mut setup = Setup::new(None, true);

  let uuid = "12345678-1234-5678-1234-567812345678".parse().unwrap();
  setup.expect_artifact_upload(
    data,
    uuid,
    [
      ("app_id".into(), "com.example.app".into()),
      ("app_version".into(), "1.0".into()),
      ("os_version".into(), "unknown".into()),
    ]
    .into(),
    crash_timestamp.into(),
    setup.monitor.session.session_id().as_str(),
  );

  // Write a crash report to the current_session directory
  std::fs::write(setup.current_session_directory().join("crash1.cap"), data).unwrap();

  // Verify that the crash log was emitted
  let crash_log = tokio::time::timeout(
    tokio::time::Duration::from_secs(2),
    setup.emit_log_rx.recv(),
  )
  .await
  .expect("Timeout waiting for crash log")
  .expect("Channel closed without receiving crash log");

  assert_eq!(
    "CurrentSessionCrash",
    crash_log.fields["_app_exit_info"].value.as_str().unwrap()
  );
  assert_eq!(
    "null pointer",
    crash_log.fields["_app_exit_details"]
      .value
      .as_str()
      .unwrap()
  );
  assert_eq!(
    "Native Crash",
    crash_log.fields["_app_exit_reason"].value.as_str().unwrap()
  );

  // Verify file was removed after processing
  assert!(
    !setup
      .current_session_directory()
      .join("crash1.cap")
      .exists()
  );
}

#[tokio::test]
async fn file_watcher_detects_previous_session_report() {
  let mut builder = FlatBufferBuilder::new();
  let name = "PreviousSessionCrash".to_fb(&mut builder);
  let reason = "segmentation fault".to_fb(&mut builder);
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
  let report = Report::create(
    &mut builder,
    &ReportArgs {
      type_: ReportType::NativeCrash,
      errors,
      ..Default::default()
    },
  );
  builder.finish(report, None);
  let data = builder.finished_data();

  let mut setup = Setup::new(None, true);

  setup.expect_artifact_upload(
    data,
    "12345678-1234-5678-1234-567812345679".parse().unwrap(),
    [
      ("app_version".into(), "unknown".into()),
      ("os_version".into(), "unknown".into()),
    ]
    .into(),
    None,
    "previous_session_id",
  );

  // Write a crash report to the previous_session directory
  std::fs::write(setup.previous_session_directory().join("crash2.cap"), data).unwrap();

  // Verify that the crash log was emitted
  let crash_log = tokio::time::timeout(2.std_seconds(), setup.emit_log_rx.recv())
    .await
    .expect("Timeout waiting for crash log")
    .expect("Channel closed without receiving crash log");

  assert_eq!(
    "PreviousSessionCrash",
    crash_log.fields["_app_exit_info"].value.as_str().unwrap()
  );
  assert_eq!(
    "segmentation fault",
    crash_log.fields["_app_exit_details"]
      .value
      .as_str()
      .unwrap()
  );

  // Verify file was removed after processing
  assert!(
    !setup
      .previous_session_directory()
      .join("crash2.cap")
      .exists()
  );
}

#[tokio::test]
async fn file_watcher_ignores_non_cap_files() {
  let mut setup = Setup::new(None, true);

  // Write files with wrong extensions
  std::fs::write(
    setup.current_session_directory().join("report.txt"),
    b"not a crash",
  )
  .unwrap();
  std::fs::write(
    setup.current_session_directory().join("report.log"),
    b"also not a crash",
  )
  .unwrap();

  // Verify that no crash log was emitted
  let result = tokio::time::timeout(
    tokio::time::Duration::from_millis(100),
    setup.emit_log_rx.recv(),
  )
  .await;
  assert!(
    result.is_err(),
    "Should timeout as no reports should be processed"
  );

  // Files should still exist since they weren't processed
  assert!(
    setup
      .current_session_directory()
      .join("report.txt")
      .exists()
  );
  assert!(
    setup
      .current_session_directory()
      .join("report.log")
      .exists()
  );
}

#[tokio::test]
async fn file_watcher_not_created_without_watcher_directory() {
  let setup = Setup::new(None, false);

  // The file watcher would typically hold this channel, so it being closed indicates no file
  // watcher was created.
  assert!(setup.emit_log_rx.is_closed());
}

#[tokio::test]
async fn file_watcher_processes_multiple_reports() {
  let mut setup = Setup::new(None, true);

  // Setup mock to return different UUIDs for each upload
  let uuid1 = "11111111-1111-1111-1111-111111111111".parse().unwrap();
  let uuid2 = "22222222-2222-2222-2222-222222222222".parse().unwrap();
  let mut seq = mockall::Sequence::new();
  make_mut(&mut setup.upload_client)
    .expect_enqueue_upload()
    .times(1)
    .in_sequence(&mut seq)
    .returning(move |_, _, _, _, _| Ok(uuid1));
  make_mut(&mut setup.upload_client)
    .expect_enqueue_upload()
    .times(1)
    .in_sequence(&mut seq)
    .returning(move |_, _, _, _, _| Ok(uuid2));

  // Create first crash report
  let mut builder1 = FlatBufferBuilder::new();
  let name1 = "Crash1".to_fb(&mut builder1);
  let reason1 = "error1".to_fb(&mut builder1);
  let error1 = Error::create(
    &mut builder1,
    &ErrorArgs {
      name: name1,
      reason: reason1,
      stack_trace: None,
      relation_to_next: ErrorRelation::CausedBy,
    },
  );
  let errors1 = Some(builder1.create_vector::<WIPOffset<Error<'_>>>(&[error1]));
  let report1 = Report::create(
    &mut builder1,
    &ReportArgs {
      type_: ReportType::NativeCrash,
      errors: errors1,
      ..Default::default()
    },
  );
  builder1.finish(report1, None);
  let data1 = builder1.finished_data();

  // Create second crash report
  let mut builder2 = FlatBufferBuilder::new();
  let name2 = "Crash2".to_fb(&mut builder2);
  let reason2 = "error2".to_fb(&mut builder2);
  let error2 = Error::create(
    &mut builder2,
    &ErrorArgs {
      name: name2,
      reason: reason2,
      stack_trace: None,
      relation_to_next: ErrorRelation::CausedBy,
    },
  );
  let errors2 = Some(builder2.create_vector::<WIPOffset<Error<'_>>>(&[error2]));
  let report2 = Report::create(
    &mut builder2,
    &ReportArgs {
      type_: ReportType::NativeCrash,
      errors: errors2,
      ..Default::default()
    },
  );
  builder2.finish(report2, None);
  let data2 = builder2.finished_data();

  // Write both crash reports
  std::fs::write(setup.current_session_directory().join("crash1.cap"), data1).unwrap();
  tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
  std::fs::write(setup.current_session_directory().join("crash2.cap"), data2).unwrap();

  // Verify that both crash logs were emitted
  let crash_log1 = tokio::time::timeout(
    tokio::time::Duration::from_secs(2),
    setup.emit_log_rx.recv(),
  )
  .await
  .expect("Timeout waiting for first crash log")
  .expect("Channel closed without receiving first crash log");

  let crash_log2 = tokio::time::timeout(
    tokio::time::Duration::from_secs(2),
    setup.emit_log_rx.recv(),
  )
  .await
  .expect("Timeout waiting for second crash log")
  .expect("Channel closed without receiving second crash log");

  // Verify both crashes were processed (order may vary)
  let mut crash_names: Vec<String> = vec![
    crash_log1.fields["_app_exit_info"]
      .value
      .as_str()
      .unwrap()
      .to_string(),
    crash_log2.fields["_app_exit_info"]
      .value
      .as_str()
      .unwrap()
      .to_string(),
  ];
  crash_names.sort();

  assert_eq!(
    crash_names,
    vec!["Crash1".to_string(), "Crash2".to_string()]
  );

  // Verify files were removed after processing
  assert!(
    !setup
      .current_session_directory()
      .join("crash1.cap")
      .exists()
  );
  assert!(
    !setup
      .current_session_directory()
      .join("crash2.cap")
      .exists()
  );
}
