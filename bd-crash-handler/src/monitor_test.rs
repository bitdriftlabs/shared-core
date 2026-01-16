// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{Monitor, global_state};
use bd_artifact_upload::SnappedFeatureFlag;
use bd_client_common::init_lifecycle::InitLifecycleState;
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
use bd_resilient_kv::StateValue;
use bd_runtime::runtime::{self};
use bd_session::fixed::{self, UUIDCallbacks};
use bd_shutdown::ComponentShutdownTrigger;
use bd_state::test::TestStore;
use bd_test_helpers::make_mut;
use bd_test_helpers::session::in_memory_store;
use bd_time::TestTimeProvider;
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

//
// CrashReportBuilder
//

/// Helper for building crash reports with less boilerplate
struct CrashReportBuilder {
  name: String,
  reason: Option<String>,
  report_type: ReportType,
  app_id: Option<String>,
  app_version: Option<String>,
  timestamp: Option<OffsetDateTime>,
}

impl CrashReportBuilder {
  fn new(name: impl Into<String>) -> Self {
    Self {
      name: name.into(),
      reason: None,
      report_type: ReportType::NativeCrash,
      app_id: None,
      app_version: None,
      timestamp: None,
    }
  }

  fn reason(mut self, reason: impl Into<String>) -> Self {
    self.reason = Some(reason.into());
    self
  }

  #[allow(dead_code)]
  fn report_type(mut self, report_type: ReportType) -> Self {
    self.report_type = report_type;
    self
  }

  fn app_id(mut self, app_id: impl Into<String>) -> Self {
    self.app_id = Some(app_id.into());
    self
  }

  fn app_version(mut self, version: impl Into<String>) -> Self {
    self.app_version = Some(version.into());
    self
  }

  fn timestamp(mut self, timestamp: OffsetDateTime) -> Self {
    self.timestamp = Some(timestamp);
    self
  }

  fn build(self) -> Vec<u8> {
    use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::{
      AppMetricsT,
      DeviceMetricsT,
      ErrorT,
      ReportT,
      TimestampT,
    };

    let mut error = ErrorT::default();
    error.name = Some(self.name);
    error.reason = self.reason;
    error.relation_to_next = ErrorRelation::CausedBy;

    let app_metrics = if self.app_id.is_some() || self.app_version.is_some() {
      let mut metrics = AppMetricsT::default();
      metrics.app_id = self.app_id;
      metrics.version = self.app_version;
      Some(Box::new(metrics))
    } else {
      None
    };

    let device_metrics = self.timestamp.map(|ts| {
      let mut metrics = DeviceMetricsT::default();
      metrics.time = Some(TimestampT {
        seconds: ts.unix_timestamp().try_into().unwrap(),
        nanos: 0,
      });
      Box::new(metrics)
    });

    let mut report_t = ReportT::default();
    report_t.type_ = self.report_type;
    report_t.errors = Some(vec![error]);
    report_t.app_metrics = app_metrics;
    report_t.device_metrics = device_metrics;

    let mut builder = FlatBufferBuilder::new();
    let report = report_t.pack(&mut builder);
    builder.finish(report, None);
    builder.finished_data().to_vec()
  }
}

//
// Setup
//

struct Setup {
  directory: TempDir,
  monitor: Monitor,
  upload_client: Arc<bd_artifact_upload::MockClient>,
  emit_log_rx: tokio::sync::mpsc::Receiver<crate::CrashLog>,
  state: TestStore,
  _shutdown: ComponentShutdownTrigger,
}

impl Setup {
  /// Helper to construct a previous run state snapshot with feature flags.
  ///
  /// In production, this snapshot is created by `Store::persistent()` which captures state
  /// before clearing ephemeral scopes. This helper allows tests to simulate that snapshot.
  async fn make_previous_run_state(flags: Vec<(&str, &str)>) -> bd_resilient_kv::ScopedMaps {
    let mut store = bd_resilient_kv::VersionedKVStore::new_in_memory(
      Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00 UTC))),
      None,
      &bd_client_stats_store::Collector::default().scope("test"),
    );

    for (name, value) in flags {
      store
        .insert(
          bd_resilient_kv::Scope::FeatureFlagExposure,
          name.to_string(),
          StateValue {
            value_type: Some(bd_resilient_kv::Value_type::StringValue(value.to_string())),
            ..Default::default()
          },
        )
        .await
        .unwrap();
    }

    store.as_hashmap().clone()
  }

  async fn new(maybe_global_state: Option<LogFields>, enable_file_watcher: bool) -> Self {
    let directory = TempDir::new().unwrap();

    let shutdown = ComponentShutdownTrigger::default();
    let store = in_memory_store();
    let upload_client = Arc::new(bd_artifact_upload::MockClient::default());

    if let Some(global_state) = maybe_global_state {
      let mut tracker =
        global_state::Tracker::new(store.clone(), runtime::Watch::new_for_testing(10.seconds()));
      tracker.maybe_update_global_state(&global_state);
    }

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

    let state = TestStore::new().await;

    // The Monitor requires two separate state representations:
    //
    // 1. `previous_run_state`: A snapshot of feature flags from the previous process run. In
    //    production, Store::persistent() captures this snapshot before clearing ephemeral scopes.
    //    This is used for crashes from the previous session.
    //
    // 2. `state` (current): The live state store for the current process run. In production, this
    //    starts empty after ephemeral scopes are cleared, then gets populated as the application
    //    runs. This is used for crashes from the current session.
    //
    // We manually construct both to test that Monitor correctly routes previous session crashes
    // to use previous_run_state and current session crashes to use the live state.

    let previous_run_state = Self::make_previous_run_state(vec![
      ("initial_flag", "true"),
      ("previous_only_flag", "enabled"),
    ])
    .await;

    // Seed the current state with initial feature flags to represent the state at startup.
    // This simulates flags being set during the current session initialization.
    state
      .insert(
        bd_state::Scope::FeatureFlagExposure,
        "initial_flag".to_string(),
        bd_state::string_value("true"),
      )
      .await
      .unwrap();
    state
      .insert(
        bd_state::Scope::FeatureFlagExposure,
        "previous_only_flag".to_string(),
        bd_state::string_value("enabled"),
      )
      .await
      .unwrap();

    let monitor = Monitor::new(
      directory.path(),
      store,
      upload_client.clone(),
      Arc::new(session),
      &InitLifecycleState::new(),
      (*state).clone(),
      previous_run_state,
      emit_log,
    );

    Self {
      directory,
      monitor,
      upload_client,
      emit_log_rx: rx,
      state,
      _shutdown: shutdown,
    }
  }

  async fn update_feature_flag(&self, key: &str, value: &str) {
    self
      .state
      .insert(
        bd_state::Scope::FeatureFlagExposure,
        key.to_string(),
        bd_state::string_value(value),
      )
      .await
      .unwrap();
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

  #[allow(dead_code)]
  fn expect_artifact_upload(
    &mut self,
    content: &[u8],
    uuid: Uuid,
    state: LogFields,
    timestamp: Option<OffsetDateTime>,
    session_id: &str,
  ) {
    self.expect_artifact_upload_with_flags(content, uuid, state, timestamp, session_id, None);
  }

  fn expect_artifact_upload_with_flags(
    &mut self,
    content: &[u8],
    uuid: Uuid,
    state: LogFields,
    timestamp: Option<OffsetDateTime>,
    session_id: &str,
    expected_flags: Option<Vec<(String, String)>>,
  ) {
    let content = content.to_vec();
    let session_id = session_id.to_string();
    make_mut(&mut self.upload_client)
      .expect_enqueue_upload()
      .withf(
        move |mut file,
              fstate,
              ftimestamp,
              fsession_id,
              feature_flags: &Vec<SnappedFeatureFlag>,
              _type_id,
              _skip_intent| {
          let mut output = vec![];
          file.read_to_end(&mut output).unwrap();
          let content_match = output == content;
          let state_match = &state == fstate;
          let timestamp_match = &timestamp == ftimestamp;
          let session_match = session_id == *fsession_id;

          let flags_match = if let Some(ref expected) = expected_flags {
            if feature_flags.len() != expected.len() {
              return false;
            }
            expected.iter().all(|(name, variant)| {
              feature_flags
                .iter()
                .any(|f| f.name() == name && f.variant() == Some(variant.as_str()))
            })
          } else {
            // When expected_flags is None, we expect an empty feature_flags vec
            feature_flags.is_empty()
          };

          content_match && state_match && timestamp_match && session_match && flags_match
        },
      )
      .returning(move |_, _, _, _, _, _, _| Ok(uuid));
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
  )
  .await;
  setup.make_crash("report.cap", data);

  let uuid = "12345678-1234-5678-1234-5678123456aa".parse().unwrap();
  setup.expect_artifact_upload_with_flags(
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
    Some(vec![
      ("initial_flag".to_string(), "true".to_string()),
      ("previous_only_flag".to_string(), "enabled".to_string()),
    ]),
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
  let crash_timestamp = datetime!(2024-01-15 12:34:56 UTC);
  let data = CrashReportBuilder::new("CurrentSessionCrash")
    .reason("null pointer")
    .app_id("com.example.app")
    .app_version("1.0")
    .timestamp(crash_timestamp)
    .build();

  let mut setup = Setup::new(None, true).await;

  let uuid = "12345678-1234-5678-1234-567812345678".parse().unwrap();
  setup.expect_artifact_upload_with_flags(
    &data,
    uuid,
    [
      ("app_id".into(), "com.example.app".into()),
      ("app_version".into(), "1.0".into()),
      ("os_version".into(), "unknown".into()),
    ]
    .into(),
    crash_timestamp.into(),
    setup.monitor.session.session_id().as_str(),
    Some(vec![
      ("initial_flag".to_string(), "true".to_string()),
      ("previous_only_flag".to_string(), "enabled".to_string()),
    ]),
  );

  // Write a crash report to the current_session directory
  std::fs::write(setup.current_session_directory().join("crash1.cap"), &data).unwrap();

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
  let data = CrashReportBuilder::new("PreviousSessionCrash")
    .reason("segmentation fault")
    .build();

  let mut setup = Setup::new(None, true).await;

  setup.expect_artifact_upload_with_flags(
    &data,
    "12345678-1234-5678-1234-567812345679".parse().unwrap(),
    [
      ("app_version".into(), "unknown".into()),
      ("os_version".into(), "unknown".into()),
    ]
    .into(),
    None,
    "previous_session_id",
    Some(vec![
      ("initial_flag".to_string(), "true".to_string()),
      ("previous_only_flag".to_string(), "enabled".to_string()),
    ]),
  );

  // Write a crash report to the previous_session directory
  std::fs::write(setup.previous_session_directory().join("crash2.cap"), &data).unwrap();

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
  let mut setup = Setup::new(None, true).await;

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
  let setup = Setup::new(None, false).await;

  // The file watcher would typically hold this channel, so it being closed indicates no file
  // watcher was created.
  assert!(setup.emit_log_rx.is_closed());
}

#[tokio::test]
async fn file_watcher_processes_multiple_reports() {
  let mut setup = Setup::new(None, true).await;

  // Setup mock to return different UUIDs for each upload
  let uuid1 = "11111111-1111-1111-1111-111111111111".parse().unwrap();
  let uuid2 = "22222222-2222-2222-2222-222222222222".parse().unwrap();
  let mut seq = mockall::Sequence::new();
  make_mut(&mut setup.upload_client)
    .expect_enqueue_upload()
    .times(1)
    .in_sequence(&mut seq)
    .returning(move |_, _, _, _, _, _, _| Ok(uuid1));
  make_mut(&mut setup.upload_client)
    .expect_enqueue_upload()
    .times(1)
    .in_sequence(&mut seq)
    .returning(move |_, _, _, _, _, _, _| Ok(uuid2));

  // Create two crash reports
  let data1 = CrashReportBuilder::new("Crash1").reason("error1").build();
  let data2 = CrashReportBuilder::new("Crash2").reason("error2").build();

  // Write both crash reports
  std::fs::write(setup.current_session_directory().join("crash1.cap"), &data1).unwrap();
  tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
  std::fs::write(setup.current_session_directory().join("crash2.cap"), &data2).unwrap();

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

#[tokio::test]
async fn previous_session_crash_uses_previous_feature_flags() {
  let data = CrashReportBuilder::new("PreviousCrash").build();

  let mut setup = Setup::new(None, true).await;

  // Update feature flags after Monitor creation - these should NOT appear in previous session
  // crash
  setup.update_feature_flag("current_only_flag", "new").await;
  setup.update_feature_flag("initial_flag", "updated").await;

  // Expect upload with previous session feature flags only
  setup.expect_artifact_upload_with_flags(
    &data,
    "12345678-1234-5678-1234-567812345679".parse().unwrap(),
    [
      ("app_version".into(), "unknown".into()),
      ("os_version".into(), "unknown".into()),
    ]
    .into(),
    None,
    "previous_session_id",
    Some(vec![
      ("initial_flag".to_string(), "true".to_string()),
      ("previous_only_flag".to_string(), "enabled".to_string()),
    ]),
  );

  // Write a crash report to the previous_session directory
  std::fs::write(setup.previous_session_directory().join("crash.cap"), &data).unwrap();

  // Wait for the crash to be processed
  tokio::time::timeout(2.std_seconds(), setup.emit_log_rx.recv())
    .await
    .expect("Timeout waiting for crash log")
    .expect("Channel closed without receiving crash log");
}

#[tokio::test]
async fn current_session_crash_uses_current_feature_flags() {
  let crash_timestamp = datetime!(2024-01-15 12:34:56 UTC);
  let data = CrashReportBuilder::new("CurrentCrash")
    .timestamp(crash_timestamp)
    .build();

  let mut setup = Setup::new(None, true).await;

  // Update feature flags after Monitor creation - these SHOULD appear in current session crash
  setup.update_feature_flag("current_only_flag", "new").await;
  setup.update_feature_flag("initial_flag", "updated").await;

  // Expect upload with current session feature flags (updated values)
  // Note: We don't remove previous_only_flag in this test, so it will appear with its original
  // value
  setup.expect_artifact_upload_with_flags(
    &data,
    "12345678-1234-5678-1234-567812345678".parse().unwrap(),
    [
      ("app_version".into(), "unknown".into()),
      ("os_version".into(), "unknown".into()),
    ]
    .into(),
    crash_timestamp.into(),
    setup.monitor.session.session_id().as_str(),
    Some(vec![
      ("initial_flag".to_string(), "updated".to_string()),
      ("current_only_flag".to_string(), "new".to_string()),
      ("previous_only_flag".to_string(), "enabled".to_string()),
    ]),
  );

  // Write a crash report to the current_session directory
  std::fs::write(setup.current_session_directory().join("crash.cap"), &data).unwrap();

  // Wait for the crash to be processed
  tokio::time::timeout(2.std_seconds(), setup.emit_log_rx.recv())
    .await
    .expect("Timeout waiting for crash log")
    .expect("Channel closed without receiving crash log");
}
