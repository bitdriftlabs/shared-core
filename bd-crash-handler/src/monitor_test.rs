// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{Monitor, global_state};
use bd_client_common::init_lifecycle::InitLifecycleState;
use bd_device::Store;
use bd_feature_flags::FeatureFlags;
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
use bd_shutdown::ComponentShutdownTrigger;
use bd_test_helpers::make_mut;
use bd_test_helpers::session::InMemoryStorage;
use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, WIPOffset};
use itertools::Itertools;
use std::io::Read;
use std::sync::Arc;
use tempfile::TempDir;
use time::OffsetDateTime;
use time::ext::NumericalDuration;
use uuid::Uuid;

struct Setup {
  directory: TempDir,
  monitor: Monitor,
  upload_client: Arc<bd_artifact_upload::MockClient>,
  _shutdown: ComponentShutdownTrigger,
}

impl Setup {
  fn new(maybe_global_state: Option<LogFields>) -> Self {
    let directory = TempDir::new().unwrap();
    std::fs::create_dir_all(directory.path().join("runtime")).unwrap();

    let shutdown = ComponentShutdownTrigger::default();
    let store = Arc::new(Store::new(Box::<InMemoryStorage>::default()));
    let upload_client = Arc::new(bd_artifact_upload::MockClient::default());

    if let Some(global_state) = maybe_global_state {
      let mut tracker =
        global_state::Tracker::new(store.clone(), runtime::Watch::new_for_testing(10.seconds()));
      tracker.maybe_update_global_state(&global_state);
    }

    let feature_flags = FeatureFlags::new(
      directory.path().join("feature_flags"),
      1024,
      None,
    ).unwrap();

    let monitor = Monitor::new(
      directory.path(),
      store,
      upload_client.clone(),
      Some("previous_session_id".to_string()),
      &InitLifecycleState::new(),
      feature_flags,
    );

    Self {
      directory,
      monitor,
      upload_client,
      _shutdown: shutdown,
    }
  }

  fn make_crash(&self, name: &str, data: &[u8]) {
    let crash_directory = self.directory.path().join("reports/new");
    std::fs::create_dir_all(&crash_directory).unwrap();
    std::fs::write(crash_directory.join(name), data).unwrap();
  }

  async fn process_new_reports(&self) -> Vec<AnnotatedLogFields> {
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
  ) {
    let content = content.to_vec();
    make_mut(&mut self.upload_client)
      .expect_enqueue_upload()
      .withf(move |mut file, fstate, ftimestamp, session_id, _| {
        let mut output = vec![];
        file.read_to_end(&mut output).unwrap();
        output == content
          && &state == fstate
          && &timestamp == ftimestamp
          && session_id == "previous_session_id"
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
      type_: ReportType::NativeCrash,
      errors,
      app_metrics,
      device_metrics,
      ..Default::default()
    },
  );
  builder.finish(report, None);
  let data = builder.finished_data();

  let mut setup = Setup::new(Some(
    [
      ("os_version".into(), "6".into()),
      ("app_version".into(), "4.16".into()),
      ("app_id".into(), "com.example.foo".into()),
      ("other_stuff".into(), "foo".into()),
    ]
    .into(),
  ));
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
