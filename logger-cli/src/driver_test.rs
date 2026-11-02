// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/licenses/strict/1.0.0

#![allow(clippy::unwrap_used)]

use super::{Driver, stop_worker, wait_for_configuration};
use crate::issue_report::{IssueKind, IssueReport, StackFrame};
use crate::logger::{LoggerArgs, SessionStrategyConfig};
use crate::types::Platform;
use bd_logger::{AdmissionError, FlushError, SdkStatusTracker};
use bd_proto::protos::client::api::configuration_update::StateOfTheWorld;
use bd_proto::protos::config::v1::config::{BufferConfigList, buffer_config};
use bd_proto::protos::logging::payload::Log;
use bd_test_helpers::config_helper::{
  configuration_update,
  default_buffer_config,
  make_buffer_matcher_matching_everything,
};
use bd_test_helpers::runtime::{ValueKind, make_update};
use bd_test_helpers::test_api_server::{ServerHandle, StreamAction, start_server};
use protobuf::Message;
use std::path::Path;
use std::time::Duration;
use tempfile::TempDir;
use time::OffsetDateTime;

#[tokio::test(start_paused = true)]
async fn readiness_waits_for_config_and_rejects_disabled_sdk() {
  let tracker = SdkStatusTracker::new();
  let waiting = wait_for_configuration(Duration::from_secs(1), || tracker.get());
  tokio::pin!(waiting);
  // Poll through the first status check, registering the monitor's timer.
  assert!(futures::poll!(&mut waiting).is_pending());
  tracker.record_config_delivery(OffsetDateTime::UNIX_EPOCH);
  tokio::time::advance(Duration::from_millis(100)).await;
  assert!(futures::poll!(&mut waiting).is_pending());
  tracker.record_running();
  waiting.await.unwrap();
  tracker.record_disabled();
  assert_eq!(
    wait_for_configuration(Duration::from_secs(1), || tracker.get())
      .await
      .unwrap_err()
      .to_string(),
    "SDK disabled"
  );
}

#[tokio::test(start_paused = true)]
async fn readiness_times_out_without_config() {
  let tracker = SdkStatusTracker::new();
  let error = wait_for_configuration(Duration::from_secs(1), || tracker.get())
    .await
    .unwrap_err();
  assert!(error.is::<tokio::time::error::Elapsed>());
}

/// Starts a driver that uploads each log as soon as it is available and stops within the test
/// deadline.
async fn start_streaming_driver(server: &mut ServerHandle, directory: &Path) -> Driver {
  let driver = Driver::start(directory, &args(server.port)).await.unwrap();
  let stream = server.next_initialized_stream(false).await.unwrap();
  stream
    .stream_action(StreamAction::SendConfiguration(configuration_update(
      "driver-config",
      StateOfTheWorld {
        buffer_config_list: Some(BufferConfigList {
          buffer_config: vec![default_buffer_config(
            buffer_config::Type::CONTINUOUS,
            make_buffer_matcher_matching_everything().into(),
          )],
          ..Default::default()
        })
        .into(),
        ..Default::default()
      },
    )))
    .await;
  server.next_configuration_ack(&stream).await;
  driver.ready(Duration::from_secs(1)).await.unwrap();
  // Upload each log as soon as it is available, using the runtime acknowledgment as a gate.
  stream
    .stream_action(StreamAction::SendRuntime(make_update(
      vec![
        ("log_uploader.batch_size", ValueKind::Int(1)),
        // A stop's explicit flush starts this interval, so the stop-time stats upload is held
        // back and only the periodic upload that follows it can send the final snapshot.
        ("stats.minimum_upload_interval_ms", ValueKind::Int(5_000)),
        ("stats.upload_flush_interval_ms", ValueKind::Int(500)),
      ],
      "driver-runtime".into(),
    )))
    .await;
  server.next_runtime_ack(&stream).await;
  driver
}

/// Receives log uploads until a log with each wanted message has arrived.
async fn next_logs(server: &mut ServerHandle, wanted: &[&str]) -> Vec<Log> {
  let mut logs = Vec::new();
  while !wanted.iter().all(|message| {
    logs
      .iter()
      .any(|log: &Log| log.message.string_data() == *message)
  }) {
    let upload = server.next_log_upload().await.unwrap();
    logs.extend(
      upload
        .proto_logs
        .iter()
        .map(|bytes| Log::parse_from_bytes(bytes).unwrap()),
    );
  }
  logs
}

#[tokio::test]
async fn driver_preserves_sessions_timestamps_and_stops_successfully() {
  let mut server = start_server(false, None);
  let directory = TempDir::new().unwrap();
  let driver = start_streaming_driver(&mut server, directory.path()).await;
  let at = OffsetDateTime::UNIX_EPOCH + time::Duration::days(20_000);
  let first = driver.begin_session("first").unwrap();
  assert_eq!(driver.holder.current_entity_id().as_deref(), Some("first"));
  driver.log_at(
    at,
    "first event",
    [("cause".into(), "timeout".into())].into(),
  );
  let second = driver.begin_session("second").unwrap();
  assert_ne!(first, second);
  assert_eq!(driver.holder.current_entity_id().as_deref(), Some("second"));
  driver.log_at(at + time::Duration::seconds(1), "second event", [].into());
  driver.flush().await.unwrap();
  let logs = next_logs(&mut server, &["first event", "second event"]).await;
  let first_log = logs
    .iter()
    .find(|log| log.message.string_data() == "first event")
    .unwrap();
  let second_log = logs
    .iter()
    .find(|log| log.message.string_data() == "second event")
    .unwrap();
  assert_eq!(first_log.session_id, first);
  assert_eq!(second_log.session_id, second);
  assert_eq!(
    first_log.timestamp_unix_micro,
    u64::try_from(at.unix_timestamp_nanos() / 1000).unwrap()
  );
  assert_eq!(
    second_log.timestamp_unix_micro,
    first_log.timestamp_unix_micro + 1_000_000
  );
  assert_eq!(field(first_log, "cause"), "timeout");
  assert_eq!(field(first_log, "app_id"), "driver-test");
  assert_eq!(field(first_log, "app_version"), "1.2.3");
  // Exercise healthy stop directly, without pre-shutting down the SDK.
  let worker = driver.worker.abort_handle();
  driver.stop().await.unwrap();
  assert!(worker.is_finished());
  // Shutting down before the stop-time stats upload is acknowledged would leave its snapshot
  // behind.
  assert_eq!(stats_snapshot_files(directory.path()), Vec::<String>::new());
}

#[tokio::test]
async fn driver_reports_issues_in_the_current_session() {
  let mut server = start_server(false, None);
  let directory = TempDir::new().unwrap();
  let driver = start_streaming_driver(&mut server, directory.path()).await;
  driver
    .set_feature_flag("payment_sdk_v2", Some("enabled"))
    .await
    .unwrap();
  let session = driver.begin_session("crasher").unwrap();
  let report = handled_error("CheckoutError");
  driver.report_issue(&report).await.unwrap();
  driver.flush().await.unwrap();

  let upload = server.next_artifact_upload().await.unwrap();
  assert_eq!(upload.session_id, session);
  assert_eq!(upload.contents, report.to_bytes().unwrap());
  assert!(
    upload
      .feature_flags
      .iter()
      .any(|flag| flag.name == "payment_sdk_v2" && flag.variant.as_deref() == Some("enabled"))
  );
  let logs = next_logs(&mut server, &["AppExit"]).await;
  let exit = logs
    .iter()
    .find(|log| log.message.string_data() == "AppExit")
    .unwrap();
  assert_eq!(exit.session_id, session);
  assert_eq!(field(exit, "_app_exit_reason"), "Handled Error");
  assert_eq!(field(exit, "_app_exit_info"), "CheckoutError");
  assert_eq!(field(exit, "_crash_artifact_id"), upload.artifact_id);

  driver.stop().await.unwrap();
  let pending: Vec<_> = std::fs::read_dir(directory.path().join("report_uploads"))
    .unwrap()
    .map(|entry| entry.unwrap().file_name().into_string().unwrap())
    .filter(|name| name != "report_index.pb")
    .collect();
  assert_eq!(pending, Vec::<String>::new());
}

#[tokio::test]
async fn driver_takes_every_concurrent_issue_report() {
  let mut server = start_server(false, None);
  let directory = TempDir::new().unwrap();
  let driver = start_streaming_driver(&mut server, directory.path()).await;
  driver.begin_session("crasher").unwrap();
  // The SDK's report watcher drops notifications that arrive while one is pending, so reports
  // handed over together would otherwise time out unprocessed.
  let reports = ["FirstError", "SecondError", "ThirdError"].map(handled_error);
  let (first, second, third) = tokio::join!(
    driver.report_issue(&reports[0]),
    driver.report_issue(&reports[1]),
    driver.report_issue(&reports[2]),
  );
  first.unwrap();
  second.unwrap();
  third.unwrap();

  let mut uploaded = Vec::new();
  for _ in &reports {
    uploaded.push(server.next_artifact_upload().await.unwrap().contents);
  }
  let mut expected: Vec<_> = reports
    .iter()
    .map(|report| report.to_bytes().unwrap())
    .collect();
  uploaded.sort();
  expected.sort();
  assert_eq!(uploaded, expected);
  driver.stop().await.unwrap();
}

fn handled_error(name: &str) -> IssueReport {
  IssueReport {
    kind: IssueKind::HandledError,
    platform: Platform::Apple,
    app_id: "driver-test".into(),
    app_version: "1.2.3".into(),
    app_build_id: "123".into(),
    name: name.into(),
    reason: "payment declined".into(),
    frames: vec![StackFrame {
      class_name: "Checkout".into(),
      symbol_name: "confirm".into(),
      file: "Checkout.swift".into(),
      line: 12,
    }],
    feature_flags: Vec::new(),
    timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::days(20_000),
  }
}

fn stats_snapshot_files(directory: &Path) -> Vec<String> {
  std::fs::read_dir(directory.join("stats_uploads"))
    .unwrap()
    .map(|entry| entry.unwrap().file_name().into_string().unwrap())
    .filter(|name| !name.starts_with("pending_aggregation_index"))
    .collect()
}

#[tokio::test]
async fn stopped_driver_reports_flush_error_and_joins_worker() {
  let server = start_server(false, None);
  let directory = TempDir::new().unwrap();
  let driver = Driver::start(directory.path(), &args(server.port))
    .await
    .unwrap();
  let logger = driver.holder.logger.clone();
  tokio::task::spawn_blocking(move || logger.lock().shutdown(true))
    .await
    .unwrap();
  assert!(matches!(
    driver
      .flush()
      .await
      .unwrap_err()
      .downcast_ref::<FlushError>(),
    Some(FlushError::Admission(AdmissionError::Closed))
  ));
  let worker = driver.worker.abort_handle();
  let error = driver.stop().await.unwrap_err();
  assert!(matches!(
    error.downcast_ref::<FlushError>(),
    Some(FlushError::Admission(AdmissionError::Closed))
  ));
  assert!(worker.is_finished());
}

fn field<'a>(log: &'a Log, key: &str) -> &'a str {
  log
    .fields
    .iter()
    .find(|field| field.key == key)
    .unwrap()
    .value
    .string_data()
}

fn args(port: u16) -> LoggerArgs {
  LoggerArgs {
    api_url: format!("http://127.0.0.1:{port}"),
    api_key: "test-key".into(),
    app_id: "driver-test".into(),
    platform: Platform::Apple,
    app_version: "1.2.3".into(),
    app_version_code: "123".into(),
    model: "test-device".into(),
    entity_id: None,
    session_strategy: SessionStrategyConfig::Fixed,
  }
}

#[tokio::test(start_paused = true)]
async fn shutdown_deadline_aborts_and_joins_worker() {
  let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
  let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel::<()>();
  let worker = tokio::spawn(async move {
    let _dropped = dropped_tx;
    entered_tx.send(()).unwrap();
    std::future::pending::<()>().await;
    Ok(())
  });
  entered_rx.await.unwrap();
  let handle = worker.abort_handle();
  let error = stop_worker(worker, Duration::from_secs(1))
    .await
    .unwrap_err();
  assert_eq!(error.to_string(), "logger worker shutdown timed out");
  assert!(handle.is_finished());
  assert!(dropped_rx.await.is_err());
}
