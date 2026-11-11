// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::*;
use crate::crash_profile::PidObservation;
use crate::test::heap_profile;
use crate::uploader::ARTIFACT_UPLOAD_DIRECTORY;
use crate::{EnqueueError, MockClient, Uploader};
use bd_client_common::file::read_compressed;
use bd_client_common::file_system::{FileSystem, RealFileSystem};
use bd_client_stats_store::Collector;
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::{
  DeviceMetrics,
  DeviceMetricsArgs,
  ReportArgs,
  Timestamp,
  root_as_report,
};
use bd_runtime::runtime::{FeatureFlag as _, attachment};
use bd_runtime::test::TestConfigLoader;
use bd_shutdown::ComponentShutdownTrigger;
use bd_test_helpers::runtime::{ValueKind, make_update};
use bd_time::TestTimeProvider;
use flatbuffers::FlatBufferBuilder;
use std::path::Path;
use tempfile::TempDir;
use tokio::sync::mpsc::error::TryRecvError;

// Trace and report clocks share a boot epoch of 1,000 s REALTIME at BOOTTIME 0.
const BOOT_EPOCH_NS: i128 = 1_000_000_000_000;
const EVENT_TIME_NS: i128 = BOOT_EPOCH_NS + 20;
const SAMPLE_NS: u64 = 25;

// Facts for a process started at BOOTTIME 0, sampled at BOOTTIME 10.
fn source(pid: u32) -> Vec<u8> {
  encode_source(pid, 1, 0, i64::try_from(BOOT_EPOCH_NS + 10).unwrap(), 10)
}

// A report of `report_type` at EVENT_TIME_NS, optionally carrying source facts.
fn report(report_type: ReportType, source: Option<&[u8]>, timed: bool) -> Vec<u8> {
  report_at(report_type, source, timed, EVENT_TIME_NS)
}

fn report_at(
  report_type: ReportType,
  source: Option<&[u8]>,
  timed: bool,
  event_time_ns: i128,
) -> Vec<u8> {
  let mut builder = FlatBufferBuilder::new();
  let time = Timestamp::new(
    u64::try_from(event_time_ns / 1_000_000_000).unwrap(),
    u32::try_from(event_time_ns % 1_000_000_000).unwrap(),
  );
  let device_metrics = DeviceMetrics::create(
    &mut builder,
    &DeviceMetricsArgs {
      time: timed.then_some(&time),
      ..Default::default()
    },
  );
  let crash_profile_source = source.map(|source| builder.create_vector(source));
  let report = Report::create(
    &mut builder,
    &ReportArgs {
      type_: report_type,
      device_metrics: Some(device_metrics),
      crash_profile_source,
      ..Default::default()
    },
  );
  builder.finish(report, None);
  builder.finished_data().to_vec()
}

fn report_type(kind: &str) -> ReportType {
  match kind {
    "anr" => ReportType::AppNotResponding,
    _ => ReportType::JVMCrash,
  }
}

fn add_report(
  attribution: &CrashProfileAttribution,
  report_id: Uuid,
  report: &[u8],
  session_id: &str,
) {
  attribution.add_report(report_id, &root_as_report(report).unwrap(), session_id);
}

fn evidence(pids: impl IntoIterator<Item = u32>) -> Evidence {
  Evidence {
    boot_epoch_ns: BOOT_EPOCH_NS,
    observations: pids
      .into_iter()
      .map(|pid| PidObservation {
        pid,
        first_ns: SAMPLE_NS,
        last_ns: SAMPLE_NS,
        process_start_ns: None,
      })
      .collect(),
  }
}

// A minimal raw Trace: one heap graph for `pid` at BOOTTIME SAMPLE_NS plus a REALTIME/BOOTTIME
// snapshot. See the reader tests for field-by-field coverage.
fn write_profile(directory: &Path, pid: u32) -> PathBuf {
  let path = directory.join(format!("{}.perfetto-trace", Uuid::new_v4()));
  std::fs::write(
    &path,
    heap_profile(pid, u64::try_from(BOOT_EPOCH_NS).unwrap(), SAMPLE_NS),
  )
  .unwrap();
  path
}

fn attribution() -> CrashProfileAttribution {
  CrashProfileAttribution::new(Uuid::new_v4())
}

fn add(attribution: &CrashProfileAttribution, kind: &str, pid: u32) -> Uuid {
  let id = Uuid::new_v4();
  add_report(
    attribution,
    id,
    &report(report_type(kind), Some(&source(pid)), true),
    &format!("session-{pid}"),
  );
  id
}

fn candidate_count(attribution: &CrashProfileAttribution) -> usize {
  attribution.candidates.reports.lock().len()
}

#[test]
fn submitted_profile_paths_are_absolute() {
  let expected = std::env::current_dir()
    .unwrap()
    .join("profile.perfetto-trace");
  for path in [PathBuf::from("profile.perfetto-trace"), expected.clone()] {
    let attribution = attribution();
    attribution.submit(path, Kind::Anr, true);
    let request = attribution
      .requests_rx
      .lock()
      .as_mut()
      .unwrap()
      .try_recv()
      .unwrap();
    assert_eq!(request.path, expected);
    assert_eq!(request.kind, Kind::Anr);
    assert!(request.retain_unmatched);
  }
}

#[test]
fn empty_profile_paths_are_rejected_before_queueing() {
  let attribution = attribution();
  attribution.submit(PathBuf::new(), Kind::Anr, false);
  assert!(matches!(
    attribution.requests_rx.lock().as_mut().unwrap().try_recv(),
    Err(TryRecvError::Empty)
  ));
}

#[test]
fn typed_report_sources_become_validated_candidates() {
  let installation_id = Uuid::new_v4();
  let report_id = Uuid::new_v4();
  let candidate =
    |bytes: &[u8]| report_candidate(installation_id, report_id, &root_as_report(bytes).unwrap());

  let oom = candidate(&report(ReportType::JVMCrash, Some(&source(7)), true))
    .unwrap()
    .unwrap();
  assert_eq!(oom.report_id, report_id);
  assert_eq!(oom.kind, Kind::JavaOutOfMemory);
  assert_eq!(oom.run.pid, 7);
  assert_eq!(oom.run.installation_id, installation_id);
  assert_eq!(oom.event_elapsed_ns, 20);
  assert_eq!(oom.run.end_elapsed_ns, None);
  let anr = candidate(&report(
    ReportType::AppNotResponding,
    Some(&source(7)),
    true,
  ))
  .unwrap()
  .unwrap();
  assert_eq!(anr.kind, Kind::Anr);

  // Reports without source facts are simply ineligible.
  assert!(
    candidate(&report(ReportType::AppNotResponding, None, true))
      .unwrap()
      .is_none()
  );
  let late_start = encode_source(7, 1, 30, i64::try_from(BOOT_EPOCH_NS + 40).unwrap(), 40);
  for invalid in [
    report(ReportType::NativeCrash, Some(&source(7)), true),
    report(ReportType::AppNotResponding, Some(b"\xff\xff"), true),
    report(ReportType::AppNotResponding, Some(&source(0)), true),
    report(ReportType::AppNotResponding, Some(&late_start), true),
    report(ReportType::AppNotResponding, Some(&source(7)), false),
  ] {
    assert!(candidate(&invalid).is_err());
  }
}

#[test]
fn recovered_anr_death_bounds_reject_later_pid_reuse_but_preserve_predeath_context() {
  let attribution = attribution();
  // Both Android wall samples are millisecond-truncated. The trace's higher-precision epoch
  // differs by less than a millisecond; process-start metadata is deliberately absent.
  let source = encode_source(
    7,
    1,
    0,
    i64::try_from(BOOT_EPOCH_NS + 10_000_000_000).unwrap(),
    10_000_900_000,
  );
  let id = Uuid::new_v4();
  add_report(
    &attribution,
    id,
    &report_at(
      ReportType::AppNotResponding,
      Some(&source),
      true,
      BOOT_EPOCH_NS + 20_000_000_000,
    ),
    "source-session",
  );
  for (sample, matched) in [
    (5_000_000_000, true),  // Context, not proximity to the fatal event.
    (20_000_999_999, true), // Millisecond truncation must not cut off the last sample.
    (21_001_900_000, true), // Exact paired-clock and millisecond-rounding allowance.
    (21_001_900_001, false),
    (600_000_000_000, false), // Same PID long after known death, without a second candidate.
  ] {
    let evidence = Evidence {
      boot_epoch_ns: BOOT_EPOCH_NS,
      observations: vec![PidObservation {
        pid: 7,
        first_ns: sample,
        last_ns: sample,
        process_start_ns: None,
      }],
    };
    let upload = attribution
      .candidates
      .attribute(Kind::Anr, &evidence)
      .unwrap();
    assert_eq!(upload.is_some(), matched, "sample {sample}");
    if let Some(upload) = upload {
      assert_eq!(upload.report_ids, vec![id]);
    }
  }
}

#[test]
fn restored_fresh_and_late_reports_form_one_deduplicated_set() {
  let attribution = attribution();
  // A restored entry and its fresh dispatch use the same artifact ID; both sources merge.
  let restored = add(&attribution, "anr", 7);
  add_report(
    &attribution,
    restored,
    &report(ReportType::AppNotResponding, Some(&source(7)), true),
    "session-7",
  );
  let fresh = add(&attribution, "anr", 8);
  add_report(
    &attribution,
    Uuid::new_v4(),
    &report(ReportType::AppNotResponding, Some(b"malformed"), true),
    "ignored",
  );
  assert_eq!(candidate_count(&attribution), 2);

  attribution.mark_ready();
  // Readiness does not freeze membership.
  let late = add(&attribution, "anr", 9);
  let upload = attribution
    .candidates
    .attribute(Kind::Anr, &evidence([7, 8, 9]))
    .unwrap()
    .unwrap();
  let mut expected = vec![restored, fresh, late];
  expected.sort_unstable();
  assert_eq!(upload.report_ids, expected);
  assert_eq!(
    upload.captured_at,
    OffsetDateTime::from_unix_timestamp_nanos(BOOT_EPOCH_NS + i128::from(SAMPLE_NS)).unwrap()
  );
  let first = attribution.candidates.reports.lock()[&expected[0]]
    .session_id
    .clone();
  assert_eq!(upload.session_id, first);

  // Kind and PID scope select reports; nothing is matched to the nearest report.
  assert!(
    attribution
      .candidates
      .attribute(Kind::JavaOutOfMemory, &evidence([7]))
      .unwrap()
      .is_none()
  );
  assert!(
    attribution
      .candidates
      .attribute(Kind::Anr, &evidence([10]))
      .unwrap()
      .is_none()
  );
}

#[test]
fn report_id_limit_is_enforced_without_truncation() {
  let attribution = attribution();
  for pid in 1 ..= 17 {
    add(&attribution, "anr", pid);
  }
  let limit = u32::try_from(CRASH_PROFILE_MAX_REPORT_IDS).unwrap();
  let upload = attribution
    .candidates
    .attribute(Kind::Anr, &evidence(1 ..= limit))
    .unwrap()
    .unwrap();
  assert_eq!(upload.report_ids.len(), CRASH_PROFILE_MAX_REPORT_IDS);
  assert!(
    attribution
      .candidates
      .attribute(Kind::Anr, &evidence(1 ..= limit + 1))
      .is_err()
  );
}

//
// Worker
//

#[tokio::test]
async fn relative_sources_persist_the_matched_bytes_and_are_retired_on_rejection() {
  std::fs::create_dir_all(".tmp").unwrap();
  for accepted in [true, false] {
    let directory = TempDir::with_prefix_in("relative-profile", ".tmp").unwrap();
    let relative_directory = Path::new(".tmp").join(directory.path().file_name().unwrap());
    let source = write_profile(&relative_directory, 7);
    assert!(source.is_relative());
    let contents = std::fs::read(&source).unwrap();
    let sdk_directory = directory.path().canonicalize().unwrap().join("sdk");
    let decoy = sdk_directory.join(&source);
    std::fs::create_dir_all(decoy.parent().unwrap()).unwrap();
    std::fs::write(&decoy, b"not the matched profile").unwrap();
    let filesystem = Arc::new(RealFileSystem::new(sdk_directory.clone()));
    let runtime = TestConfigLoader::new().await;
    let limit = u32::try_from(contents.len() - usize::from(!accepted)).unwrap();
    runtime
      .update_snapshot(make_update(
        vec![
          (AnrEnabled::path(), ValueKind::Bool(true)),
          (attachment::MaxBytes::path(), ValueKind::Int(limit)),
        ],
        "relative profile".to_string(),
      ))
      .await
      .unwrap();
    let shutdown = ComponentShutdownTrigger::default();
    let (upload_tx, _uploads) = mpsc::channel(16);
    let (uploader, client) = Uploader::new(
      filesystem.clone(),
      upload_tx,
      Arc::new(TestTimeProvider::new(OffsetDateTime::UNIX_EPOCH)),
      &runtime,
      &Collector::default(),
      [].into(),
      tokio::sync::watch::channel(Arc::default()).1,
      shutdown.make_shutdown(),
    );
    let uploader_task = tokio::spawn(uploader.run());
    let attribution = attribution();
    let report_id = add(&attribution, "anr", 7);
    let mut progress = attribution.processing_revision_for_test();
    attribution.start(Arc::new(client), &runtime, shutdown.make_shutdown());
    attribution.mark_ready();
    attribution.submit(source.clone(), Kind::Anr, false);
    progress.wait_for(|revision| *revision >= 2).await.unwrap();

    assert!(!source.exists());
    assert_eq!(std::fs::read(&decoy).unwrap(), b"not the matched profile");
    let pending = Uploader::pending_artifacts(&sdk_directory).await.unwrap();
    assert_eq!(pending.len(), usize::from(accepted));
    if accepted {
      let entry = &pending[0];
      assert_eq!(
        entry.crash_profile.as_ref().unwrap().report_ids,
        vec![report_id.to_string()]
      );
      let payload = filesystem
        .read_file(&ARTIFACT_UPLOAD_DIRECTORY.join(&entry.name))
        .await
        .unwrap();
      assert_eq!(read_compressed(&payload).unwrap(), contents);
    }
    shutdown.shutdown().await;
    uploader_task.await.unwrap();
  }
}

type Enqueued = (Vec<String>, String, String, OffsetDateTime);

struct Worker {
  attribution: CrashProfileAttribution,
  enqueued_rx: mpsc::UnboundedReceiver<Enqueued>,
  directory: TempDir,
  _runtime: TestConfigLoader,
  shutdown: ComponentShutdownTrigger,
}

impl Worker {
  async fn new(anr_enabled: bool) -> Self {
    let runtime = TestConfigLoader::new().await;
    runtime
      .update_snapshot(make_update(
        vec![(AnrEnabled::path(), ValueKind::Bool(anr_enabled))],
        "1".to_string(),
      ))
      .await
      .unwrap();
    let (enqueued_tx, enqueued_rx) = mpsc::unbounded_channel();
    let mut client = MockClient::new();
    client.expect_enqueue_crash_profile().returning(
      move |source, timestamp, session_id, content_type, links, persisted_tx| {
        let UploadSource::Path(path) = source else {
          return Err(EnqueueError::Closed);
        };
        // The uploader owns and retires the source once it accepts it.
        std::fs::remove_file(path).unwrap();
        enqueued_tx
          .send((links.report_ids, session_id, content_type, timestamp))
          .unwrap();
        persisted_tx.unwrap().send(Ok(())).unwrap();
        Ok(Uuid::new_v4())
      },
    );
    let shutdown = ComponentShutdownTrigger::default();
    let attribution = attribution();
    let mut progress = attribution.processing_revision_for_test();
    attribution.start(Arc::new(client), &runtime, shutdown.make_shutdown());
    progress.wait_for(|revision| *revision == 1).await.unwrap();
    std::fs::create_dir_all(".tmp").unwrap();
    Self {
      attribution,
      enqueued_rx,
      directory: TempDir::with_prefix_in("attribution", ".tmp").unwrap(),
      _runtime: runtime,
      shutdown,
    }
  }

  fn submit(&self, pid: u32) -> PathBuf {
    self.submit_with(pid, false)
  }

  fn submit_with(&self, pid: u32, retain_unmatched: bool) -> PathBuf {
    let path = write_profile(self.directory.path(), pid);
    self
      .attribution
      .submit(path.clone(), Kind::Anr, retain_unmatched);
    path
  }
}

#[tokio::test]
async fn submissions_wait_for_initial_report_recovery_then_upload_like_attachments() {
  let mut worker = Worker::new(true).await;
  // Callback first: it waits for the startup pass rather than seeing an empty set.
  let first = worker.submit(7);
  assert!(first.exists());
  let report = add(&worker.attribution, "anr", 7);
  worker.attribution.mark_ready();

  let (links, session_id, content_type, captured_at) = worker.enqueued_rx.recv().await.unwrap();
  assert_eq!(links, vec![report.to_string()]);
  assert_eq!(session_id, "session-7");
  assert_eq!(content_type, PERFETTO_CONTENT_TYPE);
  assert_eq!(
    captured_at,
    OffsetDateTime::from_unix_timestamp_nanos(BOOT_EPOCH_NS + i128::from(SAMPLE_NS)).unwrap()
  );

  // The parent stays eligible for another profile after the first is queued (and ACKed).
  worker.submit(7);
  assert_eq!(
    worker.enqueued_rx.recv().await.unwrap().0,
    vec![report.to_string()]
  );
  worker.shutdown.shutdown().await;
}

#[tokio::test]
async fn unmatched_profiles_are_deleted_and_not_reconsidered_for_later_reports() {
  let mut worker = Worker::new(true).await;
  worker.attribution.mark_ready();
  let unmatched = worker.submit(8);
  let corrupt = worker.directory.path().join("corrupt.perfetto-trace");
  std::fs::write(&corrupt, b"not a trace").unwrap();
  worker.attribution.submit(corrupt.clone(), Kind::Anr, true);
  // A matching submission after them proves that both were handled in order.
  let report = add(&worker.attribution, "anr", 7);
  worker.submit(7);
  assert_eq!(
    worker.enqueued_rx.recv().await.unwrap().0,
    vec![report.to_string()]
  );
  assert!(!unmatched.exists());
  assert!(!corrupt.exists());

  // A late fresh report for PID 8 helps later profiles only.
  let late = add(&worker.attribution, "anr", 8);
  worker.submit(8);
  assert_eq!(
    worker.enqueued_rx.recv().await.unwrap().0,
    vec![late.to_string()]
  );
  worker.shutdown.shutdown().await;
}

#[tokio::test]
async fn failed_profiles_have_debug_only_retention_bounded_to_one_file_per_kind() {
  std::fs::create_dir_all(".tmp").unwrap();
  let directory = TempDir::with_prefix_in("failed-profile", ".tmp").unwrap();
  let attribution = attribution();
  let client = MockClient::new();
  let debug_enabled = cfg!(debug_assertions)
    && log::log_enabled!(target: "bd_artifact_upload::crash_profile_attribution", log::Level::Debug);
  for kind in [Kind::Anr, Kind::JavaOutOfMemory] {
    for iteration in 0 .. 3 {
      let path = directory.path().join(format!("{iteration}.perfetto-trace"));
      let contents = format!("not a trace: {kind:?} {iteration}");
      std::fs::write(&path, &contents).unwrap();
      adopt(&attribution.candidates, &client, kind, path.clone(), true).await;
      assert!(!path.exists());
      let failed = directory.path().join(match kind {
        Kind::Anr => "bitdrift-attribution-failed-anr.perfetto-trace",
        Kind::JavaOutOfMemory => "bitdrift-attribution-failed-oom.perfetto-trace",
      });
      assert_eq!(failed.exists(), debug_enabled);
      if debug_enabled {
        assert_eq!(std::fs::read(&failed).unwrap(), contents.as_bytes());
      }
    }
  }
  assert_eq!(
    std::fs::read_dir(directory.path()).unwrap().count(),
    if debug_enabled { 2 } else { 0 }
  );
}

#[tokio::test]
#[cfg(debug_assertions)]
async fn failed_debug_retention_falls_back_to_deleting_the_source() {
  std::fs::create_dir_all(".tmp").unwrap();
  let directory = TempDir::with_prefix_in("failed-profile", ".tmp").unwrap();
  let path = directory.path().join("corrupt.perfetto-trace");
  std::fs::write(&path, b"not a trace").unwrap();
  let failed = failed_path(&path, Kind::Anr);
  std::fs::create_dir(&failed).unwrap();
  adopt(
    &attribution().candidates,
    &MockClient::new(),
    Kind::Anr,
    path.clone(),
    true,
  )
  .await;
  assert!(!path.exists());
  assert!(failed.is_dir());
}

#[tokio::test]
async fn disabled_kind_leaves_profiles_untouched() {
  let worker = Worker::new(false).await;
  let mut progress = worker.attribution.processing_revision_for_test();
  add(&worker.attribution, "anr", 7);
  worker.attribution.mark_ready();
  let path = worker.submit(7);
  progress.wait_for(|revision| *revision >= 2).await.unwrap();
  assert!(path.exists());
  worker.shutdown.shutdown().await;
}

#[tokio::test]
async fn unmatched_live_results_are_kept_for_one_next_process_resubmission() {
  let mut worker = Worker::new(true).await;
  worker.attribution.mark_ready();
  // A fatal ANR's profile reaches the dying process before its report exists.
  let original = worker.submit_with(7, true);
  let report = add(&worker.attribution, "anr", 8);
  worker.submit(8);
  assert_eq!(
    worker.enqueued_rx.recv().await.unwrap().0,
    vec![report.to_string()]
  );
  // Kept in place under the SDK's retained name, which carries its kind for rediscovery.
  assert!(!original.exists());
  let live = retained_path(&original, Kind::Anr);
  assert!(live.to_string_lossy().ends_with(".bitdrift-retained-anr"));
  assert!(live.exists());

  // The next process resubmits it once after its startup report pass; it then matches.
  let mut next = Worker::new(true).await;
  let source = add(&next.attribution, "anr", 7);
  next.attribution.submit(live.clone(), Kind::Anr, false);
  next.attribution.mark_ready();
  assert_eq!(
    next.enqueued_rx.recv().await.unwrap().0,
    vec![source.to_string()]
  );
  assert!(!live.exists());

  // A resubmission that still matches nothing is deleted, as is a duplicate of a consumed file.
  let unmatched = next.submit_with(9, false);
  next.attribution.submit(live, Kind::Anr, false);
  next.submit(7);
  next.enqueued_rx.recv().await.unwrap();
  assert!(!unmatched.exists());
  worker.shutdown.shutdown().await;
  next.shutdown.shutdown().await;
}

#[tokio::test]
async fn shutdown_cancels_an_outstanding_persistence_acknowledgement() {
  let runtime = TestConfigLoader::new().await;
  let attribution = attribution();
  add(&attribution, "anr", 7);
  attribution.mark_ready();
  let (waiting_tx, mut waiting_rx) = mpsc::unbounded_channel();
  let mut client = MockClient::new();
  client
    .expect_enqueue_crash_profile()
    .returning(move |_, _, _, _, _, persisted_tx| {
      // Deliberately hold only the persistence response: the fact under test is worker
      // cancellation while blocked at this boundary, not uploader storage behavior.
      waiting_tx.send(persisted_tx.unwrap()).unwrap();
      Ok(Uuid::new_v4())
    });
  let shutdown = ComponentShutdownTrigger::default();
  attribution.start(Arc::new(client), &runtime, shutdown.make_shutdown());
  std::fs::create_dir_all(".tmp").unwrap();
  let directory = TempDir::with_prefix_in("shutdown-profile", ".tmp").unwrap();
  attribution.submit(write_profile(directory.path(), 7), Kind::Anr, false);
  let held_ack = waiting_rx.recv().await.unwrap();
  shutdown.shutdown().await;
  assert!(held_ack.is_closed());
}
