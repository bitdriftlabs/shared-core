// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{CrashReportBuilder, Setup};
use crate::ReportOrigin;
use bd_api::DataUpload;
use bd_api::upload::{IntentDecision, IntentResponse, UploadResponse};
use bd_artifact_upload::crash_profile::Kind;
use bd_artifact_upload::crash_profile_attribution::CrashProfileAttribution;
use bd_artifact_upload::test::heap_profile;
use bd_artifact_upload::{Client, ReportAttribution, Uploader, encode_source};
use bd_client_common::artifact::{CLIENT_REPORT_ARTIFACT_TYPE_ID, CRASH_PROFILE_ARTIFACT_TYPE_ID};
use bd_client_common::file::{read_compressed, read_compressed_protobuf};
use bd_client_common::file_system::FileSystem;
use bd_client_common::test::TestFileSystem;
use bd_client_stats_store::Collector;
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::ReportType;
use bd_proto::protos::client::artifact::ArtifactUploadIndex;
use bd_runtime::runtime::{FeatureFlag as _, attachment};
use bd_runtime::test::TestConfigLoader;
use bd_shutdown::ComponentShutdownTrigger;
use bd_test_helpers::runtime::{ValueKind, make_update};
use bd_time::TestTimeProvider;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use time::OffsetDateTime;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use uuid::Uuid;

const BOOT_EPOCH_NS: u64 = 1_000_000_000_000;

//
// Pipeline
//

struct Pipeline {
  monitor: Setup,
  attribution: Arc<CrashProfileAttribution>,
  progress: watch::Receiver<u64>,
  filesystem: Arc<TestFileSystem>,
  uploads: mpsc::Receiver<DataUpload>,
  queue_revision: watch::Receiver<u64>,
  runtime: TestConfigLoader,
  attribution_shutdown: ComponentShutdownTrigger,
  uploader_shutdown: ComponentShutdownTrigger,
  uploader_task: JoinHandle<()>,
  session_id: String,
}

impl Pipeline {
  async fn new() -> Self {
    let mut monitor = Setup::new(None, false, None, None).await;
    let runtime = TestConfigLoader::new().await;
    let filesystem = Arc::new(TestFileSystem::new());
    let (upload_tx, uploads) = mpsc::channel(16);
    let uploader_shutdown = ComponentShutdownTrigger::default();
    let (uploader, client) = Uploader::new(
      filesystem.clone(),
      upload_tx,
      Arc::new(TestTimeProvider::new(OffsetDateTime::UNIX_EPOCH)),
      &runtime,
      &Collector::default(),
      [("app_version".to_string(), "receiving-build".to_string())].into(),
      uploader_shutdown.make_shutdown(),
    );
    let client = Arc::new(client);
    let mut queue_revision = client.upload_queue_revision();
    let uploader_task = tokio::spawn(uploader.run());
    queue_revision
      .wait_for(|revision| *revision > 0)
      .await
      .unwrap();

    let attribution = Arc::new(CrashProfileAttribution::new(Uuid::new_v4()));
    let attribution_shutdown = ComponentShutdownTrigger::default();
    let mut progress = attribution.processing_revision_for_test();
    attribution.start(
      client.clone(),
      &runtime,
      attribution_shutdown.make_shutdown(),
    );
    progress.wait_for(|revision| *revision == 1).await.unwrap();
    monitor.monitor.artifact_client = client;
    monitor.monitor.report_attribution = Some(attribution.clone());
    Self {
      monitor,
      attribution,
      progress,
      filesystem,
      uploads,
      queue_revision,
      runtime,
      attribution_shutdown,
      uploader_shutdown,
      uploader_task,
      session_id: "previous_session_id".to_string(),
    }
  }

  fn submit(&self) -> PathBuf {
    let path = self
      .monitor
      .directory
      .path()
      .canonicalize()
      .unwrap()
      .join(format!("{}.perfetto-trace", Uuid::new_v4()));
    std::fs::write(&path, heap_profile(7, BOOT_EPOCH_NS, 5_000_000_000)).unwrap();
    self.attribution.submit(path.clone(), Kind::Anr, false);
    path
  }

  fn report() -> Vec<u8> {
    let source = encode_source(
      7,
      1,
      0,
      i64::try_from(BOOT_EPOCH_NS + 10_000_000_000).unwrap(),
      10_000_000_000,
    );
    CrashReportBuilder::new("ANR")
      .report_type(ReportType::AppNotResponding)
      .timestamp(OffsetDateTime::from_unix_timestamp(1020).unwrap())
      .crash_profile_source(&source)
      .build()
  }

  async fn discover_report(&self) -> String {
    self.monitor.make_crash("anr.cap", &Self::report());
    let logs = self.monitor.process_all_pending_reports().await;
    assert_eq!(logs.len(), 1);
    logs[0]["_crash_artifact_id"]
      .value
      .as_str()
      .unwrap()
      .to_string()
  }

  async fn pending(&self) -> ArtifactUploadIndex {
    read_compressed_protobuf(
      &self
        .filesystem
        .read_file(&PathBuf::from("report_uploads/report_index.pb"))
        .await
        .unwrap(),
    )
    .unwrap()
  }

  async fn acknowledge(&mut self, parents: &[String]) -> String {
    let DataUpload::ArtifactUploadIntent(intent) = self.uploads.recv().await.unwrap() else {
      panic!("expected ordinary artifact intent");
    };
    let artifact_id = intent.payload.artifact_id.clone();
    let type_id = if parents.is_empty() {
      CLIENT_REPORT_ARTIFACT_TYPE_ID
    } else {
      CRASH_PROFILE_ARTIFACT_TYPE_ID
    };
    assert_eq!(intent.payload.type_id, type_id);
    assert_eq!(
      intent
        .payload
        .crash_profile
        .as_ref()
        .map(|links| links.report_ids.as_slice())
        .unwrap_or_default(),
      parents
    );
    intent
      .response_tx
      .send(IntentResponse {
        uuid: intent.uuid,
        decision: IntentDecision::UploadImmediately,
      })
      .unwrap();
    let DataUpload::ArtifactUpload(upload) = self.uploads.recv().await.unwrap() else {
      panic!("expected ordinary artifact upload");
    };
    assert_eq!(upload.payload.artifact_id, artifact_id);
    assert_eq!(upload.payload.type_id, type_id);
    assert_eq!(upload.payload.session_id, self.session_id);
    if !parents.is_empty() {
      assert_eq!(
        upload.payload.crash_profile.as_ref().unwrap().report_ids,
        parents
      );
      assert_eq!(
        read_compressed(&upload.payload.contents).unwrap(),
        heap_profile(7, BOOT_EPOCH_NS, 5_000_000_000)
      );
      assert_eq!(
        upload.payload.state_metadata["app_version"].string_data(),
        "receiving-build"
      );
    }
    let before_ack = *self.queue_revision.borrow_and_update();
    upload
      .response_tx
      .send(UploadResponse {
        uuid: upload.uuid,
        success: true,
      })
      .unwrap();
    // This observes the real uploader retiring its durable index entry, not merely sending ACK.
    self
      .queue_revision
      .wait_for(|revision| *revision > before_ack)
      .await
      .unwrap();
    assert!(
      self
        .pending()
        .await
        .artifact
        .iter()
        .all(|entry| entry.name != artifact_id)
    );
    artifact_id
  }

  async fn shutdown(self) -> Setup {
    self.attribution_shutdown.shutdown().await;
    self.uploader_shutdown.shutdown().await;
    self.uploader_task.await.unwrap();
    self.monitor
  }
}

#[tokio::test]
async fn real_monitor_candidates_survive_report_and_profile_upload_acks() {
  let mut pipeline = Pipeline::new().await;
  let first = pipeline.submit();
  // The worker has reached its recovery gate, but only the actual monitor pass may open it.
  assert!(first.exists());
  let parent = pipeline.discover_report().await;
  pipeline
    .progress
    .wait_for(|revision| *revision >= 2)
    .await
    .unwrap();
  assert_eq!(pipeline.acknowledge(&[]).await, parent);
  assert!(!first.exists());
  let profile = pipeline.acknowledge(std::slice::from_ref(&parent)).await;
  assert_eq!(pipeline.pending().await.artifact, Vec::new());

  // Neither real ACK removes the RAM-only candidate. A later result reuses its canonical ID.
  pipeline.submit();
  let later = pipeline.acknowledge(std::slice::from_ref(&parent)).await;
  assert_ne!(profile, later);
  pipeline
    .progress
    .wait_for(|revision| *revision >= 3)
    .await
    .unwrap();
  pipeline.shutdown().await;
}

#[tokio::test]
async fn actual_persistence_rejection_retires_source_but_not_report_candidate() {
  for disk_full in [false, true] {
    let mut pipeline = Pipeline::new().await;
    let parent = pipeline.discover_report().await;
    pipeline.acknowledge(&[]).await;
    pipeline
      .runtime
      .update_snapshot(make_update(
        vec![(
          attachment::MaxBytes::path(),
          ValueKind::Int(if disk_full { 1024 } else { 1 }),
        )],
        "reject profile".to_string(),
      ))
      .await
      .unwrap();
    pipeline
      .filesystem
      .disk_full
      .store(disk_full, Ordering::Relaxed);
    let rejected = pipeline.submit();
    // Both the attachment limit and an actual filesystem write failure cross the real uploader's
    // persistence response. The worker advances only after receiving that response.
    pipeline
      .progress
      .wait_for(|revision| *revision >= 2)
      .await
      .unwrap();
    assert!(!rejected.exists());
    assert_eq!(pipeline.pending().await.artifact, Vec::new());
    assert!(matches!(
      pipeline.uploads.try_recv(),
      Err(mpsc::error::TryRecvError::Empty)
    ));
    pipeline
      .filesystem
      .disk_full
      .store(false, Ordering::Relaxed);
    pipeline
      .runtime
      .update_snapshot(make_update(
        vec![(attachment::MaxBytes::path(), ValueKind::Int(1024))],
        "accept later profile".to_string(),
      ))
      .await
      .unwrap();
    pipeline.submit();
    pipeline.acknowledge(&[parent]).await;
    pipeline
      .progress
      .wait_for(|revision| *revision >= 3)
      .await
      .unwrap();
    pipeline.shutdown().await;
  }
}

#[tokio::test]
async fn late_current_report_uses_actual_monitor_dispatch_after_empty_startup_recovery() {
  let mut pipeline = Pipeline::new().await;
  assert_eq!(
    pipeline.monitor.process_all_pending_reports().await,
    Vec::new()
  );
  let unmatched = pipeline.submit();
  pipeline
    .progress
    .wait_for(|revision| *revision >= 2)
    .await
    .unwrap();
  assert!(!unmatched.exists());

  // Dispatch a fresh current-process file through the monitor's real parser/enqueue path after
  // readiness. No watcher polling or synthetic attribution.add_report call is involved.
  pipeline.session_id = pipeline
    .monitor
    .monitor
    .session
    .session_id()
    .unwrap()
    .to_string();
  pipeline.monitor.make_crash("late.cap", &Pipeline::report());
  let path = pipeline
    .monitor
    .directory
    .path()
    .join("reports/new/late.cap");
  let log = pipeline
    .monitor
    .monitor
    .process_file(&path, ReportOrigin::Current)
    .await
    .unwrap();
  let parent = log.fields["_crash_artifact_id"]
    .value
    .as_str()
    .unwrap()
    .to_string();
  assert!(!path.exists());
  assert_eq!(pipeline.acknowledge(&[]).await, parent);
  pipeline.submit();
  pipeline.acknowledge(&[parent]).await;
  pipeline
    .progress
    .wait_for(|revision| *revision >= 3)
    .await
    .unwrap();
  pipeline.shutdown().await;
}

#[tokio::test]
async fn shutdown_at_report_recovery_gate_leaves_unadopted_profile_in_place() {
  let pipeline = Pipeline::new().await;
  let path = pipeline.submit();
  let _monitor = pipeline.shutdown().await;
  assert!(path.exists());
}
