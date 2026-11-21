// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{CrashReportBuilder, Setup};
use crate::ReportOrigin;
use bd_api::DataUpload;
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
use bd_proto::protos::client::api::upload_artifact_response::State;
use bd_proto::protos::client::api::{
  ArtifactPayloadEncoding,
  UploadArtifactRequest,
  UploadArtifactResponse,
};
use bd_proto::protos::client::artifact::ArtifactUploadIndex;
use bd_runtime::runtime::{FeatureFlag as _, chunked_artifact_upload};
use bd_runtime::test::TestConfigLoader;
use bd_shutdown::ComponentShutdownTrigger;
use bd_test_helpers::runtime::{ValueKind, make_update};
use bd_time::TestTimeProvider;
use std::collections::HashMap;
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
  pending_uploads: HashMap<String, UploadArtifactRequest>,
  completed_uploads: Vec<UploadArtifactRequest>,
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
      tokio::sync::watch::channel(Arc::default()).1,
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
      pending_uploads: HashMap::new(),
      completed_uploads: Vec::new(),
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

  async fn next_completed_upload(&mut self) -> UploadArtifactRequest {
    loop {
      let upload = self.uploads.recv().await.unwrap();
      let DataUpload::ArtifactUpload(upload) = upload else {
        panic!("unexpected upload in crash-profile lifecycle");
      };
      let request = upload.payload;
      log::debug!(
        "acknowledging lifecycle artifact: artifact_id={} type={} chunk_index={:?}",
        request.artifact_id,
        request.type_id,
        request.chunk_index
      );
      let mut response = UploadArtifactResponse {
        upload_uuid: upload.uuid,
        ..Default::default()
      };
      assert!(request.logical_bytes.is_some());
      if request.chunk_index.is_none() {
        assert!(matches!(
          request.type_id.as_str(),
          CLIENT_REPORT_ARTIFACT_TYPE_ID | CRASH_PROFILE_ARTIFACT_TYPE_ID
        ));
        if request.type_id == CLIENT_REPORT_ARTIFACT_TYPE_ID {
          assert!(request.crash_profile.is_none());
        }
        assert_eq!(request.contents, b"");
        assert_eq!(request.chunks.len(), 1);
        assert_eq!(
          request.payload_encoding,
          ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_ZLIB.into()
        );
        assert!(
          self
            .pending_uploads
            .insert(request.artifact_id.clone(), request)
            .is_none()
        );
        response.state = State::STATE_RECEIVING.into();
        response.received_chunks = vec![0];
        upload.response_tx.send(response).unwrap();
        continue;
      }
      assert_eq!(request.chunk_index, Some(0));
      let mut begin = self.pending_uploads.remove(&request.artifact_id).unwrap();
      // Thin fragments omit descriptive metadata; retain Begin for the completed-file assertions.
      begin.contents = request.contents;
      response.state = State::STATE_FINALIZED.into();
      response.received_chunks = vec![1];
      upload.response_tx.send(response).unwrap();
      return begin;
    }
  }

  async fn acknowledge(&mut self, parents: &[String]) -> String {
    let type_id = if parents.is_empty() {
      CLIENT_REPORT_ARTIFACT_TYPE_ID
    } else {
      CRASH_PROFILE_ARTIFACT_TYPE_ID
    };
    let request = loop {
      if let Some(index) = self
        .completed_uploads
        .iter()
        .position(|upload| upload.type_id == type_id)
      {
        break self.completed_uploads.remove(index);
      }
      // Service requests in arrival order: holding another artifact's ACK blocks the shared permit.
      let upload = self.next_completed_upload().await;
      self.completed_uploads.push(upload);
    };
    let artifact_id = request.artifact_id.clone();
    assert_eq!(request.session_id, self.session_id);
    assert_eq!(
      request
        .crash_profile
        .as_ref()
        .map_or_default(|links| links.report_ids.as_slice()),
      parents
    );
    if !parents.is_empty() {
      assert_eq!(
        read_compressed(&request.contents).unwrap(),
        heap_profile(7, BOOT_EPOCH_NS, 5_000_000_000)
      );
      assert_eq!(
        request.state_metadata["app_version"].string_data(),
        "receiving-build"
      );
    }
    // This observes the real uploader retiring its durable index entry, not merely sending ACK.
    loop {
      let revision = *self.queue_revision.borrow_and_update();
      let pending = self.pending().await;
      if pending
        .artifact
        .iter()
        .all(|entry| entry.name != artifact_id)
        && pending
          .chunked_artifact
          .iter()
          .all(|entry| entry.artifact_id != artifact_id)
      {
        break;
      }
      self
        .queue_revision
        .wait_for(|current| *current > revision)
        .await
        .unwrap();
    }
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
  let pending = pipeline.pending().await;
  assert_eq!(pending.artifact, []);
  assert_eq!(pending.chunked_artifact, []);

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
          chunked_artifact_upload::MaxLogicalBytes::path(),
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
    let pending = pipeline.pending().await;
    assert_eq!(pending.artifact, []);
    assert_eq!(pending.chunked_artifact, []);
    assert_eq!(pipeline.completed_uploads, []);
    assert!(pipeline.pending_uploads.is_empty());
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
        vec![(
          chunked_artifact_upload::MaxLogicalBytes::path(),
          ValueKind::Int(1024),
        )],
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
