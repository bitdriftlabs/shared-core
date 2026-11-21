// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{TestHooks, UploadClient, Uploader};
use crate::uploader::{
  ARTIFACT_UPLOAD_DIRECTORY,
  Client,
  EnqueueError,
  NewUpload,
  PersistenceAck,
  REPORT_INDEX_FILE,
  SnappedFeatureFlag,
  UploadSource,
  retained_persistence_error,
};
use assert_matches::assert_matches;
use bd_api::DataUpload;
use bd_api::upload::TrackedArtifactUpload;
use bd_client_common::file::{
  read_checksummed_data,
  read_compressed_protobuf,
  write_checksummed_data,
};
use bd_client_common::file_system::FileSystem;
use bd_client_common::test::TestFileSystem;
use bd_client_stats_store::Collector;
use bd_log_filter::ScrubRules;
use bd_proto::protos::client::api::upload_artifact_response::State;
use bd_proto::protos::client::api::{
  ArtifactPayloadEncoding,
  UploadArtifactRequest,
  UploadArtifactResponse,
};
use bd_proto::protos::client::artifact::ArtifactUploadIndex;
use bd_proto::protos::client::feature_flag::FeatureFlag;
use bd_proto::protos::filter::scrub::ScrubConfiguration;
use bd_proto::protos::logging::payload::data::Data_type;
use bd_proto::protos::logging::payload::{BinaryData, Data};
use bd_runtime::runtime::{FeatureFlag as _, artifact_upload, chunked_artifact_upload};
use bd_runtime::test::TestConfigLoader;
use bd_test_helpers::filter::macros::regex_match_and_substitute_field;
use bd_test_helpers::filter::scrub_rule;
use bd_test_helpers::remove_field;
use bd_test_helpers::runtime::ValueKind;
use bd_time::{OffsetDateTimeExt as _, TestTimeProvider};
use flate2::read::ZlibDecoder;
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use time::OffsetDateTime;
use time::ext::NumericalStdDuration;
use time::macros::datetime;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use uuid::Uuid;

#[path = "../profile_upload_test.rs"]
mod profile_tests;

const COMMAND_ID: &str = "11111111-1111-4111-8111-111111111111";

pub(super) fn progress(operation: TrackedArtifactUpload, state: State, receipts: &[u8]) {
  operation
    .response_tx
    .send(UploadArtifactResponse {
      upload_uuid: operation.uuid,
      state: state.into(),
      received_chunks: receipts.to_vec(),
      ..Default::default()
    })
    .unwrap();
}

fn no_scrub_rules() -> watch::Receiver<Arc<ScrubRules>> {
  watch::channel(Arc::new(ScrubRules::default())).1
}

// The uploader is serial, so lifecycle callbacks describe one staging attempt at a time. Keep
// fixture notification policy here rather than adding test-only phases to the ownership ledger.
struct UploadTestHooks {
  chunk_transfer_gate: Option<watch::Receiver<bool>>,
  initialized_tx: Option<mpsc::Sender<()>>,
  upload_complete_tx: mpsc::Sender<String>,
  entry_received_tx: mpsc::Sender<String>,
  persistence_ack_tx: Option<mpsc::Sender<oneshot::Sender<()>>>,
  observe_preparation: AtomicBool,
  notify_preparation_error: AtomicBool,
}

#[async_trait::async_trait]
impl TestHooks for UploadTestHooks {
  async fn before_chunk_transfer(&self) {
    if let Some(mut gate) = self.chunk_transfer_gate.clone() {
      gate.wait_for(|enabled| *enabled).await.unwrap();
    }
  }
  async fn initialized(&self) {
    if let Some(sender) = &self.initialized_tx {
      sender.send(()).await.unwrap();
    }
  }

  fn preparation_started(&self, source: &UploadSource, request: &UploadArtifactRequest) {
    let observed = matches!(request.type_id.as_str(), "client_report" | "state_snapshot")
      || matches!(source, UploadSource::Retained(_));
    self.observe_preparation.store(observed, Ordering::Relaxed);
    self
      .notify_preparation_error
      .store(!matches!(source, UploadSource::Bytes(_)), Ordering::Relaxed);
  }

  fn payload_prepared(&self) {
    // File preparation errors notify the fixture, but index failures must not look like
    // accepted entries. Successful observed commits always notify, including byte reports.
    self
      .notify_preparation_error
      .store(false, Ordering::Relaxed);
  }

  async fn staging_completed(&self, id: &str, success: bool) {
    let observed = self.observe_preparation.swap(false, Ordering::Relaxed);
    let notify_preparation_error = self.notify_preparation_error.swap(false, Ordering::Relaxed);
    if observed && (success || notify_preparation_error) {
      let _ = self.entry_received_tx.try_send(id.to_owned());
    }
    if !success && let Some(sender) = &self.persistence_ack_tx {
      let (resume_tx, resume_rx) = oneshot::channel();
      sender.send(resume_tx).await.unwrap();
      resume_rx.await.unwrap();
    }
  }

  async fn upload_complete(&self, id: &str) {
    self.upload_complete_tx.send(id.to_owned()).await.unwrap();
  }
}

pub(super) struct Setup {
  pub(super) chunk_transfer_enabled: watch::Sender<bool>,
  pub(super) upload_complete_rx: tokio::sync::mpsc::Receiver<String>,
  pub(super) entry_received_rx: tokio::sync::mpsc::Receiver<String>,
  pub(super) client: UploadClient,
  pub(super) filesystem: Arc<TestFileSystem>,
  pub(super) data_upload_rx: tokio::sync::mpsc::Receiver<DataUpload>,
  data_upload_tx: tokio::sync::mpsc::Sender<DataUpload>,
  shutdown: bd_shutdown::ComponentShutdownTrigger,
  task_handle: JoinHandle<()>,
  pub(super) runtime: TestConfigLoader,
  pub(super) time_provider: Arc<TestTimeProvider>,
  pub(super) collector: Collector,
  scrub_rules_tx: watch::Sender<Arc<ScrubRules>>,

  tmpdir: tempfile::TempDir,
}

impl Setup {
  pub(super) async fn next_chunk_operation(&mut self) -> TrackedArtifactUpload {
    loop {
      let DataUpload::ArtifactUpload(operation) = self.data_upload_rx.recv().await.unwrap() else {
        panic!("expected file operation, not a separate intent");
      };
      assert!(operation.payload.logical_bytes.is_some());
      if !operation.response_tx.is_closed() {
        return operation;
      }
    }
  }

  async fn allow_file_begin(&mut self) -> UploadArtifactRequest {
    self.chunk_transfer_enabled.send(true).unwrap();
    let begin = self.next_chunk_operation().await;
    assert_eq!(begin.payload.chunk_index, None);
    assert_eq!(begin.payload.contents.as_slice(), b"");
    let template = begin.payload.clone();
    progress(
      begin,
      if template.chunks.is_empty() {
        State::STATE_FINALIZED
      } else {
        State::STATE_RECEIVING
      },
      &vec![0; template.chunks.len().div_ceil(8)],
    );
    template
  }

  pub(super) async fn finish_single_fragment(
    &mut self,
    id: Uuid,
    contents: &[u8],
  ) -> UploadArtifactRequest {
    let chunk = self.next_chunk_operation().await;
    assert_eq!(chunk.payload.artifact_id, id.to_string());
    assert_eq!(chunk.payload.chunk_index, Some(0));
    assert_eq!(chunk.payload.type_id, "");
    let mut decoded = Vec::new();
    ZlibDecoder::new(chunk.payload.contents.as_slice())
      .read_to_end(&mut decoded)
      .unwrap();
    assert_eq!(decoded, contents);
    let payload = chunk.payload.clone();
    progress(chunk, State::STATE_FINALIZED, &[1]);
    payload
  }

  // Tests that rewrite persisted state must stop its owner first: periodic reconciliation now
  // shares startup's inventory and can correctly remove any unindexed whole-file or chunk payload.
  pub(super) fn abort_for_recovery(&self) {
    self.task_handle.abort();
  }

  pub(super) async fn shutdown(self) {
    self.shutdown.shutdown().await;
    self.task_handle.await.unwrap();
  }

  pub(super) async fn reinitialize(self) -> Self {
    self.restart(false).await
  }

  pub(super) async fn crash_and_reinitialize(self) -> Self {
    self.restart(true).await
  }

  async fn restart(self, crash: bool) -> Self {
    if crash {
      self.task_handle.abort();
      assert!(self.task_handle.await.unwrap_err().is_cancelled());
    } else {
      self.shutdown.shutdown().await;
      self.task_handle.await.unwrap();
    }

    let shutdown = bd_shutdown::ComponentShutdownTrigger::default();

    let runtime = self.runtime;
    let time_provider = self.time_provider;
    let collector = Collector::default();

    let (mut uploader, client) = super::Uploader::new(
      self.filesystem.clone(),
      self.data_upload_tx.clone(),
      time_provider.clone(),
      &runtime,
      &collector,
      [("app_version".to_string(), "upgraded".to_string())].into(),
      self.scrub_rules_tx.subscribe(),
      shutdown.make_shutdown(),
    );

    let (upload_complete_tx, upload_complete_rx) = tokio::sync::mpsc::channel(1);
    let (entry_received_tx, entry_received_rx) = tokio::sync::mpsc::channel(1);
    let (initialized_tx, mut initialized_rx) = mpsc::channel(1);
    uploader.test_hooks = Some(Arc::new(UploadTestHooks {
      chunk_transfer_gate: Some(self.chunk_transfer_enabled.subscribe()),
      initialized_tx: Some(initialized_tx),
      upload_complete_tx,
      entry_received_tx,
      persistence_ack_tx: None,
      observe_preparation: AtomicBool::new(false),
      notify_preparation_error: AtomicBool::new(false),
    }));

    let task_handle = tokio::spawn(uploader.run());
    initialized_rx.recv().await.unwrap();

    Self {
      upload_complete_rx,
      chunk_transfer_enabled: self.chunk_transfer_enabled,
      entry_received_rx,
      client,
      filesystem: self.filesystem,
      data_upload_rx: self.data_upload_rx,
      data_upload_tx: self.data_upload_tx.clone(),
      shutdown,
      task_handle,
      runtime,
      time_provider,
      collector,
      scrub_rules_tx: self.scrub_rules_tx,
      tmpdir: self.tmpdir,
    }
  }

  pub(super) async fn new() -> Self {
    Self::with_buffer(1024 * 1024).await
  }

  pub(super) async fn with_buffer(buffer_bytes: u32) -> Self {
    let (chunk_transfer_enabled, chunk_transfer_gate) = watch::channel(false);
    let (data_upload_tx, data_upload_rx) = tokio::sync::mpsc::channel(1);
    let shutdown = bd_shutdown::ComponentShutdownTrigger::default();

    let filesystem = Arc::new(TestFileSystem::new());
    let config_loader = TestConfigLoader::new().await;
    config_loader
      .update_snapshot(bd_test_helpers::runtime::make_simple_update(vec![(
        artifact_upload::BufferByteLimit::path(),
        ValueKind::Int(buffer_bytes),
      )]))
      .await
      .unwrap();
    let (scrub_rules_tx, _) = watch::channel(Arc::new(ScrubRules::default()));

    let time_provider = Arc::new(TestTimeProvider::new(OffsetDateTime::now_utc()));
    let collector = Collector::default();
    let (mut uploader, client) = super::Uploader::new(
      filesystem.clone(),
      data_upload_tx.clone(),
      time_provider.clone(),
      &config_loader,
      &collector,
      command_metadata(),
      scrub_rules_tx.subscribe(),
      shutdown.make_shutdown(),
    );

    let (upload_complete_tx, upload_complete_rx) = tokio::sync::mpsc::channel(1);
    let (entry_received_tx, entry_received_rx) = tokio::sync::mpsc::channel(1);

    uploader.test_hooks = Some(Arc::new(UploadTestHooks {
      initialized_tx: None,
      chunk_transfer_gate: Some(chunk_transfer_gate),
      upload_complete_tx,
      entry_received_tx,
      persistence_ack_tx: None,
      observe_preparation: AtomicBool::new(false),
      notify_preparation_error: AtomicBool::new(false),
    }));

    let task_handle = tokio::spawn(uploader.run());

    Self {
      upload_complete_rx,
      chunk_transfer_enabled,
      entry_received_rx,
      client,
      filesystem,
      data_upload_rx,
      data_upload_tx,
      shutdown,
      task_handle,
      runtime: config_loader,
      time_provider,
      collector,
      scrub_rules_tx,
      tmpdir: tempfile::tempdir().unwrap(),
    }
  }

  fn make_file(&self, contents: &[u8]) -> std::fs::File {
    let mut file = tempfile::tempfile_in(self.tmpdir.path()).unwrap();
    file.write_all(contents).unwrap();

    file.seek(std::io::SeekFrom::Start(0)).unwrap();

    file
  }
}

async fn write_workflow_payload(setup: &Setup, id: Uuid) -> PathBuf {
  let source = PathBuf::from(format!("workflow-attachments/{id}.raw"));
  setup
    .filesystem
    .create_dir(source.parent().unwrap())
    .await
    .unwrap();
  setup
    .filesystem
    .write_file(&source, &write_checksummed_data(b"attachment"))
    .await
    .unwrap();
  source
}

pub(super) async fn wait_for_queue_revision(revision: &mut watch::Receiver<u64>, previous: u64) {
  let _ = timeout(
    5.std_seconds(),
    revision.wait_for(|revision| *revision > previous),
  )
  .await
  .expect("queue index must recover without another upload or a restart")
  .unwrap();
}

#[tokio::test(start_paused = true)]
async fn initialization_publishes_queue_revision_even_without_queue_files() {
  let setup = Setup::new().await;
  let mut revision = setup.client.upload_queue_revision();
  wait_for_queue_revision(&mut revision, 0).await;
  assert!(
    Uploader::pending_workflow_attachments(setup.filesystem.directory())
      .await
      .unwrap()
      .is_empty()
  );
  setup.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn healthy_spool_does_not_poll_and_failed_cleanup_remains_retryable() {
  let setup = Setup::new().await;
  let mut revision = setup.client.upload_queue_revision();
  wait_for_queue_revision(&mut revision, 0).await;
  setup.task_handle.abort();
  assert!(setup.task_handle.await.unwrap_err().is_cancelled());

  let shutdown = bd_shutdown::ComponentShutdownTrigger::default();
  let (mut uploader, _client) = Uploader::new(
    setup.filesystem.clone(),
    setup.data_upload_tx.clone(),
    setup.time_provider.clone(),
    &setup.runtime,
    &Collector::default(),
    command_metadata(),
    setup.scrub_rules_tx.subscribe(),
    shutdown.make_shutdown(),
  );
  uploader.initialize().await.unwrap();
  for backlog in [false, true] {
    if backlog {
      uploader
        .track_upload(NewUpload::new(
          UploadSource::Bytes(b"offline".to_vec()),
          UploadArtifactRequest {
            artifact_id: Uuid::new_v4().to_string(),
            command_id: Some(Uuid::new_v4().to_string()),
            type_id: "device_command_attachment".into(),
            session_id: "session".into(),
            time: OffsetDateTime::now_utc().into_proto(),
            ..Default::default()
          },
        ))
        .await;
      assert_eq!(uploader.chunked.entries.len(), 1);
    }
    let before = (
      setup.filesystem.list_calls.load(Ordering::Relaxed),
      setup.filesystem.delete_calls.load(Ordering::Relaxed),
    );
    // Poll directly so a due timer cannot escape the assertion through shutdown or scheduling.
    let mut run = Box::pin(uploader.run_loop());
    assert!(futures_util::poll!(run.as_mut()).is_pending());
    tokio::time::advance(std::time::Duration::from_secs(60)).await;
    assert!(futures_util::poll!(run.as_mut()).is_pending());
    assert_eq!(
      (
        setup.filesystem.list_calls.load(Ordering::Relaxed),
        setup.filesystem.delete_calls.load(Ordering::Relaxed),
      ),
      before
    );
    drop(run);
  }

  let orphan = ARTIFACT_UPLOAD_DIRECTORY.join("unpublished");
  setup
    .filesystem
    .write_file(&orphan, b"orphan")
    .await
    .unwrap();
  setup.filesystem.fail_deletes.store(true, Ordering::Relaxed);
  uploader.reconcile_files().await;
  assert!(uploader.cleanup_pending);
  assert!(setup.filesystem.exists(&orphan).await.unwrap());
  let mut run = Box::pin(uploader.run_loop());
  assert!(futures_util::poll!(run.as_mut()).is_pending());
  let attempts = setup.filesystem.list_calls.load(Ordering::Relaxed);
  tokio::time::advance(std::time::Duration::from_secs(1)).await;
  assert!(futures_util::poll!(run.as_mut()).is_pending());
  assert_eq!(
    setup.filesystem.list_calls.load(Ordering::Relaxed),
    attempts
  );
  tokio::time::advance(std::time::Duration::from_secs(1)).await;
  assert!(futures_util::poll!(run.as_mut()).is_pending());
  assert_eq!(
    setup.filesystem.list_calls.load(Ordering::Relaxed),
    attempts + 1
  );
  drop(run);
  setup
    .filesystem
    .fail_deletes
    .store(false, Ordering::Relaxed);
  uploader.reconcile_files().await;
  assert!(!uploader.cleanup_pending);
  assert!(!setup.filesystem.exists(&orphan).await.unwrap());
}

#[tokio::test(start_paused = true)]
async fn index_sync_failure_is_reconciled_without_reenqueue() {
  let setup = Setup::new().await;
  let id = Uuid::new_v4();
  let source = write_workflow_payload(&setup, id).await;
  let index_path = ARTIFACT_UPLOAD_DIRECTORY.join(&*REPORT_INDEX_FILE);
  setup.filesystem.fail_next_sync_at(index_path.clone());
  let sync_gate = setup.filesystem.gate_next_sync(index_path);
  let mut revision = setup.client.upload_queue_revision();
  let (persisted_tx, persisted_rx) = tokio::sync::oneshot::channel();
  setup
    .client
    .enqueue_workflow_attachment(
      id,
      source.clone(),
      "session".into(),
      None,
      Some(persisted_tx),
      None,
      None,
    )
    .unwrap();
  let resume = sync_gate.await.unwrap();
  setup.filesystem.disk_full.store(true, Ordering::Relaxed);
  resume.send(()).unwrap();
  assert_matches!(
    persisted_rx.await.unwrap(),
    Err(EnqueueError::RetryablePersistence(_))
  );
  assert_eq!(
    Uploader::pending_workflow_attachments(setup.filesystem.directory())
      .await
      .unwrap(),
    [id].into(),
  );
  let previous = *revision.borrow();
  setup.filesystem.disk_full.store(false, Ordering::Relaxed);
  wait_for_queue_revision(&mut revision, previous).await;
  assert_eq!(
    Uploader::pending_workflow_attachments(setup.filesystem.directory())
      .await
      .unwrap(),
    [id].into()
  );
  assert_eq!(
    read_checksummed_data(&setup.filesystem.read_file(&source).await.unwrap()).unwrap(),
    b"attachment"
  );
  setup.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn failed_workflow_unpin_is_retried_after_success_or_rejection() {
  for (upload_success, sync_failure) in [(true, false), (false, false), (true, true), (false, true)]
  {
    let mut setup = Setup::new().await;
    let id = Uuid::new_v4();
    let source = write_workflow_payload(&setup, id).await;
    let mut revision = setup.client.upload_queue_revision();
    let (persisted_tx, persisted_rx) = tokio::sync::oneshot::channel();
    let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
    setup
      .client
      .enqueue_workflow_attachment(
        id,
        source,
        "session".into(),
        None,
        Some(persisted_tx),
        Some(completion_tx),
        None,
      )
      .unwrap();
    persisted_rx.await.unwrap().unwrap();
    setup.entry_received_rx.recv().await.unwrap();
    setup.chunk_transfer_enabled.send(true).unwrap();
    let begin = setup.next_chunk_operation().await;
    assert_eq!(begin.payload.artifact_id, id.to_string());
    let index_path = ARTIFACT_UPLOAD_DIRECTORY.join(&*REPORT_INDEX_FILE);
    let sync_gate = sync_failure.then(|| setup.filesystem.gate_next_sync(index_path.clone()));
    let filesystem = setup.filesystem.clone();
    let fail_index = || {
      if sync_failure {
        filesystem.fail_next_sync_at(index_path.clone());
      } else {
        filesystem.disk_full.store(true, Ordering::Relaxed);
      }
    };
    if upload_success {
      progress(begin, State::STATE_RECEIVING, &[0]);
      let upload = setup.next_chunk_operation().await;
      fail_index();
      progress(upload, State::STATE_FINALIZED, &[1]);
    } else {
      fail_index();
      progress(begin, State::STATE_DROPPED, &[]);
    }
    if let Some(sync_gate) = sync_gate {
      let resume = timeout(5.std_seconds(), sync_gate)
        .await
        .expect("removing a workflow lease must sync the index")
        .unwrap();
      setup.filesystem.disk_full.store(true, Ordering::Relaxed);
      resume.send(()).unwrap();
    }
    assert_eq!(completion_rx.await.unwrap().is_ok(), upload_success);
    let pending = Uploader::pending_workflow_attachments(setup.filesystem.directory())
      .await
      .unwrap();
    if sync_failure {
      assert!(pending.is_empty());
    } else {
      assert_eq!(pending, [id].into());
    }
    let previous = *revision.borrow();
    let retry_sync = sync_failure.then(|| setup.filesystem.gate_next_sync(index_path));
    setup.filesystem.disk_full.store(false, Ordering::Relaxed);
    if let Some(retry_sync) = retry_sync {
      let resume = timeout(5.std_seconds(), retry_sync)
        .await
        .expect("failed workflow lease sync must be retried")
        .unwrap();
      resume.send(()).unwrap();
    }
    wait_for_queue_revision(&mut revision, previous).await;
    assert!(
      Uploader::pending_workflow_attachments(setup.filesystem.directory())
        .await
        .unwrap()
        .is_empty()
    );
    setup.shutdown().await;
  }
}

fn command_metadata() -> HashMap<String, String> {
  [
    ("platform".to_string(), "android".to_string()),
    ("app_id".to_string(), "io.bitdrift.test".to_string()),
    ("app_version".to_string(), "01.2".to_string()),
    ("_app_version_code".to_string(), "4294967296".to_string()),
  ]
  .into()
}

fn command_state_metadata() -> HashMap<String, Data> {
  command_metadata()
    .into_iter()
    .map(|(key, value)| {
      (
        key,
        Data {
          data_type: Some(Data_type::StringData(value)),
          ..Default::default()
        },
      )
    })
    .collect()
}

#[tokio::test]
async fn basic_flow() {
  let mut setup = Setup::new().await;

  let timestamp = datetime!(2023-10-01 12:00:00 UTC);
  let contents = vec![42; 64 * 1024 + 1];
  let mut file = setup.make_file(&[b"prefix:".as_slice(), &contents].concat());
  file.seek(SeekFrom::Start(7)).unwrap();
  for (source, expected) in [
    (UploadSource::Bytes(contents.clone()), contents.as_slice()),
    (UploadSource::File(file), contents.as_slice()),
  ] {
    let id = setup
      .client
      .enqueue_upload(
        source,
        "client_report".to_string(),
        [("foo".into(), "bar".into())].into(),
        Some(timestamp),
        "session_id".to_string(),
        vec![],
        None,
        None,
      )
      .unwrap();

    assert_eq!(
      setup.entry_received_rx.recv().await.unwrap(),
      id.to_string()
    );
    let begin = setup.allow_file_begin().await;
    assert_eq!(begin.artifact_id, id.to_string());
    assert_eq!(begin.type_id, "client_report");
    assert_eq!(begin.time, timestamp.into_proto());
    assert_eq!(begin.session_id, "session_id");
    assert_eq!(begin.logical_bytes, Some(expected.len() as u64));
    assert!(begin.state_metadata.is_empty());
    assert_eq!(begin.feature_flags, []);
    let final_chunk = setup.finish_single_fragment(id, expected).await;
    assert_eq!(
      final_chunk.state_metadata,
      string_metadata(&[("foo", "bar")])
    );

    setup.upload_complete_rx.recv().await.unwrap();

    let files = setup.filesystem.files();
    let index_file = &files[&super::ARTIFACT_UPLOAD_DIRECTORY
      .join(&*super::REPORT_INDEX_FILE)
      .to_str()
      .unwrap()
      .to_string()];
    let index_file: ArtifactUploadIndex = read_compressed_protobuf(index_file).unwrap();
    assert_eq!(index_file, ArtifactUploadIndex::default());
  }
  setup.shutdown().await;
}

#[tokio::test]
async fn metadata_is_scrubbed_with_the_rules_current_when_each_request_is_built() {
  let mut setup = Setup::new().await;

  // With no rules applied yet, Begin carries only identity keys and the final chunk carries full
  // scrubbed metadata.
  let id = setup
    .client
    .enqueue_upload(
      UploadSource::File(setup.make_file(b"abc")),
      "client_report".to_string(),
      [
        ("app_id".into(), "com.example.app".into()),
        ("secret".into(), "hunter2".into()),
        ("email".into(), "a@example.com".into()),
        ("keep".into(), "value".into()),
      ]
      .into(),
      None,
      "session_id".to_string(),
      vec![],
      None,
      None,
    )
    .unwrap();

  setup.chunk_transfer_enabled.send(true).unwrap();
  let begin = setup.next_chunk_operation().await;
  assert_eq!(
    begin.payload.state_metadata,
    string_metadata(&[("app_id", "com.example.app")])
  );

  // Rules applied while the artifact is queued reach the next request built for it.
  let (rules, failures) = ScrubRules::new(ScrubConfiguration {
    rules: vec![
      scrub_rule(remove_field!("secret")),
      scrub_rule(regex_match_and_substitute_field!(
        "email",
        "@.*",
        "@<redacted>"
      )),
    ],
    ..Default::default()
  });
  assert_eq!(failures, 0);
  setup.scrub_rules_tx.send_replace(Arc::new(rules));

  // Retry Begin after the rules change; Begin stays restricted to identity keys while the final
  // chunk carries the freshly scrubbed full metadata.
  drop(begin);
  let begin = setup.allow_file_begin().await;
  assert_eq!(begin.artifact_id, id.to_string());
  assert_eq!(
    begin.state_metadata,
    string_metadata(&[("app_id", "com.example.app")])
  );
  let final_chunk = setup.finish_single_fragment(id, b"abc").await;
  assert_eq!(
    final_chunk.state_metadata,
    string_metadata(&[
      ("app_id", "com.example.app"),
      ("email", "a@<redacted>"),
      ("keep", "value"),
    ])
  );

  setup.upload_complete_rx.recv().await.unwrap();
}

#[test]
fn scrub_metadata_keeps_untouched_values_verbatim_and_removes_unreadable_values_by_key() {
  let (rules, failures) = ScrubRules::new(ScrubConfiguration {
    rules: vec![
      scrub_rule(remove_field!("unreadable")),
      scrub_rule(regex_match_and_substitute_field!(
        "email",
        "@.*",
        "@<redacted>"
      )),
    ],
    ..Default::default()
  });
  assert_eq!(failures, 0);

  let binary = Data {
    data_type: Some(Data_type::BinaryData(BinaryData {
      type_: Some("image/png".to_string()),
      payload: vec![1, 2, 3],
      ..Default::default()
    })),
    ..Default::default()
  };
  let mut metadata = string_metadata(&[("email", "a@example.com"), ("keep", "value")]);
  metadata.insert("binary".to_string(), binary.clone());
  // A value with no type set cannot be read as a field.
  metadata.insert("unreadable".to_string(), Data::default());
  metadata.insert("unreadable_kept".to_string(), Data::default());

  let mut expected = string_metadata(&[("email", "a@<redacted>"), ("keep", "value")]);
  expected.insert("binary".to_string(), binary);
  expected.insert("unreadable_kept".to_string(), Data::default());

  assert_eq!(super::scrub_metadata(&rules, &metadata), expected);
}

fn string_metadata(entries: &[(&str, &str)]) -> HashMap<String, Data> {
  entries
    .iter()
    .map(|(key, value)| {
      (
        (*key).to_string(),
        Data {
          data_type: Some(Data_type::StringData((*value).to_string())),
          ..Default::default()
        },
      )
    })
    .collect()
}

#[tokio::test]
async fn queued_admission_owns_the_source_lease_until_processed() {
  let runtime = TestConfigLoader::new().await;
  let (data_upload_tx, _data_upload_rx) = tokio::sync::mpsc::channel(1);
  let shutdown = bd_shutdown::ComponentShutdownTrigger::default();
  let (uploader, client) = super::Uploader::new(
    Arc::new(TestFileSystem::new()),
    data_upload_tx,
    Arc::new(TestTimeProvider::new(OffsetDateTime::now_utc())),
    &runtime,
    &Collector::default(),
    command_metadata(),
    no_scrub_rules(),
    shutdown.make_shutdown(),
  );
  let ownership = Arc::new(tokio::sync::Semaphore::new(1));
  let source_lease = ownership.clone().acquire_owned().await.unwrap();
  let id = Uuid::new_v4();
  client
    .enqueue_workflow_attachment(
      id,
      format!("workflow-attachments/{id}.raw").into(),
      "session".into(),
      None,
      None,
      None,
      Some(source_lease),
    )
    .unwrap();
  drop(client);
  assert_eq!(ownership.available_permits(), 0);
  drop(uploader);
  assert_eq!(ownership.available_permits(), 1);
}

#[tokio::test]
async fn retained_source_keeps_payload_and_stages_fragments_in_the_shared_index() {
  let setup = Setup::new().await;
  let artifact_id = Uuid::new_v4();
  let source_path = format!("workflow-attachments/{artifact_id}.raw");
  setup
    .filesystem
    .create_dir(Path::new("workflow-attachments"))
    .await
    .unwrap();
  let stored = write_checksummed_data(b"attachment");
  setup
    .filesystem
    .write_file(Path::new(&source_path), &stored)
    .await
    .unwrap();
  let (persisted_tx, persisted_rx) = tokio::sync::oneshot::channel();
  setup
    .client
    .enqueue_workflow_attachment(
      artifact_id,
      source_path.clone().into(),
      "session".to_string(),
      None,
      Some(persisted_tx),
      None,
      None,
    )
    .unwrap();
  assert!(persisted_rx.await.unwrap().is_ok());
  assert_eq!(
    setup
      .filesystem
      .read_file(Path::new(&source_path))
      .await
      .unwrap(),
    stored
  );
  assert!(
    !setup
      .filesystem
      .exists(&ARTIFACT_UPLOAD_DIRECTORY.join(artifact_id.to_string()))
      .await
      .unwrap()
  );
  assert_eq!(setup.filesystem.files().len(), 3);
}

#[tokio::test]
async fn retained_source_can_retry_after_index_persistence_failure() {
  let setup = Setup::new().await;
  let id = Uuid::new_v4();
  let source = PathBuf::from(format!("workflow-attachments/{id}.raw"));
  setup
    .filesystem
    .create_dir(source.parent().unwrap())
    .await
    .unwrap();
  let stored = write_checksummed_data(b"attachment");
  setup.filesystem.write_file(&source, &stored).await.unwrap();
  setup.filesystem.disk_full.store(true, Ordering::Relaxed);
  let (persisted_tx, persisted_rx) = tokio::sync::oneshot::channel();
  setup
    .client
    .enqueue_workflow_attachment(
      id,
      source.clone(),
      "session".into(),
      None,
      Some(persisted_tx),
      None,
      None,
    )
    .unwrap();
  assert_matches!(
    persisted_rx.await.unwrap(),
    Err(EnqueueError::RetryablePersistence(_))
  );
  assert_eq!(setup.filesystem.read_file(&source).await.unwrap(), stored);
  setup.filesystem.disk_full.store(false, Ordering::Relaxed);
  let (persisted_tx, persisted_rx) = tokio::sync::oneshot::channel();
  setup
    .client
    .enqueue_workflow_attachment(
      id,
      source.clone(),
      "session".into(),
      None,
      Some(persisted_tx),
      None,
      None,
    )
    .unwrap();
  persisted_rx.await.unwrap().unwrap();
  assert_eq!(setup.filesystem.read_file(&source).await.unwrap(), stored);
  assert!(
    !setup
      .filesystem
      .exists(&ARTIFACT_UPLOAD_DIRECTORY.join(id.to_string()))
      .await
      .unwrap()
  );
}

#[tokio::test]
async fn retained_source_can_retry_after_sync_failure() {
  let mut setup = Setup::new().await;
  let artifact_id = Uuid::new_v4();
  let source_path = format!("workflow-attachments/{artifact_id}.raw");
  setup
    .filesystem
    .create_dir(Path::new("workflow-attachments"))
    .await
    .unwrap();
  setup
    .filesystem
    .write_file(
      Path::new(&source_path),
      &write_checksummed_data(b"attachment"),
    )
    .await
    .unwrap();
  setup
    .filesystem
    .fail_next_sync
    .store(true, Ordering::Relaxed);

  let (first_persisted_tx, first_persisted_rx) = tokio::sync::oneshot::channel();
  setup
    .client
    .enqueue_workflow_attachment(
      artifact_id,
      source_path.clone().into(),
      "session".to_string(),
      None,
      Some(first_persisted_tx),
      None,
      None,
    )
    .unwrap();
  assert_matches!(
    first_persisted_rx.await.unwrap(),
    Err(EnqueueError::RetryablePersistence(_))
  );
  setup.entry_received_rx.recv().await.unwrap();
  assert!(
    !setup
      .filesystem
      .exists(&ARTIFACT_UPLOAD_DIRECTORY.join(artifact_id.to_string()))
      .await
      .unwrap()
  );

  let (retry_persisted_tx, retry_persisted_rx) = tokio::sync::oneshot::channel();
  setup
    .client
    .enqueue_workflow_attachment(
      artifact_id,
      source_path.into(),
      "session".to_string(),
      None,
      Some(retry_persisted_tx),
      None,
      None,
    )
    .unwrap();
  assert!(retry_persisted_rx.await.unwrap().is_ok());
}

#[test]
fn missing_retained_source_is_not_retryable() {
  assert_matches!(
    retained_persistence_error(std::io::Error::from(std::io::ErrorKind::NotFound).into()),
    EnqueueError::Other(_)
  );
}

#[tokio::test]
async fn feature_flags() {
  let mut setup = Setup::new().await;

  let timestamp = datetime!(2023-10-01 12:00:00 UTC);
  let id = setup
    .client
    .enqueue_upload(
      UploadSource::File(setup.make_file(b"abc")),
      "client_report".to_string(),
      [("foo".into(), "bar".into())].into(),
      Some(timestamp),
      "session_id".to_string(),
      vec![
        SnappedFeatureFlag::new(
          "key".to_string(),
          Some("value".to_string()),
          timestamp - 1.std_seconds(),
        ),
        SnappedFeatureFlag::new("key2".to_string(), None, timestamp - 2.std_seconds()),
      ],
      None,
      None,
    )
    .unwrap();

  let begin = setup.allow_file_begin().await;
  assert_eq!(begin.artifact_id, id.to_string());
  assert_eq!(begin.type_id, "client_report");
  assert_eq!(begin.time, timestamp.into_proto());
  assert_eq!(begin.session_id, "session_id");
  assert!(begin.state_metadata.is_empty());
  assert_eq!(begin.feature_flags, []);
  let final_chunk = setup.finish_single_fragment(id, b"abc").await;
  assert_eq!(
    final_chunk.state_metadata,
    string_metadata(&[("foo", "bar")])
  );
  assert_eq!(
    final_chunk.feature_flags,
    vec![
      FeatureFlag {
        name: "key".to_string(),
        variant: Some("value".to_string()),
        last_updated: (timestamp - 1.std_seconds()).into_proto(),
        ..Default::default()
      },
      FeatureFlag {
        name: "key2".to_string(),
        variant: None,
        last_updated: (timestamp - 2.std_seconds()).into_proto(),
        ..Default::default()
      },
    ]
  );

  setup.upload_complete_rx.recv().await.unwrap();

  let files = setup.filesystem.files();
  let index_file = &files[&super::ARTIFACT_UPLOAD_DIRECTORY
    .join(&*super::REPORT_INDEX_FILE)
    .to_str()
    .unwrap()
    .to_string()];
  let index_file: ArtifactUploadIndex = read_compressed_protobuf(index_file).unwrap();
  assert_eq!(index_file, ArtifactUploadIndex::default());
}

#[tokio::test]
async fn inconsistent_state_missing_file() {
  let mut setup = Setup::new().await;
  let id1 = setup
    .client
    .enqueue_upload(
      UploadSource::File(setup.make_file(b"1")),
      "client_report".to_string(),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
      None,
      None,
    )
    .unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id1.to_string()
  );
  let id2 = setup
    .client
    .enqueue_upload(
      UploadSource::File(setup.make_file(b"2")),
      "client_report".to_string(),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
      None,
      None,
    )
    .unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id2.to_string()
  );

  setup
    .filesystem
    .delete_file(&super::chunked::chunk_path(&id1.to_string(), 0))
    .await
    .unwrap();

  let mut setup = setup.reinitialize().await;
  let begin = setup.allow_file_begin().await;
  assert_eq!(begin.artifact_id, id2.to_string());
  assert_eq!(begin.type_id, "client_report");
  setup.finish_single_fragment(id2, b"2").await;
}

#[tokio::test]
async fn inconsistent_state_extra_file() {
  let mut setup = Setup::new().await;
  let id1 = setup
    .client
    .enqueue_upload(
      UploadSource::File(setup.make_file(b"1")),
      "client_report".to_string(),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
      None,
      None,
    )
    .unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id1.to_string()
  );

  // Add another file that is not in the index.
  setup
    .filesystem
    .write_file(&super::ARTIFACT_UPLOAD_DIRECTORY.join("other"), b"1")
    .await
    .unwrap();

  let mut setup = setup.reinitialize().await;

  let begin = setup.allow_file_begin().await;
  assert_eq!(begin.artifact_id, id1.to_string());
  assert_eq!(begin.type_id, "client_report");
  setup.finish_single_fragment(id1, b"1").await;
  setup.upload_complete_rx.recv().await.unwrap();

  let files = setup
    .filesystem
    .list_files(&ARTIFACT_UPLOAD_DIRECTORY)
    .await
    .unwrap();
  assert_eq!(files.len(), 1);
  assert!(
    files[0].ends_with(
      &super::ARTIFACT_UPLOAD_DIRECTORY
        .join(&*REPORT_INDEX_FILE)
        .to_str()
        .unwrap()
    )
  );
}

#[tokio::test]
async fn disk_persistence() {
  let mut setup = Setup::new().await;
  let id1 = setup
    .client
    .enqueue_upload(
      UploadSource::File(setup.make_file(b"1")),
      "client_report".to_string(),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
      None,
      None,
    )
    .unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id1.to_string()
  );

  let mut setup = setup.reinitialize().await;
  let begin = setup.allow_file_begin().await;
  assert_eq!(begin.artifact_id, id1.to_string());
  assert_eq!(begin.type_id, "client_report");
  setup.finish_single_fragment(id1, b"1").await;
}

#[tokio::test]
async fn command_upload_persistence_reports_original_size() {
  let mut setup = Setup::new().await;
  let command_id = Uuid::new_v4().to_string();
  let contents = vec![42; 1000];
  let mut file = setup.make_file(&[b"prefix:".as_slice(), contents.as_slice()].concat());
  file.seek(SeekFrom::Start(7)).unwrap();
  let source_path = PathBuf::from("capture");
  setup
    .filesystem
    .write_file(&source_path, &contents)
    .await
    .unwrap();

  for (source, expected_size_bytes) in [
    (UploadSource::Bytes(contents.clone()), 1000),
    (UploadSource::File(file), 1000),
    (UploadSource::Path(source_path.clone()), 1000),
    (UploadSource::Bytes(Vec::new()), 0),
  ] {
    let (persisted_tx, persisted_rx) = tokio::sync::oneshot::channel();
    let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
    let artifact_id = setup
      .client
      .enqueue_command_upload(
        source,
        "attachment".to_string(),
        None,
        "session_id".to_string(),
        vec![],
        command_id.clone(),
        None,
        Some(persisted_tx),
        Some(completion_tx),
      )
      .unwrap();
    assert_eq!(persisted_rx.await.unwrap().unwrap(), expected_size_bytes);
    let begin = setup.allow_file_begin().await;
    assert_eq!(begin.artifact_id, artifact_id.to_string());
    assert_eq!(begin.state_metadata, command_state_metadata());
    if expected_size_bytes == 0 {
      assert_eq!(
        begin.payload_encoding.enum_value_or_default(),
        ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_RAW
      );
      assert_eq!(begin.chunks, []);
    } else {
      assert_eq!(
        begin.payload_encoding.enum_value_or_default(),
        ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_ZLIB
      );
      let upload = setup.next_chunk_operation().await;
      assert_eq!(upload.payload.artifact_id, artifact_id.to_string());
      assert_eq!(upload.payload.chunk_index, Some(0));
      let mut decoded = Vec::new();
      ZlibDecoder::new(upload.payload.contents.as_slice())
        .read_to_end(&mut decoded)
        .unwrap();
      assert_eq!(
        decoded,
        contents[.. usize::try_from(expected_size_bytes).unwrap()]
      );
      progress(upload, State::STATE_FINALIZED, &[1]);
    }
    completion_rx.await.unwrap().unwrap();
    assert_eq!(
      setup.upload_complete_rx.recv().await.unwrap(),
      artifact_id.to_string()
    );
  }
  assert!(!setup.filesystem.exists(&source_path).await.unwrap());
}

#[tokio::test]
async fn command_upload_preserves_identity_and_content_type_after_restart() {
  let setup = Setup::new().await;
  let (tx, rx) = oneshot::channel();
  let artifact_id = setup
    .client
    .enqueue_command_upload(
      UploadSource::Bytes(b"attachment".to_vec()),
      "custom_attachment".to_string(),
      None,
      "session_id".to_string(),
      vec![],
      COMMAND_ID.to_string(),
      Some("application/vnd.example.capture".to_string()),
      Some(tx),
      None,
    )
    .unwrap();
  rx.await.unwrap().unwrap();

  let mut setup = setup.reinitialize().await;
  let begin = setup.allow_file_begin().await;
  assert_eq!(begin.artifact_id, artifact_id.to_string());
  assert_eq!(begin.state_metadata, command_state_metadata());
  assert_eq!(begin.command_id.as_deref(), Some(COMMAND_ID));
  assert_eq!(
    begin.content_type.as_deref(),
    Some("application/vnd.example.capture")
  );
  let upload = setup.next_chunk_operation().await;
  assert_eq!(upload.payload.artifact_id, artifact_id.to_string());
  assert_eq!(upload.payload.command_id.as_deref(), Some(COMMAND_ID));
  progress(upload, State::STATE_FINALIZED, &[1]);
  assert_eq!(
    setup.upload_complete_rx.recv().await.unwrap(),
    artifact_id.to_string()
  );
  setup.shutdown().await;
}

#[tokio::test]
async fn workflow_upload_preserves_content_type_after_restart() {
  let mut setup = Setup::new().await;
  let artifact_id = Uuid::new_v4();
  let source = write_workflow_payload(&setup, artifact_id).await;
  let (persisted_tx, persisted_rx) = tokio::sync::oneshot::channel();
  setup
    .client
    .enqueue_workflow_attachment(
      artifact_id,
      source.clone(),
      "session_id".to_string(),
      Some("application/vnd.example.capture".to_string()),
      Some(persisted_tx),
      None,
      None,
    )
    .unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    artifact_id.to_string()
  );
  persisted_rx.await.unwrap().unwrap();

  let mut setup = setup.reinitialize().await;
  let begin = setup.allow_file_begin().await;
  assert_eq!(begin.artifact_id, artifact_id.to_string());
  assert_eq!(begin.command_id, None);
  assert_eq!(
    begin.content_type.as_deref(),
    Some("application/vnd.example.capture")
  );
  assert_eq!(
    begin.payload_encoding.enum_value().unwrap(),
    ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_ZLIB
  );
  setup
    .finish_single_fragment(artifact_id, b"attachment")
    .await;
  assert_eq!(
    setup.upload_complete_rx.recv().await.unwrap(),
    artifact_id.to_string()
  );
  assert!(setup.filesystem.exists(&source).await.unwrap());
  assert!(
    !setup
      .filesystem
      .exists(&ARTIFACT_UPLOAD_DIRECTORY.join(artifact_id.to_string()))
      .await
      .unwrap()
  );
  assert_eq!(setup.filesystem.files().len(), 2);
}

#[tokio::test]
async fn inconsistent_state_missing_index() {
  let mut setup = Setup::new().await;
  let id1 = setup
    .client
    .enqueue_upload(
      UploadSource::File(setup.make_file(b"1")),
      "client_report".to_string(),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
      None,
      None,
    )
    .unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id1.to_string()
  );

  setup
    .filesystem
    .delete_file(&super::ARTIFACT_UPLOAD_DIRECTORY.join(&*REPORT_INDEX_FILE))
    .await
    .unwrap();

  let mut setup = setup.reinitialize().await;

  let id2 = setup
    .client
    .enqueue_upload(
      UploadSource::File(setup.make_file(b"2")),
      "client_report".to_string(),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
      None,
      None,
    )
    .unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id2.to_string()
  );

  let begin = setup.allow_file_begin().await;
  assert_eq!(begin.artifact_id, id2.to_string());
  assert_eq!(begin.type_id, "client_report");
  setup.finish_single_fragment(id2, b"2").await;

  // The id1 file should have been cleaned out due to not being referenced by the index file.
  assert!(
    !setup
      .filesystem
      .exists(&super::chunked::chunk_path(&id1.to_string(), 0))
      .await
      .unwrap()
  );
}

#[tokio::test]
async fn new_entry_disk_full() {
  let mut setup = Setup::new().await;
  setup.filesystem.disk_full.store(true, Ordering::SeqCst);

  let id1 = setup
    .client
    .enqueue_upload(
      UploadSource::File(setup.make_file(b"1")),
      "client_report".to_string(),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
      None,
      None,
    )
    .unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id1.to_string()
  );

  assert_eq!(setup.filesystem.files().len(), 0);

  assert!(
    timeout(100.std_milliseconds(), setup.data_upload_rx.recv())
      .await
      .is_err()
  );
}

#[tokio::test]
async fn new_entry_disk_full_after_received() {
  let mut setup = Setup::new().await;

  let id1 = setup
    .client
    .enqueue_upload(
      UploadSource::File(setup.make_file(b"1")),
      "client_report".to_string(),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
      None,
      None,
    )
    .unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id1.to_string()
  );

  // The disk becomes full aka not writeable at this point. We'll continue to process the upload
  // since even if we were not able to update the index on disk.
  setup.filesystem.disk_full.store(true, Ordering::SeqCst);
  assert_eq!(setup.filesystem.files().len(), 2);

  let begin = setup.allow_file_begin().await;
  assert_eq!(begin.artifact_id, id1.to_string());
  assert_eq!(begin.type_id, "client_report");
  setup.finish_single_fragment(id1, b"1").await;
}

#[tokio::test]
async fn intent_retries() {
  let mut setup = Setup::new().await;

  let id1 = setup
    .client
    .enqueue_upload(
      UploadSource::File(setup.make_file(b"1")),
      "client_report".to_string(),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
      None,
      None,
    )
    .unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id1.to_string()
  );

  setup.chunk_transfer_enabled.send(true).unwrap();
  let first = setup.next_chunk_operation().await;
  assert_eq!(first.payload.artifact_id, id1.to_string());
  assert_eq!(first.payload.type_id, "client_report");
  drop(first); // Mimic a disconnect.
  let retry = setup.allow_file_begin().await;
  assert_eq!(retry.artifact_id, id1.to_string());
  assert_eq!(retry.type_id, "client_report");
  setup.finish_single_fragment(id1, b"1").await;
}

#[tokio::test]
async fn retries_scrub_metadata_with_the_rules_current_at_each_attempt() {
  let mut setup = Setup::new().await;
  let make_rules = |rules| {
    let (rules, failures) = ScrubRules::new(ScrubConfiguration {
      rules,
      ..Default::default()
    });
    assert_eq!(failures, 0);
    Arc::new(rules)
  };

  setup
    .client
    .enqueue_upload(
      UploadSource::File(setup.make_file(b"1")),
      "client_report".to_string(),
      [
        ("app_id".into(), "com.example.app".into()),
        ("secret".into(), "hunter2".into()),
        ("keep".into(), "value".into()),
      ]
      .into(),
      None,
      "session_id".to_string(),
      vec![],
      None,
      None,
    )
    .unwrap();

  // The first Begin attempt goes out before any rules are known. Rules arrive before it fails.
  setup.chunk_transfer_enabled.send(true).unwrap();
  let first = setup.next_chunk_operation().await;
  assert_eq!(
    first.payload.state_metadata,
    string_metadata(&[("app_id", "com.example.app")])
  );
  setup
    .scrub_rules_tx
    .send_replace(make_rules(vec![scrub_rule(remove_field!("secret"))]));
  drop(first);

  // The retried Begin still sends identity metadata; the final chunk carries the scrubbed full map.
  let retry = setup.allow_file_begin().await;
  assert_eq!(
    retry.state_metadata,
    string_metadata(&[("app_id", "com.example.app")])
  );

  let upload = setup.next_chunk_operation().await;
  assert_eq!(
    upload.payload.state_metadata,
    string_metadata(&[("app_id", "com.example.app"), ("keep", "value")])
  );
  setup.scrub_rules_tx.send_replace(make_rules(vec![
    scrub_rule(remove_field!("app_id")),
    scrub_rule(remove_field!("secret")),
    scrub_rule(remove_field!("keep")),
  ]));
  progress(upload, State::STATE_RETRYABLE, &[]);
  assert!(setup.allow_file_begin().await.state_metadata.is_empty());
  let final_chunk = setup
    .finish_single_fragment(Uuid::parse_str(&retry.artifact_id).unwrap(), b"1")
    .await;
  assert!(final_chunk.state_metadata.is_empty());
}

#[tokio::test]
async fn intent_drop() {
  let mut setup = Setup::new().await;

  let id1 = setup
    .client
    .enqueue_upload(
      UploadSource::File(setup.make_file(b"1")),
      "client_report".to_string(),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
      None,
      None,
    )
    .unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id1.to_string()
  );

  setup.chunk_transfer_enabled.send(true).unwrap();
  let begin = setup.next_chunk_operation().await;
  assert_eq!(begin.payload.artifact_id, id1.to_string());
  assert_eq!(begin.payload.type_id, "client_report");
  assert_eq!(begin.payload.contents.as_slice(), b"");
  progress(begin, State::STATE_DROPPED, &[]);

  assert!(
    timeout(100.std_milliseconds(), setup.data_upload_rx.recv())
      .await
      .is_err()
  );
}

#[tokio::test]
async fn file_begin_retries_apply_current_scrub_rules_without_payload_or_manifest_changes() {
  let mut setup = Setup::new().await;
  let (tx, rx) = oneshot::channel();
  setup
    .client
    .enqueue_command_upload(
      UploadSource::Bytes(b"attachment".to_vec()),
      "device_command_attachment".into(),
      None,
      "session".into(),
      Vec::new(),
      COMMAND_ID.into(),
      None,
      Some(tx),
      None,
    )
    .unwrap();
  rx.await.unwrap().unwrap();
  let make_rules = |keys: &[&str]| {
    let (rules, failures) = ScrubRules::new(ScrubConfiguration {
      rules: keys
        .iter()
        .map(|key| scrub_rule(remove_field!(key)))
        .collect(),
      ..Default::default()
    });
    assert_eq!(failures, 0);
    Arc::new(rules)
  };
  setup.scrub_rules_tx.send_replace(make_rules(&["app_id"]));
  setup.chunk_transfer_enabled.send(true).unwrap();
  let first = setup.next_chunk_operation().await;
  assert_eq!(first.payload.chunk_index, None);
  assert_eq!(first.payload.contents.as_slice(), b"");
  assert!(!first.payload.state_metadata.contains_key("app_id"));
  assert!(first.payload.state_metadata.contains_key("platform"));
  let mut original = first.payload.clone();
  setup
    .scrub_rules_tx
    .send_replace(make_rules(&["app_id", "platform"]));
  drop(first);
  let retry = setup.next_chunk_operation().await;
  assert_eq!(retry.payload.chunk_index, None);
  assert_eq!(retry.payload.contents.as_slice(), b"");
  assert!(!retry.payload.state_metadata.contains_key("app_id"));
  assert!(!retry.payload.state_metadata.contains_key("platform"));
  original.upload_uuid.clone_from(&retry.payload.upload_uuid);
  original
    .state_metadata
    .clone_from(&retry.payload.state_metadata);
  assert_eq!(original, retry.payload);
  progress(retry, State::STATE_RECEIVING, &[0]);
  let payload = setup.next_chunk_operation().await;
  assert!(!payload.payload.state_metadata.contains_key("app_id"));
  assert!(!payload.payload.state_metadata.contains_key("platform"));
  progress(payload, State::STATE_FINALIZED, &[1]);
  setup.upload_complete_rx.recv().await.unwrap();
  setup.shutdown().await;
}

#[tokio::test]
async fn command_upload_intent_drop_completes_with_failure() {
  let mut setup = Setup::new().await;
  let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
  let id = setup
    .client
    .enqueue_command_upload(
      UploadSource::File(setup.make_file(b"screenshot")),
      "screenshot".to_string(),
      None,
      "session_id".to_string(),
      vec![],
      COMMAND_ID.to_string(),
      None,
      None,
      Some(completion_tx),
    )
    .unwrap();
  setup.chunk_transfer_enabled.send(true).unwrap();
  let begin = setup.next_chunk_operation().await;
  assert_eq!(begin.payload.artifact_id, id.to_string());
  assert_eq!(begin.payload.chunk_index, None);
  assert_eq!(begin.payload.contents.as_slice(), b"");
  progress(begin, State::STATE_DROPPED, &[]);

  assert_matches!(completion_rx.await.unwrap(), Err(error) if error.contains("intent negotiation"));
  assert!(setup.data_upload_rx.try_recv().is_err());
  let index = read_compressed_protobuf::<ArtifactUploadIndex>(
    &setup
      .filesystem
      .read_file(&ARTIFACT_UPLOAD_DIRECTORY.join(&*REPORT_INDEX_FILE))
      .await
      .unwrap(),
  )
  .unwrap();
  assert_eq!(index.chunked_artifact, []);
  setup.shutdown().await;
}

#[tokio::test]
async fn command_upload_rejects_oversized_file_and_path_sources_before_persisting() {
  let setup = Setup::new().await;
  let command_id = Uuid::new_v4().to_string();
  setup
    .runtime
    .update_snapshot(bd_test_helpers::runtime::make_update(
      vec![(
        chunked_artifact_upload::MaxLogicalBytes::path(),
        ValueKind::Int(3),
      )],
      "attachment limit".to_string(),
    ))
    .await
    .unwrap();

  let (file_persisted_tx, file_persisted_rx) = tokio::sync::oneshot::channel();
  let file_id = setup
    .client
    .enqueue_command_upload(
      UploadSource::File(setup.make_file(b"four")),
      "attachment".to_string(),
      None,
      "session_id".to_string(),
      vec![],
      command_id.clone(),
      None,
      Some(file_persisted_tx),
      None,
    )
    .unwrap();
  assert_matches!(
    file_persisted_rx.await.unwrap(),
    Err(EnqueueError::Other(_))
  );
  assert!(
    !setup
      .filesystem
      .exists(&ARTIFACT_UPLOAD_DIRECTORY.join(file_id.to_string()))
      .await
      .unwrap()
  );

  let (bytes_persisted_tx, bytes_persisted_rx) = tokio::sync::oneshot::channel();
  let bytes_id = setup
    .client
    .enqueue_command_upload(
      UploadSource::Bytes(b"four".to_vec()),
      "attachment".to_string(),
      None,
      "session_id".to_string(),
      vec![],
      command_id.clone(),
      None,
      Some(bytes_persisted_tx),
      None,
    )
    .unwrap();
  assert_matches!(
    bytes_persisted_rx.await.unwrap(),
    Err(EnqueueError::Other(_))
  );
  assert!(
    !setup
      .filesystem
      .exists(&ARTIFACT_UPLOAD_DIRECTORY.join(bytes_id.to_string()))
      .await
      .unwrap()
  );

  let path = Path::new("command-attachment");
  setup.filesystem.write_file(path, b"four").await.unwrap();
  let (path_persisted_tx, path_persisted_rx) = tokio::sync::oneshot::channel();
  let path_id = setup
    .client
    .enqueue_command_upload(
      UploadSource::Path(path.into()),
      "attachment".to_string(),
      None,
      "session_id".to_string(),
      vec![],
      command_id,
      None,
      Some(path_persisted_tx),
      None,
    )
    .unwrap();
  assert_matches!(
    path_persisted_rx.await.unwrap(),
    Err(EnqueueError::Other(_))
  );
  assert!(setup.filesystem.exists(path).await.unwrap());
  assert!(
    !setup
      .filesystem
      .exists(&ARTIFACT_UPLOAD_DIRECTORY.join(path_id.to_string()))
      .await
      .unwrap()
  );
}

#[tokio::test]
async fn command_upload_negative_ack_retries_without_stopping_uploader() {
  let mut setup = Setup::new().await;
  let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
  let id = setup
    .client
    .enqueue_command_upload(
      UploadSource::File(setup.make_file(b"screenshot")),
      "screenshot".to_string(),
      None,
      "session_id".to_string(),
      vec![],
      COMMAND_ID.to_string(),
      None,
      None,
      Some(completion_tx),
    )
    .unwrap();
  setup.allow_file_begin().await;
  let upload = setup.next_chunk_operation().await;
  upload
    .response_tx
    .send(UploadArtifactResponse {
      upload_uuid: upload.uuid,
      state: State::STATE_RETRYABLE.into(),
      error: "temporary failure".into(),
      ..Default::default()
    })
    .unwrap();
  let resumed = setup.allow_file_begin().await;
  assert_eq!(resumed.artifact_id, id.to_string());
  assert_eq!(resumed.command_id.as_deref(), Some(COMMAND_ID));
  let upload = setup.next_chunk_operation().await;
  assert_eq!(upload.payload.artifact_id, id.to_string());
  progress(upload, State::STATE_FINALIZED, &[1]);
  completion_rx.await.unwrap().unwrap();

  let follow_up_id = setup
    .client
    .enqueue_upload(
      UploadSource::File(setup.make_file(b"report")),
      "client_report".to_string(),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
      None,
      None,
    )
    .unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    follow_up_id.to_string()
  );
}

#[tokio::test]
async fn corrupt_command_fragments_fail_without_retry_and_release_owned_bytes() {
  for corruption in ["checksum", "short", "oversized", "missing"] {
    let mut setup = Setup::new().await;
    let (completion_tx, completion_rx) = oneshot::channel();
    let id = setup
      .client
      .enqueue_command_upload(
        UploadSource::File(setup.make_file(b"screenshot")),
        "screenshot".to_string(),
        None,
        "session_id".to_string(),
        vec![],
        COMMAND_ID.to_string(),
        None,
        None,
        Some(completion_tx),
      )
      .unwrap();
    setup.chunk_transfer_enabled.send(true).unwrap();
    let begin = setup.next_chunk_operation().await;
    let path = super::chunked::chunk_path(&id.to_string(), 0);
    if corruption == "missing" {
      setup.filesystem.delete_file(&path).await.unwrap();
    } else {
      let mut contents = setup.filesystem.read_file(&path).await.unwrap();
      match corruption {
        "checksum" => contents[0] ^= 1,
        "short" => {
          contents.pop();
        },
        "oversized" => {
          contents.resize(usize::try_from(super::chunked::CHUNK_BYTES).unwrap() + 1, 0);
        },
        _ => panic!("unexpected corruption: {corruption}"),
      }
      setup.filesystem.write_file(&path, &contents).await.unwrap();
    }
    progress(begin, State::STATE_RECEIVING, &[0]);
    let error = timeout(5.std_seconds(), completion_rx)
      .await
      .unwrap()
      .unwrap()
      .unwrap_err();
    assert!(
      error.contains("invalid staged fragment"),
      "{corruption}: {error}"
    );
    assert!(!setup.filesystem.exists(&path).await.unwrap());
    let index = read_compressed_protobuf::<ArtifactUploadIndex>(
      &setup
        .filesystem
        .read_file(&ARTIFACT_UPLOAD_DIRECTORY.join(&*REPORT_INDEX_FILE))
        .await
        .unwrap(),
    )
    .unwrap();
    assert_eq!(index.chunked_artifact, []);
    assert!(setup.data_upload_rx.try_recv().is_err());
    assert!(setup.upload_complete_rx.try_recv().is_err());
    setup.shutdown().await;
  }
}

#[tokio::test]
async fn command_upload_retries_with_the_same_artifact_and_command_ids() {
  let mut setup = Setup::new().await;
  let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
  let id = setup
    .client
    .enqueue_command_upload(
      UploadSource::File(setup.make_file(b"screenshot")),
      "screenshot".to_string(),
      None,
      "session_id".to_string(),
      vec![],
      COMMAND_ID.to_string(),
      Some("image/jpeg".to_string()),
      None,
      Some(completion_tx),
    )
    .unwrap();
  for acknowledge in [false, true] {
    let begin = setup.allow_file_begin().await;
    assert_eq!(begin.artifact_id, id.to_string());
    assert_eq!(begin.command_id.as_deref(), Some(COMMAND_ID));
    assert_eq!(begin.content_type.as_deref(), Some("image/jpeg"));
    assert_eq!(
      begin.payload_encoding.enum_value_or_default(),
      ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_ZLIB
    );
    let upload = setup.next_chunk_operation().await;
    assert_eq!(upload.payload.artifact_id, id.to_string());
    assert_eq!(upload.payload.command_id.as_deref(), Some(COMMAND_ID));
    assert_eq!(upload.payload.chunk_index, Some(0));
    let mut contents = Vec::new();
    flate2::read::ZlibDecoder::new(upload.payload.contents.as_slice())
      .read_to_end(&mut contents)
      .unwrap();
    assert_eq!(contents, b"screenshot");
    if acknowledge {
      progress(upload, State::STATE_FINALIZED, &[1]);
    }
  }
  completion_rx.await.unwrap().unwrap();
}

#[tokio::test]
async fn upload_retries() {
  let mut setup = Setup::new().await;

  let id1 = setup
    .client
    .enqueue_upload(
      UploadSource::File(setup.make_file(b"1")),
      "client_report".to_string(),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
      None,
      None,
    )
    .unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id1.to_string()
  );

  let begin = setup.allow_file_begin().await;
  let first = setup.next_chunk_operation().await;
  assert_eq!(first.payload.artifact_id, id1.to_string());
  assert_eq!(first.payload.chunk_index, Some(0));
  drop(first); // Mimic a disconnect after the last fragment was sent.
  let retry = setup.allow_file_begin().await;
  assert_eq!(retry.chunks, begin.chunks);
  assert_eq!(retry.artifact_id, begin.artifact_id);
  setup.finish_single_fragment(id1, b"1").await;
}

#[tokio::test]
async fn enqueue_upload_acknowledges_after_disk_persist() {
  let mut setup = Setup::new().await;

  let (persisted_tx, persisted_rx) = tokio::sync::oneshot::channel();
  let id = setup
    .client
    .enqueue_upload(
      UploadSource::File(setup.make_file(b"snapshot")),
      "state_snapshot".to_string(),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
      None,
      Some(persisted_tx),
    )
    .unwrap();

  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id.to_string()
  );
  persisted_rx.await.unwrap().unwrap();
}

#[tokio::test]
async fn rejected_command_upload_deletes_owned_source() {
  let mut setup = Setup::new().await;
  setup.task_handle.abort();
  assert!((&mut setup.task_handle).await.unwrap_err().is_cancelled());
  std::fs::create_dir_all(".tmp").unwrap();
  let directory = tempfile::TempDir::with_prefix_in("command-source", ".tmp").unwrap();
  let source = directory.path().canonicalize().unwrap().join("payload");
  std::fs::write(&source, b"payload").unwrap();
  assert!(matches!(
    setup.client.enqueue_command_upload(
      UploadSource::Path(source.clone()),
      "device_command_attachment".into(),
      None,
      "session".into(),
      vec![],
      Uuid::new_v4().to_string(),
      None,
      None,
      None,
    ),
    Err(EnqueueError::Closed)
  ));
  assert!(!source.exists());
}

#[tokio::test]
async fn command_source_is_consumed_before_persistence_ack() {
  let setup = Setup::new().await;
  std::fs::create_dir_all(".tmp").unwrap();
  let directory = tempfile::TempDir::with_prefix_in("command-source", ".tmp").unwrap();
  let source = directory.path().canonicalize().unwrap().join("payload");
  std::fs::write(&source, b"payload").unwrap();
  let (persisted_tx, persisted_rx) = tokio::sync::oneshot::channel();
  let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();
  setup
    .client
    .enqueue_command_upload(
      UploadSource::Path(source.clone()),
      "device_command_attachment".into(),
      None,
      "session".into(),
      vec![],
      Uuid::new_v4().to_string(),
      None,
      Some(persisted_tx),
      Some(completed_tx),
    )
    .unwrap();
  assert_eq!(persisted_rx.await.unwrap().unwrap(), 7);
  assert!(!source.exists());
  std::fs::write(&source, b"replacement").unwrap();
  drop(completed_rx);
  setup.shutdown().await;
  assert_eq!(std::fs::read(source).unwrap(), b"replacement");
}

enum PersistenceFailure {
  Capacity,
  Payload,
  Index,
  DuplicateId,
}

async fn assert_command_source_consumed_before_error_ack(failure: PersistenceFailure) {
  fs::create_dir_all(".tmp").unwrap();
  let directory = tempfile::TempDir::with_prefix_in("command-source", ".tmp").unwrap();
  let source_path = directory.path().canonicalize().unwrap().join("payload");
  fs::write(&source_path, b"payload").unwrap();
  let filesystem = Arc::new(TestFileSystem::new());
  let runtime = TestConfigLoader::new_in_directory(directory.path()).await;
  let (data_upload_tx, _data_upload_rx) = mpsc::channel(1);
  let shutdown = bd_shutdown::ComponentShutdownTrigger::default();
  let (mut uploader, _client) = Uploader::new(
    filesystem.clone(),
    data_upload_tx,
    Arc::new(TestTimeProvider::new(OffsetDateTime::now_utc())),
    &runtime,
    &Collector::default(),
    command_metadata(),
    no_scrub_rules(),
    shutdown.make_shutdown(),
  );
  uploader.initialize().await.unwrap();
  let id = Uuid::new_v4();
  match failure {
    PersistenceFailure::Capacity => {
      runtime
        .update_snapshot(bd_test_helpers::runtime::make_simple_update(vec![(
          chunked_artifact_upload::MaxLogicalBytes::path(),
          ValueKind::Int(1),
        )]))
        .await
        .unwrap();
    },
    PersistenceFailure::Payload => filesystem.disk_full.store(true, Ordering::Relaxed),
    PersistenceFailure::Index => {
      filesystem
        .create_dir(
          &ARTIFACT_UPLOAD_DIRECTORY
            .join(&*REPORT_INDEX_FILE)
            .with_extension("tmp"),
        )
        .await
        .unwrap();
    },
    PersistenceFailure::DuplicateId => uploader.chunked.entries.push(UploadArtifactRequest {
      artifact_id: id.to_string(),
      ..Default::default()
    }),
  }
  let (upload_complete_tx, _upload_complete_rx) = mpsc::channel(1);
  let (entry_received_tx, _entry_received_rx) = mpsc::channel(1);
  let (persistence_ack_tx, mut persistence_ack_rx) = mpsc::channel(1);
  uploader.test_hooks = Some(Arc::new(UploadTestHooks {
    chunk_transfer_gate: None,
    initialized_tx: None,
    upload_complete_tx,
    entry_received_tx,
    persistence_ack_tx: Some(persistence_ack_tx),
    observe_preparation: AtomicBool::new(false),
    notify_preparation_error: AtomicBool::new(false),
  }));
  let (persisted_tx, persisted_rx) = oneshot::channel();
  let source = UploadSource::Path(source_path.clone());
  let mut upload = NewUpload::new(
    source,
    bd_proto::protos::client::api::UploadArtifactRequest {
      artifact_id: id.to_string(),
      type_id: "device_command_attachment".into(),
      session_id: "session".into(),
      command_id: Some(Uuid::new_v4().to_string()),
      time: OffsetDateTime::UNIX_EPOCH.into_proto(),
      ..Default::default()
    },
  );
  upload.persisted_tx = Some(PersistenceAck::Command(persisted_tx));
  tokio::join!(uploader.track_upload(upload), async {
    let error = persisted_rx.await.unwrap().unwrap_err();
    match failure {
      PersistenceFailure::Capacity => assert!(error.to_string().contains("logical byte admission")),
      PersistenceFailure::Payload => assert!(error.to_string().contains("insufficient free space")),
      PersistenceFailure::Index => assert!(error.to_string().contains("failed to create file")),
      PersistenceFailure::DuplicateId => {
        assert!(
          error
            .to_string()
            .contains("artifact ID already has chunked ownership")
        );
      },
    }
    let resume = persistence_ack_rx.recv().await.unwrap();
    assert!(!source_path.exists());
    fs::write(&source_path, b"replacement").unwrap();
    resume.send(()).unwrap();
  });
  assert_eq!(fs::read(source_path).unwrap(), b"replacement");
}

#[tokio::test]
async fn command_source_is_consumed_before_capacity_error_ack() {
  assert_command_source_consumed_before_error_ack(PersistenceFailure::Capacity).await;
}

#[tokio::test]
async fn command_source_is_consumed_before_payload_error_ack() {
  assert_command_source_consumed_before_error_ack(PersistenceFailure::Payload).await;
}

#[tokio::test]
async fn command_source_is_consumed_before_index_error_ack() {
  assert_command_source_consumed_before_error_ack(PersistenceFailure::Index).await;
}

#[tokio::test]
async fn command_source_is_consumed_before_duplicate_id_error_ack() {
  assert_command_source_consumed_before_error_ack(PersistenceFailure::DuplicateId).await;
}

#[tokio::test]
async fn enqueue_upload_from_path_acknowledges_after_disk_persist_and_removes_source() {
  let mut setup = Setup::new().await;

  let source_path = std::path::PathBuf::from("source_snapshot.zz");
  setup
    .filesystem
    .write_file(&source_path, b"snapshot")
    .await
    .unwrap();

  let (persisted_tx, persisted_rx) = tokio::sync::oneshot::channel();
  let id = setup
    .client
    .enqueue_upload(
      UploadSource::Path(source_path.clone()),
      "state_snapshot".to_string(),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
      None,
      Some(persisted_tx),
    )
    .unwrap();

  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id.to_string()
  );
  persisted_rx.await.unwrap().unwrap();
  assert!(!setup.filesystem.exists(&source_path).await.unwrap());
}

#[tokio::test]
async fn workflow_upload_preserves_id_source_and_sends_zlib_payload() {
  let mut setup = Setup::new().await;
  let id = Uuid::new_v4();
  let source = write_workflow_payload(&setup, id).await;

  let (persisted_tx, persisted_rx) = tokio::sync::oneshot::channel();
  let (abandoned_tx, abandoned_rx) = oneshot::channel();
  setup
    .client
    .enqueue_workflow_attachment(
      id,
      source.clone(),
      "session_id".into(),
      Some("application/vnd.example.capture".to_string()),
      Some(persisted_tx),
      Some(abandoned_tx),
      None,
    )
    .unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id.to_string()
  );
  persisted_rx.await.unwrap().unwrap();
  drop(abandoned_rx);
  assert!(setup.filesystem.exists(&source).await.unwrap());
  let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
  let (duplicate_persisted_tx, duplicate_persisted_rx) = tokio::sync::oneshot::channel();
  setup
    .client
    .enqueue_workflow_attachment(
      id,
      source.clone(),
      "session_id".into(),
      Some("application/vnd.example.capture".to_string()),
      Some(duplicate_persisted_tx),
      Some(completion_tx),
      None,
    )
    .unwrap();
  duplicate_persisted_rx.await.unwrap().unwrap();
  let (conflict_tx, conflict_rx) = tokio::sync::oneshot::channel();
  setup
    .client
    .enqueue_workflow_attachment(
      id,
      source.clone(),
      "other_session".into(),
      None,
      Some(conflict_tx),
      None,
      None,
    )
    .unwrap();
  assert_matches!(conflict_rx.await.unwrap(), Err(EnqueueError::Other(_)));
  assert!(
    !setup
      .filesystem
      .exists(&ARTIFACT_UPLOAD_DIRECTORY.join(id.to_string()))
      .await
      .unwrap()
  );

  let begin = setup.allow_file_begin().await;
  assert_eq!(begin.artifact_id, id.to_string());
  assert_eq!(begin.type_id, "workflow_attachment");
  assert_eq!(
    begin.content_type.as_deref(),
    Some("application/vnd.example.capture")
  );
  setup.finish_single_fragment(id, b"attachment").await;
  completion_rx.await.unwrap().unwrap();
}

#[tokio::test]
async fn workflow_upload_rejects_id_owned_by_another_artifact_type() {
  let mut setup = Setup::new().await;
  let (persisted_tx, persisted_rx) = tokio::sync::oneshot::channel();
  let id = setup
    .client
    .enqueue_upload(
      UploadSource::Bytes(b"existing artifact".to_vec()),
      "state_snapshot".to_string(),
      [].into(),
      None,
      "session_id".to_string(),
      vec![],
      None,
      Some(persisted_tx),
    )
    .unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    id.to_string()
  );
  persisted_rx.await.unwrap().unwrap();

  let (persisted_tx, persisted_rx) = tokio::sync::oneshot::channel();
  setup
    .client
    .enqueue_workflow_attachment(
      id,
      std::path::PathBuf::from(format!("workflow-attachments/{id}.raw")),
      "session_id".to_string(),
      None,
      Some(persisted_tx),
      None,
      None,
    )
    .unwrap();

  assert_matches!(persisted_rx.await.unwrap(), Err(EnqueueError::Other(_)));
}

#[tokio::test]
async fn path_upload_restores_source_if_index_persistence_fails() {
  let setup = Setup::new().await;
  let source_path = std::path::PathBuf::from("retryable_snapshot.zz");
  setup
    .filesystem
    .write_file(&source_path, b"snapshot")
    .await
    .unwrap();
  setup.filesystem.disk_full.store(true, Ordering::Relaxed);

  let (persisted_tx, persisted_rx) = tokio::sync::oneshot::channel();
  let id = setup
    .client
    .enqueue_upload(
      UploadSource::Path(source_path.clone()),
      "state_snapshot".to_string(),
      [].into(),
      None,
      String::new(),
      vec![],
      None,
      Some(persisted_tx),
    )
    .unwrap();

  assert!(persisted_rx.await.unwrap().is_err());
  assert!(setup.filesystem.exists(&source_path).await.unwrap());
  assert!(
    !setup
      .filesystem
      .exists(&ARTIFACT_UPLOAD_DIRECTORY.join(id.to_string()))
      .await
      .unwrap()
  );
}

#[tokio::test]
async fn snapshot_upload_preserves_journal_bytes() {
  let mut setup = Setup::new().await;
  let mut revision = setup.client.upload_queue_revision();
  wait_for_queue_revision(&mut revision, 0).await;
  let source = PathBuf::from("snapshot.zz");
  let bytes = b"self-validating snapshot bytes";
  setup.filesystem.write_file(&source, bytes).await.unwrap();
  let (tx, rx) = oneshot::channel();
  let id = setup
    .client
    .enqueue_upload(
      UploadSource::Path(source.clone()),
      "state_snapshot".into(),
      [].into(),
      None,
      String::new(),
      vec![],
      None,
      Some(tx),
    )
    .unwrap();
  rx.await.unwrap().unwrap();
  setup.entry_received_rx.recv().await.unwrap();
  assert!(!setup.filesystem.exists(&source).await.unwrap());
  let begin = setup.allow_file_begin().await;
  assert_eq!(begin.artifact_id, id.to_string());
  assert_eq!(begin.type_id, "state_snapshot");
  assert_eq!(begin.session_id, "");
  setup.finish_single_fragment(id, bytes).await;
  setup.upload_complete_rx.recv().await.unwrap();
  setup.shutdown().await;
}
