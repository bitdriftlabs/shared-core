// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used, clippy::panic)]

use super::admission::FREE_SPACE_HEADROOM;
use super::chunked::{CHUNK_BYTES, MAX_INDEX_BYTES, chunk_path, source_path};
use super::tests::{Setup, progress, wait_for_queue_revision};
use super::{
  ARTIFACT_UPLOAD_DIRECTORY,
  ArtifactStore,
  Client,
  EnqueueError,
  NewUpload,
  PersistenceAck,
  REPORT_INDEX_FILE,
  SnappedFeatureFlag,
  UploadSource,
  Uploader,
};
use bd_api::DataUpload;
use bd_api::upload::TrackedArtifactUpload;
use bd_client_common::artifact::MAX_ARTIFACT_LOGICAL_BYTES;
use bd_client_common::file::{
  read_compressed,
  read_compressed_protobuf,
  write_compressed_protobuf,
};
use bd_client_common::file_system::{FileSystem, RealFileSystem};
use bd_client_common::test::TestFileSystem;
use bd_log_primitives::{DataValue, LogFields};
use bd_metadata::artifact::artifact_encoded_bound;
use bd_proto::protos::client::api::upload_artifact_response::State;
use bd_proto::protos::client::api::{
  ArtifactChunkDescriptor,
  ArtifactPayloadEncoding,
  CrashProfileMetadata,
  UploadArtifactRequest,
  UploadArtifactResponse,
};
use bd_proto::protos::client::artifact::artifact_upload_index::Artifact;
use bd_proto::protos::client::artifact::{ArtifactUploadIndex, WorkflowAttachmentState};
use bd_proto::protos::logging::payload::Data;
use bd_proto::protos::logging::payload::data::Data_type;
use bd_runtime::runtime::{FeatureFlag as _, chunked_artifact_upload};
use bd_runtime::test::TestConfigLoader;
use bd_test_helpers::runtime::{ValueKind, make_update};
use bd_time::{OffsetDateTimeExt as _, TimeProvider as _};
use flate2::Compression;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use protobuf::Message;
use rand::rngs::StdRng;
use rand::{RngExt as _, SeedableRng as _};
use sha2::{Digest, Sha256};
use std::io::{Read, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::Instant;
use tokio::sync::oneshot;
use uuid::Uuid;

impl Setup {
  // Keep tiny protocol fixtures independent of routing thresholds. Validation and persistence still
  // run through the production staging transaction.
  fn enqueue_capture(
    &self,
    path: PathBuf,
    request: UploadArtifactRequest,
    persisted_tx: oneshot::Sender<Result<u64, EnqueueError>>,
    completion_tx: Option<oneshot::Sender<Result<(), String>>>,
  ) -> Result<(), EnqueueError> {
    let mut upload = NewUpload::new(UploadSource::Path(path), request);
    upload.persisted_tx = Some(PersistenceAck::Command(persisted_tx));
    upload.completion_tx = completion_tx;
    self.client.submit(upload)
  }

  async fn configure_admission(&self, extra: Vec<(&str, ValueKind)>) {
    self
      .runtime
      .update_snapshot(make_update(extra, Uuid::new_v4().to_string()))
      .await
      .unwrap();
  }

  async fn capture(&self, id: Uuid, bytes: &[u8]) -> PathBuf {
    let path = self.filesystem.directory().join(format!("capture-{id}"));
    self.filesystem.write_file(&path, bytes).await.unwrap();
    path
  }

  fn stage_capture(
    &self,
    path: PathBuf,
    request: UploadArtifactRequest,
  ) -> oneshot::Receiver<Result<u64, EnqueueError>> {
    let (tx, rx) = oneshot::channel();
    self.enqueue_capture(path, request, tx, None).unwrap();
    rx
  }

  async fn chunk_index(&self) -> ArtifactUploadIndex {
    let bytes = self
      .filesystem
      .read_file(&ARTIFACT_UPLOAD_DIRECTORY.join(&*REPORT_INDEX_FILE))
      .await
      .unwrap();
    read_compressed_protobuf(&bytes).unwrap()
  }

  async fn write_chunk_index(&self, index: &ArtifactUploadIndex) {
    self.abort_for_recovery();
    self
      .filesystem
      .write_file(
        &ARTIFACT_UPLOAD_DIRECTORY.join(&*REPORT_INDEX_FILE),
        &write_compressed_protobuf(index).unwrap(),
      )
      .await
      .unwrap();
  }
  fn request(&self, id: Uuid) -> UploadArtifactRequest {
    let now = self.time_provider.now();
    UploadArtifactRequest {
      artifact_id: id.to_string(),
      command_id: Some(Uuid::new_v4().to_string()),
      type_id: "java_heap_dump".into(),
      session_id: "session".into(),
      time: now.into_proto(),
      content_type: Some("application/octet-stream".into()),
      ..Default::default()
    }
  }
}

fn thin(template: &UploadArtifactRequest, operation: &TrackedArtifactUpload, index: u32) {
  let request = &operation.payload;
  let expected = UploadArtifactRequest {
    upload_uuid: operation.uuid.clone(),
    artifact_id: template.artifact_id.clone(),
    command_id: template.command_id.clone(),
    logical_bytes: template.logical_bytes,
    chunk_index: Some(index),
    contents: request.contents.clone(),
    ..Default::default()
  };
  assert_eq!(request, &expected);
  assert_eq!(
    Sha256::digest(&request.contents).as_slice(),
    template.chunks[index as usize].encoded_sha256
  );
}

#[tokio::test]
async fn commands_and_profiles_negotiate_then_transfer_one_encoded_fragment_at_every_logical_size()
{
  for (profile, source_kind) in [false, true]
    .into_iter()
    .flat_map(|profile| ["path", "file", "bytes"].map(|kind| (profile, kind)))
  {
    for size in [CHUNK_BYTES - 1, CHUNK_BYTES, CHUNK_BYTES + 1] {
      let mut setup = Setup::with_buffer(2 * 1024 * 1024).await;
      let bytes = vec![42; usize::try_from(size).unwrap()];
      let path = setup.capture(Uuid::new_v4(), &bytes).await;
      let source = match source_kind {
        "path" => UploadSource::Path(path.clone()),
        "file" => {
          tokio::fs::write(&path, [b"prefix".as_slice(), &bytes].concat())
            .await
            .unwrap();
          let mut file = std::fs::File::open(&path).unwrap();
          file.seek(SeekFrom::Start(6)).unwrap();
          UploadSource::File(file)
        },
        "bytes" => UploadSource::Bytes(bytes.clone()),
        _ => panic!("unknown test source"),
      };
      let id = if profile {
        let (tx, rx) = oneshot::channel();
        let id = setup
          .client
          .enqueue_crash_profile(
            source,
            setup.time_provider.now(),
            "session".into(),
            "application/octet-stream".into(),
            CrashProfileMetadata {
              report_ids: vec![Uuid::new_v4().to_string()],
              ..Default::default()
            },
            Some(tx),
          )
          .unwrap();
        rx.await.unwrap().unwrap();
        id
      } else {
        let (tx, rx) = oneshot::channel();
        let id = setup
          .client
          .enqueue_command_upload(
            source,
            "device_command_attachment".into(),
            None,
            "session".into(),
            Vec::new(),
            Uuid::new_v4().to_string(),
            None,
            Some(tx),
            None,
          )
          .unwrap();
        assert_eq!(rx.await.unwrap().unwrap(), size);
        id
      };
      assert_eq!(path.exists(), source_kind != "path");
      let index = setup.chunk_index().await;
      assert_eq!(index.artifact, []);
      assert_eq!(index.chunked_artifact.len(), 1);
      setup.chunk_transfer_enabled.send(true).unwrap();
      let begin = setup.next_chunk_operation().await;
      assert_eq!(begin.payload.artifact_id, id.to_string());
      assert_eq!(begin.payload.logical_bytes, Some(size));
      assert_eq!(begin.payload.chunks.len(), 1);
      assert_eq!(begin.payload.contents, b"");
      assert_eq!(begin.payload.chunk_index, None);
      progress(begin, State::STATE_RECEIVING, &[0]);
      let upload = setup.next_chunk_operation().await;
      assert_eq!(upload.payload.chunk_index, Some(0));
      let mut decoded = Vec::new();
      ZlibDecoder::new(upload.payload.contents.as_slice())
        .read_to_end(&mut decoded)
        .unwrap();
      assert_eq!(decoded, bytes);
      progress(upload, State::STATE_FINALIZED, &[1]);
      setup.shutdown().await;
    }
  }
}

#[tokio::test(start_paused = true)]
async fn chunk_transfer_replays_each_lost_ack_and_waits_for_durable_finalization() {
  let mut setup = Setup::new().await;
  setup.configure_admission(Vec::new()).await;
  let id = Uuid::new_v4();
  let mut bytes = vec![0; usize::try_from(CHUNK_BYTES).unwrap() + 1];
  StdRng::seed_from_u64(42).fill(&mut bytes[..]);
  let path = setup.capture(id, &bytes).await;
  let (persisted_tx, persisted_rx) = oneshot::channel();
  let (completion_tx, mut completion_rx) = oneshot::channel();
  setup
    .enqueue_capture(path, setup.request(id), persisted_tx, Some(completion_tx))
    .unwrap();
  assert_eq!(persisted_rx.await.unwrap().unwrap(), CHUNK_BYTES + 1);
  let template = setup.chunk_index().await.chunked_artifact.remove(0);
  let original_bytes = template.write_to_bytes().unwrap();
  setup.chunk_transfer_enabled.send(true).unwrap();
  let first = setup.next_chunk_operation().await;
  drop(first);
  let begin = setup.next_chunk_operation().await;
  let mut expected = template.clone();
  expected.upload_uuid.clone_from(&begin.uuid);
  assert_eq!(begin.payload, expected);
  progress(begin, State::STATE_RECEIVING, &[0]);
  // Lose chunk zero's ACK, then prove resume reconciles its durable receipt rather than resending
  // an already-received payload or assuming that a sent chunk has been committed.
  let chunk = setup.next_chunk_operation().await;
  thin(&template, &chunk, 0);
  drop(chunk);
  let resume = setup.next_chunk_operation().await;
  assert_eq!(resume.payload.chunks, template.chunks);
  progress(resume, State::STATE_RECEIVING, &[1]);
  let chunk = setup.next_chunk_operation().await;
  thin(&template, &chunk, 1);
  let chunk_bytes = chunk.payload.contents.clone();
  drop(chunk);
  let resume = setup.next_chunk_operation().await;
  progress(resume, State::STATE_RECEIVING, &[1]);
  let chunk = setup.next_chunk_operation().await;
  assert_eq!(chunk.payload.contents, chunk_bytes);
  progress(chunk, State::STATE_RECEIVING, &[3]);
  let resume = setup.next_chunk_operation().await;
  expected.upload_uuid.clone_from(&resume.uuid);
  assert_eq!(resume.payload, expected);
  assert!(completion_rx.try_recv().is_err());
  assert!(
    setup
      .filesystem
      .exists(&chunk_path(&id.to_string(), 0))
      .await
      .unwrap()
  );
  progress(resume, State::STATE_RECEIVING, &[3]);
  let resume = setup.next_chunk_operation().await;
  expected.upload_uuid.clone_from(&resume.uuid);
  assert_eq!(resume.payload, expected);
  drop(resume);
  let resume = setup.next_chunk_operation().await;
  let mut replayed = resume.payload.clone();
  replayed.upload_uuid.clear();
  assert_eq!(replayed.write_to_bytes().unwrap(), original_bytes);
  progress(resume, State::STATE_FINALIZED, &[3]);
  completion_rx.await.unwrap().unwrap();
  // Completion is sent after the durable retirement, so no extra wait is needed for index state.
  assert_eq!(setup.chunk_index().await.chunked_artifact, []);
  assert!(
    !setup
      .filesystem
      .exists(&chunk_path(&id.to_string(), 1))
      .await
      .unwrap()
  );
  setup.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn chunk_transfer_is_fair_resumes_after_restart_and_does_not_expire() {
  let mut setup = Setup::new().await;
  setup.configure_admission(Vec::new()).await;
  let ids = [Uuid::new_v4(), Uuid::new_v4()];
  for id in ids {
    let path = setup.capture(id, b"file").await;
    setup
      .stage_capture(path, setup.request(id))
      .await
      .unwrap()
      .unwrap();
  }
  setup.chunk_transfer_enabled.send(true).unwrap();
  let first = setup.next_chunk_operation().await;
  assert_eq!(first.payload.artifact_id, ids[0].to_string());
  assert!(
    setup.data_upload_rx.try_recv().is_err(),
    "only one outstanding chunk-mode RPC"
  );
  progress(first, State::STATE_RECEIVING, &[0]);
  let second = setup.next_chunk_operation().await;
  assert_eq!(second.payload.artifact_id, ids[1].to_string());
  progress(second, State::STATE_RECEIVING, &[0]);
  let first = setup.next_chunk_operation().await;
  assert_eq!(first.payload.artifact_id, ids[0].to_string());
  let bytes = first.payload.contents.clone();
  drop(first);
  // Restart owns committed bytes but has no predecessor command waiter or execution authority.
  setup = setup.crash_and_reinitialize().await;
  let resumed = setup.next_chunk_operation().await;
  assert!(resumed.payload.time.is_some());
  assert_eq!(resumed.payload.artifact_id, ids[0].to_string());
  progress(resumed, State::STATE_RECEIVING, &[0]);
  let second = setup.next_chunk_operation().await;
  progress(second, State::STATE_RECEIVING, &[0]);
  let chunk = setup.next_chunk_operation().await;
  assert_eq!(chunk.payload.contents, bytes);
  setup.time_provider.advance(time::Duration::days(3));
  tokio::time::advance(std::time::Duration::from_secs(2)).await;
  assert_eq!(setup.chunk_index().await.chunked_artifact.len(), 2);
  assert!(!chunk.response_tx.is_closed());
  setup.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn invalid_chunk_acknowledgements_and_retryable_errors_resume_without_releasing_bytes() {
  let mut setup = Setup::new().await;
  setup.configure_admission(Vec::new()).await;
  let id = Uuid::new_v4();
  let path = setup.capture(id, b"file").await;
  let (persisted_tx, persisted_rx) = oneshot::channel();
  let (completion_tx, mut completion_rx) = oneshot::channel();
  setup
    .enqueue_capture(path, setup.request(id), persisted_tx, Some(completion_tx))
    .unwrap();
  persisted_rx.await.unwrap().unwrap();
  setup.chunk_transfer_enabled.send(true).unwrap();
  let begin = setup.next_chunk_operation().await;
  let template = begin.payload.clone();
  progress(begin, State::STATE_RECEIVING, &[0]);
  for case in 0 .. 9 {
    let operation = setup.next_chunk_operation().await;
    let mut response = UploadArtifactResponse {
      upload_uuid: operation.uuid,
      state: State::STATE_RECEIVING.into(),
      received_chunks: vec![0],
      ..Default::default()
    };
    match case {
      0 => response.state = protobuf::EnumOrUnknown::from_i32(99),
      1 => response.state = State::STATE_UNSPECIFIED.into(),
      2 => response.upload_uuid = "wrong-correlation".into(),
      3 => response.received_chunks.clear(),
      4 => response.received_chunks = vec![2],
      5 => response.received_chunks = vec![0, 0],
      6 => response.state = State::STATE_FINALIZED.into(),
      7 => response.error = "unexpected error on progress".into(),
      8 => {
        response.state = State::STATE_RETRYABLE.into();
        response.error = "admission temporarily unavailable".into();
        response.received_chunks.clear();
      },
      _ => panic!("unknown acknowledgement case"),
    }
    operation.response_tx.send(response).unwrap();
    let resume = setup.next_chunk_operation().await;
    let mut request = resume.payload.clone();
    request.upload_uuid.clone_from(&template.upload_uuid);
    assert_eq!(request, template);
    assert!(completion_rx.try_recv().is_err());
    assert!(
      setup
        .filesystem
        .exists(&chunk_path(&id.to_string(), 0))
        .await
        .unwrap()
    );
    progress(resume, State::STATE_RECEIVING, &[0]);
  }
  let operation = setup.next_chunk_operation().await;
  operation
    .response_tx
    .send(UploadArtifactResponse {
      upload_uuid: operation.uuid,
      state: State::STATE_REJECTED.into(),
      error: "permanent retirement".into(),
      ..Default::default()
    })
    .unwrap();
  assert_eq!(
    completion_rx.await.unwrap(),
    Err("permanent retirement".into())
  );
  assert_eq!(setup.chunk_index().await.chunked_artifact, []);
  setup.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn command_and_report_chunks_share_the_same_scheduler() {
  let mut setup = Setup::new().await;
  setup.configure_admission(Vec::new()).await;
  let mut completions = Vec::new();
  for _ in 0 .. 2 {
    let path = setup.capture(Uuid::new_v4(), b"file").await;
    let (persisted_tx, persisted_rx) = oneshot::channel();
    let (completion_tx, completion_rx) = oneshot::channel();
    setup
      .enqueue_capture(
        path,
        setup.request(Uuid::new_v4()),
        persisted_tx,
        Some(completion_tx),
      )
      .unwrap();
    persisted_rx.await.unwrap().unwrap();
    completions.push(completion_rx);
  }
  setup.chunk_transfer_enabled.send(true).unwrap();
  let begin = setup.next_chunk_operation().await;
  let first = begin.payload.artifact_id.clone();
  let (tx, rx) = oneshot::channel();
  let report = setup
    .client
    .enqueue_upload(
      UploadSource::Bytes(b"report".to_vec()),
      "client_report".into(),
      [].into(),
      None,
      "session".into(),
      Vec::new(),
      None,
      Some(tx),
    )
    .unwrap();
  rx.await.unwrap().unwrap();
  progress(begin, State::STATE_RECEIVING, &[0]);
  // Both types go through Begin and PutChunk, in the same fair queue.
  let mut chunk_operations = Vec::new();
  let mut report_done = false;
  let mut completed = 0;
  while chunk_operations.len() < 5 || completed < 3 {
    let upload = tokio::select! {
      upload = setup.data_upload_rx.recv() => upload.unwrap(),
      Some(_) = setup.upload_complete_rx.recv() => { completed += 1; continue; }
    };
    match upload {
      DataUpload::ArtifactUpload(operation) if operation.payload.logical_bytes.is_some() => {
        let request = &operation.payload;
        chunk_operations.push(request.artifact_id.clone());
        let state = if request.chunk_index.is_some() {
          report_done |= request.artifact_id == report.to_string();
          State::STATE_FINALIZED
        } else {
          State::STATE_RECEIVING
        };
        let receipts: &[u8] = if request.chunk_index.is_some() {
          &[1]
        } else {
          &[0]
        };
        progress(operation, state, receipts);
      },
      other => panic!("unexpected upload: {other:?}"),
    }
  }
  assert_ne!(chunk_operations[0], first);
  assert!(report_done);
  for completion in completions {
    completion.await.unwrap().unwrap();
  }
  setup.shutdown().await;
}

async fn recovered(setup: Setup) -> Setup {
  let setup = setup.crash_and_reinitialize().await;
  let mut revision = setup.client.upload_queue_revision();
  wait_for_queue_revision(&mut revision, 0).await;
  setup
}

#[tokio::test]
async fn chunked_shared_index_preserves_published_snapshot_on_sync_failure() {
  let setup = Setup::new().await;
  setup.configure_admission(Vec::new()).await;
  let chunk_id = Uuid::new_v4();
  let capture = setup.capture(chunk_id, b"chunked").await;
  setup
    .stage_capture(capture, setup.request(chunk_id))
    .await
    .unwrap()
    .unwrap();
  // An ambiguous commit retains both the staged fragments and the producer's original journal.
  let source = PathBuf::from("snapshot.zz");
  setup
    .filesystem
    .write_file(&source, b"snapshot")
    .await
    .unwrap();
  setup
    .filesystem
    .fail_next_sync_at(ARTIFACT_UPLOAD_DIRECTORY.join(&*REPORT_INDEX_FILE));
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
  assert!(matches!(
    rx.await.unwrap(),
    Err(EnqueueError::RetryablePersistence(_))
  ));
  assert!(setup.filesystem.exists(&source).await.unwrap());
  let payload = chunk_path(&id.to_string(), 0);
  assert_eq!(
    read_compressed(&setup.filesystem.read_file(&payload).await.unwrap()).unwrap(),
    b"snapshot"
  );
  let setup = recovered(setup).await;
  assert_eq!(
    read_compressed(&setup.filesystem.read_file(&payload).await.unwrap()).unwrap(),
    b"snapshot"
  );
  let index = setup.chunk_index().await;
  assert!(
    index
      .chunked_artifact
      .iter()
      .any(|entry| entry.artifact_id == id.to_string())
  );
  assert_eq!(index.artifact, []);
  assert_eq!(index.chunked_artifact.len(), 2);
  setup.shutdown().await;
}

#[tokio::test]
async fn chunked_unpublished_index_failure_rolls_back_only_created_paths() {
  let setup = Setup::new().await;
  setup.configure_admission(Vec::new()).await;
  let mut revision = setup.client.upload_queue_revision();
  wait_for_queue_revision(&mut revision, 0).await;
  let id = Uuid::new_v4();
  let path = setup
    .capture(id, &vec![42; usize::try_from(CHUNK_BYTES).unwrap() + 1])
    .await;
  setup
    .filesystem
    .create_dir(
      &ARTIFACT_UPLOAD_DIRECTORY
        .join(&*REPORT_INDEX_FILE)
        .with_extension("tmp"),
    )
    .await
    .unwrap();
  assert!(matches!(
    setup
      .stage_capture(path.clone(), setup.request(id))
      .await
      .unwrap(),
    Err(EnqueueError::Other(_))
  ));
  assert!(!path.exists());
  for owned in [
    source_path(&id.to_string()),
    chunk_path(&id.to_string(), 0),
    chunk_path(&id.to_string(), 1),
  ] {
    assert!(!setup.filesystem.exists(&owned).await.unwrap());
  }
  setup.shutdown().await;
}

#[tokio::test]
async fn chunked_single_stream_hashes_encoded_geometry_and_restart() {
  for (kind, expected_chunks) in [
    ("compressible", 1),
    ("mixed", 2),
    ("incompressible", 3),
    ("already_compressed", 3),
  ] {
    let setup = Setup::new().await;
    setup.configure_admission(Vec::new()).await;
    let id = Uuid::new_v4();
    let chunk_bytes = usize::try_from(CHUNK_BYTES).unwrap();
    let mut bytes = vec![0; chunk_bytes * 2];
    if kind != "compressible" {
      let offset = if kind == "mixed" { chunk_bytes } else { 0 };
      StdRng::seed_from_u64(0x1234_5678).fill(&mut bytes[offset ..]);
    }
    bytes.extend_from_slice(b"short last chunk");
    if kind == "already_compressed" {
      let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
      encoder.write_all(&bytes).unwrap();
      bytes = encoder.finish().unwrap();
    }
    let path = setup.capture(id, &bytes).await;
    let metadata = setup.request(id);
    let capture_time = metadata.time.clone();
    let gate = setup
      .filesystem
      .gate_next_sync(chunk_path(&id.to_string(), expected_chunks - 1));
    let started = Instant::now();
    let staged = setup.stage_capture(path.clone(), metadata);
    let release = gate.await.unwrap();
    // At the last payload sync, ownership still retains the source alongside every fragment.
    let mut peak_payload_bytes = setup
      .filesystem
      .read_file(&source_path(&id.to_string()))
      .await
      .unwrap()
      .len();
    for index in 0 .. expected_chunks {
      peak_payload_bytes += setup
        .filesystem
        .read_file(&chunk_path(&id.to_string(), index))
        .await
        .unwrap()
        .len();
    }
    release.send(()).unwrap();
    assert_eq!(staged.await.unwrap().unwrap(), bytes.len() as u64);
    let preparation = started.elapsed();
    assert!(!path.exists());
    assert!(
      !setup
        .filesystem
        .exists(&source_path(&id.to_string()))
        .await
        .unwrap()
    );
    let index = setup.chunk_index().await;
    assert_eq!(index.artifact, []);
    let template = &index.chunked_artifact[0];
    assert_eq!(template.chunks.len(), expected_chunks, "{kind}");
    assert_eq!(template.time, capture_time);
    assert!(template.contents.is_empty() && template.upload_uuid.is_empty());
    assert!(template.chunk_index.is_none());
    assert_eq!(template.logical_sha256, Sha256::digest(&bytes).as_slice());
    let mut compressed = Vec::new();
    for (position, descriptor) in template.chunks.iter().enumerate() {
      let encoded = setup
        .filesystem
        .read_file(&chunk_path(&id.to_string(), position))
        .await
        .unwrap();
      assert_eq!(encoded.len(), descriptor.encoded_bytes as usize);
      assert_eq!(
        Sha256::digest(&encoded).as_slice(),
        descriptor.encoded_sha256
      );
      assert!(encoded.len() <= chunk_bytes);
      if position + 1 != template.chunks.len() {
        assert_eq!(encoded.len(), chunk_bytes);
      }
      compressed.extend(encoded);
    }
    assert_eq!(template.encoded_bytes, Some(compressed.len() as u64));
    assert_eq!(peak_payload_bytes, bytes.len() + compressed.len());
    assert!(compressed.len() as u64 <= artifact_encoded_bound(bytes.len() as u64));
    assert_eq!(
      template.payload_encoding.enum_value().unwrap(),
      ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_ZLIB
    );
    let mut reconstructed = Vec::new();
    ZlibDecoder::new(compressed.as_slice())
      .read_to_end(&mut reconstructed)
      .unwrap();
    assert_eq!(reconstructed, bytes);
    let started = Instant::now();
    let mut setup = recovered(setup).await;
    log::info!(
      "chunked codec sample: input={kind} logical={} encoded={} fragments={expected_chunks} \
       peak_payload_bytes={peak_payload_bytes} preparation={preparation:?} recovery={:?}",
      bytes.len(),
      compressed.len(),
      started.elapsed()
    );
    assert_eq!(setup.chunk_index().await, index);
    assert!(
      setup.data_upload_rx.try_recv().is_err(),
      "M2 must not transmit chunked spools"
    );
    setup.shutdown().await;
  }
}

#[tokio::test]
async fn descriptor_and_byte_sources_stage_without_a_source_copy_or_per_chunk_directory_sync() {
  for bytes_source in [false, true] {
    let setup = Setup::with_buffer(4 * 1024 * 1024).await;
    let size = CHUNK_BYTES * 2 + 1;
    let mut bytes = vec![0; usize::try_from(size).unwrap()];
    StdRng::seed_from_u64(42).fill(&mut bytes[..]);
    let id = Uuid::new_v4();
    let path = setup.capture(id, &bytes).await;
    let source = if bytes_source {
      UploadSource::Bytes(bytes)
    } else {
      UploadSource::File(std::fs::File::open(&path).unwrap())
    };
    let mut revision = setup.client.upload_queue_revision();
    wait_for_queue_revision(&mut revision, 0).await;
    let file_syncs = setup.filesystem.file_sync_calls.load(Ordering::Relaxed);
    let directory_syncs = setup
      .filesystem
      .directory_sync_calls
      .load(Ordering::Relaxed);
    let (tx, rx) = oneshot::channel();
    let uploaded = setup
      .client
      .enqueue_command_upload(
        source,
        "device_command_attachment".into(),
        None,
        "session".into(),
        Vec::new(),
        Uuid::new_v4().to_string(),
        None,
        Some(tx),
        None,
      )
      .unwrap();
    let gate = setup
      .filesystem
      .gate_next_sync(chunk_path(&uploaded.to_string(), 0));
    let resume = gate.await.unwrap();
    assert!(
      !setup
        .filesystem
        .exists(&source_path(&uploaded.to_string()))
        .await
        .unwrap()
    );
    resume.send(()).unwrap();
    assert_eq!(rx.await.unwrap().unwrap(), size);
    assert_eq!(
      setup.chunk_index().await.chunked_artifact[0].chunks.len(),
      3
    );
    assert_eq!(
      setup.filesystem.file_sync_calls.load(Ordering::Relaxed) - file_syncs,
      4
    );
    assert_eq!(
      setup
        .filesystem
        .directory_sync_calls
        .load(Ordering::Relaxed)
        - directory_syncs,
      2
    );
    setup.shutdown().await;
  }
}

#[tokio::test]
async fn chunked_persistence_requires_payload_and_index_fsync() {
  for phase in 0 .. 3 {
    let fail_index = phase == 2;
    let setup = Setup::new().await;
    setup.configure_admission(Vec::new()).await;
    let id = Uuid::new_v4();
    let path = setup.capture(id, b"file").await;
    let target = match phase {
      0 => chunk_path(&id.to_string(), 0),
      1 => ARTIFACT_UPLOAD_DIRECTORY.to_path_buf(),
      _ => ARTIFACT_UPLOAD_DIRECTORY.join(&*REPORT_INDEX_FILE),
    };
    setup.filesystem.fail_next_sync_at(target.clone());
    let gate = setup.filesystem.gate_next_sync(target);
    let mut rx = setup.stage_capture(path.clone(), setup.request(id));
    let resume = gate.await.unwrap();
    assert!(matches!(
      rx.try_recv(),
      Err(oneshot::error::TryRecvError::Empty)
    ));
    if fail_index {
      assert!(
        setup
          .filesystem
          .exists(&source_path(&id.to_string()))
          .await
          .unwrap()
      );
    } else {
      assert!(
        setup
          .filesystem
          .read_file(&ARTIFACT_UPLOAD_DIRECTORY.join(&*REPORT_INDEX_FILE))
          .await
          .is_err()
      );
    }
    resume.send(()).unwrap();
    assert!(rx.await.unwrap().is_err());
    let setup = recovered(setup).await;
    if fail_index {
      assert_eq!(setup.chunk_index().await.chunked_artifact.len(), 1);
      assert!(
        setup
          .filesystem
          .exists(&chunk_path(&id.to_string(), 0))
          .await
          .unwrap()
      );
    } else {
      assert!(
        !setup
          .filesystem
          .exists(&chunk_path(&id.to_string(), 0))
          .await
          .unwrap()
      );
    }
    assert!(
      !setup
        .filesystem
        .exists(&source_path(&id.to_string()))
        .await
        .unwrap()
    );
    assert!(!path.exists());
    setup.shutdown().await;
  }
}

#[tokio::test]
async fn chunked_cancelled_waiter_keeps_durable_ownership() {
  let setup = Setup::new().await;
  setup.configure_admission(Vec::new()).await;
  let id = Uuid::new_v4();
  let path = setup.capture(id, b"file").await;
  let gate = setup
    .filesystem
    .gate_next_sync(chunk_path(&id.to_string(), 0));
  let rx = setup.stage_capture(path, setup.request(id));
  let resume = gate.await.unwrap();
  drop(rx);
  let mut revision = setup.client.upload_queue_revision();
  let previous = *revision.borrow();
  resume.send(()).unwrap();
  wait_for_queue_revision(&mut revision, previous).await;
  let setup = recovered(setup).await;
  assert_eq!(setup.chunk_index().await.chunked_artifact.len(), 1);
  setup.shutdown().await;
}

#[tokio::test]
async fn chunked_source_change_and_disk_full_reclaim_preparation() {
  for change in 0 .. 4 {
    let setup = Setup::new().await;
    setup.configure_admission(Vec::new()).await;
    let id = Uuid::new_v4();
    let path = setup.capture(id, b"file").await;
    let source = source_path(&id.to_string());
    let gate = setup.filesystem.gate_next_sync(source.clone());
    let rx = setup.stage_capture(path, setup.request(id));
    let resume = gate.await.unwrap();
    match change {
      0 => {
        let file = std::fs::OpenOptions::new()
          .write(true)
          .open(setup.filesystem.directory().join(&source))
          .unwrap();
        file.set_len(1).unwrap();
      },
      1 => setup
        .filesystem
        .write_file(&source, b"file grew")
        .await
        .unwrap(),
      2 => {
        setup.filesystem.write_file(&source, b"edit").await.unwrap();
        std::fs::File::open(setup.filesystem.directory().join(&source))
          .unwrap()
          .set_modified(std::time::SystemTime::UNIX_EPOCH)
          .unwrap();
      },
      3 => setup.filesystem.disk_full.store(true, Ordering::Relaxed),
      _ => panic!("unknown source change"),
    }
    resume.send(()).unwrap();
    assert!(rx.await.unwrap().is_err());
    setup.filesystem.disk_full.store(false, Ordering::Relaxed);
    assert!(!setup.filesystem.exists(&source).await.unwrap());
    assert!(
      !setup
        .filesystem
        .exists(&chunk_path(&id.to_string(), 0))
        .await
        .unwrap()
    );
    let id = Uuid::new_v4();
    let path = setup.capture(id, b"retry").await;
    assert_eq!(
      setup
        .stage_capture(path, setup.request(id))
        .await
        .unwrap()
        .unwrap(),
      5
    );
    setup.shutdown().await;
  }
}

#[tokio::test]
async fn chunked_count_bound_preserves_accepted_entries_and_rejects_overflow() {
  let mut setup = Setup::new().await;
  setup.configure_admission(Vec::new()).await;
  for _ in 0 .. 32 {
    let id = Uuid::new_v4();
    let path = setup.capture(id, b"file").await;
    setup
      .stage_capture(path, setup.request(id))
      .await
      .unwrap()
      .unwrap();
  }
  for _ in 0 .. 31 {
    let (tx, rx) = oneshot::channel();
    setup
      .client
      .enqueue_upload(
        UploadSource::Bytes(b"whole".to_vec()),
        "client_report".into(),
        [].into(),
        None,
        "session".into(),
        Vec::new(),
        None,
        Some(tx),
      )
      .unwrap();
    rx.await.unwrap().unwrap();
    setup.entry_received_rx.recv().await.unwrap();
  }
  setup
    .filesystem
    .create_dir(Path::new("workflow-attachments"))
    .await
    .unwrap();
  ArtifactStore::new(&setup.runtime)
    .retain(
      setup.filesystem.as_ref(),
      UploadSource::Bytes(b"retained".to_vec()),
      Uuid::new_v4(),
      &WorkflowAttachmentState::default(),
    )
    .await
    .unwrap();
  let source = setup.capture(Uuid::new_v4(), b"small").await;
  let (tx, rx) = oneshot::channel();
  setup
    .client
    .enqueue_command_upload(
      UploadSource::Path(source.clone()),
      "device_command_attachment".into(),
      None,
      "session".into(),
      Vec::new(),
      Uuid::new_v4().to_string(),
      None,
      Some(tx),
      None,
    )
    .unwrap();
  assert!(rx.await.unwrap().is_err());
  assert!(!source.exists());
  let index = setup.chunk_index().await;
  assert_eq!(index.artifact, []);
  assert_eq!(index.chunked_artifact.len(), 63);
  assert!(
    ArtifactStore::new(&setup.runtime)
      .retain(
        setup.filesystem.as_ref(),
        UploadSource::Bytes(b"overflow".to_vec()),
        Uuid::new_v4(),
        &WorkflowAttachmentState::default(),
      )
      .await
      .is_err()
  );
  let id = Uuid::new_v4();
  let path = setup.capture(id, b"overflow").await;
  assert!(
    setup
      .stage_capture(path.clone(), setup.request(id))
      .await
      .unwrap()
      .is_err()
  );
  assert!(!path.exists());
  setup
    .configure_admission(vec![(
      chunked_artifact_upload::MaxLogicalBytes::path(),
      ValueKind::Int(1),
    )])
    .await;
  let id = Uuid::new_v4();
  let path = setup.capture(id, b"another").await;
  assert!(
    setup
      .stage_capture(path.clone(), setup.request(id))
      .await
      .unwrap()
      .is_err()
  );
  assert!(!path.exists());
  let setup = recovered(setup).await;
  assert_eq!(setup.chunk_index().await, index);
  setup.shutdown().await;
}

#[tokio::test]
async fn relative_sdk_directory_counts_retained_slots() {
  tokio::fs::create_dir_all(".tmp").await.unwrap();
  let directory = tempfile::TempDir::with_prefix_in("artifact-slots", ".tmp").unwrap();
  let filesystem = RealFileSystem::new(
    directory
      .path()
      .strip_prefix(std::env::current_dir().unwrap())
      .unwrap()
      .to_path_buf(),
  );
  let runtime = TestConfigLoader::new().await;
  let store = ArtifactStore::new(&runtime);
  filesystem
    .create_dir(Path::new("workflow-attachments"))
    .await
    .unwrap();
  let first = Uuid::new_v4();
  for index in 0 .. 64 {
    store
      .retain(
        &filesystem,
        UploadSource::Bytes(b"retained".to_vec()),
        if index == 0 { first } else { Uuid::new_v4() },
        &WorkflowAttachmentState::default(),
      )
      .await
      .unwrap();
  }
  assert!(
    store
      .check_capacity(&filesystem, 4, 4, None, 4)
      .await
      .is_err()
  );
  store
    .remove_retained_payload(&filesystem, first, false)
    .await
    .unwrap();
  store
    .check_capacity(&filesystem, 4, 4, None, 4)
    .await
    .unwrap();
}

#[tokio::test]
async fn disk_admission_protects_unmaterialized_raw_reservations() {
  let filesystem = TestFileSystem::new();
  let runtime = TestConfigLoader::new().await;
  let store = ArtifactStore::new(&runtime);
  filesystem
    .create_dir(&ARTIFACT_UPLOAD_DIRECTORY)
    .await
    .unwrap();
  filesystem
    .create_dir(Path::new("workflow-attachments"))
    .await
    .unwrap();
  let id = Uuid::new_v4().to_string();
  filesystem
    .create_file(Path::new(&format!("workflow-attachments/{id}.raw")))
    .await
    .unwrap()
    .set_len(104)
    .await
    .unwrap();
  // Physical bytes are already reflected in available space; only unwritten output is added.
  for staged in [0, 40, 100, 120] {
    filesystem
      .create_file(&chunk_path(&id, 0))
      .await
      .unwrap()
      .set_len(staged)
      .await
      .unwrap();
    let required = FREE_SPACE_HEADROOM + 4 + artifact_encoded_bound(100).saturating_sub(staged);
    for (available, accepted) in [(required - 1, false), (required, true)] {
      *filesystem.available_bytes.lock() = Some(available);
      assert_eq!(
        store
          .check_capacity(&filesystem, 4, 4, None, 4)
          .await
          .is_ok(),
        accepted,
        "staged={staged}, available={available}"
      );
    }
  }
}

#[tokio::test]
async fn common_byte_capacity_includes_chunked_and_retained_payloads() {
  tokio::fs::create_dir_all(".tmp").await.unwrap();
  for layout in ["chunked", "retained"] {
    for (spare_bytes, accepted) in [(7, false), (8, true)] {
      let directory = tempfile::TempDir::with_prefix_in("artifact-bytes", ".tmp").unwrap();
      let filesystem = RealFileSystem::new(
        directory
          .path()
          .strip_prefix(std::env::current_dir().unwrap())
          .unwrap()
          .to_path_buf(),
      );
      let runtime = TestConfigLoader::new().await;
      filesystem
        .create_dir(&ARTIFACT_UPLOAD_DIRECTORY)
        .await
        .unwrap();
      filesystem
        .create_dir(Path::new("workflow-attachments"))
        .await
        .unwrap();
      let id = Uuid::new_v4().to_string();
      let path = match layout {
        "chunked" => source_path(&id),
        _ => PathBuf::from(format!("workflow-attachments/{id}.raw")),
      };
      filesystem
        .create_file(&path)
        .await
        .unwrap()
        .set_len(640 * 1024 * 1024 - spare_bytes)
        .await
        .unwrap();
      if layout == "retained" {
        // An indexed workflow source has already staged its encoded fragments.
        filesystem
          .write_file(
            &ARTIFACT_UPLOAD_DIRECTORY.join(&*REPORT_INDEX_FILE),
            &write_compressed_protobuf(&ArtifactUploadIndex {
              chunked_artifact: vec![UploadArtifactRequest {
                artifact_id: id,
                ..Default::default()
              }],
              ..Default::default()
            })
            .unwrap(),
          )
          .await
          .unwrap();
      }
      assert_eq!(
        ArtifactStore::new(&runtime)
          .check_capacity(&filesystem, 4, 4, None, 4,)
          .await
          .is_ok(),
        accepted,
        "{layout}: {spare_bytes}"
      );
    }
  }
}

#[tokio::test]
async fn chunked_logical_and_source_plus_staging_byte_admission() {
  for (used_bytes, accepted) in [
    (640 * 1024 * 1024 - 4 - artifact_encoded_bound(4) + 1, false),
    (640 * 1024 * 1024 - 4 - artifact_encoded_bound(4), true),
  ] {
    let setup = Setup::new().await;
    setup.configure_admission(Vec::new()).await;
    let mut revision = setup.client.upload_queue_revision();
    wait_for_queue_revision(&mut revision, 0).await;
    let padding = setup
      .filesystem
      .directory()
      .join(source_path(&Uuid::new_v4().to_string()));
    std::fs::File::create(padding)
      .unwrap()
      .set_len(used_bytes)
      .unwrap();
    let id = Uuid::new_v4();
    let path = setup.capture(id, b"file").await;
    assert_eq!(
      setup
        .stage_capture(path.clone(), setup.request(id))
        .await
        .unwrap()
        .is_ok(),
      accepted
    );
    assert!(!path.exists());
    setup.shutdown().await;
  }
}

#[tokio::test]
async fn chunked_logical_limit_updates_command_and_workflow_admission() {
  let setup = Setup::new().await;
  let admission = ArtifactStore::new(&setup.runtime);
  for (limit, accepted) in [(4, true), (3, false), (4, true)] {
    setup
      .configure_admission(vec![(
        chunked_artifact_upload::MaxLogicalBytes::path(),
        ValueKind::Int(limit),
      )])
      .await;
    assert_eq!(admission.check_logical_bytes(4).is_ok(), accepted);
    let id = Uuid::new_v4();
    let path = setup.capture(id, b"file").await;
    assert_eq!(
      setup
        .stage_capture(path.clone(), setup.request(id))
        .await
        .unwrap()
        .is_ok(),
      accepted
    );
    assert!(!path.exists());
  }
  setup.shutdown().await;
}

#[tokio::test]
async fn chunked_hard_logical_ceiling_rejects_oversized_files() {
  let setup = Setup::new().await;
  setup
    .configure_admission(vec![(
      chunked_artifact_upload::MaxLogicalBytes::path(),
      ValueKind::Int(u32::MAX),
    )])
    .await;
  let id = Uuid::new_v4();
  let path = setup.capture(id, b"").await;
  std::fs::OpenOptions::new()
    .write(true)
    .open(&path)
    .unwrap()
    .set_len(u64::from(MAX_ARTIFACT_LOGICAL_BYTES) + 1)
    .unwrap();
  assert!(
    setup
      .stage_capture(path.clone(), setup.request(id))
      .await
      .unwrap()
      .is_err()
  );
  assert!(!path.exists());
  assert!(
    !setup
      .filesystem
      .exists(&chunk_path(&id.to_string(), 0))
      .await
      .unwrap()
  );
  setup.shutdown().await;
}

#[tokio::test]
async fn chunked_recovery_rejects_invalid_templates_and_inconsistent_chunks() {
  for corruption in 0 .. 8 {
    let setup = Setup::new().await;
    setup.configure_admission(Vec::new()).await;
    let id = Uuid::new_v4();
    let path = setup.capture(id, b"file").await;
    setup
      .stage_capture(path, setup.request(id))
      .await
      .unwrap()
      .unwrap();
    let mut index = setup.chunk_index().await;
    let template = &mut index.chunked_artifact[0];
    match corruption {
      0 => template.artifact_id = "../escape".into(),
      1 => template.upload_uuid = Uuid::new_v4().to_string(),
      2 => template.contents = b"transient payload".to_vec(),
      3 => template.chunk_index = Some(0),
      4 => template.chunks[0].encoded_bytes += 1,
      5 => setup
        .filesystem
        .delete_file(&chunk_path(&id.to_string(), 0))
        .await
        .unwrap(),
      6 => setup
        .filesystem
        .write_file(&chunk_path(&id.to_string(), 0), b"wrong")
        .await
        .unwrap(),
      7 => index
        .chunked_artifact
        .push(index.chunked_artifact[0].clone()),
      _ => panic!("unknown corruption"),
    }
    setup.write_chunk_index(&index).await;
    let setup = recovered(setup).await;
    assert!(
      setup.chunk_index().await.chunked_artifact.is_empty(),
      "corruption {corruption}"
    );
    assert!(
      !setup
        .filesystem
        .exists(&chunk_path(&id.to_string(), 0))
        .await
        .unwrap()
    );
    setup.shutdown().await;
  }
}

#[tokio::test]
async fn chunked_recovery_checks_stream_integrity_with_valid_encoded_descriptors() {
  for corruption in [
    "valid",
    "truncated_trailer",
    "trailing_data",
    "logical_expansion",
  ] {
    let setup = Setup::new().await;
    setup.configure_admission(Vec::new()).await;
    let id = Uuid::new_v4();
    let path = setup.capture(id, &vec![b'x'; 64 * 1024 + 1]).await;
    setup
      .stage_capture(path, setup.request(id))
      .await
      .unwrap()
      .unwrap();
    setup.abort_for_recovery();
    let mut index = setup.chunk_index().await;
    let template = &mut index.chunked_artifact[0];
    assert_eq!(template.chunks.len(), 1);
    let path = chunk_path(&id.to_string(), 0);
    let mut encoded = setup.filesystem.read_file(&path).await.unwrap();
    match corruption {
      "valid" => {},
      "truncated_trailer" => encoded.truncate(encoded.len() - 1),
      "trailing_data" => encoded.push(0),
      "logical_expansion" => {
        template.logical_bytes = Some(template.logical_bytes.unwrap() - 1);
      },
      _ => panic!("unknown corruption"),
    }
    // Keep metadata and encoded integrity valid so recovery must check the continuous zlib stream.
    let encoded_bytes = u64::try_from(encoded.len()).unwrap();
    assert!(encoded_bytes <= artifact_encoded_bound(template.logical_bytes.unwrap()));
    template.encoded_bytes = Some(encoded_bytes);
    template.chunks[0].encoded_bytes = u32::try_from(encoded_bytes).unwrap();
    template.chunks[0].encoded_sha256 = Sha256::digest(&encoded).to_vec();
    setup.filesystem.write_file(&path, &encoded).await.unwrap();
    setup.write_chunk_index(&index).await;

    let setup = recovered(setup).await;
    let expected = if corruption == "valid" {
      index.chunked_artifact
    } else {
      Vec::new()
    };
    assert_eq!(
      setup.chunk_index().await.chunked_artifact,
      expected,
      "{corruption}"
    );
    assert_eq!(
      setup.filesystem.exists(&path).await.unwrap(),
      corruption == "valid",
      "{corruption}"
    );
    setup.shutdown().await;
  }
}

#[tokio::test]
async fn chunked_recovery_bounds_compressed_and_decoded_index() {
  for compressed in [false, true] {
    let setup = Setup::new().await;
    let mut revision = setup.client.upload_queue_revision();
    wait_for_queue_revision(&mut revision, 0).await;
    let bytes = vec![0; usize::try_from(MAX_INDEX_BYTES).unwrap() + 1];
    let bytes = if compressed {
      bd_client_common::file::write_compressed(&bytes).unwrap()
    } else {
      bytes
    };
    setup
      .filesystem
      .write_file(&ARTIFACT_UPLOAD_DIRECTORY.join(&*REPORT_INDEX_FILE), &bytes)
      .await
      .unwrap();
    let id = Uuid::new_v4();
    setup
      .filesystem
      .write_file(&source_path(&id.to_string()), b"interrupted")
      .await
      .unwrap();
    let setup = recovered(setup).await;
    assert!(
      Uploader::pending_workflow_attachments(setup.filesystem.directory())
        .await
        .unwrap()
        .is_empty()
    );
    assert!(
      !setup
        .filesystem
        .exists(&source_path(&id.to_string()))
        .await
        .unwrap()
    );
    setup.shutdown().await;
  }
}

#[tokio::test]
async fn chunked_crash_reclaims_uncommitted_files_and_preserves_published_index() {
  for phase in 0 .. 3 {
    let setup = Setup::new().await;
    setup.configure_admission(Vec::new()).await;
    let id = Uuid::new_v4();
    let path = setup.capture(id, b"file").await;
    let target = match phase {
      0 => source_path(&id.to_string()),
      1 => chunk_path(&id.to_string(), 0),
      2 => ARTIFACT_UPLOAD_DIRECTORY.join(&*REPORT_INDEX_FILE),
      _ => panic!("unknown crash phase"),
    };
    let gate = setup.filesystem.gate_next_sync(target);
    let mut rx = setup.stage_capture(path.clone(), setup.request(id));
    let resume = gate.await.unwrap();
    assert!(matches!(
      rx.try_recv(),
      Err(oneshot::error::TryRecvError::Empty)
    ));
    let setup = setup.crash_and_reinitialize().await;
    drop(resume);
    assert!(
      rx.await.is_err(),
      "aborted preparation must never report success"
    );
    let mut revision = setup.client.upload_queue_revision();
    wait_for_queue_revision(&mut revision, 0).await;
    assert!(!path.exists());
    assert!(
      !setup
        .filesystem
        .exists(&source_path(&id.to_string()))
        .await
        .unwrap()
    );
    assert_eq!(
      setup
        .filesystem
        .exists(&chunk_path(&id.to_string(), 0))
        .await
        .unwrap(),
      phase == 2,
    );
    if phase == 2 {
      assert_eq!(setup.chunk_index().await.chunked_artifact.len(), 1);
    }
    // Both an interrupted transaction and a committed one leave capacity for more work.
    let next = Uuid::new_v4();
    let path = setup.capture(next, b"next").await;
    setup
      .stage_capture(path, setup.request(next))
      .await
      .unwrap()
      .unwrap();
    setup.shutdown().await;
  }
}

#[tokio::test]
async fn chunked_recovery_retains_old_bytes_and_keeps_admission_charged() {
  let setup = Setup::new().await;
  setup.configure_admission(Vec::new()).await;
  let id = Uuid::new_v4();
  let metadata = setup.request(id);
  let path = setup.capture(id, b"file").await;
  setup.stage_capture(path, metadata).await.unwrap().unwrap();
  let second = Uuid::new_v4();
  let path = setup.capture(second, b"second").await;
  setup
    .stage_capture(path, setup.request(second))
    .await
    .unwrap()
    .unwrap();
  let index = setup.chunk_index().await;
  setup
    .configure_admission(vec![(
      chunked_artifact_upload::MaxLogicalBytes::path(),
      ValueKind::Int(1),
    )])
    .await;
  setup.time_provider.advance(time::Duration::days(3));
  let setup = recovered(setup).await;
  assert_eq!(setup.chunk_index().await, index);
  assert!(
    setup
      .filesystem
      .exists(&chunk_path(&id.to_string(), 0))
      .await
      .unwrap()
  );
  let id = Uuid::new_v4();
  let path = setup.capture(id, b"next").await;
  assert!(
    setup
      .stage_capture(path, setup.request(id))
      .await
      .unwrap()
      .is_err()
  );
  setup.shutdown().await;
}

#[tokio::test]
async fn chunked_remote_files_may_be_empty_but_workflow_files_must_be_nonempty() {
  for (workflow, empty, accepted) in [
    (false, true, true),
    (true, true, false),
    (true, false, true),
  ] {
    let setup = Setup::new().await;
    let id = Uuid::new_v4();
    let mut metadata = setup.request(id);
    if workflow {
      metadata.type_id = super::WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID.into();
      metadata.command_id = None;
    }
    let path = setup.capture(id, if empty { b"" } else { b"file" }).await;
    assert_eq!(
      setup.stage_capture(path, metadata).await.unwrap().is_ok(),
      accepted
    );
    if accepted && empty {
      let index = setup.chunk_index().await;
      let template = &index.chunked_artifact[0];
      assert_eq!(template.chunks, []);
      assert_eq!(template.logical_sha256, Sha256::digest([]).as_slice());
      let setup = recovered(setup).await;
      assert_eq!(setup.chunk_index().await, index);
      setup.shutdown().await;
    } else {
      setup.shutdown().await;
    }
  }
}

#[tokio::test]
async fn chunked_disk_headroom_rejects_but_slow_preparation_does_not_expire() {
  for slow_preparation in [false, true] {
    let setup = Setup::new().await;
    setup.configure_admission(Vec::new()).await;
    let id = Uuid::new_v4();
    let path = setup.capture(id, b"file").await;
    let metadata = setup.request(id);
    if slow_preparation {
      let gate = setup
        .filesystem
        .gate_next_sync(chunk_path(&id.to_string(), 0));
      let rx = setup.stage_capture(path.clone(), metadata);
      let resume = gate.await.unwrap();
      setup.time_provider.advance(time::Duration::days(3));
      resume.send(()).unwrap();
      rx.await.unwrap().unwrap();
    } else {
      // Wait until startup has created the spool before injecting full-disk admission.
      let mut revision = setup.client.upload_queue_revision();
      wait_for_queue_revision(&mut revision, 0).await;
      setup.filesystem.disk_full.store(true, Ordering::Relaxed);
      assert!(
        setup
          .stage_capture(path.clone(), metadata)
          .await
          .unwrap()
          .is_err()
      );
      setup.filesystem.disk_full.store(false, Ordering::Relaxed);
    }
    assert!(!path.exists());
    assert_eq!(
      setup
        .filesystem
        .exists(&chunk_path(&id.to_string(), 0))
        .await
        .unwrap(),
      slow_preparation
    );
    setup.shutdown().await;
  }
}

#[cfg(unix)]
#[tokio::test]
async fn chunked_hard_linked_sources_are_charged_once_without_requiring_links() {
  let setup = Setup::new().await;
  setup.configure_admission(Vec::new()).await;
  let first = Uuid::new_v4();
  let path = setup.capture(first, b"file").await;
  setup
    .stage_capture(path, setup.request(first))
    .await
    .unwrap()
    .unwrap();
  let padding = setup
    .filesystem
    .directory()
    .join(source_path(&Uuid::new_v4().to_string()));
  let encoded_len = setup
    .filesystem
    .read_file(&chunk_path(&first.to_string(), 0))
    .await
    .unwrap()
    .len() as u64;
  std::fs::File::create(padding)
    .unwrap()
    .set_len(640 * 1024 * 1024 - encoded_len - artifact_encoded_bound(encoded_len))
    .unwrap();
  let second = Uuid::new_v4();
  let path = setup.filesystem.directory().join("linked-capture");
  std::fs::hard_link(
    setup
      .filesystem
      .directory()
      .join(chunk_path(&first.to_string(), 0)),
    &path,
  )
  .unwrap();
  setup
    .stage_capture(path, setup.request(second))
    .await
    .unwrap()
    .unwrap();
  assert_eq!(setup.chunk_index().await.chunked_artifact.len(), 2);
  assert_eq!(
    read_compressed(
      &setup
        .filesystem
        .read_file(&chunk_path(&first.to_string(), 0))
        .await
        .unwrap()
    )
    .unwrap(),
    b"file"
  );
  setup.shutdown().await;
}

#[tokio::test]
async fn chunked_invalid_capture_metadata_consumes_owned_source_without_index_commit() {
  let setup = Setup::new().await;
  setup.configure_admission(Vec::new()).await;
  for case in 0 .. 7 {
    let id = Uuid::new_v4();
    let path = setup.capture(id, b"file").await;
    let mut metadata = setup.request(id);
    match case {
      0 => metadata.artifact_id = "../escape".into(),
      1 => metadata.contents = vec![1],
      2 => metadata
        .state_metadata
        .insert(
          "app_id".into(),
          Data {
            data_type: Some(Data_type::StringData("x".repeat(256))),
            ..Default::default()
          },
        )
        .map_or((), |_| ()),
      3 => metadata.content_type = Some("invalid\nmime".into()),
      4 => metadata.chunks.push(ArtifactChunkDescriptor::default()),
      5 => metadata.session_id = "x".repeat(256),
      6 => metadata.time.as_mut().unwrap().seconds = -62_135_596_801,
      _ => panic!("unknown invalid metadata"),
    }
    let (tx, rx) = oneshot::channel();
    setup
      .enqueue_capture(path.clone(), metadata, tx, None)
      .unwrap();
    assert!(rx.await.unwrap().is_err());
    assert!(!path.exists());
  }
  setup.shutdown().await;
}

#[tokio::test]
async fn shared_index_report_writes_preserve_workflow_chunk_ownership() {
  let mut setup = Setup::new().await;
  setup.configure_admission(Vec::new()).await;
  let id = Uuid::new_v4();
  let mut metadata = setup.request(id);
  metadata.type_id = super::WORKFLOW_ATTACHMENT_ARTIFACT_TYPE_ID.into();
  metadata.command_id = None;
  let path = setup.capture(id, b"file").await;
  setup.stage_capture(path, metadata).await.unwrap().unwrap();
  let before = setup.chunk_index().await.chunked_artifact;
  let gate = setup
    .filesystem
    .gate_next_sync(ARTIFACT_UPLOAD_DIRECTORY.join(&*REPORT_INDEX_FILE));
  let (tx, mut rx) = oneshot::channel();
  setup
    .client
    .enqueue_upload(
      super::UploadSource::Bytes(b"whole-file".to_vec()),
      "client_report".into(),
      LogFields::default(),
      None,
      "session".into(),
      Vec::new(),
      None,
      Some(tx),
    )
    .unwrap();
  let resume = gate.await.unwrap();
  assert!(matches!(
    rx.try_recv(),
    Err(oneshot::error::TryRecvError::Empty)
  ));
  resume.send(()).unwrap();
  rx.await.unwrap().unwrap();
  assert_eq!(setup.chunk_index().await.chunked_artifact[.. 1], before);
  assert_eq!(
    Uploader::pending_workflow_attachments(setup.filesystem.directory())
      .await
      .unwrap(),
    [id].into()
  );
  // Consume the test hook so shutdown/recovery cannot leave a producer blocked on its observer.
  setup.entry_received_rx.recv().await.unwrap();
  let setup = recovered(setup).await;
  assert_eq!(setup.chunk_index().await.chunked_artifact[.. 1], before);
  setup.shutdown().await;
}

#[tokio::test]
async fn chunked_failed_source_cleanup_stays_charged_until_recovery_retries_it() {
  let setup = Setup::new().await;
  setup.configure_admission(Vec::new()).await;
  let id = Uuid::new_v4();
  let path = setup.capture(id, b"file").await;
  setup.filesystem.fail_deletes.store(true, Ordering::Relaxed);
  setup
    .stage_capture(path, setup.request(id))
    .await
    .unwrap()
    .unwrap();
  assert!(
    setup
      .filesystem
      .exists(&source_path(&id.to_string()))
      .await
      .unwrap()
  );
  let padding = setup
    .filesystem
    .directory()
    .join(source_path(&Uuid::new_v4().to_string()));
  std::fs::File::create(padding)
    .unwrap()
    .set_len(640 * 1024 * 1024 - 10)
    .unwrap();
  let rejected = Uuid::new_v4();
  let path = setup.capture(rejected, b"ab").await;
  assert!(
    setup
      .stage_capture(path.clone(), setup.request(rejected))
      .await
      .unwrap()
      .is_err()
  );
  assert!(!path.exists());
  setup
    .filesystem
    .fail_deletes
    .store(false, Ordering::Relaxed);
  let setup = recovered(setup).await;
  assert!(
    !setup
      .filesystem
      .exists(&source_path(&id.to_string()))
      .await
      .unwrap()
  );
  let next = Uuid::new_v4();
  let path = setup.capture(next, b"ab").await;
  setup
    .stage_capture(path, setup.request(next))
    .await
    .unwrap()
    .unwrap();
  assert_eq!(setup.chunk_index().await.chunked_artifact.len(), 2);
  setup.shutdown().await;
}

#[tokio::test]
async fn legacy_entry_does_not_delete_accepted_chunks_with_same_id() {
  let setup = Setup::new().await;
  setup.configure_admission(Vec::new()).await;
  let id = Uuid::new_v4();
  let path = setup.capture(id, b"file").await;
  setup
    .stage_capture(path, setup.request(id))
    .await
    .unwrap()
    .unwrap();
  let original = setup.chunk_index().await;
  let path = setup.capture(id, b"other").await;
  assert!(
    setup
      .stage_capture(path, setup.request(id))
      .await
      .unwrap()
      .is_err()
  );
  assert_eq!(setup.chunk_index().await, original);
  assert_eq!(
    read_compressed(
      &setup
        .filesystem
        .read_file(&chunk_path(&id.to_string(), 0))
        .await
        .unwrap()
    )
    .unwrap(),
    b"file"
  );
  let mut index = original.clone();
  index.artifact.push(Artifact {
    name: id.to_string(),
    ..Default::default()
  });
  setup
    .filesystem
    .write_file(
      &ARTIFACT_UPLOAD_DIRECTORY.join(id.to_string()),
      b"whole-file",
    )
    .await
    .unwrap();
  setup.write_chunk_index(&index).await;
  let setup = recovered(setup).await;
  assert_eq!(setup.chunk_index().await, original);
  assert_eq!(
    setup
      .collector
      .scope("artifact_upload")
      .counter("dropped")
      .get(),
    1
  );
  assert!(
    !setup
      .filesystem
      .exists(&ARTIFACT_UPLOAD_DIRECTORY.join(id.to_string()))
      .await
      .unwrap()
  );
  setup.shutdown().await;
}

#[tokio::test]
async fn client_report_accepts_large_typed_metadata_and_feature_flags() {
  let setup = Setup::new().await;
  setup.configure_admission(Vec::new()).await;

  let mut fields = LogFields::new();
  for index in 0_u8 .. 40 {
    fields.insert(
      format!("custom_field_{index}").into(),
      match index % 3 {
        0 => DataValue::from(format!("value-{index}")),
        1 => DataValue::from(vec![index; 8]),
        _ => DataValue::from(vec![DataValue::from(format!("item-{index}"))]),
      },
    );
  }
  let now = setup.time_provider.now();
  // Exceed the 8 KiB origin-metadata cap while remaining within the 64 KiB manifest limit.
  let flags: Vec<_> = (0 .. 160)
    .map(|index| {
      SnappedFeatureFlag::new(
        format!("feature_flag_experiment_name_{index:03}"),
        Some(format!("treatment_variant_{index:03}")),
        now,
      )
    })
    .collect();

  let (tx, rx) = oneshot::channel();
  let id = setup
    .client
    .enqueue_upload(
      UploadSource::Bytes(b"report-body".to_vec()),
      "client_report".into(),
      fields.clone(),
      Some(now),
      "session".into(),
      flags.clone(),
      None,
      Some(tx),
    )
    .unwrap();
  rx.await.unwrap().unwrap();

  // Invalid empty feature-flag names and attached metadata over 960 KiB are rejected synchronously,
  // while empty session_id (e.g. previous-run crashes) and large metadata (>64 KiB) succeed.
  assert!(
    setup
      .client
      .enqueue_upload(
        UploadSource::Bytes(b"report-body".to_vec()),
        "client_report".into(),
        LogFields::new(),
        Some(now),
        String::new(),
        vec![SnappedFeatureFlag::new(String::new(), None, now)],
        None,
        None,
      )
      .is_err()
  );
  let oversized_flags: Vec<_> = (0 .. 12_000)
    .map(|index| {
      SnappedFeatureFlag::new(
        format!("oversized_feature_flag_name_{index:05}"),
        Some("x".repeat(48)),
        now,
      )
    })
    .collect();
  assert!(
    setup
      .client
      .enqueue_upload(
        UploadSource::Bytes(b"report-body".to_vec()),
        "client_report".into(),
        LogFields::new(),
        Some(now),
        String::new(),
        oversized_flags,
        None,
        None,
      )
      .is_err()
  );

  let mut setup = recovered(setup).await;
  setup.chunk_transfer_enabled.send(true).unwrap();
  let begin = setup.next_chunk_operation().await;
  assert_eq!(begin.payload.artifact_id, id.to_string());
  assert!(begin.payload.state_metadata.is_empty());
  assert_eq!(begin.payload.feature_flags, []);
  progress(begin, State::STATE_RECEIVING, &[0]);
  let final_chunk = setup.finish_single_fragment(id, b"report-body").await;
  assert_eq!(final_chunk.state_metadata.len(), 40);
  assert_eq!(final_chunk.feature_flags.len(), 160);
  assert!(final_chunk.compute_size() > 8 * 1024);
  assert_eq!(
    setup.upload_complete_rx.recv().await.unwrap(),
    id.to_string()
  );

  // Empty session_id (e.g. previous-run crash report) is accepted and preserves identity keys on
  // Begin and full metadata on the final chunk.
  let (empty_session_tx, empty_session_rx) = oneshot::channel();
  let empty_session_id = setup
    .client
    .enqueue_upload(
      UploadSource::Bytes(b"prev-run-crash".to_vec()),
      "client_report".into(),
      [("app_id".into(), "com.example.app".into())].into(),
      Some(now),
      String::new(),
      Vec::new(),
      None,
      Some(empty_session_tx),
    )
    .unwrap();
  empty_session_rx.await.unwrap().unwrap();
  let begin = setup.next_chunk_operation().await;
  assert_eq!(begin.payload.artifact_id, empty_session_id.to_string());
  assert_eq!(begin.payload.session_id, "");
  assert_eq!(begin.payload.state_metadata.len(), 1);
  assert_eq!(begin.payload.feature_flags, []);
  progress(begin, State::STATE_RECEIVING, &[0]);
  let final_chunk = setup
    .finish_single_fragment(empty_session_id, b"prev-run-crash")
    .await;
  assert_eq!(final_chunk.state_metadata.len(), 1);
  assert_eq!(
    setup.upload_complete_rx.recv().await.unwrap(),
    empty_session_id.to_string()
  );
  setup.shutdown().await;
}

#[tokio::test]
async fn state_snapshot_uses_stored_zlib_and_preserves_source_on_mid_staging_failure() {
  let setup = Setup::new().await;
  setup.configure_admission(Vec::new()).await;

  let mut rng = StdRng::seed_from_u64(0x5a7e_5a90);
  let mut journal = vec![0_u8; usize::try_from(CHUNK_BYTES).unwrap() + 4096];
  rng.fill(&mut journal[..]);
  let source = PathBuf::from("state-snapshot-mid-fail.zz");
  setup
    .filesystem
    .write_file(&source, &journal)
    .await
    .unwrap();

  // Fail syncing the second chunk after the first chunk is written to disk.
  let failed_id = Uuid::new_v4();
  setup
    .filesystem
    .fail_next_sync_at(chunk_path(&failed_id.to_string(), 1));
  let failed_request = UploadArtifactRequest {
    artifact_id: failed_id.to_string(),
    type_id: "state_snapshot".into(),
    time: setup.time_provider.now().into_proto(),
    ..Default::default()
  };
  assert!(
    setup
      .stage_capture(source.clone(), failed_request)
      .await
      .unwrap()
      .is_err()
  );
  assert_eq!(setup.filesystem.read_file(&source).await.unwrap(), journal);
  assert!(
    !setup
      .filesystem
      .exists(&chunk_path(&failed_id.to_string(), 0))
      .await
      .unwrap()
  );
  assert!(
    !setup
      .filesystem
      .exists(&chunk_path(&failed_id.to_string(), 1))
      .await
      .unwrap()
  );

  // Retry succeeds with stored zlib blocks (level 0) and removes the original source after commit.
  let (tx, rx) = oneshot::channel();
  let id = setup
    .client
    .enqueue_upload(
      UploadSource::Path(source.clone()),
      "state_snapshot".into(),
      LogFields::new(),
      None,
      String::new(),
      Vec::new(),
      None,
      Some(tx),
    )
    .unwrap();
  rx.await.unwrap().unwrap();
  assert!(!setup.filesystem.exists(&source).await.unwrap());

  let index = setup.chunk_index().await;
  let entry = index
    .chunked_artifact
    .iter()
    .find(|entry| entry.artifact_id == id.to_string())
    .unwrap();
  assert_eq!(entry.chunks.len(), 2);
  let mut encoded = setup
    .filesystem
    .read_file(&chunk_path(&id.to_string(), 0))
    .await
    .unwrap();
  // RFC 1950 zlib header (2 bytes) followed by an uncompressed DEFLATE block (BTYPE = 00).
  assert_eq!((encoded[2] >> 1) & 0b11, 0);
  encoded.extend(
    setup
      .filesystem
      .read_file(&chunk_path(&id.to_string(), 1))
      .await
      .unwrap(),
  );
  assert_eq!(read_compressed(&encoded).unwrap(), journal);
  setup.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn chunked_rejects_symlinks_and_non_files_without_touching_targets() {
  let setup = Setup::new().await;
  setup.configure_admission(Vec::new()).await;
  let target = setup.capture(Uuid::new_v4(), b"keep").await;
  let symlink = setup.filesystem.directory().join("symlink");
  std::os::unix::fs::symlink(&target, &symlink).unwrap();
  assert!(
    setup
      .stage_capture(symlink, setup.request(Uuid::new_v4()))
      .await
      .unwrap()
      .is_err()
  );
  assert_eq!(std::fs::read(target).unwrap(), b"keep");
  let directory = setup.filesystem.directory().join("directory");
  std::fs::create_dir(&directory).unwrap();
  assert!(
    setup
      .stage_capture(directory.clone(), setup.request(Uuid::new_v4()))
      .await
      .unwrap()
      .is_err()
  );
  assert!(directory.is_dir());
  setup.shutdown().await;
}
