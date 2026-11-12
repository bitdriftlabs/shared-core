// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used, clippy::panic)]

use super::chunked::{CHUNK_BYTES, MAX_INDEX_BYTES, chunk_path, source_path};
use super::tests::{Setup, wait_for_queue_revision};
use super::{
  ARTIFACT_UPLOAD_DIRECTORY,
  Client,
  EnqueueError,
  REPORT_INDEX_FILE,
  UploadSource,
  Uploader,
};
use bd_api::DataUpload;
use bd_api::upload::{IntentDecision, IntentResponse, TrackedArtifactUpload};
use bd_client_common::artifact::MAX_CHUNKED_ARTIFACT_LOGICAL_BYTES;
use bd_client_common::file::{read_compressed_protobuf, write_compressed_protobuf};
use bd_client_common::file_system::FileSystem;
use bd_log_primitives::LogFields;
use bd_proto::protos::client::api::upload_artifact_response::State;
use bd_proto::protos::client::api::{
  ArtifactChunkDescriptor,
  ArtifactPayloadEncoding,
  UploadArtifactRequest,
  UploadArtifactResponse,
};
use bd_proto::protos::client::artifact::ArtifactUploadIndex;
use bd_proto::protos::client::artifact::artifact_upload_index::Artifact;
use bd_proto::protos::logging::payload::Data;
use bd_proto::protos::logging::payload::data::Data_type;
use bd_runtime::runtime::{FeatureFlag as _, chunked_artifact_upload};
use bd_test_helpers::runtime::{ValueKind, make_update};
use bd_time::{OffsetDateTimeExt as _, TimeProvider as _};
use flate2::read::ZlibDecoder;
use protobuf::Message;
use sha2::{Digest, Sha256};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use tokio::sync::oneshot;
use uuid::Uuid;

impl Setup {
  async fn next_chunk_operation(&mut self) -> TrackedArtifactUpload {
    loop {
      let DataUpload::ArtifactUpload(operation) = self.data_upload_rx.recv().await.unwrap() else {
        panic!("expected chunk-mode operation");
      };
      assert!(operation.payload.logical_bytes.is_some());
      if !operation.response_tx.is_closed() {
        return operation;
      }
    }
  }
  async fn enable_chunked(&self, extra: Vec<(&str, ValueKind)>) {
    let mut flags = vec![
      (
        chunked_artifact_upload::Enabled::path(),
        ValueKind::Bool(true),
      ),
      (
        chunked_artifact_upload::WorkflowEnabled::path(),
        ValueKind::Bool(true),
      ),
    ];
    flags.extend(extra);
    self
      .runtime
      .update_snapshot(make_update(flags, Uuid::new_v4().to_string()))
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
    self
      .client
      .enqueue_chunked_file(path, request, tx, None)
      .unwrap();
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

fn progress(operation: TrackedArtifactUpload, state: State, receipts: &[u8]) {
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

fn thin(template: &UploadArtifactRequest, operation: &TrackedArtifactUpload, chunk: Option<u32>) {
  let request = &operation.payload;
  let expected = UploadArtifactRequest {
    upload_uuid: operation.uuid.clone(),
    artifact_id: template.artifact_id.clone(),
    command_id: template.command_id.clone(),
    logical_bytes: template.logical_bytes,
    chunk_index: chunk,
    contents: request.contents.clone(),
    ..Default::default()
  };
  assert_eq!(request, &expected);
  if let Some(index) = chunk {
    assert_eq!(
      Sha256::digest(&request.contents).as_slice(),
      template.chunks[index as usize].encoded_sha256
    );
  } else {
    assert_eq!(request.contents, b"");
  }
}

#[tokio::test(start_paused = true)]
async fn chunk_transfer_replays_each_lost_ack_and_waits_for_durable_finalization() {
  let mut setup = Setup::new(1).await;
  setup.enable_chunked(Vec::new()).await;
  let id = Uuid::new_v4();
  let path = setup
    .capture(id, &vec![42; usize::try_from(CHUNK_BYTES).unwrap() + 1])
    .await;
  let (persisted_tx, persisted_rx) = oneshot::channel();
  let (completion_tx, mut completion_rx) = oneshot::channel();
  setup
    .client
    .enqueue_chunked_file(path, setup.request(id), persisted_tx, Some(completion_tx))
    .unwrap();
  assert_eq!(persisted_rx.await.unwrap().unwrap(), CHUNK_BYTES + 1);
  let template = setup
    .chunk_index()
    .await
    .chunked_artifact
    .remove(0)
    .request
    .unwrap();
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
  thin(&template, &chunk, Some(0));
  drop(chunk);
  let resume = setup.next_chunk_operation().await;
  assert_eq!(resume.payload.chunks, template.chunks);
  progress(resume, State::STATE_RECEIVING, &[1]);
  let chunk = setup.next_chunk_operation().await;
  thin(&template, &chunk, Some(1));
  let chunk_bytes = chunk.payload.contents.clone();
  drop(chunk);
  let resume = setup.next_chunk_operation().await;
  progress(resume, State::STATE_RECEIVING, &[1]);
  let chunk = setup.next_chunk_operation().await;
  assert_eq!(chunk.payload.contents, chunk_bytes);
  progress(chunk, State::STATE_RECEIVING, &[3]);
  let query = setup.next_chunk_operation().await;
  thin(&template, &query, None);
  assert!(completion_rx.try_recv().is_err());
  assert!(
    setup
      .filesystem
      .exists(&chunk_path(&id.to_string(), 0))
      .await
      .unwrap()
  );
  progress(query, State::STATE_FINALIZING, &[3]);
  let query = setup.next_chunk_operation().await;
  thin(&template, &query, None);
  drop(query);
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
  let mut setup = Setup::new(1).await;
  setup.enable_chunked(Vec::new()).await;
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
  let mut setup = Setup::new(1).await;
  setup.enable_chunked(Vec::new()).await;
  let id = Uuid::new_v4();
  let path = setup.capture(id, b"file").await;
  let (persisted_tx, persisted_rx) = oneshot::channel();
  let (completion_tx, mut completion_rx) = oneshot::channel();
  setup
    .client
    .enqueue_chunked_file(path, setup.request(id), persisted_tx, Some(completion_tx))
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
async fn chunk_transfers_interleave_whole_file_traffic_without_eviction_or_fallback() {
  let mut setup = Setup::new(1).await;
  setup.enable_chunked(Vec::new()).await;
  let mut completions = Vec::new();
  for _ in 0 .. 2 {
    let path = setup.capture(Uuid::new_v4(), b"file").await;
    let (persisted_tx, persisted_rx) = oneshot::channel();
    let (completion_tx, completion_rx) = oneshot::channel();
    setup
      .client
      .enqueue_command_upload(
        UploadSource::Path(path),
        "device_command_attachment".into(),
        None,
        "session".into(),
        Vec::new(),
        Uuid::new_v4().to_string(),
        None,
        Some(persisted_tx),
        Some(completion_tx),
      )
      .unwrap();
    persisted_rx.await.unwrap().unwrap();
    completions.push(completion_rx);
  }
  // Exhausting dedicated chunk capacity is explicit rejection, never whole-file admission/eviction.
  let id = Uuid::new_v4();
  let path = setup.capture(id, b"third").await;
  assert!(
    setup
      .stage_capture(path, setup.request(id))
      .await
      .unwrap()
      .is_err()
  );
  setup.chunk_transfer_enabled.send(true).unwrap();
  let begin = setup.next_chunk_operation().await;
  let first = begin.payload.artifact_id.clone();
  let report = setup
    .client
    .enqueue_upload(
      UploadSource::Bytes(b"report".to_vec()),
      "report".into(),
      [].into(),
      None,
      "session".into(),
      Vec::new(),
      None,
      None,
    )
    .unwrap();
  assert_eq!(
    setup.entry_received_rx.recv().await.unwrap(),
    report.to_string()
  );
  progress(begin, State::STATE_RECEIVING, &[0]);
  // Depending on task polling, a waiting whole-file intent may precede the next Begin; both must
  // get capacity without allowing another chunk RPC to pass its predecessor's acknowledgement.
  let mut chunk_operations = Vec::new();
  let mut report_done = false;
  let mut completed = 0;
  while chunk_operations.len() < 3 || !report_done || completed < 3 {
    let upload = tokio::select! {
      upload = setup.data_upload_rx.recv() => upload.unwrap(),
      Some(_) = setup.upload_complete_rx.recv() => { completed += 1; continue; }
    };
    match upload {
      DataUpload::ArtifactUpload(operation) if operation.payload.logical_bytes.is_some() => {
        let request = &operation.payload;
        chunk_operations.push(request.artifact_id.clone());
        let state = if request.chunk_index.is_some() {
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
      DataUpload::ArtifactUploadIntent(intent) => {
        assert_eq!(intent.payload.artifact_id, report.to_string());
        intent
          .response_tx
          .send(IntentResponse {
            uuid: intent.uuid,
            decision: IntentDecision::UploadImmediately,
          })
          .unwrap();
      },
      DataUpload::ArtifactUpload(upload) => {
        assert_eq!(upload.payload.artifact_id, report.to_string());
        upload
          .response_tx
          .send(UploadArtifactResponse {
            upload_uuid: upload.uuid,
            ..Default::default()
          })
          .unwrap();
        report_done = true;
      },
      other => panic!("unexpected upload: {other:?}"),
    }
  }
  assert_ne!(chunk_operations[0], first);
  assert_eq!(chunk_operations[1], first);
  assert_ne!(chunk_operations[2], first);
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
  let setup = Setup::new(2).await;
  setup.enable_chunked(Vec::new()).await;
  let chunk_id = Uuid::new_v4();
  let capture = setup.capture(chunk_id, b"chunked").await;
  setup
    .stage_capture(capture, setup.request(chunk_id))
    .await
    .unwrap()
    .unwrap();
  // A chunked entry requires durability for every subsequent shared-index publication, including
  // whole-file admissions. Failure after rename must not restore/unlink the now-indexed snapshot.
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
  assert!(!setup.filesystem.exists(&source).await.unwrap());
  let payload = ARTIFACT_UPLOAD_DIRECTORY.join(id.to_string());
  assert_eq!(
    setup.filesystem.read_file(&payload).await.unwrap(),
    b"snapshot"
  );
  let setup = recovered(setup).await;
  assert_eq!(
    setup.filesystem.read_file(&payload).await.unwrap(),
    b"snapshot"
  );
  let index = setup.chunk_index().await;
  assert!(
    index
      .artifact
      .iter()
      .any(|entry| entry.name == id.to_string())
  );
  assert_eq!(index.chunked_artifact.len(), 1);
  setup.shutdown().await;
}

#[tokio::test]
async fn chunked_unpublished_index_failure_rolls_back_only_created_paths() {
  let setup = Setup::new(1).await;
  setup.enable_chunked(Vec::new()).await;
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
async fn chunked_mixed_encoding_hashes_geometry_and_restart() {
  let setup = Setup::new(1).await;
  setup.enable_chunked(Vec::new()).await;
  let id = Uuid::new_v4();
  let chunk_bytes = usize::try_from(CHUNK_BYTES).unwrap();
  let mut bytes = vec![0; chunk_bytes];
  let mut random = 0x1234_5678_u32;
  for _ in 0 .. CHUNK_BYTES {
    random ^= random << 13;
    random ^= random >> 17;
    random ^= random << 5;
    bytes.push(random.to_le_bytes()[0]);
  }
  bytes.extend_from_slice(b"short last chunk");
  let path = setup.capture(id, &bytes).await;
  let metadata = setup.request(id);
  let capture_time = metadata.time.clone();
  assert_eq!(
    setup
      .stage_capture(path.clone(), metadata)
      .await
      .unwrap()
      .unwrap(),
    bytes.len() as u64
  );
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
  let template = index.chunked_artifact[0].request.as_ref().unwrap();
  assert_eq!(template.chunks.len(), 3);
  assert_eq!(template.time, capture_time);
  assert!(template.contents.is_empty() && template.upload_uuid.is_empty());
  assert!(template.chunk_index.is_none());
  assert_eq!(template.logical_sha256, Sha256::digest(&bytes).as_slice());
  let mut reconstructed = Vec::new();
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
    let start = position * chunk_bytes;
    let raw = &bytes[start .. bytes.len().min(start + chunk_bytes)];
    assert_eq!(Sha256::digest(raw).as_slice(), descriptor.logical_sha256);
    if position != 0 {
      assert_eq!(
        descriptor.encoding.enum_value().unwrap(),
        ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_RAW
      );
      reconstructed.extend(encoded);
    } else {
      assert_eq!(
        descriptor.encoding.enum_value().unwrap(),
        ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_ZLIB
      );
      ZlibDecoder::new(encoded.as_slice())
        .read_to_end(&mut reconstructed)
        .unwrap();
    }
  }
  assert_eq!(reconstructed, bytes);
  let mut setup = recovered(setup).await;
  assert_eq!(setup.chunk_index().await, index);
  assert!(
    setup.data_upload_rx.try_recv().is_err(),
    "M2 must not transmit chunked spools"
  );
  setup.shutdown().await;
}

#[tokio::test]
async fn chunked_persistence_requires_payload_and_index_fsync() {
  for fail_index in [false, true] {
    let setup = Setup::new(1).await;
    setup.enable_chunked(Vec::new()).await;
    let id = Uuid::new_v4();
    let path = setup.capture(id, b"file").await;
    let target = if fail_index {
      ARTIFACT_UPLOAD_DIRECTORY.join(&*REPORT_INDEX_FILE)
    } else {
      chunk_path(&id.to_string(), 0)
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
  let setup = Setup::new(1).await;
  setup.enable_chunked(Vec::new()).await;
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
    let setup = Setup::new(1).await;
    setup.enable_chunked(Vec::new()).await;
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
async fn chunked_runtime_reduction_preserves_accepted_slots_and_bytes() {
  let setup = Setup::new(1).await;
  setup.enable_chunked(Vec::new()).await;
  for _ in 0 .. 2 {
    let id = Uuid::new_v4();
    let path = setup.capture(id, b"file").await;
    setup
      .stage_capture(path, setup.request(id))
      .await
      .unwrap()
      .unwrap();
  }
  let index = setup.chunk_index().await;
  let id = Uuid::new_v4();
  let path = setup.capture(id, b"third slot").await;
  assert!(
    setup
      .stage_capture(path.clone(), setup.request(id))
      .await
      .unwrap()
      .is_err()
  );
  assert!(!path.exists());
  setup
    .enable_chunked(vec![
      (
        chunked_artifact_upload::MaxAdmittedFiles::path(),
        ValueKind::Int(0),
      ),
      (
        chunked_artifact_upload::MaxLogicalBytes::path(),
        ValueKind::Int(0),
      ),
      (
        chunked_artifact_upload::MaxOwnedPayloadBytes::path(),
        ValueKind::Int(0),
      ),
    ])
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
async fn chunked_logical_and_source_plus_staging_byte_admission() {
  for (logical_limit, budget, accepted) in [(3, 100, false), (4, 7, false), (4, 8, true)] {
    let setup = Setup::new(1).await;
    setup
      .enable_chunked(vec![
        (
          chunked_artifact_upload::MaxLogicalBytes::path(),
          ValueKind::Int(logical_limit),
        ),
        (
          chunked_artifact_upload::MaxOwnedPayloadBytes::path(),
          ValueKind::Int(budget),
        ),
      ])
      .await;
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
async fn chunked_hard_logical_ceiling_cannot_be_raised_by_runtime() {
  let setup = Setup::new(1).await;
  setup
    .enable_chunked(vec![(
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
    .set_len(u64::from(MAX_CHUNKED_ARTIFACT_LOGICAL_BYTES) + 1)
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
async fn chunked_admission_reserves_bounded_shared_index_metadata() {
  let setup = Setup::new(1).await;
  setup.enable_chunked(Vec::new()).await;
  let mut revision = setup.client.upload_queue_revision();
  wait_for_queue_revision(&mut revision, 0).await;
  let whole_file_id = Uuid::new_v4();
  setup.abort_for_recovery();
  let index = ArtifactUploadIndex {
    artifact: vec![Artifact {
      name: whole_file_id.to_string(),
      pending_intent_negotiation: true,
      metadata: [(
        "whole_file".into(),
        Data {
          data_type: Some(Data_type::StringData(
            "x".repeat(usize::try_from(MAX_INDEX_BYTES).unwrap() - 32 * 1024),
          )),
          ..Default::default()
        },
      )]
      .into(),
      ..Default::default()
    }],
    ..Default::default()
  };
  setup
    .filesystem
    .write_file(
      &ARTIFACT_UPLOAD_DIRECTORY.join(whole_file_id.to_string()),
      b"whole-file",
    )
    .await
    .unwrap();
  setup.write_chunk_index(&index).await;
  let setup = recovered(setup).await;
  let id = Uuid::new_v4();
  let path = setup.capture(id, b"file").await;
  assert!(
    setup
      .stage_capture(path.clone(), setup.request(id))
      .await
      .unwrap()
      .is_err()
  );
  assert!(!path.exists());
  assert_eq!(setup.chunk_index().await, index);
  setup.shutdown().await;
}

#[tokio::test]
async fn chunked_recovery_rejects_invalid_templates_and_inconsistent_chunks() {
  for corruption in 0 .. 8 {
    let setup = Setup::new(1).await;
    setup.enable_chunked(Vec::new()).await;
    let id = Uuid::new_v4();
    let path = setup.capture(id, b"file").await;
    setup
      .stage_capture(path, setup.request(id))
      .await
      .unwrap()
      .unwrap();
    let mut index = setup.chunk_index().await;
    let template = index.chunked_artifact[0].request.as_mut().unwrap();
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
async fn chunked_recovery_bounds_compressed_and_decoded_index() {
  for compressed in [false, true] {
    let setup = Setup::new(1).await;
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
    let setup = Setup::new(1).await;
    setup.enable_chunked(Vec::new()).await;
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
    // Both an interrupted transaction and a committed one leave exact capacity for one more.
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
  let setup = Setup::new(1).await;
  setup
    .enable_chunked(vec![
      (
        chunked_artifact_upload::MaxAdmittedFiles::path(),
        ValueKind::Int(1),
      ),
      (
        chunked_artifact_upload::MaxOwnedPayloadBytes::path(),
        ValueKind::Int(8),
      ),
    ])
    .await;
  let id = Uuid::new_v4();
  let metadata = setup.request(id);
  let path = setup.capture(id, b"file").await;
  setup.stage_capture(path, metadata).await.unwrap().unwrap();
  let index = setup.chunk_index().await;
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
async fn chunked_empty_remote_and_nonempty_workflow_and_master_gate() {
  for (workflow, master_enabled, workflow_enabled, empty, accepted) in [
    (false, true, false, true, true),
    (true, true, true, true, false),
    (true, false, true, false, false),
    (true, true, false, false, false),
    (true, true, true, false, true),
  ] {
    let setup = Setup::new(1).await;
    setup
      .enable_chunked(vec![
        (
          chunked_artifact_upload::Enabled::path(),
          ValueKind::Bool(master_enabled),
        ),
        (
          chunked_artifact_upload::WorkflowEnabled::path(),
          ValueKind::Bool(workflow_enabled),
        ),
      ])
      .await;
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
      let template = index.chunked_artifact[0].request.as_ref().unwrap();
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
    let setup = Setup::new(1).await;
    setup.enable_chunked(Vec::new()).await;
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
  let setup = Setup::new(1).await;
  setup
    .enable_chunked(vec![(
      chunked_artifact_upload::MaxOwnedPayloadBytes::path(),
      ValueKind::Int(8),
    )])
    .await;
  let first = Uuid::new_v4();
  let path = setup.capture(first, b"file").await;
  setup
    .stage_capture(path, setup.request(first))
    .await
    .unwrap()
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
    setup
      .filesystem
      .read_file(&chunk_path(&first.to_string(), 0))
      .await
      .unwrap(),
    b"file"
  );
  setup.shutdown().await;
}

#[tokio::test]
async fn chunked_invalid_capture_metadata_consumes_owned_source_without_index_commit() {
  let setup = Setup::new(1).await;
  setup.enable_chunked(Vec::new()).await;
  for case in 0 .. 6 {
    let id = Uuid::new_v4();
    let path = setup.capture(id, b"file").await;
    let mut metadata = setup.request(id);
    match case {
      0 => metadata.artifact_id = "../escape".into(),
      1 => metadata.contents = vec![1],
      2 => metadata
        .state_metadata
        .insert(
          "origin".into(),
          Data {
            data_type: Some(Data_type::StringData("x".repeat(256))),
            ..Default::default()
          },
        )
        .map_or((), |_| ()),
      3 => metadata.content_type = Some("invalid\nmime".into()),
      4 => metadata.chunks.push(ArtifactChunkDescriptor::default()),
      5 => metadata.session_id = "x".repeat(256),
      _ => panic!("unknown invalid metadata"),
    }
    let (tx, _) = oneshot::channel();
    assert!(
      setup
        .client
        .enqueue_chunked_file(path.clone(), metadata, tx, None)
        .is_err()
    );
    assert!(!path.exists());
  }
  setup.shutdown().await;
}

#[tokio::test]
async fn chunked_whole_file_index_writes_preserve_workflow_chunk_ownership() {
  let mut setup = Setup::new(1).await;
  setup.enable_chunked(Vec::new()).await;
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
      "report".into(),
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
  assert_eq!(setup.chunk_index().await.chunked_artifact, before);
  assert_eq!(
    Uploader::pending_workflow_attachments(setup.filesystem.directory())
      .await
      .unwrap(),
    [id].into()
  );
  // Consume the test hook so shutdown/recovery cannot leave a producer blocked on its observer.
  setup.entry_received_rx.recv().await.unwrap();
  let setup = recovered(setup).await;
  assert_eq!(setup.chunk_index().await.chunked_artifact, before);
  setup.shutdown().await;
}

#[tokio::test]
async fn chunked_failed_source_cleanup_stays_charged_until_recovery_retries_it() {
  let setup = Setup::new(1).await;
  setup
    .enable_chunked(vec![(
      chunked_artifact_upload::MaxOwnedPayloadBytes::path(),
      ValueKind::Int(10),
    )])
    .await;
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
async fn chunked_cross_mode_collision_does_not_delete_accepted_chunks() {
  let setup = Setup::new(1).await;
  setup.enable_chunked(Vec::new()).await;
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
    setup
      .filesystem
      .read_file(&chunk_path(&id.to_string(), 0))
      .await
      .unwrap(),
    b"file"
  );
  let mut index = original;
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
  let index = setup.chunk_index().await;
  assert!(index.artifact.is_empty() && index.chunked_artifact.is_empty());
  setup.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn chunked_rejects_symlinks_and_non_files_without_touching_targets() {
  let setup = Setup::new(1).await;
  setup.enable_chunked(Vec::new()).await;
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
  let (tx, _) = oneshot::channel();
  assert!(
    setup
      .client
      .enqueue_chunked_file(
        Path::new("relative").into(),
        setup.request(Uuid::new_v4()),
        tx,
        None
      )
      .is_err()
  );
  setup.shutdown().await;
}
