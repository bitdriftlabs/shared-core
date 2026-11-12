// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::*;
use crate::uploader::NewUpload;
use bd_api::upload::TrackedArtifactUpload;
use bd_client_common::artifact::{CLIENT_REPORT_ARTIFACT_TYPE_ID, CRASH_PROFILE_ARTIFACT_TYPE_ID};
use bd_client_common::file::{read_compressed, write_compressed};
use bd_macros::ApproximateSize;
use bd_proto::protos::client::api::CrashProfileMetadata;
use bd_proto::protos::client::api::upload_artifact_response::State;
use bd_runtime::runtime::chunked_artifact_upload;

async fn enable_chunks(setup: &Setup) {
  setup
    .runtime
    .update_snapshot(bd_test_helpers::runtime::make_update(
      vec![
        (
          chunked_artifact_upload::Enabled::path(),
          ValueKind::Bool(true),
        ),
        (attachment::MaxBytes::path(), ValueKind::Int(4)),
      ],
      "profiles".into(),
    ))
    .await
    .unwrap();
}

async fn chunk_request(setup: &mut Setup) -> TrackedArtifactUpload {
  loop {
    match timeout(5.std_seconds(), setup.data_upload_rx.recv())
      .await
      .unwrap()
      .unwrap()
    {
      DataUpload::ArtifactUpload(upload) if !upload.response_tx.is_closed() => return upload,
      DataUpload::ArtifactUpload(_) => {},
      _ => panic!("profile chunks must not use a whole-file intent"),
    }
  }
}

#[tokio::test]
async fn profile_chunks_survive_restart_and_policy_reduction_with_original_parents() {
  let mut setup = Setup::new(4).await;
  enable_chunks(&setup).await;
  let relative = PathBuf::from("crash_profiles/profile.data");
  fs::create_dir_all(setup.filesystem.directory().join("crash_profiles")).unwrap();
  let bytes = vec![37; 1024 * 1024 + 19];
  setup
    .filesystem
    .write_file(&relative, &bytes)
    .await
    .unwrap();
  let source = setup.filesystem.directory().join(&relative);
  let captured = datetime!(2025-01-02 03:04:05 UTC);
  let parents = CrashProfileMetadata {
    report_ids: (0 .. 16).map(|_| Uuid::new_v4().to_string()).collect(),
    ..Default::default()
  };
  let (tx, rx) = oneshot::channel();
  let id = setup
    .client
    .enqueue_crash_profile(
      UploadSource::Path(source.clone()),
      captured,
      "original-session".into(),
      "application/vnd.bitdrift.perfetto-trace".into(),
      parents.clone(),
      Some(tx),
    )
    .unwrap();
  rx.await.unwrap().unwrap();
  assert!(!source.exists());
  // Only durable uploader ownership survives admission-policy reductions.
  setup
    .runtime
    .update_snapshot(bd_test_helpers::runtime::make_update(
      vec![
        (
          chunked_artifact_upload::Enabled::path(),
          ValueKind::Bool(false),
        ),
        (
          chunked_artifact_upload::MaxLogicalBytes::path(),
          ValueKind::Int(1),
        ),
        (
          chunked_artifact_upload::MaxAdmittedFiles::path(),
          ValueKind::Int(0),
        ),
      ],
      "reduced".into(),
    ))
    .await
    .unwrap();
  setup = setup.crash_and_reinitialize().await;
  setup.chunk_transfer_enabled.send(true).unwrap();
  let begin = chunk_request(&mut setup).await;
  let template = begin.payload.clone();
  assert_eq!(template.artifact_id, id.to_string());
  assert_eq!(template.session_id, "original-session");
  assert_eq!(template.time, captured.into_proto());
  assert_eq!(template.crash_profile.as_ref(), Some(&parents));
  assert_eq!(template.state_metadata, command_state_metadata());
  assert!(template.command_id.is_none());
  let chunks = template.chunks.len();
  // Lose the first Begin ACK, then recover without attribution or a parent re-enqueue.
  drop(begin);
  setup = setup.crash_and_reinitialize().await;
  let begin = chunk_request(&mut setup).await;
  let mut replay = begin.payload.clone();
  replay.upload_uuid = template.upload_uuid.clone();
  assert_eq!(replay, template);
  let reply = |request: TrackedArtifactUpload, finalized: bool, received_chunks: Vec<u8>| {
    request
      .response_tx
      .send(UploadArtifactResponse {
        upload_uuid: request.uuid,
        state: if finalized {
          State::STATE_FINALIZED
        } else {
          State::STATE_RECEIVING
        }
        .into(),
        received_chunks,
        ..Default::default()
      })
      .unwrap();
  };
  reply(begin, false, vec![0]);
  let mut reconstructed = Vec::new();
  for (index, descriptor) in template.chunks.iter().enumerate() {
    let upload = chunk_request(&mut setup).await;
    assert_eq!(
      upload.payload.chunk_index,
      Some(u32::try_from(index).unwrap())
    );
    assert!(upload.payload.crash_profile.is_none());
    assert!(upload.payload.state_metadata.is_empty());
    assert_eq!(upload.payload.session_id, "");
    if descriptor.encoding.enum_value_or_default()
      == ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_ZLIB
    {
      reconstructed.extend(read_compressed(&upload.payload.contents).unwrap());
    } else {
      reconstructed.extend_from_slice(&upload.payload.contents);
    }
    if index + 1 == chunks {
      // Persistence and intermediate ACKs do not delete the durable upload.
      assert!(!setup.filesystem.files().is_empty());
    }
    reply(upload, index + 1 == chunks, vec![(1 << (index + 1)) - 1]);
  }
  assert_eq!(reconstructed, bytes);
  assert_eq!(
    setup.upload_complete_rx.recv().await.unwrap(),
    id.to_string()
  );
  setup.shutdown().await;
}

#[tokio::test]
async fn chunked_profile_parent_validation_and_owned_failure_cleanup() {
  let setup = Setup::new(4).await;
  enable_chunks(&setup).await;
  let parent = Uuid::new_v4().to_string();
  for parents in [
    vec![],
    vec![parent.clone(); 2],
    vec![parent.clone(); 17],
    vec![Uuid::nil().to_string()],
    vec![parent.to_uppercase()],
    vec!["invalid".into()],
  ] {
    let source = setup
      .filesystem
      .directory()
      .join(Uuid::new_v4().to_string());
    fs::write(&source, b"profile larger than ordinary limit").unwrap();
    let (tx, rx) = oneshot::channel();
    assert!(
      setup
        .client
        .enqueue_crash_profile(
          UploadSource::Path(source.clone()),
          OffsetDateTime::now_utc(),
          "session".into(),
          "application/vnd.bitdrift.perfetto-trace".into(),
          CrashProfileMetadata {
            report_ids: parents,
            ..Default::default()
          },
          Some(tx),
        )
        .is_err()
    );
    assert!(rx.await.is_err());
    assert!(!source.exists());
  }
  setup.shutdown().await;
}

#[tokio::test]
async fn profile_handoff_applies_current_admission_regardless_of_filename() {
  let mut setup = Setup::new(4).await;
  enable_chunks(&setup).await;
  fs::create_dir_all(setup.filesystem.directory().join("crash_profiles")).unwrap();
  for enabled in [true, false] {
    setup
      .runtime
      .update_snapshot(bd_test_helpers::runtime::make_update(
        vec![
          (
            chunked_artifact_upload::Enabled::path(),
            ValueKind::Bool(enabled),
          ),
          (
            chunked_artifact_upload::MaxLogicalBytes::path(),
            ValueKind::Int(16),
          ),
          (attachment::MaxBytes::path(), ValueKind::Int(4)),
        ],
        format!("policy-{enabled}"),
      ))
      .await
      .unwrap();
    for name in [
      "profile.anr".to_string(),
      format!("{}.chunked.oom", Uuid::new_v4()),
      format!("{}.chunked.anr.bitdrift-retained-anr", Uuid::new_v4()),
    ] {
      let source = setup
        .filesystem
        .directory()
        .join("crash_profiles")
        .join(name);
      fs::write(&source, [7; 32]).unwrap();
      let (tx, rx) = oneshot::channel();
      setup
        .client
        .enqueue_crash_profile(
          UploadSource::Path(source.clone()),
          OffsetDateTime::now_utc(),
          "session".into(),
          "application/vnd.bitdrift.perfetto-trace".into(),
          CrashProfileMetadata {
            report_ids: vec![Uuid::new_v4().to_string()],
            ..Default::default()
          },
          Some(tx),
        )
        .unwrap();
      assert!(rx.await.unwrap().is_err());
      assert!(!source.exists());
      if !enabled {
        // Whole-file test hooks also report rejected preparation attempts.
        setup.entry_received_rx.recv().await.unwrap();
      }
    }
  }
  setup.shutdown().await;
}

#[test]
fn legacy_indexes_decode_without_profile_metadata() {
  let index = read_compressed_protobuf::<ArtifactUploadIndex>(
    &write_compressed(b"\x0a\x08\x0a\x06legacy").unwrap(),
  )
  .unwrap();
  assert_eq!(index.artifact[0].name, "legacy");
  assert!(index.artifact[0].crash_profile.is_none());
}

#[tokio::test]
async fn profile_links_survive_restart_and_use_standard_intent_and_ack() {
  let linked = CrashProfileMetadata {
    report_ids: vec![Uuid::new_v4().to_string(), Uuid::new_v4().to_string()],
    ..Default::default()
  };
  for links in [None, Some(linked)] {
    let mut setup = Setup::new(4).await;
    let type_id = if links.is_some() {
      CRASH_PROFILE_ARTIFACT_TYPE_ID
    } else {
      CLIENT_REPORT_ARTIFACT_TYPE_ID
    };
    let (tx, rx) = oneshot::channel();
    let id = if let Some(links) = links.clone() {
      // Profiles use command-attachment storage: compressed, with the receiving build metadata.
      setup.client.enqueue_crash_profile(
        UploadSource::Bytes(b"artifact".to_vec()),
        OffsetDateTime::now_utc(),
        "source-session".into(),
        "application/vnd.bitdrift.perfetto-trace".into(),
        links,
        Some(tx),
      )
    } else {
      setup.client.enqueue_upload(
        UploadSource::Bytes(b"artifact".to_vec()),
        type_id.into(),
        [].into(),
        None,
        "source-session".into(),
        vec![],
        None,
        Some(tx),
      )
    }
    .unwrap()
    .to_string();
    rx.await.unwrap().unwrap();
    setup.entry_received_rx.recv().await.unwrap();
    let index = read_compressed_protobuf::<ArtifactUploadIndex>(
      &setup
        .filesystem
        .read_file(&ARTIFACT_UPLOAD_DIRECTORY.join(&*REPORT_INDEX_FILE))
        .await
        .unwrap(),
    )
    .unwrap();
    assert_eq!(index.artifact[0].name, id);
    assert_eq!(index.artifact[0].crash_profile.as_ref(), links.as_ref());
    setup = setup.reinitialize().await;
    let mut retried = false;
    loop {
      match setup.data_upload_rx.recv().await.unwrap() {
        DataUpload::ArtifactUploadIntent(intent) => {
          assert_eq!(intent.payload.artifact_id, id);
          assert_eq!(intent.payload.crash_profile.as_ref(), links.as_ref());
          if intent
            .response_tx
            .send(IntentResponse {
              uuid: intent.uuid,
              decision: IntentDecision::UploadImmediately,
            })
            .is_err()
          {
            log::debug!("pre-restart test intent responder was cancelled");
          }
        },
        DataUpload::ArtifactUpload(upload) => {
          assert_eq!(upload.payload.artifact_id, id);
          let contents = if links.is_some() {
            assert_eq!(
              upload.payload.payload_encoding.enum_value_or_default(),
              ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_ZLIB
            );
            assert_eq!(upload.payload.state_metadata, command_state_metadata());
            assert_eq!(
              upload.payload.content_type.as_deref(),
              Some("application/vnd.bitdrift.perfetto-trace")
            );
            read_compressed(&upload.payload.contents).unwrap()
          } else {
            upload.payload.contents.clone()
          };
          assert_eq!(contents, b"artifact");
          assert_eq!(upload.payload.crash_profile.as_ref(), links.as_ref());
          let success = links.is_none() || retried;
          upload
            .response_tx
            .send(UploadArtifactResponse {
              upload_uuid: upload.uuid,
              error: if success {
                String::new()
              } else {
                "temporary failure".into()
              },
              ..Default::default()
            })
            .unwrap();
          if success {
            break;
          }
          retried = true;
        },
        _ => panic!("unexpected upload kind"),
      }
    }
    assert_eq!(setup.upload_complete_rx.recv().await.unwrap(), id);
    setup.shutdown().await;
  }
}

#[test]
fn linked_report_storage_counts_toward_queue_size() {
  let mut upload = NewUpload::new(
    UploadSource::Bytes(vec![]),
    UploadArtifactRequest {
      artifact_id: Uuid::new_v4().to_string(),
      type_id: CRASH_PROFILE_ARTIFACT_TYPE_ID.into(),
      ..Default::default()
    },
    PreparationMode::WholeFile,
  );
  let without_links = upload.approximate_size_bytes();
  let mut report_id = String::with_capacity(1024);
  report_id.push_str(&Uuid::new_v4().to_string());
  upload.request.crash_profile = Some(CrashProfileMetadata {
    report_ids: vec![report_id],
    ..Default::default()
  })
  .into();
  assert!(upload.approximate_size_bytes() >= without_links + 1024);
}

#[tokio::test]
async fn profile_path_sources_follow_command_attachment_ownership_and_size_limit() {
  let mut setup = Setup::new(2).await;
  setup
    .runtime
    .update_snapshot(bd_test_helpers::runtime::make_update(
      vec![(attachment::MaxBytes::path(), ValueKind::Int(4))],
      "attachment limit".to_string(),
    ))
    .await
    .unwrap();
  std::fs::create_dir_all(".tmp").unwrap();
  let directory = tempfile::TempDir::with_prefix_in("profile-source", ".tmp").unwrap();
  let links = CrashProfileMetadata {
    report_ids: vec![Uuid::new_v4().to_string()],
    ..Default::default()
  };

  for (contents, accepted) in [(b"four".as_slice(), true), (b"fives", false)] {
    let source = directory
      .path()
      .canonicalize()
      .unwrap()
      .join(Uuid::new_v4().to_string());
    std::fs::write(&source, contents).unwrap();
    let (tx, rx) = oneshot::channel();
    let id = setup
      .client
      .enqueue_crash_profile(
        UploadSource::Path(source.clone()),
        OffsetDateTime::now_utc(),
        "session".into(),
        "application/vnd.bitdrift.perfetto-trace".into(),
        links.clone(),
        Some(tx),
      )
      .unwrap();
    assert_eq!(rx.await.unwrap().is_ok(), accepted);
    assert_eq!(
      setup.entry_received_rx.recv().await.unwrap(),
      id.to_string()
    );
    // The platform result is retired whether or not the attachment limit admits it.
    assert!(!source.exists());
    let stored = setup
      .filesystem
      .read_file(&ARTIFACT_UPLOAD_DIRECTORY.join(id.to_string()))
      .await;
    if accepted {
      assert_eq!(read_compressed(&stored.unwrap()).unwrap(), contents);
    } else {
      assert!(stored.is_err());
    }
  }
  setup.shutdown().await;
}
