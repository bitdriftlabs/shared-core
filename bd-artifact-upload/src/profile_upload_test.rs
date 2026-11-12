// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::*;
use crate::uploader::NewUpload;
use bd_client_common::artifact::{CLIENT_REPORT_ARTIFACT_TYPE_ID, CRASH_PROFILE_ARTIFACT_TYPE_ID};
use bd_client_common::file::{read_compressed, write_compressed};
use bd_macros::ApproximateSize;
use bd_proto::protos::client::api::CrashProfileMetadata;

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
