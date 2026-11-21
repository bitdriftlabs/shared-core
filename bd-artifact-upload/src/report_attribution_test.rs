// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::*;
use bd_client_common::file::{write_compressed, write_compressed_protobuf};
use bd_metadata::artifact::ARTIFACT_CHUNK_BYTES;
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::{
  ReportArgs,
  ReportType,
};
use bd_proto::protos::client::api::{
  ArtifactChunkDescriptor,
  ArtifactPayloadEncoding,
  UploadArtifactRequest,
};
use bd_proto::protos::client::artifact::ArtifactUploadIndex;
use bd_time::OffsetDateTimeExt as _;
use flatbuffers::FlatBufferBuilder;
use flate2::Compression;
use flate2::write::ZlibEncoder;
use parking_lot::Mutex;
use sha2::{Digest as _, Sha256};
use std::io::Write as _;
use time::OffsetDateTime;
use tokio::fs;

#[derive(Default)]
struct Recorder(Mutex<Vec<(Uuid, ReportType, String)>>);

impl ReportAttribution for Recorder {
  fn add_report(&self, report_id: Uuid, report: &Report<'_>, session_id: &str) {
    self
      .0
      .lock()
      .push((report_id, report.type_(), session_id.into()));
  }

  fn mark_ready(&self) {}

  fn start(&self, _: Arc<dyn Client>, _: &ConfigLoader, _: ComponentShutdown) {}
}

#[tokio::test]
async fn restores_attribution_from_valid_queued_chunks() {
  let directory = tempfile::tempdir().unwrap();
  let uploads = directory.path().join("report_uploads");
  fs::create_dir_all(&uploads).await.unwrap();
  let mut builder = FlatBufferBuilder::new();
  let report = Report::create(
    &mut builder,
    &ReportArgs {
      type_: ReportType::AppNotResponding,
      ..Default::default()
    },
  );
  builder.finish(report, None);
  let logical = builder.finished_data();
  let encoded = write_compressed(logical).unwrap();
  let entry = |id: Uuid, type_id: &str| UploadArtifactRequest {
    artifact_id: id.to_string(),
    type_id: type_id.into(),
    session_id: "session".into(),
    time: OffsetDateTime::UNIX_EPOCH.into_proto(),
    logical_bytes: Some(logical.len() as u64),
    logical_sha256: Sha256::digest(logical).to_vec(),
    encoded_bytes: Some(encoded.len() as u64),
    payload_encoding: ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_ZLIB.into(),
    chunks: vec![ArtifactChunkDescriptor {
      encoded_bytes: u32::try_from(encoded.len()).unwrap(),
      encoded_sha256: Sha256::digest(&encoded).to_vec(),
      ..Default::default()
    }],
    ..Default::default()
  };
  let report_id = Uuid::new_v4();
  let corrupt_id = Uuid::new_v4();
  let attachment_id = Uuid::new_v4();
  for id in [report_id, corrupt_id, attachment_id] {
    let bytes = if id == corrupt_id {
      b"corrupt".as_slice()
    } else {
      encoded.as_slice()
    };
    fs::write(uploads.join(format!("chunk-{id}-0")), bytes)
      .await
      .unwrap();
  }
  let index = ArtifactUploadIndex {
    chunked_artifact: vec![
      entry(report_id, CLIENT_REPORT_ARTIFACT_TYPE_ID),
      entry(corrupt_id, CLIENT_REPORT_ARTIFACT_TYPE_ID),
      entry(Uuid::new_v4(), CLIENT_REPORT_ARTIFACT_TYPE_ID),
      entry(attachment_id, "workflow_attachment"),
    ],
    ..Default::default()
  };
  fs::write(
    uploads.join("report_index.pb"),
    write_compressed_protobuf(&index).unwrap(),
  )
  .await
  .unwrap();

  let recorder = Recorder::default();
  restore_pending_reports(&recorder, directory.path()).await;
  assert_eq!(
    *recorder.0.lock(),
    vec![(report_id, ReportType::AppNotResponding, "session".into())]
  );

  // A missing index restores nothing and is not an error for ordinary report delivery.
  let empty = Recorder::default();
  restore_pending_reports(&empty, &directory.path().join("missing")).await;
  assert_eq!(*empty.0.lock(), Vec::new());
}

#[tokio::test]
async fn restores_attribution_from_multichunk_queued_report() {
  let directory = tempfile::tempdir().unwrap();
  let uploads = directory.path().join("report_uploads");
  fs::create_dir_all(&uploads).await.unwrap();
  let chunk_size = usize::try_from(ARTIFACT_CHUNK_BYTES).unwrap();
  let mut builder = FlatBufferBuilder::new();
  let padding = builder.create_vector(&vec![0xab_u8; chunk_size + 1024]);
  let report = Report::create(
    &mut builder,
    &ReportArgs {
      type_: ReportType::AppNotResponding,
      crash_profile_source: Some(padding),
      ..Default::default()
    },
  );
  builder.finish(report, None);
  let logical = builder.finished_data();
  let mut encoder = ZlibEncoder::new(Vec::new(), Compression::none());
  encoder.write_all(logical).unwrap();
  let encoded = encoder.finish().unwrap();
  let report_id = Uuid::new_v4();
  let mut chunks = Vec::new();
  for (index, fragment) in encoded.chunks(chunk_size).enumerate() {
    fs::write(uploads.join(format!("chunk-{report_id}-{index}")), fragment)
      .await
      .unwrap();
    chunks.push(ArtifactChunkDescriptor {
      encoded_bytes: u32::try_from(fragment.len()).unwrap(),
      encoded_sha256: Sha256::digest(fragment).to_vec(),
      ..Default::default()
    });
  }
  assert!(chunks.len() > 1);
  let index = ArtifactUploadIndex {
    chunked_artifact: vec![UploadArtifactRequest {
      artifact_id: report_id.to_string(),
      type_id: CLIENT_REPORT_ARTIFACT_TYPE_ID.into(),
      session_id: "session".into(),
      time: OffsetDateTime::UNIX_EPOCH.into_proto(),
      logical_bytes: Some(logical.len() as u64),
      logical_sha256: Sha256::digest(logical).to_vec(),
      encoded_bytes: Some(encoded.len() as u64),
      payload_encoding: ArtifactPayloadEncoding::ARTIFACT_PAYLOAD_ENCODING_ZLIB.into(),
      chunks,
      ..Default::default()
    }],
    ..Default::default()
  };
  fs::write(
    uploads.join("report_index.pb"),
    write_compressed_protobuf(&index).unwrap(),
  )
  .await
  .unwrap();

  let recorder = Recorder::default();
  restore_pending_reports(&recorder, directory.path()).await;
  assert_eq!(
    *recorder.0.lock(),
    vec![(report_id, ReportType::AppNotResponding, "session".into())]
  );
}
