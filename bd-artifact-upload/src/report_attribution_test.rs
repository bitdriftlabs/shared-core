// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::*;
use bd_client_common::file::{write_checksummed_data, write_compressed_protobuf};
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::{
  ReportArgs,
  ReportType,
};
use bd_proto::protos::client::artifact::ArtifactUploadIndex;
use bd_proto::protos::client::artifact::artifact_upload_index::Artifact;
use flatbuffers::FlatBufferBuilder;
use parking_lot::Mutex;

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

fn entry(name: &str, type_id: &str) -> Artifact {
  Artifact {
    name: name.into(),
    type_id: Some(type_id.into()),
    session_id: "session".into(),
    ..Default::default()
  }
}

fn anr_report() -> Vec<u8> {
  let mut builder = FlatBufferBuilder::new();
  let report = Report::create(
    &mut builder,
    &ReportArgs {
      type_: ReportType::AppNotResponding,
      ..Default::default()
    },
  );
  builder.finish(report, None);
  builder.finished_data().to_vec()
}

#[tokio::test]
async fn restores_readable_pending_client_reports_from_their_payloads() {
  let directory = tempfile::tempdir().unwrap();
  let uploads = directory.path().join("report_uploads");
  std::fs::create_dir_all(&uploads).unwrap();
  let report_id = Uuid::new_v4();
  let corrupt_id = Uuid::new_v4();
  let attachment_id = Uuid::new_v4();
  std::fs::write(
    uploads.join(report_id.to_string()),
    write_checksummed_data(&anr_report()),
  )
  .unwrap();
  std::fs::write(uploads.join(corrupt_id.to_string()), b"corrupt").unwrap();
  std::fs::write(
    uploads.join(attachment_id.to_string()),
    write_checksummed_data(&anr_report()),
  )
  .unwrap();
  let index = ArtifactUploadIndex {
    artifact: vec![
      entry(&report_id.to_string(), CLIENT_REPORT_ARTIFACT_TYPE_ID),
      entry(&corrupt_id.to_string(), CLIENT_REPORT_ARTIFACT_TYPE_ID),
      entry(&Uuid::new_v4().to_string(), CLIENT_REPORT_ARTIFACT_TYPE_ID),
      entry(&attachment_id.to_string(), "crash_profile"),
      entry("not-a-uuid", CLIENT_REPORT_ARTIFACT_TYPE_ID),
    ],
    ..Default::default()
  };
  std::fs::write(
    uploads.join("report_index.pb"),
    write_compressed_protobuf(&index).unwrap(),
  )
  .unwrap();

  let recorder = Recorder::default();
  restore_pending_reports(&recorder, directory.path()).await;
  assert_eq!(
    *recorder.0.lock(),
    vec![(
      report_id,
      ReportType::AppNotResponding,
      "session".to_string()
    )]
  );

  // A missing index restores nothing and is not an error for ordinary report delivery.
  let empty = Recorder::default();
  restore_pending_reports(&empty, &directory.path().join("missing")).await;
  assert_eq!(*empty.0.lock(), Vec::new());
}
