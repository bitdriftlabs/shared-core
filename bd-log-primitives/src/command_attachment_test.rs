// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{AttachmentSource, LogAttachment, extract_command_attachment};
use bd_proto::protos::logging::payload::{CommandAttachment, Log};
use bd_proto_util::serialization::ProtoMessageSerialize;
use protobuf::{CodedOutputStream, Message};

#[test]
fn native_attachment_encoding() {
  let attachment = LogAttachment {
    artifact_id: "artifact".to_string(),
    content_type: "image/jpeg".to_string(),
    size_bytes: 123,
    source: AttachmentSource::WORKFLOW,
  };
  let mut bytes = Vec::new();
  let mut stream = CodedOutputStream::vec(&mut bytes);
  attachment.serialize_message(&mut stream).unwrap();
  stream.flush().unwrap();
  drop(stream);
  let decoded = CommandAttachment::parse_from_bytes(&bytes).unwrap();
  assert_eq!(decoded.artifact_id, attachment.artifact_id);
  assert_eq!(decoded.content_type, attachment.content_type);
  assert_eq!(decoded.size_bytes, attachment.size_bytes);
  assert_eq!(decoded.source.enum_value().unwrap(), attachment.source);
  assert_eq!(attachment.compute_message_size(), bytes.len() as u64);
}

#[test]
fn reads_metadata_without_inflating_contents() {
  let attachment = CommandAttachment {
    artifact_id: "artifact".to_string(),
    content_type: "image/jpeg".to_string(),
    size_bytes: 0,
    source: AttachmentSource::WORKFLOW.into(),
    ..Default::default()
  };
  let log = Log {
    session_id: "session".to_string(),
    command_attachment: Some(attachment.clone()).into(),
    compressed_contents: b"not a zlib stream".to_vec(),
    ..Default::default()
  };
  assert_eq!(
    extract_command_attachment(&log.write_to_bytes().unwrap()).unwrap(),
    Some(("session".to_string(), attachment))
  );
  assert_eq!(extract_command_attachment(&[]).unwrap(), None);
  assert!(extract_command_attachment(&[82, 4, 10]).is_err());
  assert!(extract_command_attachment(&[80, 1]).is_err());
}

#[test]
fn reordered_and_repeated_metadata_matches_log_decoder() {
  let mut bytes = Vec::new();
  let mut stream = CodedOutputStream::vec(&mut bytes);
  stream
    .write_message(
      10,
      &CommandAttachment {
        artifact_id: "artifact".to_string(),
        content_type: "image/jpeg".to_string(),
        size_bytes: 7,
        source: AttachmentSource::WORKFLOW.into(),
        ..Default::default()
      },
    )
    .unwrap();
  stream.write_string(5, "first-session").unwrap();
  stream.write_bytes(100, b"unknown field").unwrap();
  stream.write_bytes(9, b"not a zlib stream").unwrap();
  stream
    .write_message(
      10,
      &CommandAttachment {
        content_type: "text/plain".to_string(),
        ..Default::default()
      },
    )
    .unwrap();
  stream.write_string(5, "last-session").unwrap();
  stream.flush().unwrap();
  drop(stream);

  let decoded = Log::parse_from_bytes(&bytes).unwrap();
  let attachment = decoded.command_attachment.into_option().unwrap();
  assert_eq!(attachment.artifact_id, "");
  assert_eq!(attachment.content_type, "text/plain");
  assert_eq!(attachment.size_bytes, 0);
  assert_eq!(
    extract_command_attachment(&bytes).unwrap(),
    Some((decoded.session_id, attachment))
  );
}

#[test]
fn malformed_metadata_is_rejected() {
  for bytes in [
    &[42, 2, b'x'][..],
    &[45, 1, 2, 3, 4],
    &[82, 128],
    &[85, 1, 2, 3, 4],
    &[0],
  ] {
    assert!(extract_command_attachment(bytes).is_err(), "{bytes:?}");
  }
}
