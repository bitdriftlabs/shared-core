// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/licenses/strict/1.0.0

use super::{COMMAND_NAME, FakePerfettoCommand, TRACE};
use bd_artifact_upload::UploadSource;
use bd_logger::{CommandError, CommandInvocation, CommandResult, RegisteredCommandHandler};
use bd_proto::protos::logging::payload::Data;
use protobuf::{CodedInputStream, UnknownFields, UnknownValueRef, rt};

fn decode(bytes: &[u8]) -> UnknownFields {
  let mut fields = UnknownFields::new();
  let mut input = CodedInputStream::from_bytes(bytes);
  while let Some(tag) = input.read_raw_tag_or_eof().unwrap() {
    rt::read_unknown_or_skip_group(tag, &mut input, &mut fields).unwrap();
  }
  fields
}

fn bytes(fields: &UnknownFields, number: u32) -> &[u8] {
  match fields.get(number) {
    Some(UnknownValueRef::LengthDelimited(value)) => value,
    value => panic!("expected length-delimited field {number}, got {value:?}"),
  }
}

fn number(fields: &UnknownFields, number: u32) -> u64 {
  match fields.get(number) {
    Some(UnknownValueRef::Varint(value)) => value,
    value => panic!("expected varint field {number}, got {value:?}"),
  }
}

#[test]
fn fake_perfetto_decodes_to_a_supported_sample_with_resolvable_interning() {
  // Use the Perfetto wire field numbers without depending on backend-only generated schemas.
  let trace = decode(TRACE);
  let packets: Vec<_> = trace
    .iter()
    .map(|(field, value)| {
      assert_eq!(field, 1);
      let UnknownValueRef::LengthDelimited(packet) = value else {
        panic!("expected a TracePacket");
      };
      decode(packet)
    })
    .collect();
  let [descriptor_packet, event_packet, sample_packet] = packets.as_slice() else {
    panic!("expected a descriptor, event, and sample");
  };
  assert_eq!(number(descriptor_packet, 8), 1);
  let track = decode(bytes(descriptor_packet, 60));
  assert_eq!(number(&track, 1), 1);
  assert_eq!(bytes(&track, 2), b"logger-cli");

  assert_eq!(number(event_packet, 8), 2);
  assert_eq!(number(event_packet, 10), 1);
  let event = decode(bytes(event_packet, 11));
  assert_eq!(number(&event, 9), 3);
  assert_eq!(number(&event, 11), number(&track, 1));
  assert_eq!(bytes(&event, 23), b"fake-perfetto");

  assert_eq!(number(sample_packet, 8), 3);
  assert_eq!(number(sample_packet, 10), number(event_packet, 10));
  assert_eq!(number(sample_packet, 13), 1);
  let interned = decode(bytes(sample_packet, 12));
  let function = decode(bytes(&interned, 5));
  let frame = decode(bytes(&interned, 6));
  let callstack = decode(bytes(&interned, 7));
  let sample = decode(bytes(sample_packet, 66));
  assert_eq!(number(&sample, 1), 0);
  assert_eq!(number(&sample, 2), 1);
  assert_eq!(number(&sample, 3), 1);
  assert_eq!(number(&sample, 6), 1);
  assert_eq!(number(&sample, 4), 1);
  assert_eq!(number(&sample, 4), number(&callstack, 1));
  assert_eq!(number(&callstack, 2), number(&frame, 1));
  assert_eq!(number(&frame, 2), number(&function, 1));
  assert_eq!(bytes(&function, 2), b"fake_perfetto");
}

fn invocation() -> CommandInvocation {
  CommandInvocation {
    command_id: None,
    registered_command_id: COMMAND_NAME.to_string(),
    arguments: [].into(),
    session_id: "workflow-session".to_string(),
  }
}

#[tokio::test]
async fn fake_perfetto_returns_deterministic_trace_bytes_and_content_type() {
  for _ in 0 .. 2 {
    let CommandResult::Completed {
      fields,
      attachment: Some(attachment),
    } = FakePerfettoCommand.execute(invocation()).await
    else {
      panic!("expected a successful fake Perfetto attachment");
    };
    assert!(fields.is_empty());
    assert_eq!(
      attachment.content_type.as_deref(),
      Some("application/vnd.bitdrift.perfetto-trace")
    );
    let UploadSource::Bytes(bytes) = attachment.source else {
      panic!("expected an in-memory trace");
    };
    assert_eq!(bytes, TRACE);
    assert_eq!(bytes.len(), 103);
  }
}

#[tokio::test]
async fn fake_perfetto_rejects_unsupported_arguments() {
  let mut invocation = invocation();
  invocation
    .arguments
    .insert("unknown".to_string(), Data::new());
  assert!(matches!(
    FakePerfettoCommand.execute(invocation).await,
    CommandResult::Failed {
      error: CommandError::InvalidArguments(_),
      ..
    }
  ));
}
