#![allow(clippy::unwrap_used)]

use super::{ApiResponse, Response};
use bd_grpc_codec::{Decoder, DecodingResult, OptimizeFor};
use bd_proto::protos::client::api::api_response::Response_type;
use bd_proto::protos::client::api::{ApiResponse as ProtoResponse, HandshakeResponse};
use protobuf::Message;

#[test]
fn handshake_leaf_matches_generated_decoder() {
  let handshake = HandshakeResponse::default();
  let bytes = ProtoResponse {
    response_type: Some(Response_type::Handshake(handshake.clone())),
    ..Default::default()
  }
  .write_to_bytes()
  .unwrap();
  let response = ApiResponse::from_flags_and_bytes(0, bytes.into()).unwrap();
  let Some(Response::Handshake(decoded)) = response.response_type else {
    panic!("handshake expected");
  };
  assert_eq!(decoded, handshake);
}

#[test]
fn every_retained_response_variant_matches_generated_decoder() {
  macro_rules! check_leaf {
    ($variant:ident) => {{
      let expected = ProtoResponse {
        response_type: Some(Response_type::$variant(Default::default())),
        ..Default::default()
      };
      let bytes = expected.write_to_bytes().unwrap();
      let decoded = ApiResponse::from_flags_and_bytes(0, bytes.into()).unwrap();
      let (Some(Response::$variant(decoded)), Some(Response_type::$variant(expected))) =
        (decoded.response_type, expected.response_type)
      else {
        panic!("wrong response variant")
      };
      assert_eq!(decoded, expected);
    }};
  }
  check_leaf!(Handshake);
  check_leaf!(LogUpload);
  check_leaf!(RuntimeUpdate);
  check_leaf!(ErrorShutdown);
  check_leaf!(StatsUpload);
  check_leaf!(LogUploadIntent);
  check_leaf!(FlushBuffers);
  check_leaf!(SankeyDiagramUpload);
  check_leaf!(SankeyIntentResponse);
  check_leaf!(ArtifactUpload);
  check_leaf!(ArtifactIntent);
  check_leaf!(DeviceCommandUpdateAck);
  assert!(matches!(
    ApiResponse::decode(&[26, 0]).unwrap().response_type,
    Some(Response::Pong)
  ));
  assert!(matches!(
    ApiResponse::decode(&[130, 1, 0]).unwrap().response_type,
    Some(Response::StateUpdate)
  ));
}

#[test]
fn oneof_switch_does_not_merge_previous_alternative() {
  let bytes = [18, 5, 10, 3, b'o', b'l', b'd', 26, 0, 18, 0];
  let Some(Response::LogUpload(response)) = ApiResponse::decode(&bytes).unwrap().response_type
  else {
    panic!("log response expected");
  };
  assert_eq!(response.upload_uuid, "");
  assert_eq!(
    ProtoResponse::parse_from_bytes(&bytes)
      .unwrap()
      .log_upload(),
    &response
  );
}

#[test]
fn framing_leaves_workflow_bytes_undecoded() {
  let bytes = vec![34, 8, 10, 1, b'v', 18, 3, 34, 1, 0];
  assert!(ProtoResponse::parse_from_bytes(&bytes).is_err());
  let mut framed = vec![0, 0, 0, 0, 10];
  framed.extend(bytes);
  let mut decoder = Decoder::<ApiResponse>::new(None, None, OptimizeFor::Cpu);
  assert!(decoder.decode_data(&framed[.. 4]).unwrap().is_empty());
  let responses = decoder.decode_data(&framed[4 ..]).unwrap();
  let Some(Response::ConfigurationUpdate(update)) = &responses[0].response_type else {
    panic!("raw configuration expected");
  };
  assert_eq!(update.version_nonce, "v");
  assert_eq!(update.bytes, [10, 1, b'v', 18, 3, 34, 1, 0]);
}
