#![allow(clippy::unwrap_used)]

use super::Tree;
use bd_proto::protos::log_matcher::log_matcher::LogMatcher;
use protobuf::Message;

#[test]
fn compiled_trees_match_generated_decoder() {
  for bytes in [
    vec![10, 4, 42, 2, 8, 42],
    vec![34, 6, 10, 4, 42, 2, 8, 42],
    vec![18, 8, 10, 6, 10, 4, 42, 2, 8, 42],
    vec![26, 0],
  ] {
    let generated = LogMatcher::parse_from_bytes(&bytes).unwrap();
    assert_eq!(
      Tree::from_proto_bytes(&bytes).unwrap(),
      Tree::new(&generated).unwrap()
    );
  }
}

#[test]
fn rejects_missing_variants_and_excessive_recursion() {
  assert!(Tree::from_proto_bytes(&[]).is_err());
  assert!(Tree::from_proto_bytes(&[10, 0]).is_err());
  let mut bytes = vec![10, 4, 42, 2, 8, 1];
  for _ in 0 .. 101 {
    let mut outer = Vec::new();
    {
      let mut output = protobuf::CodedOutputStream::vec(&mut outer);
      output.write_bytes(4, &bytes).unwrap();
      output.flush().unwrap();
    }
    bytes = outer;
  }
  assert!(Tree::from_proto_bytes(&bytes).is_err());
}
