#![allow(clippy::unwrap_used)]

use super::super::{Config, MetricMultiTag, WorkflowDebugMode, WorkflowsConfiguration};
use bd_proto::protos::state::scope::StateScope;
use bd_proto::protos::workflow::workflow::workflow::rule::Rule_type;
use bd_proto::protos::workflow::workflow::workflow::{Rule, State, Transition};
use bd_proto::protos::workflow::workflow::{
  MultiTag,
  Workflow,
  WorkflowsConfiguration as ProtoConfigurations,
};
use bd_proto_util::serialization::inline::{Message as InlineMessage, ProtoDeserialize};
use protobuf::{CodedOutputStream, EnumOrUnknown, Message};

fn workflow(id: &str) -> Workflow {
  Workflow {
    id: id.to_owned(),
    states: vec![State {
      id: "initial".to_owned(),
      transitions: vec![Transition {
        target_state_id: "initial".to_owned(),
        rule: Some(Rule {
          rule_type: Some(Rule_type::OnNewSession(true)),
          ..Default::default()
        })
        .into(),
        ..Default::default()
      }],
      ..Default::default()
    }],
    ..Default::default()
  }
}

#[test]
fn direct_config_matches_generated_conversion() {
  let proto = workflow("deployed");
  let bytes = proto.write_to_bytes().unwrap();
  assert_eq!(
    Config::from_proto_bytes(&bytes, WorkflowDebugMode::None).unwrap(),
    Config::new(proto, WorkflowDebugMode::None).unwrap()
  );
}

#[test]
fn deployment_and_debug_modes_match_existing_path() {
  let deployed = vec![workflow("deployed"), Workflow::default()];
  let debug = vec![workflow("deployed"), workflow("debug-only")];
  let deployed_bytes = ProtoConfigurations {
    workflows: deployed.clone(),
    ..Default::default()
  }
  .write_to_bytes()
  .unwrap();
  let debug_bytes = ProtoConfigurations {
    workflows: debug.clone(),
    ..Default::default()
  }
  .write_to_bytes()
  .unwrap();
  assert_eq!(
    WorkflowsConfiguration::from_proto_bytes(&deployed_bytes, &debug_bytes).unwrap(),
    WorkflowsConfiguration::new(deployed, debug)
  );
}

#[test]
fn rejects_invalid_workflows_and_truncated_input() {
  assert!(Config::from_proto_bytes(&[], WorkflowDebugMode::None).is_err());
  assert!(Config::from_proto_bytes(&[18, 10, 1], WorkflowDebugMode::None).is_err());
  let mut proto = workflow("invalid");
  proto.states[0].transitions[0].target_state_id = "missing".to_owned();
  assert!(Config::new(proto.clone(), WorkflowDebugMode::None).is_err());
  assert!(
    Config::from_proto_bytes(&proto.write_to_bytes().unwrap(), WorkflowDebugMode::None).is_err()
  );
}

fn assert_multi_tag_parity(bytes: &[u8]) {
  let expected = MetricMultiTag::new(MultiTag::parse_from_bytes(bytes).unwrap());
  let actual = MetricMultiTag::from_proto_bytes(bytes);
  match (actual, expected) {
    (Ok(actual), Ok(expected)) => assert_eq!(actual, expected),
    (Err(actual), Err(expected)) => assert_eq!(actual.to_string(), expected.to_string()),
    (actual, expected) => panic!("multi-tag decoding differs: {actual:?} vs {expected:?}"),
  }
}

#[test]
fn multi_tag_defaults_regexes_and_scope_match_generated_conversion() {
  for scope in [
    StateScope::UNSPECIFIED.into(),
    StateScope::GLOBAL_STATE.into(),
    StateScope::FEATURE_FLAG.into(),
    StateScope::SYSTEM.into(),
    EnumOrUnknown::from_i32(127),
  ] {
    for (key_regex, value_regex) in [
      (None, None),
      (Some(""), Some("")),
      (Some("^key"), Some("value$")),
      (Some("["), None),
      (None, Some("[")),
      (Some("["), Some("[")),
    ] {
      let proto = MultiTag {
        scope,
        key_tag_name: "key".to_owned(),
        value_tag_name: "value".to_owned(),
        key_regex: key_regex.map(str::to_owned),
        value_regex: value_regex.map(str::to_owned),
        ..Default::default()
      };
      assert_multi_tag_parity(&proto.write_to_bytes().unwrap());
    }
  }
}

#[test]
fn multi_tag_conversions_use_last_scalar_value() {
  let mut bytes = MultiTag {
    scope: StateScope::GLOBAL_STATE.into(),
    ..Default::default()
  }
  .write_to_bytes()
  .unwrap();
  bytes.extend_from_slice(&[34, 1, b'[', 34, 1, b'k', 42, 1, b'[', 42, 1, b'v']);
  assert_multi_tag_parity(&bytes);
  bytes.extend_from_slice(&[34, 1, b'[']);
  assert_multi_tag_parity(&bytes);
}

#[test]
fn multi_tag_singular_fragments_merge_before_regex_conversion() {
  let first = MultiTag {
    scope: StateScope::GLOBAL_STATE.into(),
    key_tag_name: "key".to_owned(),
    key_regex: Some("[".to_owned()),
    ..Default::default()
  }
  .write_to_bytes()
  .unwrap();
  let second = MultiTag {
    value_tag_name: "value".to_owned(),
    key_regex: Some("^key".to_owned()),
    ..Default::default()
  }
  .write_to_bytes()
  .unwrap();
  let mut bytes = Vec::new();
  {
    let mut output = CodedOutputStream::vec(&mut bytes);
    output.write_bytes(7, &first).unwrap();
    output.write_bytes(7, &second).unwrap();
    output.flush().unwrap();
  }
  let parent = InlineMessage::new(&bytes).unwrap();
  let actual = MetricMultiTag::from_inline(&parent.required_message(7).unwrap()).unwrap();
  let mut merged = first;
  merged.extend_from_slice(&second);
  let expected = MetricMultiTag::new(MultiTag::parse_from_bytes(&merged).unwrap()).unwrap();
  assert_eq!(actual, expected);
}

#[test]
fn multi_tag_rejects_malformed_wire_values_even_when_overwritten() {
  for bytes in [
    vec![34, 1, 0xff, 34, 1, b'k'],
    vec![34, 3, b'k'],
    vec![8, 0x80],
  ] {
    let mut wire = MultiTag {
      scope: StateScope::GLOBAL_STATE.into(),
      ..Default::default()
    }
    .write_to_bytes()
    .unwrap();
    wire.extend_from_slice(&bytes);
    assert!(MultiTag::parse_from_bytes(&wire).is_err(), "{wire:?}");
    assert!(MetricMultiTag::from_proto_bytes(&wire).is_err(), "{wire:?}");
  }
}
