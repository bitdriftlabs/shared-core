use bd_proto::protos::client::api::UploadArtifactRequest;
use bd_proto::protos::client::artifact::artifact_upload_index::Artifact;
use bd_proto::protos::workflow::workflow::workflow::Rule;
use bd_proto::protos::workflow::workflow::workflow::rule::Rule_type;
use bd_proto::protos::workflow::workflow::{
  CrashTraversalContext,
  WorkflowCrashContinuation,
  WorkflowCrashHandoff,
};
use protobuf::well_known_types::timestamp::Timestamp;
use protobuf::{Message, MessageField};
use std::collections::HashMap;

fn workflow_crash_handoff() -> WorkflowCrashHandoff {
  WorkflowCrashHandoff {
    continuations: vec![WorkflowCrashContinuation {
      client_workflow_id: "workflow-hash".to_string(),
      traversals: vec![CrashTraversalContext {
        extracted_fields: HashMap::from([("request_id".to_string(), "abc123".to_string())]),
        extracted_timestamps: HashMap::from([(
          "started_at".to_string(),
          Timestamp {
            seconds: 1_717_171_717,
            nanos: 123_000_000,
            ..Default::default()
          },
        )]),
        ..Default::default()
      }],
      ..Default::default()
    }],
    ..Default::default()
  }
}

#[test]
fn artifact_and_upload_request_round_trip_workflow_crash_handoff() -> protobuf::Result<()> {
  let handoff = workflow_crash_handoff();

  let artifact = Artifact {
    workflow_crash_handoff: MessageField::some(handoff.clone()),
    ..Default::default()
  };
  let parsed_artifact = Artifact::parse_from_bytes(&artifact.write_to_bytes()?)?;
  assert_eq!(
    parsed_artifact.workflow_crash_handoff.as_ref(),
    Some(&handoff)
  );

  let request = UploadArtifactRequest {
    workflow_crash_handoff: MessageField::some(handoff.clone()),
    ..Default::default()
  };
  let parsed_request = UploadArtifactRequest::parse_from_bytes(&request.write_to_bytes()?)?;
  assert_eq!(
    parsed_request.workflow_crash_handoff.as_ref(),
    Some(&handoff)
  );

  Ok(())
}

#[test]
fn workflow_rule_round_trips_on_crash_marker() -> protobuf::Result<()> {
  let rule = Rule {
    rule_type: Some(Rule_type::OnCrash(true)),
    ..Default::default()
  };

  let parsed = Rule::parse_from_bytes(&rule.write_to_bytes()?)?;
  assert_eq!(parsed.rule_type, Some(Rule_type::OnCrash(true)));

  Ok(())
}
