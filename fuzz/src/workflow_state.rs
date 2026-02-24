// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_proto_util::serialization::{ProtoMessageDeserialize, ProtoMessageSerialize};
use bd_workflows::workflow::Workflow;

#[derive(Debug)]
pub struct WorkflowStateFuzzTestCase {
  workflow: Workflow,
}

impl<'a> arbitrary::Arbitrary<'a> for WorkflowStateFuzzTestCase {
  fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
    Ok(Self {
      workflow: u.arbitrary()?,
    })
  }
}

pub struct WorkflowStateFuzzTest {
  test_case: WorkflowStateFuzzTestCase,
}

impl WorkflowStateFuzzTest {
  #[must_use]
  pub const fn new(test_case: WorkflowStateFuzzTestCase) -> Self {
    Self { test_case }
  }

  pub fn run(self) {
    let bytes = match self.test_case.workflow.serialize_message_to_bytes() {
      Ok(bytes) => bytes,
      Err(err) => panic!("workflow serialization should succeed: {err}"),
    };
    let roundtripped = match Workflow::deserialize_message_from_bytes(&bytes) {
      Ok(roundtripped) => roundtripped,
      Err(err) => panic!("workflow deserialization should succeed: {err}"),
    };
    assert_eq!(self.test_case.workflow, roundtripped);
  }
}

#[test]
fn run_all_corpus() {
  crate::run_all_corpus(
    "corpus/workflow_state",
    |input: WorkflowStateFuzzTestCase| {
      WorkflowStateFuzzTest::new(input).run();
    },
  );
}
