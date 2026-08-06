// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.

#![allow(clippy::unwrap_used)]

use crate::bootstrap::load_workflows_config;
use std::fs;

#[test]
fn parses_configuration_update_from_api_response() {
  let directory = tempfile::tempdir().unwrap();
  let path = directory.path().join("config.json");
  fs::write(
    &path,
    r#"{"configurationUpdate":{"stateOfTheWorld":{"workflowsConfiguration":{"workflows":[]}}}}"#,
  )
  .unwrap();

  let bootstrap = load_workflows_config(&path).unwrap();

  assert_eq!(0, bootstrap.declared_workflow_count);
  assert_eq!(0, bootstrap.loaded_workflow_count);
}
