// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

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
