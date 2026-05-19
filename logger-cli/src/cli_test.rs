// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{Command, Options};
use clap::Parser;

#[test]
fn parses_set_entity_id_command() {
  let options = Options::parse_from(["logger-cli", "set-entity-id", "entity-123"]);

  assert!(matches!(
    options.command,
    Command::SetEntityId(cmd) if cmd.entity_id == "entity-123"
  ));
}

#[test]
fn parses_start_entity_id_flag() {
  let options = Options::parse_from([
    "logger-cli",
    "start",
    "--api-key",
    "test-key",
    "--entity-id",
    "entity-123",
  ]);

  assert!(matches!(
    options.command,
    Command::Start(cmd) if cmd.entity_id.as_deref() == Some("entity-123")
  ));
}
