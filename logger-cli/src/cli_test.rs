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

#[test]
fn parses_enqueue_fake_crash_command() {
  let options = Options::parse_from([
    "logger-cli",
    "enqueue-fake-crash",
    "--reason",
    "SIGABRT",
    "--detail",
    "Generated in test",
    "--platform",
    "android",
    "--app-id",
    "com.example.app",
    "--app-version",
    "2.3.4",
    "--app-build-id",
    "42",
    "--file-name",
    "fake-report",
    "--upload",
  ]);

  assert!(matches!(
    options.command,
    Command::EnqueueFakeCrash(cmd)
      if cmd.reason == "SIGABRT"
        && cmd.detail == "Generated in test"
        && cmd.app_id == "com.example.app"
        && cmd.app_version == "2.3.4"
        && cmd.app_build_id == "42"
        && cmd.file_name.as_deref() == Some("fake-report")
        && cmd.upload
  ));
}
