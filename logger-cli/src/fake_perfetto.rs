// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./fake_perfetto_test.rs"]
mod tests;

use bd_artifact_upload::UploadSource;
use bd_logger::{
  CommandAttachment,
  CommandError,
  CommandInvocation,
  CommandResult,
  RegisteredCommandHandler,
};

pub const COMMAND_NAME: &str = "fake_perfetto";
const CONTENT_TYPE: &str = "application/vnd.bitdrift.perfetto-trace";

// A named track, an instant event, and one PerfSample with an interned, already-symbolized frame.
// The explicit incremental-state clear makes the profile usable without earlier producer packets.
const TRACE: &[u8] = b"\x0a\x13\x40\x01\xe2\x03\x0e\x08\x01\x12\x0alogger-cli\
  \x0a\x1a\x40\x02\x50\x01\x5a\x14\x48\x03\x58\x01\xba\x01\x0dfake-perfetto\
  \x0a\x34\x40\x03\x50\x01\x68\x01\x62\x1f\
  \x2a\x11\x08\x01\x12\x0dfake_perfetto\
  \x32\x04\x08\x01\x10\x01\x3a\x04\x08\x01\x10\x01\
  \x92\x04\x0a\x08\x00\x10\x01\x18\x01\x20\x01\x30\x01";

//
// FakePerfettoCommand
//

pub struct FakePerfettoCommand;

#[async_trait::async_trait]
impl RegisteredCommandHandler for FakePerfettoCommand {
  async fn execute(&self, invocation: CommandInvocation) -> CommandResult {
    if !invocation.arguments.is_empty() {
      return CommandResult::Failed {
        error: CommandError::InvalidArguments(
          "fake_perfetto does not accept arguments".to_string(),
        ),
        fields: [].into(),
      };
    }
    log::info!(
      "returning fake Perfetto trace: command={} session={} bytes={}",
      invocation.registered_command_id,
      invocation.session_id,
      TRACE.len(),
    );
    CommandResult::Completed {
      fields: [].into(),
      attachment: Some(CommandAttachment {
        source: UploadSource::Bytes(TRACE.to_vec()),
        content_type: Some(CONTENT_TYPE.to_string()),
      }),
    }
  }
}
