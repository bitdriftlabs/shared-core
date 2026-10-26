// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./fake_screenshot_test.rs"]
mod tests;

use bd_logger::{
  CommandAttachment,
  CommandInvocation,
  CommandResult,
  LogFields,
  RegisteredCommandHandler,
};

// A deterministic 1x1 JPEG returned for every logger-cli remote screenshot command.
const SCREENSHOT: &[u8] = include_bytes!("testdata/fake_screenshot.jpg");

//
// FakeScreenshotTarget
//

pub struct FakeScreenshotTarget;

impl bd_session_replay::Target for FakeScreenshotTarget {
  fn capture_screen(&self) {}
}

#[async_trait::async_trait]
impl RegisteredCommandHandler for FakeScreenshotTarget {
  async fn execute(&self, _invocation: CommandInvocation) -> CommandResult {
    CommandResult::Completed {
      fields: LogFields::default(),
      attachment: Some(CommandAttachment {
        source: bd_artifact_upload::UploadSource::Bytes(SCREENSHOT.to_vec()),
        content_type: Some("image/jpeg".into()),
      }),
    }
  }
}
