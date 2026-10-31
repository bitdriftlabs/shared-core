// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/licenses/strict/1.0.0

#![allow(clippy::unwrap_used)]

use super::*;
use bd_proto::protos::logging::payload::Data;
use std::collections::HashMap;

#[tokio::test]
async fn rejects_screenshot_arguments() {
  let result = FakeScreenshotTarget
    .execute(CommandInvocation {
      command_id: None,
      registered_command_id: "take_screenshot".into(),
      arguments: HashMap::from([("extra".into(), Data::default())]),
      session_id: "session".into(),
    })
    .await;
  let CommandResult::Failed { error, fields } = result else {
    panic!("expected invalid screenshot arguments");
  };
  assert_eq!(
    error,
    CommandError::InvalidArguments("screenshot does not accept arguments".into())
  );
  assert_eq!(fields, LogFields::default());
}

#[tokio::test]
async fn returns_a_valid_jpeg_for_device_command_capture() {
  let result = FakeScreenshotTarget
    .execute(CommandInvocation {
      command_id: None,
      registered_command_id: "take_screenshot".into(),
      arguments: std::collections::HashMap::new(),
      session_id: "session".into(),
    })
    .await;
  let CommandResult::Completed {
    attachment: Some(attachment),
    ..
  } = result
  else {
    panic!("expected screenshot attachment");
  };
  assert_eq!(attachment.content_type.as_deref(), Some("image/jpeg"));
  let bd_artifact_upload::UploadSource::Bytes(screenshot) = attachment.source else {
    panic!("expected JPEG bytes");
  };
  assert_eq!(screenshot, SCREENSHOT);
  assert!(screenshot.starts_with(&[0xff, 0xd8, 0xff]));
  assert!(screenshot.ends_with(&[0xff, 0xd9]));
}
