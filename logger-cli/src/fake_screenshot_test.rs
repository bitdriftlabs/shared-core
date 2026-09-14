// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::*;
use bd_session_replay::Target;
use std::sync::mpsc;

#[test]
fn returns_a_valid_jpeg_for_device_command_capture() {
  let (result_tx, result_rx) = mpsc::channel();

  FakeScreenshotTarget.capture_device_command_screenshot(Box::new(move |result| {
    result_tx.send(result).unwrap();
  }));

  let screenshot = result_rx.recv().unwrap().unwrap();
  assert_eq!(screenshot, SCREENSHOT);
  assert!(screenshot.starts_with(&[0xff, 0xd8, 0xff]));
  assert!(screenshot.ends_with(&[0xff, 0xd9]));
}
