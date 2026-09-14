// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./fake_screenshot_test.rs"]
mod tests;

use bd_session_replay::DeviceCommandScreenshotCompletion;

// A deterministic 1x1 JPEG returned for every logger-cli remote screenshot command.
const SCREENSHOT: &[u8] = include_bytes!("testdata/fake_screenshot.jpg");

//
// FakeScreenshotTarget
//

pub struct FakeScreenshotTarget;

impl bd_session_replay::Target for FakeScreenshotTarget {
  fn capture_screen(&self) {}

  fn capture_device_command_screenshot(&self, completion: DeviceCommandScreenshotCompletion) {
    completion(Ok(SCREENSHOT.to_vec()));
  }
}
