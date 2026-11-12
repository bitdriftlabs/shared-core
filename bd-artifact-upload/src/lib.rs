// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![deny(
  clippy::expect_used,
  clippy::panic,
  clippy::todo,
  clippy::unimplemented,
  clippy::unreachable,
  clippy::unwrap_used
)]

mod crash_profile_source;
mod report_attribution;
mod uploader;

#[cfg(any(target_os = "android", test, feature = "test-support"))]
pub mod crash_profile;
#[cfg(any(target_os = "android", test, feature = "test-support"))]
pub mod crash_profile_attribution;

#[cfg(any(test, feature = "test-support"))]
pub mod test;

pub use bd_metadata::artifact::PERFETTO_CONTENT_TYPE;
pub use crash_profile_source::encode_source;
pub use report_attribution::{ReportAttribution, restore_pending_reports};
pub use uploader::{
  ChunkedFileAdmission,
  Client,
  EnqueueError,
  MockClient,
  SnappedFeatureFlag,
  UploadSource,
  Uploader,
};

#[cfg(test)]
#[ctor::ctor(unsafe)]
fn global_init() {
  bd_test_helpers_core::test_global_init();
}
