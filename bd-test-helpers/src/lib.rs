// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_client_common::error::UnexpectedErrorHandler;
use bd_panic::PanicType;
use std::borrow::Cow;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

pub mod config_helper;
pub mod events;
#[cfg(feature = "runtime")]
#[cfg(target_family = "unix")]
pub mod feature_flags;
pub mod fields;
pub mod filter;
pub mod metadata;
pub mod metadata_provider;
pub mod resource_utilization;
pub mod runtime;
pub mod session;
pub mod session_replay;
pub mod stats;
pub mod test_api_server;
pub mod workflow;

// mockall does not support interior mutability, even though from a quick look it is mutex locked
// on the inside because calls require &self references. This is a horrible hack to be able to
// set expectations which should be safe.
// https://github.com/asomers/mockall/issues/385
#[allow(clippy::needless_pass_by_ref_mut)]
pub fn make_mut<T>(mock: &mut Arc<T>) -> &mut T {
  let ptr = Arc::as_ptr(mock).cast_mut();
  unsafe { ptr.as_mut().unwrap() }
}

pub fn test_global_init() {
  // Call this before we initialize the logger as there is an issue where a log emitted /w thread
  // ids (always set by SwapLogger) emitted during ctor will panic.
  // See https://github.com/tokio-rs/tracing/issues/2063#issuecomment-2024185427, ideally this will
  // be resolved somehow.
  bd_panic::default(PanicType::ForceAbort);

  bd_log::SwapLogger::initialize();
}

//
// RecordingErrorReporter
//

/// Test helper that is used to capture an error reported via `error::handle_unexpected`.
#[derive(Default)]
pub struct RecordingErrorReporter {
  recorded_error: Mutex<Option<String>>,
}

impl RecordingErrorReporter {
  /// Executes the provided closure with the expectation that it should result in an error
  /// reported via `error::handle_unexpected`. Returns the return value of the closure and the
  /// captured error message.
  pub fn record_error<T>(f: impl FnOnce() -> T) -> (T, String) {
    let reporter = Arc::new(Self {
      recorded_error: Mutex::new(None),
    });

    let r = UnexpectedErrorHandler::with_reporter(reporter.clone(), f);

    (r, reporter.error().unwrap())
  }

  pub async fn async_record_error<T: 'static>(f: impl Future<Output = T> + 'static) -> (T, String) {
    let (r, maybe_error) = Self::maybe_async_record_error(f).await;
    (r, maybe_error.unwrap())
  }

  pub async fn maybe_async_record_error<T: 'static>(
    f: impl Future<Output = T> + 'static,
  ) -> (T, Option<String>) {
    let reporter = Arc::new(Self {
      recorded_error: Mutex::new(None),
    });

    let r = UnexpectedErrorHandler::async_with_reporter(reporter.clone(), f).await;

    (r, reporter.error())
  }
}

impl RecordingErrorReporter {
  pub fn error(&self) -> Option<String> {
    self.recorded_error.lock().unwrap().take()
  }
}

impl Drop for RecordingErrorReporter {
  fn drop(&mut self) {
    assert!(self.recorded_error.lock().unwrap().is_none());
  }
}
impl bd_client_common::error::Reporter for RecordingErrorReporter {
  fn report(
    &self,
    message: &str,
    _details: &Option<String>,
    _fields: &HashMap<Cow<'_, str>, Cow<'_, str>>,
  ) {
    let mut l = self.recorded_error.lock().unwrap();

    assert!(l.is_none(), "multiple errors captured");
    *l = Some(message.to_string());
  }
}
