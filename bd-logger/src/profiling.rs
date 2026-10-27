// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! The platform seam behind the built-in stack sampling command.
//!
//! The command itself (arguments, timeout, upload, result log) lives in `device_command`. The
//! platform only records the trace: it receives a [`StackSampleRequest`], hands the resulting file
//! back through a [`StackSampleCompletion`], and stops early when asked to. The default
//! implementations make a platform without a profiler fail the command cleanly as unsupported.

#[cfg(test)]
#[path = "./profiling_test.rs"]
mod profiling_test;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::oneshot;

/// Error name reported when the platform has no profiler for the current OS or OS version.
pub const UNSUPPORTED_ERROR_NAME: &str = "unsupported";

/// One stack sampling capture as the platform should perform it. Every value is already validated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StackSampleRequest {
  /// Identifies the capture in the completion and in a later cancellation.
  pub request_id: u64,
  pub duration: time::Duration,
  pub frequency_hz: u32,
  pub buffer_size_kb: u32,
}

/// A recorded trace as the platform reports it: an absolute path the core takes ownership of.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StackSampleOutcome {
  pub path: PathBuf,
}

/// A recorded trace the core owns. The file is deleted when this is dropped, so a trace whose
/// command was cancelled, timed out or failed before the uploader took it never outlives the
/// command, wherever the drop happens (a dropped future, an unread channel, an abandoned result).
/// The uploader consumes the file itself; the later drop then finds nothing to delete.
#[derive(Debug)]
pub struct OwnedTrace {
  path: PathBuf,
}

impl OwnedTrace {
  #[must_use]
  pub fn path(&self) -> &Path {
    &self.path
  }
}

impl Drop for OwnedTrace {
  fn drop(&mut self) {
    // A single unlink of a small file; cheap enough to do inline wherever the drop lands.
    if let Err(error) = std::fs::remove_file(&self.path)
      && error.kind() != std::io::ErrorKind::NotFound
    {
      log::debug!(
        "failed to remove stack sampling trace {}: {error}",
        self.path.display()
      );
    }
  }
}

/// Why the platform could not produce a trace. `code` and `name` carry the platform profiler's
/// own error (for example Android's `ProfilingResult.ERROR_*`), or `0` and a short SDK name such
/// as [`UNSUPPORTED_ERROR_NAME`] when the failure happened before the profiler was asked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StackSampleError {
  pub code: i32,
  pub name: String,
  pub message: String,
}

impl StackSampleError {
  #[must_use]
  pub fn unsupported(message: impl Into<String>) -> Self {
    Self {
      code: 0,
      name: UNSUPPORTED_ERROR_NAME.to_string(),
      message: message.into(),
    }
  }
}

pub type StackSampleCompletion =
  Box<dyn FnOnce(Result<StackSampleOutcome, StackSampleError>) + Send>;

/// Implemented by the platform layer. Both methods return immediately; the capture runs
/// asynchronously and reports through the completion exactly once, including after a cancel.
pub trait ProfilingTarget: Send + Sync {
  fn capture_stack_sample(&self, request: StackSampleRequest, completion: StackSampleCompletion) {
    let _ = request;
    completion(Err(StackSampleError::unsupported(
      "stack sampling is not supported on this platform",
    )));
  }

  /// Asks the platform to stop a capture early. Its completion is still expected, and ignored.
  fn cancel_stack_sample(&self, request_id: u64) {
    let _ = request_id;
  }
}

/// The target for platforms without a profiler: every capture fails as unsupported.
pub struct NoopProfilingTarget;

impl ProfilingTarget for NoopProfilingTarget {}

/// Runs one capture at a time against the platform target and turns its callback into a future.
/// Dropping an unfinished capture future, as the execution timeout does, cancels the platform
/// capture so that sampling and its file do not outlive the command.
#[derive(Clone)]
pub struct ProfilingHandler {
  target: Arc<dyn ProfilingTarget>,
  active_request_id: Arc<AtomicU64>,
  next_request_id: Arc<AtomicU64>,
}

struct ActiveCaptureGuard {
  target: Arc<dyn ProfilingTarget>,
  active_request_id: Arc<AtomicU64>,
  request_id: u64,
  completed: Arc<AtomicBool>,
}

impl Drop for ActiveCaptureGuard {
  fn drop(&mut self) {
    if !self.completed.load(Ordering::Acquire) {
      self.target.cancel_stack_sample(self.request_id);
    }
    let _ = self.active_request_id.compare_exchange(
      self.request_id,
      0,
      Ordering::AcqRel,
      Ordering::Acquire,
    );
  }
}

impl ProfilingHandler {
  pub fn new(target: Arc<dyn ProfilingTarget>) -> Self {
    Self {
      target,
      active_request_id: Arc::new(AtomicU64::new(0)),
      next_request_id: Arc::new(AtomicU64::new(1)),
    }
  }

  /// Waits for the platform callback. The caller owns the deadline; dropping the future releases
  /// the capture slot and cancels the platform capture if it has not completed yet. A trace the
  /// platform reports after that is deleted, since nobody is left to upload it.
  pub async fn capture_stack_sample(
    &self,
    duration: time::Duration,
    frequency_hz: u32,
    buffer_size_kb: u32,
  ) -> Result<OwnedTrace, StackSampleError> {
    let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
    if self
      .active_request_id
      .compare_exchange(0, request_id, Ordering::AcqRel, Ordering::Acquire)
      .is_err()
    {
      return Err(StackSampleError {
        code: 0,
        name: "already_executing".to_string(),
        message: "a stack sampling capture is already in progress".to_string(),
      });
    }

    let completed = Arc::new(AtomicBool::new(false));
    let _guard = ActiveCaptureGuard {
      target: self.target.clone(),
      active_request_id: self.active_request_id.clone(),
      request_id,
      completed: completed.clone(),
    };
    let (completion_tx, completion_rx) = oneshot::channel();
    self.target.capture_stack_sample(
      StackSampleRequest {
        request_id,
        duration,
        frequency_hz,
        buffer_size_kb,
      },
      Box::new(move |result| {
        completed.store(true, Ordering::Release);
        let result = result.and_then(|outcome| {
          // Every consumer resolves the path differently (uploader root, SDK directory, process
          // working directory), so only an absolute path names one file to all of them.
          if outcome.path.is_absolute() {
            Ok(OwnedTrace { path: outcome.path })
          } else {
            Err(StackSampleError {
              code: 0,
              name: "invalid_path".to_string(),
              message: format!(
                "the platform reported a relative trace path {}",
                outcome.path.display()
              ),
            })
          }
        });
        // A receiver that already gave up (timeout or cancellation) hands the trace back here,
        // and dropping it deletes the file.
        let _ = completion_tx.send(result);
      }),
    );

    completion_rx.await.map_err(|_| StackSampleError {
      code: 0,
      name: "interrupted".to_string(),
      message: "the stack sampling capture was interrupted".to_string(),
    })?
  }
}
