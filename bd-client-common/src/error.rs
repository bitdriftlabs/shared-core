// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./error_test.rs"]
mod error_test;

use anyhow::anyhow;
use bd_client_stats_store::{Counter, Scope};
use protobuf::EnumOrUnknown;
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, LazyLock};

//
// Reporter
//

/// Trait for a reporter that reports a single error message.
pub trait Reporter: Send + Sync {
  /// Report a single unexpected error.
  fn report(
    &self,
    message: &str,
    details: &Option<String>,
    fields: &HashMap<Cow<'_, str>, Cow<'_, str>>,
  );
}

// By default we use a reporter which fires a debug assert. This ensures that unless overriden,
// the errors bubble up as debug assert in test.
struct DefaultErrorReporter {}

impl Reporter for DefaultErrorReporter {
  fn report(
    &self,
    message: &str,
    _details: &Option<String>,
    _fields: &HashMap<Cow<'_, str>, Cow<'_, str>>,
  ) {
    debug_assert!(false, "unexpected error: {message}");
  }
}

//
// PanickingErrorReporter
//

#[derive(Default)]
pub struct PanickingErrorReporter {}

impl PanickingErrorReporter {
  pub fn enable() {
    UnexpectedErrorHandler::set_reporter(Arc::new(Self::default()));
  }
}

impl Reporter for PanickingErrorReporter {
  fn report(
    &self,
    message: &str,
    details: &Option<String>,
    fields: &HashMap<Cow<'_, str>, Cow<'_, str>>,
  ) {
    panic!("unexpected error: {message} {details:?} {fields:?}");
  }
}

//
// SessionProvider
//

pub trait SessionProvider {
  fn session_id(&self) -> String;
}

//
// MetadataErrorReporter
//

pub struct MetadataErrorReporter {
  reporter: Arc<dyn Reporter>,
  session_strategy: Arc<dyn SessionProvider + Send + Sync>,
  metadata: Arc<dyn bd_metadata::Metadata + Send + Sync>,
}

impl MetadataErrorReporter {
  pub fn new(
    reporter: Arc<dyn Reporter>,
    session_strategy: Arc<dyn SessionProvider + Send + Sync>,
    metadata: Arc<dyn bd_metadata::Metadata + Send + Sync>,
  ) -> Self {
    Self {
      reporter,
      session_strategy,
      metadata,
    }
  }
}

impl Reporter for MetadataErrorReporter {
  fn report(
    &self,
    message: &str,
    details: &Option<String>,
    fields: &HashMap<Cow<'_, str>, Cow<'_, str>>,
  ) {
    let mut fields = fields.clone();

    let session_id = self.session_strategy.session_id();
    fields.insert("x-session-id".into(), session_id.into());

    for (key, value) in self.metadata.collect() {
      fields.insert(format!("x-{}", key.replace('_', "-")).into(), value.into());
    }

    self.reporter.report(message, details, &fields);
  }
}

//
// UnexpectedErrorHandler
//

/// Wrapper for a handler that allows reporting unexpected errors to the platform layer.
pub struct UnexpectedErrorHandler {
  reporter: Arc<dyn Reporter>,
  remaining_reports: usize,

  /// Tracks how many times we attempted to fire an error but hit the reporting limit.
  dropped_errors: Option<Counter>,
}

impl UnexpectedErrorHandler {
  fn new() -> Self {
    Self {
      reporter: Arc::new(DefaultErrorReporter {}),
      // TODO(snowp): Make this limit more dynamic (per unit of time) or per error type.
      remaining_reports: 5,
      dropped_errors: None,
    }
  }

  // Updates the global error reporter.
  pub fn set_reporter(reporter: Arc<dyn Reporter>) {
    ERROR_HANDLER.lock().reporter = reporter;
  }

  // Disables by setting remaining reports to 0.
  pub fn disable() {
    ERROR_HANDLER.lock().remaining_reports = 0;
  }

  #[must_use]
  pub fn with_reporter<T>(reporter: Arc<dyn Reporter>, f: impl FnOnce() -> T) -> T {
    PER_THREAD_REPORTER.with(|per_thread_reporter| {
      *per_thread_reporter.borrow_mut() = Some(reporter);
      let r = f();
      *per_thread_reporter.borrow_mut() = None;
      r
    })
  }

  /// Runs the provided future with the provided error reporter overriding the default handler,
  /// with the expectation that the future should result in a single unexpected error being
  /// reported.
  /// Returns the result of the future as well as the reported error message.
  pub async fn async_with_reporter<T: 'static>(
    reporter: Arc<dyn Reporter>,
    f: impl Future<Output = T> + 'static,
  ) -> T {
    PER_THREAD_REPORTER.with(|per_thread_reporter| {
      *per_thread_reporter.borrow_mut() = Some(reporter);
    });

    let r = f.await;

    PER_THREAD_REPORTER.with(|per_thread_reporter| {
      *per_thread_reporter.borrow_mut() = None;
    });
    r
  }

  pub fn register_stats(stats: &Scope) {
    let mut l = ERROR_HANDLER.lock();

    l.dropped_errors = Some(stats.scope("error_reporter").counter("dropped_errors"));
  }

  // Clears the active error reporter. This will revert the handler back to the default.
  pub fn clear_reporter() {
    ERROR_HANDLER.lock().reporter = Arc::new(DefaultErrorReporter {});
  }

  // Reports an unexpected error. The string will be forwarded to the reporter if one has been
  // attached.
  fn report(&mut self, message: &str, description: &str, details: impl FnOnce() -> Option<String>) {
    let formatted = format!("{description}: {message}");

    log::warn!("unexpected error: {}", &formatted);

    if self.remaining_reports == 0 {
      log::warn!("not reporting error, limit hit");
      if let Some(dropped_errors) = &self.dropped_errors {
        dropped_errors.inc();
      }
      return;
    }
    self.remaining_reports -= 1;

    let details = details();

    // If a per-thread override has been set, use this instead of the static handler. This allows
    // overriding the handling in test while also working around the fact that the tests run in
    // parallel.
    if PER_THREAD_REPORTER.with(|reporter| {
      (*reporter.borrow()).as_ref().map_or(false, |reporter| {
        reporter.report(&formatted, &details, &HashMap::new());
        true
      })
    }) {
      return;
    }

    self.reporter.report(&formatted, &details, &HashMap::new());
  }
}

// Static error handler used for error reporting. We keep this static to allow for errors to be
// reported that aren't tied to a specific logger (e.g. invalid logger id), which has the downside
// of not linking the lifetime of the error handler to a specific logger. As such tracking dropped
// errors becomes somewhat of a best effort when multiple loggers are used, as a new counter is used
// whenever the error handler is reset.
static ERROR_HANDLER: LazyLock<parking_lot::Mutex<UnexpectedErrorHandler>> =
  LazyLock::new(|| parking_lot::Mutex::new(UnexpectedErrorHandler::new()));

thread_local! {
  static PER_THREAD_REPORTER: RefCell<Option<Arc<dyn Reporter>>> = RefCell::new(None);
}

pub fn handle_unexpected_error_with_details<E: Into<anyhow::Error> + Sync>(
  e: E,
  description: &str,
  details: impl FnOnce() -> Option<String>,
) {
  let e: anyhow::Error = e.into();
  ERROR_HANDLER
    .lock()
    .report(e.to_string().as_str(), description, details);
}

pub fn handle_unexpected<T, E: Into<anyhow::Error> + Sync>(
  r: std::result::Result<T, E>,
  description: &str,
) {
  if let Err(e) = r {
    handle_unexpected_error_with_details(e, description, || None);
  }
}

pub fn handle_unexpected_or<T>(r: anyhow::Result<T>, other: T, description: &str) -> T {
  match r {
    Err(e) => {
      handle_unexpected_error_with_details(e, description, || None);
      other
    },
    Ok(v) => v,
  }
}

/// Wraps a function invocation with unexpected error handling. Errors are captured and reported via
/// the error logger.
pub fn with_handle_unexpected<T, E: Into<anyhow::Error> + Sync>(
  f: impl FnOnce() -> std::result::Result<T, E>,
  description: &str,
) {
  handle_unexpected(f(), description);
}

/// Wraps a function invocation with unexpected error handling, with a default value should the call
/// result in an error. Errors are captured and reported via the error logger.
pub fn with_handle_unexpected_or<T>(
  f: impl FnOnce() -> std::result::Result<T, anyhow::Error>,
  default: T,
  description: &str,
) -> T {
  handle_unexpected_or(f(), default, description)
}

pub fn required_proto_enum<T: protobuf::Enum>(
  e: EnumOrUnknown<T>,
  detail: &str,
) -> anyhow::Result<T> {
  e.enum_value().map_err(|_| anyhow!("unknown enum {detail}"))
}

// Flattens a tokio::task::JoinHandle into a single Result<T>.
// TODO(snowp): Once https://github.com/rust-lang/rust/issues/70142 lands use the standard version
// of this.
pub async fn flatten<T>(handle: tokio::task::JoinHandle<anyhow::Result<T>>) -> anyhow::Result<T> {
  match handle.await {
    Ok(Ok(result)) => Ok(result),
    Ok(Err(err)) => Err(err),
    Err(_) => Err(anyhow!("A tokio task failed")),
  }
}
