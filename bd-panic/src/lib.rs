// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Standard Panic Handler Configuration
//! ====================================
//!
//! We want panics to behave the same way across all our programs, generally
//! speaking. We typically build with panic=abort for releases, so this is
//! mostly useful for debugging where we expect to have a backtrace to look at.
//!
//! Note that on some platforms the set of things you can do with a panic is
//! different. Most notably, wasm doesn't have normal stdout/stderr, so you'll
//! need to do something else with panic backtraces, like dumping to console or
//! sending them over the wire somewhere.

#[cfg(target_arch = "wasm32")]
fn platform_default() {
  console_error_panic_hook::set_once();
}

#[cfg(not(target_arch = "wasm32"))]
fn platform_default() {
  // In development we typically will want the nice color handler. In production
  // we want panics to be on a single log line for easy searching.
  if std::env::var("LOG_PANIC").is_ok() {
    std::panic::set_hook(Box::new(move |info| {
      let message = info.payload().downcast_ref::<&str>().map_or_else(
        || {
          info
            .payload()
            .downcast_ref::<String>()
            .map_or("<none>", |s| s)
        },
        |s| s,
      );

      let location = info.location().map_or_else(
        || "<none>".to_string(),
        |location| format!("{}:{}", location.file(), location.line()),
      );

      // TODO(mattklein123): Potentially do a more complete stack trace in single log
      // line form.
      log::error!("panic: message=\"{}\" location={}", message, location);
    }));
  } else {
    let trace_printer =
      color_backtrace::BacktracePrinter::default().message("Panic triggered backtrace");
    trace_printer.install(color_backtrace::default_output_stream());
  }
}

/// Type of panic to perform.
#[derive(Clone, Copy)]
pub enum PanicType {
  /// Will print a colorized backtrace and proceed with default handling, which
  /// may eat the panic and continue running the process (e.g., Tokio).
  BacktraceOnly,
  /// Will print a backtrace as in [`PanicType::BacktraceOnly`] and then force
  /// exit the application. Note that Tokio eats panics in spawned tasks, so
  /// if not continuing execution after a panic inside of a task is
  /// preferable, choose this option.
  ForceAbort,
}

/// Configure the default panic handler based on the specified panic type.
pub fn default(panic_type: PanicType) {
  platform_default();

  if matches!(panic_type, PanicType::ForceAbort) {
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
      default_panic(info);
      log::error!("Forcing process exit after panic");
      #[allow(clippy::exit)]
      std::process::exit(1);
    }));
  }

  log::debug!("Registered default panic handler");
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn defaults_dont_panic() {
    default(PanicType::BacktraceOnly);
    default(PanicType::ForceAbort);
  }
}
