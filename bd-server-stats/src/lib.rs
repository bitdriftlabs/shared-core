// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

pub mod stats;
pub mod test;

use regex::Regex;
use std::sync::LazyLock;

// TODO(mattklein123): Figure out what if anything to do about this precision loss. The Rust
// prometheus library does the same thing. It seems like technically we should see if it would
// overflow and then wrap it.
#[allow(clippy::cast_precision_loss)]
#[must_use]
pub const fn prom_u64_to_f64(value: u64) -> f64 {
  value as f64
}

pub mod error {
  #[derive(Debug, thiserror::Error)]
  pub enum Error {
    #[error("A histogram error occurred: {0}")]
    Histogram(String),
  }

  pub type Result<T> = std::result::Result<T, Error>;
}

// https://prometheus.io/docs/concepts/data_model/
pub static PROMETHEUS_NAME_REGEX: LazyLock<Regex> =
  LazyLock::new(|| Regex::new("[a-zA-Z_:][a-zA-Z0-9_:]*").unwrap());
pub static PROMETHEUS_LABEL_REGEX: LazyLock<Regex> =
  LazyLock::new(|| Regex::new("[a-zA-Z_][a-zA-Z0-9_]*").unwrap());
