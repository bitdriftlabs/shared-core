// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

pub mod buffer_selector;
pub mod matcher;

#[derive(thiserror::Error, Debug)]
pub enum Error {
  #[error("Invalid regex: {0}")]
  Regex(#[from] regex::Error),

  #[error("Invalid matcher configuration")]
  InvalidConfig,

  #[error("Configuration has unknown fields or enum")]
  UnknownFieldOrEnum,
}

type Result<T> = std::result::Result<T, Error>;
