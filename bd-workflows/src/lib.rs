// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

pub mod actions_flush_buffers;
pub mod config;
pub mod engine;
pub mod matcher;
pub mod metrics;
pub mod workflow;

#[derive(thiserror::Error, Debug)]
pub enum Error {
  #[error("Invalid regex: {0}")]
  Regex(#[from] regex::Error),

  #[error("Invalid workflow configuration: {0}")]
  InvalidConfig(&'static str),

  #[error("An io error ocurred: {0}")]
  Io(#[from] std::io::Error),

  #[error("Configuration has unknown fields or enum")]
  UnknownFieldOrEnum,

  #[error("An unexpected int conversion error occurred: {0}")]
  TryFromInt(#[from] std::num::TryFromIntError),

  #[error("Serialization Error: {0}")]
  SerializationError(#[from] std::boxed::Box<bincode::ErrorKind>),

  #[error("Tokio Send Error: {0}")]
  TokioSend(&'static str),
}

pub type Result<T> = std::result::Result<T, Error>;
