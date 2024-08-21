// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[derive(Debug, thiserror::Error)]
pub enum Error {
  #[error("A proto validation error occurred: {0}")]
  ProtoValidation(String),
}

pub type Result<T> = std::result::Result<T, Error>;
