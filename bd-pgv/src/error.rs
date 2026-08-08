// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#[derive(Debug, thiserror::Error)]
pub enum Error {
  #[error("A proto validation error occurred: {0}")]
  ProtoValidation(String),
}

pub type Result<T> = std::result::Result<T, Error>;
