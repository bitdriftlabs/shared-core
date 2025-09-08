// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use anyhow::anyhow;
use protobuf::EnumOrUnknown;

#[derive(thiserror::Error, Debug)]
pub enum InvariantError {
  #[error("Invariant violation")]
  Invariant,
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
