// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//
// Code
//

// Wrapper for supported gRPC status codes. Unknown is a synthetic code if mapping is not possible.
#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub enum Code {
  Ok,
  Unknown,
  InvalidArgument,
  FailedPrecondition,
  Internal,
  Unavailable,
  Unauthenticated,
  NotFound,
  PermissionDenied,
  ResourceExhausted,
}

impl Code {
  // Convert to an int via https://grpc.github.io/grpc/core/md_doc_statuscodes.html.
  #[must_use]
  pub const fn to_int(&self) -> i32 {
    match self {
      Self::Ok => 0,
      Self::Unknown => 2,
      Self::InvalidArgument => 3,
      Self::NotFound => 5,
      Self::PermissionDenied => 7,
      Self::ResourceExhausted => 8,
      Self::FailedPrecondition => 9,
      Self::Internal => 13,
      Self::Unavailable => 14,
      Self::Unauthenticated => 16,
    }
  }

  // Convert from a string via https://grpc.github.io/grpc/core/md_doc_statuscodes.html.
  #[must_use]
  pub fn from_string(status: &str) -> Self {
    match status {
      "0" => Self::Ok,
      "3" => Self::InvalidArgument,
      "5" => Self::NotFound,
      "7" => Self::PermissionDenied,
      "8" => Self::ResourceExhausted,
      "9" => Self::FailedPrecondition,
      "13" => Self::Internal,
      "14" => Self::Unavailable,
      "16" => Self::Unauthenticated,
      _ => Self::Unknown,
    }
  }
}
