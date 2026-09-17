// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//
// Code
//

// Wrapper for gRPC status codes. Unknown is the fallback for invalid wire values.
#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub enum Code {
  Ok,
  Cancelled,
  Unknown,
  InvalidArgument,
  DeadlineExceeded,
  AlreadyExists,
  FailedPrecondition,
  Aborted,
  OutOfRange,
  Unimplemented,
  Internal,
  Unavailable,
  DataLoss,
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
      Self::Cancelled => 1,
      Self::Unknown => 2,
      Self::InvalidArgument => 3,
      Self::DeadlineExceeded => 4,
      Self::NotFound => 5,
      Self::AlreadyExists => 6,
      Self::PermissionDenied => 7,
      Self::ResourceExhausted => 8,
      Self::FailedPrecondition => 9,
      Self::Aborted => 10,
      Self::OutOfRange => 11,
      Self::Unimplemented => 12,
      Self::Internal => 13,
      Self::Unavailable => 14,
      Self::DataLoss => 15,
      Self::Unauthenticated => 16,
    }
  }

  // Convert from a string via https://grpc.github.io/grpc/core/md_doc_statuscodes.html.
  #[must_use]
  #[allow(clippy::should_implement_trait)] // Infallible
  pub fn from_str(status: &str) -> Self {
    match status {
      "0" => Self::Ok,
      "1" => Self::Cancelled,
      "3" => Self::InvalidArgument,
      "4" => Self::DeadlineExceeded,
      "5" => Self::NotFound,
      "6" => Self::AlreadyExists,
      "7" => Self::PermissionDenied,
      "8" => Self::ResourceExhausted,
      "9" => Self::FailedPrecondition,
      "10" => Self::Aborted,
      "11" => Self::OutOfRange,
      "12" => Self::Unimplemented,
      "13" => Self::Internal,
      "14" => Self::Unavailable,
      "15" => Self::DataLoss,
      "16" => Self::Unauthenticated,
      _ => Self::Unknown,
    }
  }
}
