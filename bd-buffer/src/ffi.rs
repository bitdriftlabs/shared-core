// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// FFI representation that matches the absl::Status code enum.
// TODO(mattklein123): In a follow up remove/rename all mention of Absl and switch to more
// granular top level errors.
#[derive(Debug, PartialEq, Eq)]
pub enum AbslCode {
  Ok                 = 0,
  Cancelled          = 1,
  Unknown            = 2,
  InvalidArgument    = 3,
  DeadlineExceeded   = 4,
  NotFound           = 5,
  AlreadyExists      = 6,
  PermissionDenied   = 7,
  ResourceExhausted  = 8,
  FailedPrecondition = 9,
  Aborted            = 10,
  OutOfRange         = 11,
  Unimplemented      = 12,
  Internal           = 13,
  Unavailable        = 14,
  DataLoss           = 15,
  Unauthenticated    = 16,
}
