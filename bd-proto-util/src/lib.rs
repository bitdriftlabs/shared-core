// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

pub mod proto;

use anyhow::bail;
use bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::{
  Log,
  root_as_log_unchecked,
};

unsafe extern "C" {
  fn verify_log_buffer(buf: *const u8, buf_len: usize) -> bool;
}

pub fn call_verify_log_buffer(buf: &[u8]) -> anyhow::Result<Log<'_>> {
  // The Rust verification code is noted as experimental, which was quickly proven via a failing
  // fuzz test. The following code calls out to the C++ verifier code (see src/cpp/verify.cc) to
  // check it. Once checked, we use the unchecked rust version to return the log.
  unsafe {
    if !verify_log_buffer(buf.as_ptr(), buf.len()) {
      bail!("invalid flatbuffer binary data");
    }
    Ok(root_as_log_unchecked(buf))
  }
}
