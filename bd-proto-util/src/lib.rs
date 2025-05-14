// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

pub mod proto;

use anyhow::bail;
use bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::{
  root_as_log_unchecked,
  Log,
};
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use protobuf::MessageFull;
use std::borrow::Cow;
use std::fmt::Debug;

extern "C" {
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

//
// ProtoVecDebugWrapper
//

// This is a helper that will use the display implementation for debug printing protos. We should
// probably patch proto to just always use the friendly version.
pub struct ProtoVecDebugWrapper<'a, T>(pub &'a Vec<T>);

impl<T> Debug for ProtoVecDebugWrapper<'_, T>
where
  T: MessageFull,
{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_list()
      .entries(self.0.iter().map(|x| format!("{x}")))
      .finish()
  }
}

/// Convenience trait for converting anything String-like into a Flatbuffer string.
pub trait ToFlatBufferString {
  fn to_fb<'a>(&self, fbb: &mut FlatBufferBuilder<'a>) -> Option<WIPOffset<&'a str>>;
}

impl ToFlatBufferString for &str {
  fn to_fb<'a>(&self, fbb: &mut FlatBufferBuilder<'a>) -> Option<WIPOffset<&'a str>> {
    Some(fbb.create_string(self))
  }
}

impl ToFlatBufferString for String {
  fn to_fb<'a>(&self, fbb: &mut FlatBufferBuilder<'a>) -> Option<WIPOffset<&'a str>> {
    Some(fbb.create_string(self))
  }
}

impl ToFlatBufferString for Cow<'_, str> {
  fn to_fb<'a>(&self, fbb: &mut FlatBufferBuilder<'a>) -> Option<WIPOffset<&'a str>> {
    Some(fbb.create_string(self))
  }
}

impl<S: ToFlatBufferString> ToFlatBufferString for Option<S> {
  fn to_fb<'a>(&self, fbb: &mut FlatBufferBuilder<'a>) -> Option<WIPOffset<&'a str>> {
    self.as_ref().and_then(|inner| inner.to_fb(fbb))
  }
}
