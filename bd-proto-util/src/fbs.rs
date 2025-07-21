// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use flatbuffers::{FlatBufferBuilder, WIPOffset};
use std::borrow::Cow;

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
