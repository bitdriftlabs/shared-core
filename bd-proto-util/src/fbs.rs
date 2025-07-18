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
