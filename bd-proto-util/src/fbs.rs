// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_proto::flatbuffers::common::bitdrift_public::fbs::common::v_1::{Timestamp, TimestampArgs};
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

/// Convenience trait for converting anything Timestamp-like into a Flatbuffer Timestamp.
pub trait ToFlatBufferTimestamp {
  fn to_fb<'a>(&self, fbb: &mut FlatBufferBuilder<'a>) -> Option<WIPOffset<Timestamp<'a>>>;
}

impl ToFlatBufferTimestamp for time::OffsetDateTime {
  fn to_fb<'a>(&self, fbb: &mut FlatBufferBuilder<'a>) -> Option<WIPOffset<Timestamp<'a>>> {
    Some(Timestamp::create(
      fbb,
      &TimestampArgs {
        seconds: self.unix_timestamp(),
        // This should never overflow because the nanos field is always less than 1_000_000_000 aka
        // 1 second.
        #[allow(clippy::cast_possible_truncation)]
        nanos: (self.unix_timestamp_nanos() - (i128::from(self.unix_timestamp()) * 1_000_000_000))
          as i32,
      },
    ))
  }
}

impl ToFlatBufferTimestamp for protobuf::well_known_types::timestamp::Timestamp {
  fn to_fb<'a>(&self, fbb: &mut FlatBufferBuilder<'a>) -> Option<WIPOffset<Timestamp<'a>>> {
    Some(Timestamp::create(
      fbb,
      &TimestampArgs {
        seconds: self.seconds,
        nanos: self.nanos,
      },
    ))
  }
}

// Convenient implementations to allow Option<T> to be converted to flatbuffers if T can be.

macro_rules! define_optional_fb_impl {
  ($trait:ident, $output:ty) => {
    impl<S: $trait> $trait for Option<S> {
      fn to_fb<'a>(&self, fbb: &mut FlatBufferBuilder<'a>) -> Option<WIPOffset<$output>> {
        self.as_ref().and_then(|inner| inner.to_fb(fbb))
      }
    }
  };
}

define_optional_fb_impl!(ToFlatBufferTimestamp, Timestamp<'a>);
define_optional_fb_impl!(ToFlatBufferString, &'a str);
