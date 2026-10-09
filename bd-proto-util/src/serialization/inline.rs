//! Borrowed protobuf fields for decoding directly into application-owned representations.

#[cfg(test)]
#[path = "./inline_test.rs"]
mod tests;

mod message;
mod response;
pub mod views;

use super::runtime::Tag;
use anyhow::{anyhow, bail};
pub use message::Message;
use protobuf::CodedInputStream;
use protobuf::rt::WireType;

/// Decodes an owned runtime representation from a validated borrowed protobuf message.
pub trait ProtoDeserialize<'a>: Sized {
  fn from_inline(message: &Message<'a>) -> anyhow::Result<Self>;

  fn from_proto_bytes(bytes: &'a [u8]) -> anyhow::Result<Self> {
    Self::from_inline(&Message::new(bytes)?)
  }
}

pub trait ProtoOneofDeserialize<'a>: Sized {
  const FIELD_NUMBERS: &'static [u32];
  fn from_oneof(message: &Message<'a>) -> anyhow::Result<Option<Self>>;
}

impl<'a, Target: ProtoDeserialize<'a>> ProtoDeserialize<'a> for Box<Target> {
  fn from_inline(message: &Message<'a>) -> anyhow::Result<Self> {
    Ok(Self::new(Target::from_inline(message)?))
  }
}

//
// Value
//

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Value<'a> {
  Varint(u64),
  Fixed64(u64),
  Bytes(&'a [u8]),
  Fixed32(u32),
  Group,
}

impl<'a> Value<'a> {
  pub fn varint(self) -> anyhow::Result<u64> {
    match self {
      Self::Varint(value) => Ok(value),
      _ => bail!("expected protobuf varint"),
    }
  }

  pub fn bytes(self) -> anyhow::Result<&'a [u8]> {
    match self {
      Self::Bytes(value) => Ok(value),
      _ => bail!("expected length-delimited protobuf field"),
    }
  }

  pub fn string(self) -> anyhow::Result<&'a str> {
    Ok(std::str::from_utf8(self.bytes()?)?)
  }
}

//
// Field
//

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Field<'a> {
  pub number: u32,
  pub value: Value<'a>,
}

//
// Fields
//

pub struct Fields<'a> {
  bytes: &'a [u8],
  input: CodedInputStream<'a>,
  failed: bool,
}

impl<'a> Fields<'a> {
  #[must_use]
  pub fn new(bytes: &'a [u8]) -> Self {
    Self {
      bytes,
      input: CodedInputStream::from_bytes(bytes),
      failed: false,
    }
  }

  #[must_use]
  pub fn position(&self) -> u64 {
    self.input.pos()
  }

  fn read(&mut self) -> anyhow::Result<Option<Field<'a>>> {
    let Some(raw_tag) = self.input.read_raw_tag_or_eof()? else {
      return Ok(None);
    };
    let tag = Tag::new(raw_tag)?;
    let value = match tag.wire_type {
      WireType::Varint => Value::Varint(self.input.read_raw_varint64()?),
      WireType::Fixed64 => Value::Fixed64(self.input.read_fixed64()?),
      WireType::Fixed32 => Value::Fixed32(self.input.read_fixed32()?),
      WireType::LengthDelimited => {
        let length = self.input.read_raw_varint32()?;
        let start = usize::try_from(self.input.pos())?;
        self.input.skip_raw_bytes(length)?;
        let end = usize::try_from(self.input.pos())?;
        Value::Bytes(
          self
            .bytes
            .get(start .. end)
            .ok_or_else(|| anyhow!("truncated protobuf field"))?,
        )
      },
      WireType::StartGroup => {
        self.input.skip_field(tag.wire_type)?;
        Value::Group
      },
      WireType::EndGroup => bail!("unexpected protobuf end-group tag"),
    };
    Ok(Some(Field {
      number: tag.field_number,
      value,
    }))
  }
}

impl<'a> Iterator for Fields<'a> {
  type Item = anyhow::Result<Field<'a>>;

  fn next(&mut self) -> Option<Self::Item> {
    if self.failed {
      return None;
    }
    match self.read() {
      Ok(field) => field.map(Ok),
      Err(error) => {
        self.failed = true;
        Some(Err(error))
      },
    }
  }
}
