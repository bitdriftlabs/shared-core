#![allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]

use super::{Field, Fields, Value};
use anyhow::{anyhow, bail};

//
// Chunks
//

// Keep the usual single fragment inline; allocate metadata only when singular messages merge.
#[derive(Clone, Debug)]
enum Chunks<'a> {
  Single([&'a [u8]; 1]),
  Merged(Vec<&'a [u8]>),
}

impl Default for Chunks<'_> {
  fn default() -> Self {
    Self::Single([&[]])
  }
}

impl<'a> Chunks<'a> {
  fn as_slice(&self) -> &[&'a [u8]] {
    match self {
      Self::Single(bytes) => bytes,
      Self::Merged(bytes) => bytes,
    }
  }

  fn push(chunks: &mut Option<Self>, bytes: &'a [u8]) {
    match chunks {
      None => *chunks = Some(Self::Single([bytes])),
      Some(Self::Single([first])) => *chunks = Some(Self::Merged(vec![*first, bytes])),
      Some(Self::Merged(bytes_list)) => bytes_list.push(bytes),
    }
  }
}

//
// Message
//

/// A borrowed message, including fragments merged by repeated singular-message occurrences.
#[derive(Clone, Debug, Default)]
pub struct Message<'a> {
  chunks: Chunks<'a>,
  depth: u32,
}

impl<'a> Message<'a> {
  pub fn new(bytes: &'a [u8]) -> anyhow::Result<Self> {
    Self::from_chunks(Chunks::Single([bytes]), 0)
  }

  fn from_chunks(chunks: Chunks<'a>, depth: u32) -> anyhow::Result<Self> {
    if depth >= 100 {
      bail!("protobuf recursion limit exceeded");
    }
    let message = Self { chunks, depth };
    for field in message.fields() {
      field?;
    }
    Ok(message)
  }

  pub fn fields(&self) -> impl Iterator<Item = anyhow::Result<Field<'a>>> + '_ {
    self
      .chunks
      .as_slice()
      .iter()
      .flat_map(|bytes| Fields::new(bytes))
  }

  pub fn optional_uint(&self, number: u32) -> anyhow::Result<Option<u64>> {
    let mut result = None;
    for field in self.fields() {
      let field = field?;
      if field.number == number {
        result = Some(field.value.varint()?);
      }
    }
    Ok(result)
  }

  pub fn uint(&self, number: u32) -> anyhow::Result<u64> {
    Ok(self.optional_uint(number)?.unwrap_or_default())
  }

  // Protobuf truncates integer wire values to the declared width, including signed values.
  pub fn uint32(&self, number: u32) -> anyhow::Result<u32> {
    Ok(self.uint(number)? as u32)
  }

  pub fn int32(&self, number: u32) -> anyhow::Result<i32> {
    Ok(self.uint(number)? as i32)
  }

  pub fn int64(&self, number: u32) -> anyhow::Result<i64> {
    Ok(self.uint(number)? as i64)
  }

  pub fn optional_string(&self, number: u32) -> anyhow::Result<Option<&'a str>> {
    let mut result = None;
    for field in self.fields() {
      let field = field?;
      if field.number == number {
        result = Some(field.value.string()?);
      }
    }
    Ok(result)
  }

  pub fn string(&self, number: u32) -> anyhow::Result<&'a str> {
    Ok(self.optional_string(number)?.unwrap_or_default())
  }

  pub fn bytes(&self, number: u32) -> anyhow::Result<&'a [u8]> {
    let mut result = &[][..];
    for field in self.fields() {
      let field = field?;
      if field.number == number {
        result = field.value.bytes()?;
      }
    }
    Ok(result)
  }

  pub fn strings(&self, number: u32) -> anyhow::Result<Vec<String>> {
    let mut result = Vec::new();
    for field in self.fields() {
      let field = field?;
      if field.number == number {
        result.push(field.value.string()?.to_owned());
      }
    }
    Ok(result)
  }

  pub fn double(&self, number: u32) -> anyhow::Result<f64> {
    let mut result = 0.0;
    for field in self.fields() {
      let field = field?;
      if field.number == number {
        let Value::Fixed64(bits) = field.value else {
          bail!("expected protobuf fixed64 field");
        };
        result = f64::from_bits(bits);
      }
    }
    Ok(result)
  }

  pub fn message(&self, number: u32) -> anyhow::Result<Option<Self>> {
    let mut chunks = None;
    for field in self.fields() {
      let field = field?;
      if field.number == number {
        Chunks::push(&mut chunks, field.value.bytes()?);
      }
    }
    chunks
      .map(|chunks| Self::from_chunks(chunks, self.depth + 1))
      .transpose()
  }

  pub fn required_message(&self, number: u32) -> anyhow::Result<Self> {
    self
      .message(number)?
      .ok_or_else(|| anyhow!("missing protobuf message field {number}"))
  }

  pub fn messages(&self, number: u32) -> anyhow::Result<Vec<Self>> {
    let mut messages = Vec::new();
    self.visit_messages(number, &mut |message| {
      messages.push(message.clone());
      Ok(())
    })?;
    Ok(messages)
  }

  // Share wire iteration across output types rather than specializing an iterator chain per field.
  #[inline(never)]
  pub fn visit_messages(
    &self,
    number: u32,
    visit: &mut dyn FnMut(&Self) -> anyhow::Result<()>,
  ) -> anyhow::Result<()> {
    for field in self.fields() {
      let field = field?;
      if field.number == number {
        let message = Self::from_chunks(Chunks::Single([field.value.bytes()?]), self.depth + 1)?;
        visit(&message)?;
      }
    }
    Ok(())
  }

  pub fn oneof(&self, numbers: &[u32]) -> anyhow::Result<Option<Field<'a>>> {
    let mut result = None;
    for field in self.fields() {
      let field = field?;
      if numbers.contains(&field.number) {
        result = Some(field);
      }
    }
    Ok(result)
  }

  /// Merge the selected message alternative only since the most recent oneof switch.
  pub fn oneof_message(&self, numbers: &[u32]) -> anyhow::Result<Option<(u32, Self)>> {
    let mut selected = None;
    let mut chunks = None;
    for field in self.fields() {
      let field = field?;
      if numbers.contains(&field.number) {
        if selected != Some(field.number) {
          chunks = None;
          selected = Some(field.number);
        }
        Chunks::push(&mut chunks, field.value.bytes()?);
      }
    }
    selected
      .zip(chunks)
      .map(|(number, chunks)| {
        Self::from_chunks(chunks, self.depth + 1).map(|message| (number, message))
      })
      .transpose()
  }

  /// Read a message alternative in a oneof that also contains scalar alternatives.
  pub fn selected_message(&self, number: u32, alternatives: &[u32]) -> anyhow::Result<Self> {
    let mut chunks = None;
    for field in self.fields() {
      let field = field?;
      if alternatives.contains(&field.number) {
        if field.number == number {
          Chunks::push(&mut chunks, field.value.bytes()?);
        } else {
          chunks = None;
        }
      }
    }
    let chunks =
      chunks.ok_or_else(|| anyhow!("missing selected protobuf message field {number}"))?;
    Self::from_chunks(chunks, self.depth + 1)
  }

  /// Materialize merged wire fragments when a caller requires owned bytes.
  #[must_use]
  pub fn to_bytes(&self) -> Vec<u8> {
    self.chunks.as_slice().concat()
  }

  pub fn without_fields(&self, excluded: &[u32]) -> anyhow::Result<Vec<u8>> {
    let mut payload = Vec::new();
    for bytes in self.chunks.as_slice() {
      let mut fields = Fields::new(bytes);
      let mut start = 0;
      while let Some(field) = fields.next() {
        let field = field?;
        let end = usize::try_from(fields.position())?;
        if !excluded.contains(&field.number) {
          payload.extend_from_slice(&bytes[start .. end]);
        }
        start = end;
      }
    }
    Ok(payload)
  }
}
