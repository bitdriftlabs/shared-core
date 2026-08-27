// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::JsonPathToken;
use std::borrow::Cow;

const MAX_JSON_DEPTH: usize = 128;

pub(super) fn resolve<'a>(input: &'a str, path: &[JsonPathToken]) -> Option<Cow<'a, str>> {
  let mut parser = Parser::new(input);
  parser.walk_value(path, 0)
}

/// A streaming JSON path parser that scans only the requested path rather than constructing a DOM.
/// Unescaped strings borrow from the input; decoding escaped keys or values allocates. Parsing is
/// limited to `MAX_JSON_DEPTH` nested containers.
struct Parser<'a> {
  input: &'a str,
  pos: usize,
}

#[derive(Clone, Copy)]
struct StringToken {
  start: usize,
  end: usize,
  escaped: bool,
}

impl<'a> Parser<'a> {
  const fn new(input: &'a str) -> Self {
    Self { input, pos: 0 }
  }

  fn walk_value(&mut self, path: &[JsonPathToken], depth: usize) -> Option<Cow<'a, str>> {
    self.skip_whitespace();
    let Some((token, remaining_path)) = path.split_first() else {
      return self.extract_scalar();
    };

    match token {
      JsonPathToken::Key(key) if self.peek()? == b'{' => {
        self.walk_object(key, remaining_path, depth)
      },
      JsonPathToken::Index(index) if self.peek()? == b'[' => {
        self.walk_array(*index, remaining_path, depth)
      },
      JsonPathToken::Key(_) | JsonPathToken::Index(_) => {
        self.parse_value(depth)?;
        None
      },
    }
  }

  fn extract_scalar(&mut self) -> Option<Cow<'a, str>> {
    let start = self.pos;
    match self.peek()? {
      b'"' => {
        let value = self.parse_string()?;
        self.string_value(value)
      },
      b't' => {
        self.consume_literal(b"true")?;
        Some(Cow::Borrowed(&self.input[start .. self.pos]))
      },
      b'f' => {
        self.consume_literal(b"false")?;
        Some(Cow::Borrowed(&self.input[start .. self.pos]))
      },
      b'-' | b'0' ..= b'9' => {
        self.parse_number()?;
        Some(Cow::Borrowed(&self.input[start .. self.pos]))
      },
      b'n' => {
        self.consume_literal(b"null")?;
        None
      },
      _ => None,
    }
  }

  fn walk_object(
    &mut self,
    wanted_key: &str,
    path: &[JsonPathToken],
    depth: usize,
  ) -> Option<Cow<'a, str>> {
    (depth < MAX_JSON_DEPTH).then_some(())?;
    self.consume(b'{')?;
    self.skip_whitespace();
    if self.consume_if(b'}') {
      return None;
    }

    loop {
      let key = self.parse_string()?;
      self.skip_whitespace();
      self.consume(b':')?;
      if self.string_equals(key, wanted_key)? {
        // The extractor uses the first matching key and does not inspect later members.
        let result = self.walk_value(path, depth + 1)?;
        return self.finish_container(b'}', result);
      }
      self.parse_value(depth + 1)?;
      self.skip_whitespace();
      if self.consume_if(b'}') {
        return None;
      }
      self.consume(b',')?;
      self.skip_whitespace();
    }
  }

  fn walk_array(
    &mut self,
    requested_index: i32,
    path: &[JsonPathToken],
    depth: usize,
  ) -> Option<Cow<'a, str>> {
    self.walk_positive_array_index(usize::try_from(requested_index).ok()?, path, depth)
  }

  fn walk_positive_array_index(
    &mut self,
    requested_index: usize,
    path: &[JsonPathToken],
    depth: usize,
  ) -> Option<Cow<'a, str>> {
    (depth < MAX_JSON_DEPTH).then_some(())?;
    self.consume(b'[')?;
    self.skip_whitespace();
    if self.consume_if(b']') {
      return None;
    }

    let mut index = 0;
    loop {
      if index == requested_index {
        let result = self.walk_value(path, depth + 1)?;
        return self.finish_container(b']', result);
      }
      self.parse_value(depth + 1)?;
      self.skip_whitespace();
      if self.consume_if(b']') {
        return None;
      }
      self.consume(b',')?;
      self.skip_whitespace();
      index = index.checked_add(1)?;
    }
  }

  // Validate delimiters on the selected path, but deliberately do not parse trailing siblings.
  fn finish_container(&mut self, closing: u8, value: Cow<'a, str>) -> Option<Cow<'a, str>> {
    self.skip_whitespace();
    (self.consume_if(closing) || self.peek() == Some(b',')).then_some(value)
  }

  fn parse_value(&mut self, depth: usize) -> Option<()> {
    self.skip_whitespace();
    match self.peek()? {
      b'{' => self.parse_object(depth),
      b'[' => self.parse_array(depth),
      b'"' => self.parse_string().map(|_| ()),
      b'-' | b'0' ..= b'9' => self.parse_number(),
      b't' => self.consume_literal(b"true"),
      b'f' => self.consume_literal(b"false"),
      b'n' => self.consume_literal(b"null"),
      _ => None,
    }
  }

  fn parse_object(&mut self, depth: usize) -> Option<()> {
    (depth < MAX_JSON_DEPTH).then_some(())?;
    self.consume(b'{')?;
    self.skip_whitespace();
    if self.consume_if(b'}') {
      return Some(());
    }
    loop {
      self.parse_string()?;
      self.skip_whitespace();
      self.consume(b':')?;
      self.parse_value(depth + 1)?;
      self.skip_whitespace();
      if self.consume_if(b'}') {
        return Some(());
      }
      self.consume(b',')?;
      self.skip_whitespace();
    }
  }

  fn parse_array(&mut self, depth: usize) -> Option<()> {
    (depth < MAX_JSON_DEPTH).then_some(())?;
    self.consume(b'[')?;
    self.skip_whitespace();
    if self.consume_if(b']') {
      return Some(());
    }
    loop {
      self.parse_value(depth + 1)?;
      self.skip_whitespace();
      if self.consume_if(b']') {
        return Some(());
      }
      self.consume(b',')?;
      self.skip_whitespace();
    }
  }

  fn parse_string(&mut self) -> Option<StringToken> {
    self.consume(b'"')?;
    let start = self.pos;
    let mut escaped = false;
    loop {
      let byte = self.next()?;
      match byte {
        b'"' => {
          return Some(StringToken {
            start,
            end: self.pos - 1,
            escaped,
          });
        },
        b'\\' => {
          escaped = true;
          match self.next()? {
            b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => {},
            b'u' => self.parse_unicode_escape()?,
            _ => return None,
          }
        },
        0 ..= 0x1f => return None,
        _ => {},
      }
    }
  }

  fn parse_unicode_escape(&mut self) -> Option<()> {
    let code = self.parse_hex_code_unit()?;
    if (0xdc00 ..= 0xdfff).contains(&code) {
      return None;
    }
    if (0xd800 ..= 0xdbff).contains(&code) {
      self.consume(b'\\')?;
      self.consume(b'u')?;
      let low = self.parse_hex_code_unit()?;
      if !(0xdc00 ..= 0xdfff).contains(&low) {
        return None;
      }
    }
    Some(())
  }

  fn parse_hex_code_unit(&mut self) -> Option<u16> {
    let mut value = 0_u16;
    for _ in 0 .. 4 {
      value = value
        .checked_mul(16)?
        .checked_add(u16::from(hex_digit(self.next()?)?))?;
    }
    Some(value)
  }

  fn parse_number(&mut self) -> Option<()> {
    self.consume_if(b'-');
    match self.next()? {
      b'0' => {},
      b'1' ..= b'9' => {
        while matches!(self.peek(), Some(b'0' ..= b'9')) {
          self.pos += 1;
        }
      },
      _ => return None,
    }
    if self.consume_if(b'.') {
      self.consume_digits()?;
    }
    if matches!(self.peek(), Some(b'e' | b'E')) {
      self.pos += 1;
      if matches!(self.peek(), Some(b'+' | b'-')) {
        self.pos += 1;
      }
      self.consume_digits()?;
    }
    Some(())
  }

  fn consume_digits(&mut self) -> Option<()> {
    self.next()?.is_ascii_digit().then_some(())?;
    while matches!(self.peek(), Some(b'0' ..= b'9')) {
      self.pos += 1;
    }
    Some(())
  }

  fn string_equals(&self, token: StringToken, wanted: &str) -> Option<bool> {
    if !token.escaped {
      return Some(&self.input[token.start .. token.end] == wanted);
    }
    Some(self.decode_string(token)? == wanted)
  }

  fn string_value(&self, token: StringToken) -> Option<Cow<'a, str>> {
    if token.escaped {
      Some(Cow::Owned(self.decode_string(token)?))
    } else {
      Some(Cow::Borrowed(&self.input[token.start .. token.end]))
    }
  }

  fn decode_string(&self, token: StringToken) -> Option<String> {
    let mut output = String::with_capacity(token.end - token.start);
    let mut pos = token.start;
    while pos < token.end {
      let byte = self.input.as_bytes()[pos];
      if byte != b'\\' {
        let next = self.input[pos .. token.end]
          .find('\\')
          .map_or(token.end, |i| pos + i);
        output.push_str(&self.input[pos .. next]);
        pos = next;
        continue;
      }
      pos += 1;
      match self.input.as_bytes().get(pos)? {
        b'"' => output.push('"'),
        b'\\' => output.push('\\'),
        b'/' => output.push('/'),
        b'b' => output.push('\u{0008}'),
        b'f' => output.push('\u{000c}'),
        b'n' => output.push('\n'),
        b'r' => output.push('\r'),
        b't' => output.push('\t'),
        b'u' => {
          pos += 1;
          let code = decode_hex(&self.input[pos .. pos + 4])?;
          pos += 4;
          let character = if (0xd800 ..= 0xdbff).contains(&code) {
            pos += 2; // Validated by parse_string: the following bytes are `\\u`.
            let low = decode_hex(&self.input[pos .. pos + 4])?;
            pos += 4;
            char::from_u32(
              0x10000 + ((u32::from(code) - 0xd800) << 10) + (u32::from(low) - 0xdc00),
            )?
          } else {
            char::from_u32(u32::from(code))?
          };
          output.push(character);
          continue;
        },
        _ => return None,
      }
      pos += 1;
    }
    Some(output)
  }

  fn consume_literal(&mut self, literal: &[u8]) -> Option<()> {
    for expected in literal {
      (*expected == self.next()?).then_some(())?;
    }
    Some(())
  }

  fn skip_whitespace(&mut self) {
    while matches!(self.peek(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
      self.pos += 1;
    }
  }

  fn consume(&mut self, byte: u8) -> Option<()> {
    (self.next()? == byte).then_some(())
  }

  fn consume_if(&mut self, byte: u8) -> bool {
    if self.peek() == Some(byte) {
      self.pos += 1;
      true
    } else {
      false
    }
  }

  fn peek(&self) -> Option<u8> {
    self.input.as_bytes().get(self.pos).copied()
  }

  fn next(&mut self) -> Option<u8> {
    let byte = self.peek()?;
    self.pos += 1;
    Some(byte)
  }
}

fn decode_hex(value: &str) -> Option<u16> {
  u16::from_str_radix(value, 16).ok()
}

const fn hex_digit(byte: u8) -> Option<u8> {
  match byte {
    b'0' ..= b'9' => Some(byte - b'0'),
    b'a' ..= b'f' => Some(byte - b'a' + 10),
    b'A' ..= b'F' => Some(byte - b'A' + 10),
    _ => None,
  }
}
