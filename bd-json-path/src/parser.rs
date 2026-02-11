// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./parser_test.rs"]
mod tests;

/// Extracted value type - contains a reference to the raw JSON slice.
/// The caller is responsible for parsing/interpreting the value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Value<'a> {
  /// A JSON string value. The slice contains the string content WITHOUT quotes.
  /// Note: If the string contains escape sequences, they are NOT decoded.
  /// The caller must handle escape sequences if needed.
  String(&'a str),
  /// A JSON number value. The slice contains the raw number text.
  /// The caller is responsible for parsing it as integer/float.
  Number(&'a str),
}

/// Zero-allocation JSON path extractor.
///
/// Incrementally parses JSON to find a value at the specified path, bailing early when the path
/// cannot be matched. Uses O(n) time and O(1) heap allocations.
pub struct Parser<'a> {
  /// The JSON bytes being parsed
  json: &'a [u8],
  /// Current position in the JSON
  pos: usize,
  /// Path segments to match
  path: &'a [&'a str],
  /// Current depth in the path (number of segments matched so far)
  depth: usize,
}

impl<'a> Parser<'a> {
  /// Create a new parser for the given JSON and path.
  #[must_use]
  pub const fn new(json: &'a str, path: &'a [&'a str]) -> Self {
    Self {
      json: json.as_bytes(),
      pos: 0,
      path,
      depth: 0,
    }
  }

  /// Extract the value at the configured path.
  ///
  /// Returns `Some(Value)` if found, `None` if not found or JSON is malformed.
  pub fn extract(&mut self) -> Option<Value<'a>> {
    self.parse_value()
  }

  /// Parse a JSON value. If we're at target depth, extract it; otherwise navigate deeper.
  fn parse_value(&mut self) -> Option<Value<'a>> {
    self.skip_ws();

    if self.depth == self.path.len() {
      // We're at the target depth - extract the value
      return self.extract_value();
    }

    // We need to go deeper - check what type of container we have
    match self.peek()? {
      b'{' => self.parse_object(),
      b'[' => self.parse_array(),
      // Not a container but we need to go deeper - path doesn't exist
      _ => None,
    }
  }

  /// Parse an object, looking for the key at current path depth.
  fn parse_object(&mut self) -> Option<Value<'a>> {
    self.advance(); // consume '{'
    self.skip_ws();

    // Empty object
    if self.peek()? == b'}' {
      return None; // Key not found
    }

    let target_key = self.path[self.depth];

    loop {
      self.skip_ws();

      // Parse the key
      if self.peek()? != b'"' {
        return None; // Malformed: expected string key
      }

      let key_matches = self.key_matches(target_key);

      self.skip_ws();
      if self.peek()? != b':' {
        return None; // Malformed: expected ':'
      }
      self.advance(); // consume ':'
      self.skip_ws();

      if key_matches {
        // Found our key - recurse into the value
        self.depth += 1;
        return self.parse_value();
      }

      // Not our key - skip the value
      if !self.skip_value() {
        return None;
      }

      self.skip_ws();
      match self.peek()? {
        b',' => self.advance(),
        _ => return None,
      }
    }
  }

  /// Parse an array, looking for the index at current path depth.
  fn parse_array(&mut self) -> Option<Value<'a>> {
    self.advance(); // consume '['
    self.skip_ws();

    // Empty array
    if self.peek()? == b']' {
      return None; // Index not found
    }

    let target_index = Self::parse_index(self.path[self.depth])?;
    let mut current_index = 0usize;

    loop {
      self.skip_ws();

      if current_index == target_index {
        // Found our index - recurse into the value
        self.depth += 1;
        return self.parse_value();
      }

      // Not our index - skip the value
      if !self.skip_value() {
        return None;
      }

      current_index += 1;

      self.skip_ws();
      match self.peek()? {
        b',' => self.advance(),
        _ => return None,
      }
    }
  }

  /// Extract the value at current position (we're at target depth).
  fn extract_value(&mut self) -> Option<Value<'a>> {
    self.skip_ws();
    match self.peek()? {
      b'"' => self.extract_string(),
      b'-' | b'0'..=b'9' => self.extract_number(),
      // For other types (object, array, bool, null), we return None since they're not
      // string or number
      _ => None,
    }
  }

  /// Extract a string value, returning a slice of the content (without quotes).
  fn extract_string(&mut self) -> Option<Value<'a>> {
    self.advance(); // consume opening '"'
    let start = self.pos;

    loop {
      match self.peek()? {
        b'"' => {
          let end = self.pos;
          self.advance(); // consume closing '"'
          // SAFETY: We're working with valid UTF-8 JSON input, and string content between quotes
          // is valid UTF-8 (JSON spec guarantees this for well-formed JSON).
          let s = std::str::from_utf8(&self.json[start..end]).ok()?;
          return Some(Value::String(s));
        },
        b'\\' => {
          self.advance();
          self.peek()?;
          self.advance();
        },
        _ => self.advance(),
      }
    }
  }

  /// Extract a number value, returning the raw slice.
  fn extract_number(&mut self) -> Option<Value<'a>> {
    let start = self.pos;

    // Optional leading minus
    if self.peek() == Some(b'-') {
      self.advance();
    }

    // Integer part
    match self.peek()? {
      b'0' => self.advance(),
      b'1'..=b'9' => {
        self.advance();
        while matches!(self.peek(), Some(b'0'..=b'9')) {
          self.advance();
        }
      },
      _ => return None, // Malformed
    }

    // Optional fraction
    if self.peek() == Some(b'.') {
      self.advance();
      if !matches!(self.peek(), Some(b'0'..=b'9')) {
        return None; // Malformed: need at least one digit after '.'
      }
      while matches!(self.peek(), Some(b'0'..=b'9')) {
        self.advance();
      }
    }

    // Optional exponent
    if matches!(self.peek(), Some(b'e' | b'E')) {
      self.advance();
      if matches!(self.peek(), Some(b'+' | b'-')) {
        self.advance();
      }
      if !matches!(self.peek(), Some(b'0'..=b'9')) {
        return None; // Malformed: need at least one digit in exponent
      }
      while matches!(self.peek(), Some(b'0'..=b'9')) {
        self.advance();
      }
    }

    let end = self.pos;
    // SAFETY: Number characters are ASCII and thus valid UTF-8
    let s = std::str::from_utf8(&self.json[start..end]).ok()?;
    Some(Value::Number(s))
  }

  /// Check if the current string key matches the expected key, advancing past the string.
  /// Returns true if it matches, false otherwise.
  fn key_matches(&mut self, expected: &str) -> bool {
    if self.peek() != Some(b'"') {
      return false;
    }
    self.advance(); // consume opening '"'

    let expected_bytes = expected.as_bytes();
    let mut expected_pos = 0;
    let mut matches = true;

    loop {
      match self.peek() {
        Some(b'"') => {
          self.advance(); // consume closing '"'
          // Match only if we consumed all expected bytes
          return matches && expected_pos == expected_bytes.len();
        },
        Some(b'\\') => {
          // Escape sequence - for simplicity, we don't try to match escaped characters
          // against the expected key. If there's an escape, we skip the string and return
          // false. This means keys with escape sequences won't match.
          // This is a reasonable limitation for most use cases.
          matches = false;
          self.advance();
          if self.peek().is_some() {
            self.advance();
          } else {
            return false;
          }
        },
        Some(c) => {
          if matches {
            if expected_pos < expected_bytes.len() && c == expected_bytes[expected_pos] {
              expected_pos += 1;
            } else {
              matches = false;
            }
          }
          self.advance();
        },
        None => return false, // Malformed: unterminated string
      }
    }
  }

  /// Skip an entire JSON value without extracting it.
  fn skip_value(&mut self) -> bool {
    self.skip_ws();
    match self.peek() {
      Some(b'"') => self.skip_string(),
      Some(b'{') => self.skip_object(),
      Some(b'[') => self.skip_array(),
      Some(b't') => self.skip_literal(b"true"),
      Some(b'f') => self.skip_literal(b"false"),
      Some(b'n') => self.skip_literal(b"null"),
      Some(b'-' | b'0'..=b'9') => {
        self.skip_number();
        true
      },
      _ => false,
    }
  }

  /// Skip a string value.
  fn skip_string(&mut self) -> bool {
    self.advance(); // consume opening '"'
    loop {
      match self.peek() {
        Some(b'"') => {
          self.advance();
          return true;
        },
        Some(b'\\') => {
          self.advance();
          if self.peek().is_none() {
            return false;
          }
          self.advance();
        },
        Some(_) => self.advance(),
        None => return false,
      }
    }
  }

  /// Skip an object value.
  fn skip_object(&mut self) -> bool {
    self.advance(); // consume '{'
    self.skip_ws();

    if self.peek() == Some(b'}') {
      self.advance();
      return true;
    }

    loop {
      self.skip_ws();
      // Skip key
      if !self.skip_string() {
        return false;
      }
      self.skip_ws();
      if self.peek() != Some(b':') {
        return false;
      }
      self.advance();
      // Skip value
      if !self.skip_value() {
        return false;
      }
      self.skip_ws();
      match self.peek() {
        Some(b',') => self.advance(),
        Some(b'}') => {
          self.advance();
          return true;
        },
        _ => return false,
      }
    }
  }

  /// Skip an array value.
  fn skip_array(&mut self) -> bool {
    self.advance();
    self.skip_ws();

    if self.peek() == Some(b']') {
      self.advance();
      return true;
    }

    loop {
      if !self.skip_value() {
        return false;
      }
      self.skip_ws();
      match self.peek() {
        Some(b',') => self.advance(),
        Some(b']') => {
          self.advance();
          return true;
        },
        _ => return false,
      }
    }
  }

  /// Skip a literal value (true, false, null).
  fn skip_literal(&mut self, literal: &[u8]) -> bool {
    for &expected in literal {
      if self.peek() != Some(expected) {
        return false;
      }
      self.advance();
    }
    true
  }

  /// Skip a number value.
  fn skip_number(&mut self) {
    // Optional leading minus
    if self.peek() == Some(b'-') {
      self.advance();
    }

    // Integer part
    while matches!(self.peek(), Some(b'0'..=b'9')) {
      self.advance();
    }

    // Optional fraction
    if self.peek() == Some(b'.') {
      self.advance();
      while matches!(self.peek(), Some(b'0'..=b'9')) {
        self.advance();
      }
    }

    // Optional exponent
    if matches!(self.peek(), Some(b'e' | b'E')) {
      self.advance();
      if matches!(self.peek(), Some(b'+' | b'-')) {
        self.advance();
      }
      while matches!(self.peek(), Some(b'0'..=b'9')) {
        self.advance();
      }
    }
  }

  /// Parse a path segment as an array index.
  fn parse_index(s: &str) -> Option<usize> {
    // Don't allow leading zeros (except for "0" itself)
    let bytes = s.as_bytes();
    if bytes.is_empty() {
      return None;
    }
    if bytes.len() > 1 && bytes[0] == b'0' {
      return None;
    }
    s.parse().ok()
  }

  /// Skip whitespace.
  fn skip_ws(&mut self) {
    while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
      self.advance();
    }
  }

  /// Peek at the current byte without advancing.
  fn peek(&self) -> Option<u8> {
    self.json.get(self.pos).copied()
  }

  /// Advance position by one byte.
  fn advance(&mut self) {
    self.pos += 1;
  }
}

/// Extract a value at the given path from JSON.
///
/// Path segments are interpreted as:
/// - Object keys when inside an object
/// - Array indices when inside an array (must be valid non-negative integers)
///
/// Returns `Some(Value)` containing a reference to the raw JSON slice if found.
/// Returns `None` if:
/// - Path doesn't exist
/// - JSON is malformed (before or at the target location)
/// - Value at path is not a string or number (e.g., object, array, bool, null)
///
/// # Zero Allocation
///
/// This function performs no heap allocations. The returned `Value` contains references to slices
/// of the original JSON string.
///
/// # Early Bail
///
/// The parser stops as soon as it finds the target value or determines the path cannot exist.
/// It does not validate the remainder of the JSON after extraction.
///
/// # Example
///
/// ```
/// use bd_json_path::{extract, Value};
///
/// let json = r#"{"users": [{"name": "Alice", "age": 30}]}"#;
///
/// // Extract a string
/// let name = extract(json, &["users", "0", "name"]);
/// assert_eq!(name, Some(Value::String("Alice")));
///
/// // Extract a number
/// let age = extract(json, &["users", "0", "age"]);
/// assert_eq!(age, Some(Value::Number("30")));
///
/// // Path not found
/// let missing = extract(json, &["users", "0", "email"]);
/// assert_eq!(missing, None);
/// ```
#[must_use]
pub fn extract<'a>(json: &'a str, path: &'a [&'a str]) -> Option<Value<'a>> {
  Parser::new(json, path).extract()
}
