// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use nom::{AsChar, Input};
use std::collections::VecDeque;
use std::iter::{Skip, Take};

#[cfg(test)]
#[path = "./input_tests.rs"]
mod tests;

/// A view into a memory-mapped file
#[derive(Clone)]
pub struct MemmapView<'a> {
  source: &'a memmap2::Mmap,
  /// Beginning index (inclusive)
  start_index: usize,
  /// Terminating index (exclusive)
  end_index: usize,
}

impl<'a> MemmapView<'a> {
  pub fn new(source: &'a memmap2::Mmap) -> Self {
    Self {
      source,
      start_index: 0,
      end_index: source.len(),
    }
  }

  #[inline]
  pub fn is_empty(&self) -> bool {
    self.len() == 0
  }

  #[inline]
  pub fn len(&self) -> usize {
    self.end_index - self.start_index
  }

  pub fn peek(&self) -> Option<u8> {
    (!self.is_empty()).then(|| self.source[self.start_index])
  }

  pub fn find(&self, character: char) -> Option<usize> {
    self.position(|c| c.as_char() == character)
  }
}

impl std::fmt::Display for MemmapView<'_> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let contents = &self.source[self.start_index .. self.end_index];
    write!(f, "{}", str::from_utf8(contents).unwrap_or_default())
  }
}

impl std::fmt::Debug for MemmapView<'_> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let contents = &self.source[self.start_index .. self.end_index];
    write!(f, "{:?}", str::from_utf8(contents))
  }
}

impl nom::Compare<&str> for MemmapView<'_> {
  fn compare(&self, t: &str) -> nom::CompareResult {
    if self.len() < t.len() {
      return nom::CompareResult::Error;
    }
    let content = self.take(t.len()).to_string();
    content.as_str().compare(t)
  }

  fn compare_no_case(&self, t: &str) -> nom::CompareResult {
    if self.len() < t.len() {
      return nom::CompareResult::Error;
    }
    let content = self.take(t.len()).to_string();
    content.as_str().compare_no_case(t)
  }
}

impl nom::FindSubstring<&str> for MemmapView<'_> {
  fn find_substring(&self, substr: &str) -> Option<usize> {
    let substr_len = substr.len();
    let start_offset = self.start_index + substr_len;
    if self.end_index < start_offset {
      return None;
    }
    let initial = Vec::from(&self.source[self.start_index .. start_offset]);
    let mut buffer = VecDeque::from(initial);
    for index in start_offset ..= self.end_index {
      if let Ok(contents) = str::from_utf8(buffer.make_contiguous())
        && contents == substr
      {
        return Some(index - start_offset);
      }
      if index == self.end_index {
        break;
      }
      buffer.pop_front();
      buffer.push_back(self.source[index]);
    }
    None
  }
}

impl nom::Offset for MemmapView<'_> {
  fn offset(&self, second: &Self) -> usize {
    second.start_index - self.start_index
  }
}

impl<'a> nom::Input for MemmapView<'a> {
  type Item = &'a u8;
  type Iter = Take<Skip<core::slice::Iter<'a, u8>>>;
  type IterIndices = std::iter::Enumerate<Self::Iter>;

  fn input_len(&self) -> usize {
    self.len()
  }

  fn take(&self, index: usize) -> Self {
    Self {
      source: self.source,
      start_index: self.start_index,
      end_index: self.start_index + index,
    }
  }

  fn take_from(&self, index: usize) -> Self {
    Self {
      source: self.source,
      start_index: self.start_index + index,
      end_index: self.end_index,
    }
  }

  fn take_split(&self, index: usize) -> (Self, Self) {
    (self.take(index), self.take_from(index))
  }

  fn position<P>(&self, predicate: P) -> Option<usize>
  where
    P: Fn(Self::Item) -> bool,
  {
    self
      .iter_indices()
      .find(|(_, value)| predicate(value))
      .map(|(index, _)| index)
  }

  fn iter_elements(&self) -> Self::Iter {
    self.source.iter().skip(self.start_index).take(self.len())
  }

  fn iter_indices(&self) -> Self::IterIndices {
    self.iter_elements().enumerate()
  }

  fn slice_index(&self, count: usize) -> Result<usize, nom::Needed> {
    if self.len() >= count {
      Ok(count)
    } else {
      Err(nom::Needed::new(count - self.len()))
    }
  }
}
