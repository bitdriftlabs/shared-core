// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_log_primitives::{AnnotatedLogField, LogFieldKey, LogFieldValue, LogFields, StringOrBytes};
use std::borrow::Cow;
use std::mem::{size_of, size_of_val};

//
// MemorySized
//

// Interface for reporting the amount of memory a given object
// uses. It takes into account both stack and heap allocations and hence
// is different than calling `std::mem::size_of`.
pub trait MemorySized {
  // Returns the size a given object takes in memory, expressed in bytes.
  fn size(&self) -> usize;
}

impl MemorySized for AnnotatedLogField {
  fn size(&self) -> usize {
    size_of_val(self) + self.value.size() + size_of_val(&self.kind)
  }
}

impl MemorySized for LogFieldKey {
  fn size(&self) -> usize {
    size_of_val(self)
      + match self {
        Cow::Borrowed(_) => 0,
        Cow::Owned(s) => s.len(),
      }
  }
}

impl MemorySized for LogFields {
  fn size(&self) -> usize {
    let empty_reserved_mem =
      (self.capacity() - self.len()) * size_of::<(String, StringOrBytes<String, Vec<u8>>)>();
    size_of_val(self)
      + self.iter().map(|(k, v)| k.size() + v.size()).sum::<usize>()
      + empty_reserved_mem
  }
}

impl MemorySized for String {
  fn size(&self) -> usize {
    size_of_val(self) + self.len()
  }
}

impl MemorySized for LogFieldValue {
  fn size(&self) -> usize {
    size_of_val(self)
      + match self {
        Self::String(s) => s.len(),
        // TODO(snowp): Can we avoid counting the size of the string if we know that it's "shared"?
        Self::SharedString(s) => s.len(),
        Self::Bytes(b) => b.capacity(),
      }
  }
}
