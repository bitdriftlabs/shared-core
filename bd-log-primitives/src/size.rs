// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//
// MemorySized
//

use ahash::AHashMap;
use std::borrow::Cow;

// Interface for reporting the amount of memory a given object
// uses. It takes into account both stack and heap allocations and hence
// is different than calling `std::mem::size_of`.
pub trait MemorySized {
  // Returns the size a given object takes in memory, expressed in bytes.
  fn size(&self) -> usize;
}

impl MemorySized for Cow<'_, str> {
  fn size(&self) -> usize {
    size_of_val(self)
      + match self {
        Cow::Borrowed(_) => 0,
        Cow::Owned(s) => s.len(),
      }
  }
}

impl MemorySized for String {
  fn size(&self) -> usize {
    size_of_val(self) + self.len()
  }
}

impl<K: MemorySized, V: MemorySized> MemorySized for AHashMap<K, V> {
  fn size(&self) -> usize {
    let empty_reserved_mem = (self.capacity() - self.len()) * size_of::<(K, V)>();
    self.iter().map(|(k, v)| k.size() + v.size()).sum::<usize>()
      + std::mem::size_of::<Self>()
      + empty_reserved_mem
  }
}

impl<T: MemorySized> MemorySized for Vec<T> {
  fn size(&self) -> usize {
    let empty_reserved_mem = (self.capacity() - self.len()) * size_of::<T>();
    self.iter().map(MemorySized::size).sum::<usize>()
      + std::mem::size_of::<Self>()
      + empty_reserved_mem
  }
}
