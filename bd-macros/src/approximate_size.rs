// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use std::borrow::Cow;
use std::sync::Arc;

/// Estimates storage retained by a value while it is held in a bounded queue.
///
/// The estimate contains the value's inline storage and allocations it owns directly. It is
/// intentionally cheap rather than a precise allocator measurement: capacity is used for
/// `String` and `Vec`, and shared data behind `Arc` is not charged. This gives each queued item
/// a stable, conservative-enough backpressure cost without double-charging shared allocations.
///
/// Implementations should report only allocations outside the value's inline storage from
/// [`Self::approximate_size_children_bytes`]. The default
/// [`Self::approximate_size_bytes`] method adds the inline storage exactly once.
pub trait ApproximateSize {
  /// Returns inline storage plus retained allocations owned by this value.
  fn approximate_size_bytes(&self) -> usize {
    std::mem::size_of_val(self).saturating_add(self.approximate_size_children_bytes())
  }

  /// Returns retained allocations outside this value's inline storage.
  ///
  /// This method is the extension point used by `#[derive(ApproximateSize)]`. Derived
  /// implementations sum their fields' child allocations, leaving the outer value's inline
  /// storage to [`Self::approximate_size_bytes`].
  fn approximate_size_children_bytes(&self) -> usize;
}

// Primitive and fixed-size value types retain no storage outside their inline representation.
macro_rules! impl_fixed_approximate_size {
  ($($ty:ty),+ $(,)?) => {
    $(
      impl ApproximateSize for $ty {
        fn approximate_size_children_bytes(&self) -> usize {
          0
        }
      }
    )+
  };
}

impl_fixed_approximate_size!(
  (),
  bool,
  char,
  f32,
  f64,
  i8,
  i16,
  i32,
  i64,
  i128,
  isize,
  u8,
  u16,
  u32,
  u64,
  u128,
  usize,
  time::Date,
  time::Duration,
  time::OffsetDateTime,
  uuid::Uuid,
);

// A String's capacity is the allocation it retains in addition to its inline descriptor.
impl ApproximateSize for String {
  fn approximate_size_children_bytes(&self) -> usize {
    self.capacity()
  }
}

// Borrowed strings do not retain an allocation. Owned strings use their retained capacity rather
// than their current length, matching the queue's reservation model.
impl ApproximateSize for Cow<'_, str> {
  fn approximate_size_children_bytes(&self) -> usize {
    match self {
      Self::Borrowed(_) => 0,
      Self::Owned(value) => value.capacity(),
    }
  }
}

impl<T: ApproximateSize> ApproximateSize for Option<T> {
  fn approximate_size_children_bytes(&self) -> usize {
    self
      .as_ref()
      .map_or(0, ApproximateSize::approximate_size_children_bytes)
  }
}

// A Vec owns its backing allocation and may contain elements with their own retained storage.
impl<T: ApproximateSize> ApproximateSize for Vec<T> {
  fn approximate_size_children_bytes(&self) -> usize {
    self.iter().fold(
      self.capacity().saturating_mul(std::mem::size_of::<T>()),
      |size, value| size.saturating_add(value.approximate_size_children_bytes()),
    )
  }
}

impl<T: ApproximateSize, const N: usize> ApproximateSize for [T; N] {
  fn approximate_size_children_bytes(&self) -> usize {
    self.iter().fold(0, |size, value| {
      size.saturating_add(value.approximate_size_children_bytes())
    })
  }
}

// Box owns its pointee, including the pointee's inline storage and nested allocations.
impl<T: ApproximateSize + ?Sized> ApproximateSize for Box<T> {
  fn approximate_size_children_bytes(&self) -> usize {
    std::mem::size_of_val(&**self)
      .saturating_add(ApproximateSize::approximate_size_children_bytes(&**self))
  }
}

// Arc does not own a unique allocation, so charging its pointee would double-count shared data.
impl<T: ?Sized> ApproximateSize for Arc<T> {
  fn approximate_size_children_bytes(&self) -> usize {
    0
  }
}

impl<A: ApproximateSize, B: ApproximateSize> ApproximateSize for (A, B) {
  fn approximate_size_children_bytes(&self) -> usize {
    self
      .0
      .approximate_size_children_bytes()
      .saturating_add(self.1.approximate_size_children_bytes())
  }
}

impl<A: ApproximateSize, B: ApproximateSize, C: ApproximateSize> ApproximateSize for (A, B, C) {
  fn approximate_size_children_bytes(&self) -> usize {
    self
      .0
      .approximate_size_children_bytes()
      .saturating_add(self.1.approximate_size_children_bytes())
      .saturating_add(self.2.approximate_size_children_bytes())
  }
}
