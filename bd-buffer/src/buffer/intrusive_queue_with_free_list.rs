// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use intrusive_collections::{LinkedList, LinkedListLink, intrusive_adapter};
use std::cell::UnsafeCell;

//
// Container
//

// Container for intrusive list entries.
pub struct Container<T> {
  link: LinkedListLink,
  // TODO(mattklein123): This is only needed because CursorMut doesn't have a get_mut() function.
  // There is no actual interior mutability used in this file. The only mutable access is via
  // &mut self access to the list.
  value: UnsafeCell<T>,
}

intrusive_adapter!(ContainerAdapter<T> = Box<Container<T>>:
                   Container<T> { link => LinkedListLink });

//
// IntrusiveQueueWithFreeList
//

// An intrusive queue which also contains a free list to avoid allocations in the common case.
pub struct IntrusiveQueueWithFreeList<T> {
  queue: LinkedList<ContainerAdapter<T>>,
  free: LinkedList<ContainerAdapter<T>>,
}

impl<T> Default for IntrusiveQueueWithFreeList<T> {
  fn default() -> Self {
    Self {
      queue: LinkedList::default(),
      free: LinkedList::default(),
    }
  }
}

impl<T> IntrusiveQueueWithFreeList<T> {
  // Given a container entry pointer, allows for mutating the entry.
  #[allow(clippy::unused_self, clippy::needless_pass_by_ref_mut)]
  pub fn map_mut<ResultType, F: FnOnce(&mut T) -> ResultType>(
    &mut self,
    item: *mut Container<T>,
    f: F,
  ) -> Option<ResultType> {
    let item = unsafe {
      // Safety: We hold exclusive (&mut self) access. Thus, we can dereference safely assuming no
      // dangling entry pointers that have been popped.
      item.as_mut()?
    };
    Some(f(item.value.get_mut()))
  }

  // Return whether the queue is empty.
  pub fn is_empty(&self) -> bool {
    self.queue.is_empty()
  }

  // Get the first item in the queue, if any.
  pub fn front(&self) -> Option<&T> {
    self.queue.front().get().map(|c| unsafe {
      // Safety: See comment above for UnsafeCell.
      &*c.value.get()
    })
  }

  // Pop the first item from the queue, if any, and put it on the free list.
  pub fn pop_front(&mut self) {
    if let Some(front) = self.queue.pop_front() {
      self.free.push_front(front);
    }
  }

  // Emplace a new item on the back of the queue, using memory from the free list if available.
  pub fn emplace_back(&mut self, value: T) -> *mut Container<T> {
    let mut emplaced = if let Some(mut popped) = self.free.pop_front() {
      *popped.value.get_mut() = value;
      popped
    } else {
      Box::new(Container {
        link: LinkedListLink::new(),
        value: UnsafeCell::new(value),
      })
    };
    let address = std::ptr::addr_of_mut!(*emplaced);
    self.queue.push_back(emplaced);
    address
  }
}
