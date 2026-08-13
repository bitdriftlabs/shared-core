// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! A synchronous mutex for state that can be accessed by caller-owned threads.
//!
//! SDK-owned Tokio tasks should use `parking_lot` directly. Use [`PlatformMutex`] only when a
//! caller-owned thread can contend with SDK work. On iOS it uses `os_unfair_lock`, which supports
//! priority inheritance and avoids priority inversion when contending threads have different `QoS`.
//!
//! Its guard is intentionally `!Send`. This keeps an iOS unfair lock on its acquiring thread and,
//! in the SDK's `Send` async APIs, makes holding the guard across an await a compile error.

#[cfg(test)]
#[path = "./platform_mutex_test.rs"]
mod tests;

use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::rc::Rc;

#[cfg(not(target_os = "ios"))]
mod implementation {
  use std::ops::{Deref, DerefMut};

  // TODO(snowp): If ever in the future we decide that we need a mutex that supports priority
  // inheritance on non-iOS platforms we can consider using a feature to switch this to using
  // std::sync::Mutex. For now we're defaulting to parking_lot::Mutex as it should be faster in
  // the common case.
  pub(super) struct Mutex<T>(parking_lot::Mutex<T>);

  impl<T> Mutex<T> {
    pub(super) const fn new(value: T) -> Self {
      Self(parking_lot::Mutex::new(value))
    }

    pub(super) fn lock(&self) -> MutexGuard<'_, T> {
      MutexGuard(self.0.lock())
    }
  }

  pub(super) struct MutexGuard<'a, T>(parking_lot::MutexGuard<'a, T>);

  impl<T> Deref for MutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
      &self.0
    }
  }

  impl<T> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
      &mut self.0
    }
  }
}

#[cfg(target_os = "ios")]
mod implementation {
  use std::cell::UnsafeCell;
  use std::ops::{Deref, DerefMut};

  // `os_unfair_lock_s` is intentionally opaque. Apple defines its initialized representation as
  // all zeroes, which lets it live directly alongside the protected value.
  #[repr(C)]
  struct OsUnfairLock {
    _os_unfair_lock_opaque: u32,
  }

  unsafe extern "C" {
    fn os_unfair_lock_lock(lock: *mut OsUnfairLock);
    fn os_unfair_lock_unlock(lock: *mut OsUnfairLock);
  }

  pub(super) struct Mutex<T> {
    lock: UnsafeCell<OsUnfairLock>,
    value: UnsafeCell<T>,
  }

  // The lock serializes access to `value`; moving the mutex itself is safe when its value can be
  // moved between threads.
  unsafe impl<T: Send> Send for Mutex<T> {}
  unsafe impl<T: Send> Sync for Mutex<T> {}

  impl<T> Mutex<T> {
    pub(super) const fn new(value: T) -> Self {
      Self {
        lock: UnsafeCell::new(OsUnfairLock {
          _os_unfair_lock_opaque: 0,
        }),
        value: UnsafeCell::new(value),
      }
    }

    pub(super) fn lock(&self) -> MutexGuard<'_, T> {
      // Apple requires the same thread that acquires an unfair lock to release it. The public
      // guard is non-Send, so it cannot be moved to a different thread before `Drop` unlocks it.
      unsafe {
        os_unfair_lock_lock(self.lock.get());
      }
      MutexGuard { mutex: self }
    }
  }

  pub(super) struct MutexGuard<'a, T> {
    mutex: &'a Mutex<T>,
  }

  impl<T> Deref for MutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
      // The guard proves this thread holds the lock for the duration of the reference.
      unsafe { &*self.mutex.value.get() }
    }
  }

  impl<T> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
      // The guard provides exclusive access until it is dropped.
      unsafe { &mut *self.mutex.value.get() }
    }
  }

  impl<T> Drop for MutexGuard<'_, T> {
    fn drop(&mut self) {
      unsafe {
        os_unfair_lock_unlock(self.mutex.lock.get());
      }
    }
  }
}

//
// PlatformMutex
//

/// A mutex for state that can be contended by caller-owned threads.
///
/// SDK-owned Tokio tasks should use `parking_lot` directly. On iOS this uses `os_unfair_lock` so
/// a higher-QoS caller thread does not experience priority inversion while waiting on SDK work.
/// The returned guard is `!Send`, enforcing same-thread unlock and preventing it from crossing an
/// await in the SDK's `Send` async APIs.
pub struct PlatformMutex<T> {
  inner: implementation::Mutex<T>,
}

impl<T> PlatformMutex<T> {
  #[must_use]
  pub const fn new(value: T) -> Self {
    Self {
      inner: implementation::Mutex::new(value),
    }
  }

  /// Acquires the mutex on the calling thread.
  pub fn lock(&self) -> PlatformMutexGuard<'_, T> {
    PlatformMutexGuard {
      inner: self.inner.lock(),
      // Keep guards on the acquiring thread so an iOS unfair lock is always released by its
      // owner.
      _not_send: PhantomData,
    }
  }
}

//
// PlatformMutexGuard
//

/// An RAII guard returned by [`PlatformMutex::lock`].
///
/// This guard is intentionally non-`Send`. On iOS, that ensures the acquiring thread unlocks the
/// unfair lock. It also cannot be retained across an await in the SDK's `Send` async APIs.
pub struct PlatformMutexGuard<'a, T> {
  inner: implementation::MutexGuard<'a, T>,
  _not_send: PhantomData<Rc<()>>,
}

impl<T> Deref for PlatformMutexGuard<'_, T> {
  type Target = T;

  fn deref(&self) -> &Self::Target {
    &self.inner
  }
}

impl<T> DerefMut for PlatformMutexGuard<'_, T> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.inner
  }
}
