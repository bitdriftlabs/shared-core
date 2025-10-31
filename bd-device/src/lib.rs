// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![deny(
  clippy::expect_used,
  clippy::panic,
  clippy::todo,
  clippy::unimplemented,
  clippy::unreachable,
  clippy::unwrap_used
)]

#[cfg(test)]
#[path = "./device_test.rs"]
mod device_test;

use bd_key_value::Key;
pub use bd_key_value::Store;
use std::sync::Arc;
use uuid::Uuid;

/// The key used to store the device state.
pub(crate) static DEVICE_ID_KEY: Key<String> = Key::new("device.state");

//
// Device
//

pub struct Device {
  store: Arc<Store>,
  id: parking_lot::Mutex<Option<String>>,
}

impl Device {
  #[must_use]
  pub const fn new(store: Arc<Store>) -> Self {
    Self {
      store,
      id: parking_lot::Mutex::new(None),
    }
  }

  pub fn id(&self) -> String {
    let mut guard = self.id.lock();

    #[allow(clippy::option_if_let_else)]
    if let Some(id) = guard.as_ref() {
      id.clone()
    } else {
      let id = self.store.get(&DEVICE_ID_KEY).map_or_else(
        || {
          let id = Uuid::new_v4().to_string();
          self.store.set(&DEVICE_ID_KEY, &id);
          id
        },
        |id| id,
      );

      log::info!("bitdrift Capture device ID: {id:?}");

      *guard = Some(id.clone());
      id
    }
  }
}
