// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./store_test.rs"]
mod store_test;
use base64::Engine;
use bd_log::warn_every;
use time::ext::NumericalDuration;

#[cfg(test)]
#[ctor::ctor]
fn test_global_init() {
  bd_test_helpers::test_global_init();
}

//
// Storable
//

/// The interface of a type that can be stored in a `Store`.
pub trait Storable: serde::Serialize + serde::de::DeserializeOwned {}

impl Storable for String {}

//
// Storage
//

pub trait Storage {
  // TODO(Augustyniak): Make async variants of these operations.
  fn set_string(&self, key: &str, value: &str) -> anyhow::Result<()>;
  fn get_string(&self, key: &str) -> anyhow::Result<Option<String>>;
  fn delete(&self, key: &str) -> anyhow::Result<()>;
}

//
// Store
//

pub struct Store {
  storage: Box<dyn Storage + Sync + Send>,
}

impl Store {
  #[must_use]
  pub fn new(storage: Box<dyn Storage + Sync + Send>) -> Self {
    Self { storage }
  }
}

impl Store {
  pub fn set<T: Storable>(&self, key: &Key<T>, value: &T) {
    if let Err(e) = self.set_internal(key, value) {
      warn_every!(
        15.seconds(),
        "failed to set value for {:?} key: {:?}",
        key.key,
        e
      );
    }
  }

  #[must_use]
  pub fn get<T: Storable>(&self, key: &Key<T>) -> Option<T> {
    self
      .get_internal(key)
      .map_err(|e| {
        warn_every!(
          15.seconds(),
          "failed to get value for {:?} key: {:?}",
          key.key,
          e
        );

        if let Err(e) = self.storage.delete(key.key()) {
          warn_every!(
            15.seconds(),
            "failed to delete value for {:?} key: {:?}",
            key.key,
            e
          );
        }
      })
      .ok()
      .flatten()
  }

  pub fn set_internal<T: Storable>(&self, key: &Key<T>, value: &T) -> anyhow::Result<()> {
    let mut bytes = vec![];
    if let Err(e) = bincode::serialize_into(&mut bytes, value) {
      anyhow::bail!("failed to serialize value: {:?}", e);
    }

    let base64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
    if let Err(e) = self.storage.set_string(key.key, &base64) {
      anyhow::bail!("failed to set string: {:?}", e);
    }

    Ok(())
  }

  pub fn get_internal<T: Storable>(&self, key: &Key<T>) -> anyhow::Result<Option<T>> {
    match self.storage.get_string(key.key) {
      Ok(base64) => {
        let Some(base64) = base64 else {
          return Ok(None);
        };

        let result = base64::engine::general_purpose::STANDARD.decode(base64);
        Ok(result.map_or_else(
          |e| anyhow::bail!("failed to decode base64 value: {:?}", e),
          |bytes| match bincode::deserialize_from::<_, T>(&bytes[..]) {
            Ok(model) => Ok(Some(model)),
            Err(e) => anyhow::bail!("failed to deserialize model: {:?}", e),
          },
        )?)
      },
      Err(e) => anyhow::bail!("failed to get string: {:?}", e),
    }
  }
}

//
// Key
//

pub struct Key<T: Storable> {
  key: &'static str,
  _phantom: std::marker::PhantomData<T>,
}

impl<T> Key<T>
where
  T: Storable,
{
  #[must_use]
  pub const fn new(key: &'static str) -> Self {
    Self {
      key,
      _phantom: std::marker::PhantomData,
    }
  }

  #[must_use]
  pub const fn key(&self) -> &str {
    self.key
  }
}
