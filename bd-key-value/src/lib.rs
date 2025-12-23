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
// Storage
//

pub trait Storage: Send + Sync {
  // TODO(Augustyniak): Make async variants of these operations.
  fn set_string(&self, key: &str, value: &str) -> anyhow::Result<()>;
  fn get_string(&self, key: &str) -> anyhow::Result<Option<String>>;
  fn delete(&self, key: &str) -> anyhow::Result<()>;
}

//
// Store
//

pub struct Store {
  storage: Box<dyn Storage + Send + Sync>,
}

impl Store {
  #[must_use]
  pub fn new(storage: Box<dyn Storage + Send + Sync>) -> Self {
    Self { storage }
  }
}

impl Store {
  pub fn set_string(&self, key: &Key<String>, value: &str) {
    if let Err(e) = self.set_internal_string(key, value) {
      warn_every!(
        15.seconds(),
        "failed to set value for {:?} key: {:?}",
        key.key,
        e
      );
    }
  }

  pub fn set<T: protobuf::Message>(&self, key: &Key<T>, value: &T) {
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
  pub fn get_string(&self, key: &Key<String>) -> Option<String> {
    self
      .get_internal_string(key)
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

  #[must_use]
  pub fn get<T: protobuf::Message>(&self, key: &Key<T>) -> Option<T> {
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

  pub fn set_internal<T: protobuf::Message>(&self, key: &Key<T>, value: &T) -> anyhow::Result<()> {
    let bytes = match value.write_to_bytes() {
      Ok(b) => b,
      Err(e) => anyhow::bail!("failed to serialize value: {e:?}"),
    };

    let base64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
    if let Err(e) = self.storage.set_string(key.key, &base64) {
      anyhow::bail!("failed to set string: {e:?}");
    }

    Ok(())
  }

  pub fn set_internal_string(&self, key: &Key<String>, value: &str) -> anyhow::Result<()> {
    // In order to maintain compatibility with the previous bincode-based encoding, for strings
    // we will store them with an 8 byte length prefix, then base64 encode the whole thing.
    let mut encoded = (value.len() as u64).to_le_bytes().to_vec();
    encoded.extend_from_slice(value.as_bytes());

    self.storage.set_string(
      key.key,
      &base64::engine::general_purpose::STANDARD.encode(&encoded),
    )?;
    Ok(())
  }

  pub fn get_internal_string(&self, key: &Key<String>) -> anyhow::Result<Option<String>> {
    match self.storage.get_string(key.key) {
      Ok(base64) => {
        let Some(base64) = base64 else {
          return Ok(None);
        };

        let result = base64::engine::general_purpose::STANDARD.decode(base64);
        Ok(result.map_or_else(
          |e| anyhow::bail!("failed to decode base64 value: {e:?}"),
          |bytes| {
            let (len_bytes, rest) = match bytes.split_first_chunk::<8>() {
              Some(v) => v,
              None => anyhow::bail!("stored string is too short to contain length prefix"),
            };

            let len = usize::from_le_bytes(*len_bytes);

            if rest.len() != len {
              anyhow::bail!("stored string length prefix does not match actual length");
            }

            match str::from_utf8(rest) {
              Ok(s) => Ok(Some(s.to_string())),
              Err(e) => anyhow::bail!("failed to decode UTF-8 string: {e:?}"),
            }
          },
        )?)
      },
      Err(e) => anyhow::bail!("failed to get string: {e:?}"),
    }
  }

  pub fn get_internal<T: protobuf::Message>(&self, key: &Key<T>) -> anyhow::Result<Option<T>> {
    match self.storage.get_string(key.key) {
      Ok(base64) => {
        let Some(base64) = base64 else {
          return Ok(None);
        };

        let result = base64::engine::general_purpose::STANDARD.decode(base64);
        Ok(result.map_or_else(
          |e| anyhow::bail!("failed to decode base64 value: {e:?}"),
          |bytes| match T::parse_from_bytes(&bytes) {
            Ok(value) => Ok(Some(value)),
            Err(e) => anyhow::bail!("failed to deserialize protobuf: {e:?}"),
          },
        )?)
      },
      Err(e) => anyhow::bail!("failed to get string: {e:?}"),
    }
  }
}

//
// Key
//

pub struct Key<T> {
  key: &'static str,
  _phantom: std::marker::PhantomData<T>,
}

impl<T> Key<T> {
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
