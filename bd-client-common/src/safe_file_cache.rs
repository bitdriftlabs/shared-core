// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./safe_file_cache_test.rs"]
mod safe_file_cache_test;

use crate::file::{read_checksummed_data, read_compressed_protobuf, write_checksummed_data};
use anyhow::bail;
use parking_lot::Mutex;
use protobuf::Message;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

const MAX_RETRY_COUNT: u8 = 5;

pub struct SafeFileCache<T> {
  directory: PathBuf,
  locked_state: Mutex<LockedState>,
  name: &'static str,
  phantom: PhantomData<T>,
}
#[derive(Default)]
struct LockedState {
  cached_config_validated: bool,
  cached_nonce: Vec<u8>,
  current_retry_count: u8,
}

impl<T: Message> SafeFileCache<T> {
  #[must_use]
  pub fn new(name: &'static str, sdk_directory: &Path) -> Self {
    // Create the directory if it doesn't exist.
    let directory = sdk_directory.join(name);
    log::debug!(
      "creating file cache directory for {name:?} at {}",
      directory.display()
    );
    let _ignored = std::fs::create_dir(&directory);

    Self {
      name,
      directory,
      locked_state: Mutex::default(),
      phantom: PhantomData,
    }
  }

  /// Called to mark the cached config as "safe", meaning that we feel comfortable about letting
  /// the app continue to read this from disk.
  pub async fn mark_safe(&self) {
    // We load the config from cache only at startup, so we only need to update the file once.
    if !{
      let mut state = self.locked_state.lock();
      std::mem::replace(&mut state.cached_config_validated, true)
    } {
      // If this fails worst case we'll use a stale retry count and eventually disable caching.
      let _ignored = self.persist_cache_load_retry_count(0).await;
      log::debug!("marked cached config for {} as safe", self.name);
    }
  }

  fn retry_count_file(&self) -> PathBuf {
    self.directory.join("retry_count")
  }

  fn protobuf_file(&self) -> PathBuf {
    self.directory.join("protobuf.pb")
  }

  fn last_nonce_file(&self) -> PathBuf {
    self.directory.join("last_nonce")
  }

  async fn persist_cache_load_retry_count(&self, retry_count: u8) -> anyhow::Result<()> {
    // This could fail, but by being defensive when we read this we should ideally worst case just
    // fall back to not reading from cache.
    tokio::fs::write(&self.retry_count_file(), &[retry_count]).await?;
    log::debug!("wrote retry count {retry_count} for {}", self.name);
    Ok(())
  }

  pub async fn reset(&self) {
    // Recreate the directory instead of deleting individual files, making sure we really clean
    // up any bad state.
    let _ignored = tokio::fs::remove_dir_all(&self.directory).await;
    let _ignored = tokio::fs::create_dir(&self.directory).await;
  }

  pub async fn handle_cached_config(&self) -> Option<T> {
    // Attempt to load the cached config from disk. Should we run into any unexpected issues,
    // eagerly wipe out all the disk state by recreating the directory. This should help
    // us clean up any dirty state we see on disk and avoid issues persisting between process
    // restarts. As the errors bubble up through the error handler we may find we can handle some
    // of these failures. We also wipe the cache on a few expected cases, like hitting the retry
    // limit or a partial cache state.
    let (reset, cached) = match self.try_load_cached_config().await {
      Ok(result) => result,
      Err(e) => {
        log::debug!("failed to load cached config {:?}: {e}", self.name);
        (true, None)
      },
    };

    if reset {
      self.reset().await;
    }

    cached
  }

  // Attempts to apply cached configuration. Returns true if the underlying cache state should be
  // reset.
  async fn try_load_cached_config(&self) -> anyhow::Result<(bool, Option<T>)> {
    log::debug!("attempting to load cached config for {:?}", self.name);

    // We expect at most two files in this directory: a protobuf.pb which contains the cached
    // protobuf and a retry_count file which contains the number of times this cached
    // file has been attempted applied during startup. The idea behind the retry count is
    // allow a client that received bad configuration to eventually recover, avoiding an infinite
    // crash loop.

    // If either of the files don't exist, we're not going to try to load the config and we'll wipe
    // out the other file if it's there. This could handle naturally if the system shuts down in the
    // middle of caching config.
    if !tokio::fs::try_exists(self.retry_count_file())
      .await
      .is_ok_and(|e| e)
      || !tokio::fs::try_exists(self.protobuf_file())
        .await
        .is_ok_and(|e| e)
      || !tokio::fs::try_exists(self.last_nonce_file())
        .await
        .is_ok_and(|e| e)
    {
      log::debug!(
        "cached retry count, config, or last nonce not found for {:?}, resetting cache",
        self.name
      );
      return Ok((true, None));
    }

    // If the retry count file contains invalid data we defensively bail on reading the cached
    // value. If we were to treat an empty file as count=0 we could theoretically find ourselves in
    // a loop where the file is not properly updated.
    let Ok(retry_count) =
      Self::parse_retry_count(&tokio::fs::read(&self.retry_count_file()).await?)
    else {
      return Ok((true, None));
    };
    log::debug!("loaded retry count {retry_count} for {}", self.name);

    // Same for the cached nonce file.
    let Ok(nonce) =
      async { read_checksummed_data(&tokio::fs::read(&self.last_nonce_file()).await?) }.await
    else {
      return Ok((true, None));
    };
    log::debug!(
      "loaded nonce {} for {}",
      std::str::from_utf8(&nonce).unwrap_or_default(),
      self.name
    );

    {
      let mut locked = self.locked_state.lock();
      locked.current_retry_count = retry_count;
      locked.cached_nonce = nonce;
    }

    // TODO(snowp): Should we read this from runtime as well? It would make it possible for a bad
    // runtime config to accidentally set this really high, but right now this is not
    // configurable at all.
    if retry_count >= MAX_RETRY_COUNT {
      // Note that eventually if the client is killed this is going to kick in since we attempt to
      // load cached runtime very early on. Since we don't clean state so that we can do changed
      // nonce detection in the cache_update() function, this could cause killed clients to not get
      // new config when they come back online since there is no crash loop and the nonce hasn't
      // changed. We handle this directly in the API code by having the state reset when it enters
      // killed mode.
      log::debug!(
        "cached config for {} has retry count {retry_count} >= {MAX_RETRY_COUNT}, refusing to read",
        self.name
      );
      return Ok((false, None));
    }

    // Update the retry count before we apply the config. If this fails, we bail out (which will
    // attempt to clear the cache directory) and disable caching. We do this because being unable
    // to update the retry count may result in us getting stuck processing what we think is retry
    // 0 over and over again.
    self.persist_cache_load_retry_count(retry_count + 1).await?;

    let bytes = tokio::fs::read(&self.protobuf_file()).await?;
    let protobuf: T = read_compressed_protobuf(&bytes)?;

    Ok((false, Some(protobuf)))
  }

  fn parse_retry_count(data: &[u8]) -> anyhow::Result<u8> {
    // Currently we do not bother with trying to prevent single byte corruption for this file using
    // a CRC, etc.
    if data.len() != 1 || data[0] > MAX_RETRY_COUNT {
      bail!("invalid retry count file");
    }

    Ok(data[0])
  }

  pub async fn cache_update(
    &self,
    compressed_protobuf: Vec<u8>,
    version_nonce: &str,
    apply_fn: impl Future<Output = anyhow::Result<()>>,
  ) -> anyhow::Result<()> {
    let res = {
      let state = self.locked_state.lock();
      state.current_retry_count >= MAX_RETRY_COUNT && state.cached_nonce == version_nonce.as_bytes()
    };
    if res {
      log::debug!(
        "refusing to cache config for {} at nonce {version_nonce} since retry count is already at \
         max and nonce has not changed",
        self.name
      );
      bail!("refusing to cache config during suspected crash loop with no nonce change");
    }

    apply_fn.await?;

    if let Err(e) = async {
      tokio::fs::write(
        &self.last_nonce_file(),
        write_checksummed_data(version_nonce.as_bytes()),
      )
      .await?;
      Ok::<_, anyhow::Error>(tokio::fs::write(&self.protobuf_file(), compressed_protobuf).await?)
    }
    .await
    {
      log::debug!("failed to write cached config for {}: {e}", self.name,);
    }

    // Failing here is fine, worst case we'll use an old retry count or leave it missing, which
    // will eventually disable caching.
    self.locked_state.lock().cached_config_validated = true;
    if let Err(e) = self.persist_cache_load_retry_count(0).await {
      log::debug!("failed to write retry count for {}: {e}", self.name);
    }

    log::debug!(
      "cached config for {} successfully at nonce {version_nonce}",
      self.name
    );
    Ok(())
  }
}
