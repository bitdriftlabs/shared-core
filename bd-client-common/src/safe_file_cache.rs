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
use bd_time::{SystemTimeProvider, TimeProvider};
use parking_lot::Mutex;
use protobuf::Message;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const MAX_RETRY_COUNT: u8 = 5;
const CRASH_LOOP_BYPASS_TIMEOUT_SECONDS: i64 = 4 * 60 * 60;

pub struct SafeFileCache<T> {
  directory: PathBuf,
  locked_state: Mutex<LockedState>,
  name: &'static str,
  time_provider: Arc<dyn TimeProvider>,
  phantom: PhantomData<T>,
}
#[derive(Default)]
struct LockedState {
  cached_config_validated: bool,
  cached_nonce: Vec<u8>,
  current_retry_count: u8,
  last_successful_cache_at: Option<i64>,
}

impl<T: Message> SafeFileCache<T> {
  #[must_use]
  pub fn new(name: &'static str, sdk_directory: &Path) -> Self {
    Self::new_with_time_provider(name, sdk_directory, Arc::new(SystemTimeProvider))
  }

  #[must_use]
  pub fn new_with_time_provider(
    name: &'static str,
    sdk_directory: &Path,
    time_provider: Arc<dyn TimeProvider>,
  ) -> Self {
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
      time_provider,
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

  fn last_successful_cache_at_file(&self) -> PathBuf {
    self.directory.join("last_successful_cache_at")
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

    // Legacy caches may not have this file; treat that as sufficiently old and allow bypass.
    let last_successful_cache_at = if tokio::fs::try_exists(self.last_successful_cache_at_file())
      .await
      .is_ok_and(|e| e)
    {
      let Ok(last_successful_cache_at) = Self::parse_last_successful_cache_at(
        &read_checksummed_data(&tokio::fs::read(&self.last_successful_cache_at_file()).await?)?,
      ) else {
        return Ok((true, None));
      };

      log::debug!(
        "loaded last successful cache time {last_successful_cache_at} for {}",
        self.name
      );
      Some(last_successful_cache_at)
    } else {
      None
    };

    {
      let mut locked = self.locked_state.lock();
      locked.current_retry_count = retry_count;
      locked.cached_nonce = nonce;
      locked.last_successful_cache_at = last_successful_cache_at;
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

  fn parse_last_successful_cache_at(data: &[u8]) -> anyhow::Result<i64> {
    if data.len() != std::mem::size_of::<i64>() {
      bail!("invalid cache timestamp file");
    }

    Ok(i64::from_le_bytes(data.try_into()?))
  }

  fn now_unix_seconds(&self) -> i64 {
    self.time_provider.now().unix_timestamp()
  }

  fn is_bypass_elapsed(last_successful_cache_at: Option<i64>, now_unix_seconds: i64) -> bool {
    let Some(last_successful_cache_at) = last_successful_cache_at else {
      return true;
    };

    now_unix_seconds.saturating_sub(last_successful_cache_at) >= CRASH_LOOP_BYPASS_TIMEOUT_SECONDS
  }

  pub async fn cache_update(
    &self,
    compressed_protobuf: Vec<u8>,
    version_nonce: &str,
    apply_fn: impl Future<Output = anyhow::Result<()>>,
  ) -> anyhow::Result<()> {
    let now_unix_seconds = self.now_unix_seconds();
    let (refuse_update, bypassed_due_to_elapsed) = {
      let state = self.locked_state.lock();
      let in_suspected_crash_loop = state.current_retry_count >= MAX_RETRY_COUNT;
      let same_nonce = state.cached_nonce == version_nonce.as_bytes();
      let bypassed_due_to_elapsed =
        Self::is_bypass_elapsed(state.last_successful_cache_at, now_unix_seconds);

      (
        in_suspected_crash_loop && same_nonce && !bypassed_due_to_elapsed,
        in_suspected_crash_loop && same_nonce && bypassed_due_to_elapsed,
      )
    };
    if refuse_update {
      log::debug!(
        "refusing to cache config for {} at nonce {version_nonce} since retry count is already at \
         max and nonce has not changed",
        self.name
      );
      bail!("refusing to cache config during suspected crash loop with no nonce change");
    }

    if bypassed_due_to_elapsed {
      log::debug!(
        "allowing cache update for {} at nonce {version_nonce} despite suspected crash loop due \
         to elapsed timeout",
        self.name
      );
    }

    apply_fn.await?;

    if let Err(e) = async {
      tokio::fs::write(
        &self.last_nonce_file(),
        write_checksummed_data(version_nonce.as_bytes()),
      )
      .await?;
      tokio::fs::write(&self.protobuf_file(), compressed_protobuf).await?;
      tokio::fs::write(
        &self.last_successful_cache_at_file(),
        write_checksummed_data(&now_unix_seconds.to_le_bytes()),
      )
      .await?;
      Ok::<_, anyhow::Error>(())
    }
    .await
    {
      log::debug!("failed to write cached config for {}: {e}", self.name,);
    }

    // Failing here is fine, worst case we'll use an old retry count or leave it missing, which
    // will eventually disable caching.
    {
      let mut state = self.locked_state.lock();
      state.cached_config_validated = true;
      state.cached_nonce = version_nonce.as_bytes().to_vec();
      state.current_retry_count = 0;
      state.last_successful_cache_at = Some(now_unix_seconds);
    }
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
