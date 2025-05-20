// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::file::{read_compressed_protobuf, write_compressed_protobuf};
use anyhow::bail;
use protobuf::Message;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

const MAX_RETRY_COUNT: u8 = 5;

pub struct SafeFileCache<T> {
  directory: PathBuf,
  cached_config_validated: AtomicBool,
  phantom: PhantomData<T>,
}

impl<T: Message> SafeFileCache<T> {
  #[must_use]
  pub fn new(sdk_directory: &Path, subdirectory: &str) -> Self {
    // Create the directory if it doesn't exist.
    let directory = sdk_directory.join(subdirectory);
    log::debug!("creating file cache directory at {}", directory.display());
    let _ignored = std::fs::create_dir(&directory);

    Self {
      directory,
      cached_config_validated: AtomicBool::new(false),
      phantom: PhantomData,
    }
  }

  /// Called to mark the cached config as "safe", meaning that we feel comfortable about letting
  /// the app continue to read this from disk.
  pub async fn mark_safe(&self) {
    // We load the config from cache only at startup, so we only need to update the file once.
    if !self.cached_config_validated.swap(true, Ordering::SeqCst) {
      // If this fails worst case we'll use a stale retry count and eventually disable caching.
      let _ignored = self.persist_cache_load_retry_count(0).await;
    }
  }

  fn retry_count_file(&self) -> PathBuf {
    self.directory.join("retry_count")
  }

  fn protobuf_file(&self) -> PathBuf {
    self.directory.join("protobuf.pb")
  }

  async fn persist_cache_load_retry_count(&self, retry_count: u8) -> anyhow::Result<()> {
    // This could fail, but by being defensive when we read this we should ideally worst case just
    // fall back to not reading from cache.
    Ok(tokio::fs::write(&self.retry_count_file(), &[retry_count]).await?)
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
        log::debug!("failed to load cached config: {e}");
        (true, None)
      },
    };

    if reset {
      // Recreate the directory instead of deleting individual files, making sure we really clean
      // up any bad state.
      let _ignored = tokio::fs::remove_dir_all(&self.directory).await;
      let _ignored = tokio::fs::create_dir(&self.directory).await;
    }

    cached
  }

  // Attempts to apply cached configuration. Returns true if the underlying cache state should be
  // reset.
  async fn try_load_cached_config(&self) -> anyhow::Result<(bool, Option<T>)> {
    log::debug!("attempting to load cached config");

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
    {
      log::debug!("cached retry count or config not found, resetting cache");
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

    log::debug!("loaded file at retry count {retry_count}");

    // Update the retry count before we apply the config. If this fails, we bail out (which will
    // attempt to clear the cache directory) and disable caching. We do this because being unable
    // to update the retry count may result in us getting stuck processing what we think is retry
    // 0 over and over again.
    self.persist_cache_load_retry_count(retry_count + 1).await?;

    // TODO(snowp): Should we read this from runtime as well? It would make it possible for a bad
    // runtime config to accidentally set this really high, but right now this is not
    // configurable at all.
    if retry_count > MAX_RETRY_COUNT {
      // Note that eventually if the client is killed this is going to kick in and delete cached
      // config. This is OK since we are still covered by the kill file, and ultimately we would
      // like the client to come up and get fresh config anyway.
      return Ok((true, None));
    }

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

  pub async fn cache_update(&self, protobuf: &T) {
    if let Err(e) =
      tokio::fs::write(&self.protobuf_file(), write_compressed_protobuf(protobuf)).await
    {
      log::debug!("failed to write cached config: {e}");
    }

    // Failing here is fine, worst case we'll use an old retry count or leave it missing, which
    // will eventually disable caching.
    self.cached_config_validated.store(true, Ordering::SeqCst);
    if let Err(e) = self.persist_cache_load_retry_count(0).await {
      log::debug!("failed to write retry count: {e}");
    }
  }
}
