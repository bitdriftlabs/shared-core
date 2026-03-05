// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::file::write_compressed_protobuf;
use crate::safe_file_cache::{CRASH_LOOP_BYPASS_TIMEOUT_SECONDS, MAX_RETRY_COUNT, SafeFileCache};
use bd_proto::protos::client::api::ClientKillFile;
use bd_time::TestTimeProvider;
use protobuf::Message;
use std::sync::Arc;
use tempfile::tempdir;
use time::{Duration, OffsetDateTime};

#[tokio::test]
async fn basic_flow() {
  let tempdir = tempdir().unwrap();
  let cache = SafeFileCache::<ClientKillFile>::new("test", tempdir.path());
  assert!(cache.handle_cached_config().await.is_none());
  cache
    .cache_update(
      write_compressed_protobuf(ClientKillFile::default_instance()).unwrap(),
      "123",
      async { Ok(()) },
    )
    .await
    .unwrap();

  // Reload and make sure we get the cached config.
  let cache = SafeFileCache::<ClientKillFile>::new("test", tempdir.path());
  assert_eq!(
    ClientKillFile::default(),
    cache.handle_cached_config().await.unwrap()
  );
  cache.mark_safe().await;

  // Simulate a crash loop by coming up several times without marking safe.
  for _i in 0 .. (MAX_RETRY_COUNT) {
    let cache = SafeFileCache::<ClientKillFile>::new("test", tempdir.path());
    assert_eq!(
      ClientKillFile::default(),
      cache.handle_cached_config().await.unwrap()
    );
  }

  // The next time we come up we should refuse to load the cached config.
  let cache = SafeFileCache::<ClientKillFile>::new("test", tempdir.path());
  assert!(cache.handle_cached_config().await.is_none());

  // We should delete any state so make sure we come up again but get no config.
  let cache = SafeFileCache::<ClientKillFile>::new("test", tempdir.path());
  assert!(cache.handle_cached_config().await.is_none());

  // Now that we are up, deliver the same config again with the same nonce, it should not be
  // applied since it's the same version.
  assert_eq!(
    cache
      .cache_update(
        write_compressed_protobuf(ClientKillFile::default_instance()).unwrap(),
        "123",
        async { panic!() },
      )
      .await
      .unwrap_err()
      .to_string(),
    "refusing to cache config during suspected crash loop with no nonce change"
  );

  // Delivering a new nonce should work.
  cache
    .cache_update(
      write_compressed_protobuf(ClientKillFile::default_instance()).unwrap(),
      "456",
      async { Ok(()) },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn same_nonce_allowed_after_bypass_timeout() {
  let tempdir = tempdir().unwrap();
  let time_provider = Arc::new(TestTimeProvider::new(OffsetDateTime::UNIX_EPOCH));
  let cache = SafeFileCache::<ClientKillFile>::new_with_time_provider(
    "test",
    tempdir.path(),
    time_provider.clone(),
  );
  cache
    .cache_update(
      write_compressed_protobuf(ClientKillFile::default_instance()).unwrap(),
      "123",
      async { Ok(()) },
    )
    .await
    .unwrap();

  // We should read cached config successfully until we hit the retry limit.
  for _i in 0 .. MAX_RETRY_COUNT {
    let cache = SafeFileCache::<ClientKillFile>::new_with_time_provider(
      "test",
      tempdir.path(),
      time_provider.clone(),
    );
    assert_eq!(
      ClientKillFile::default(),
      cache.handle_cached_config().await.unwrap()
    );
  }

  let cache = SafeFileCache::<ClientKillFile>::new_with_time_provider(
    "test",
    tempdir.path(),
    time_provider.clone(),
  );
  assert!(cache.handle_cached_config().await.is_none());

  // Simulate enough wall-clock time passing since the last successful cache write.
  time_provider.advance(Duration::seconds(CRASH_LOOP_BYPASS_TIMEOUT_SECONDS + 1));

  let cache = SafeFileCache::<ClientKillFile>::new_with_time_provider(
    "test",
    tempdir.path(),
    time_provider.clone(),
  );
  assert!(cache.handle_cached_config().await.is_none());

  cache
    .cache_update(
      write_compressed_protobuf(ClientKillFile::default_instance()).unwrap(),
      "123",
      async { Ok(()) },
    )
    .await
    .unwrap();

  // Persisted retry count should be reset to zero after a successful cache update.
  assert_eq!(
    vec![0],
    tokio::fs::read(cache.retry_count_file()).await.unwrap()
  );

  // After bypassing, the same nonce should be persisted and loadable again.
  let cache = SafeFileCache::<ClientKillFile>::new_with_time_provider(
    "test",
    tempdir.path(),
    time_provider.clone(),
  );
  assert_eq!(
    ClientKillFile::default(),
    cache.handle_cached_config().await.unwrap()
  );

  // A successful cache update should reset retry count, so same nonce updates should no longer be
  // blocked by crash-loop protection.
  cache
    .cache_update(
      write_compressed_protobuf(ClientKillFile::default_instance()).unwrap(),
      "123",
      async { Ok(()) },
    )
    .await
    .unwrap();

  // Drive retry count back to the maximum and verify the crash-loop guard blocks same nonce
  // updates again.
  for _i in 0 .. MAX_RETRY_COUNT {
    let cache = SafeFileCache::<ClientKillFile>::new_with_time_provider(
      "test",
      tempdir.path(),
      time_provider.clone(),
    );
    assert_eq!(
      ClientKillFile::default(),
      cache.handle_cached_config().await.unwrap()
    );
  }

  let cache =
    SafeFileCache::<ClientKillFile>::new_with_time_provider("test", tempdir.path(), time_provider);
  assert!(cache.handle_cached_config().await.is_none());
  assert_eq!(
    cache
      .cache_update(
        write_compressed_protobuf(ClientKillFile::default_instance()).unwrap(),
        "123",
        async { Ok(()) },
      )
      .await
      .unwrap_err()
      .to_string(),
    "refusing to cache config during suspected crash loop with no nonce change"
  );
}
