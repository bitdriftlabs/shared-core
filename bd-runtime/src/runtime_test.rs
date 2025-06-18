// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::runtime::{ConfigLoader, FeatureFlag};
use crate::{bool_feature_flag, duration_feature_flag, int_feature_flag};
use bd_client_common::{ConfigurationUpdate, HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE};
use bd_test_helpers::runtime::{ValueKind, make_update};
use std::borrow::Borrow;
use std::sync::Arc;

#[tokio::test]
async fn feature_flag_registration() {
  int_feature_flag!(TestFlag, "test.path", 1);
  bool_feature_flag!(BoolFlag, "test.bool", false);

  let sdk_directory = tempfile::TempDir::with_prefix("sdk").unwrap();
  let loader = ConfigLoader::new(sdk_directory.path());

  let mut int_feature_flag = TestFlag::register(&loader).unwrap();
  let mut bool_feature_flag = BoolFlag::register(&loader).unwrap();

  // Initially the value is the specified default.
  assert_eq!(*int_feature_flag.read_mark_update(), 1);
  assert!(!*bool_feature_flag.read_mark_update());

  // After updating the value it now reflects the updated value.
  loader
    .update_snapshot(&make_update(
      vec![
        (TestFlag::path(), ValueKind::Int(10)),
        (BoolFlag::path(), ValueKind::Bool(true)),
      ],
      "1".to_string(),
    ))
    .await;
  assert_eq!(*int_feature_flag.read_mark_update(), 10);
  assert!(*bool_feature_flag.read_mark_update());

  // When we clear out the runtime, it reverts to the default.
  loader
    .update_snapshot(&make_update(vec![], String::new()))
    .await;
  assert_eq!(*int_feature_flag.read_mark_update(), 1);
  assert!(!*bool_feature_flag.read_mark_update());

  // If the value doesn't change, no events are pushed.
  loader
    .update_snapshot(&make_update(vec![], String::new()))
    .await;
  assert!(!int_feature_flag.watch.has_changed().unwrap());
  assert!(!bool_feature_flag.watch.has_changed().unwrap());
}

#[tokio::test]
async fn registration_after_update() {
  int_feature_flag!(TestFlag, "test.path", 1);

  let sdk_directory = tempfile::TempDir::with_prefix("sdk").unwrap();
  let loader = ConfigLoader::new(sdk_directory.path());

  loader
    .update_snapshot(&make_update(
      vec![(TestFlag::path(), ValueKind::Int(10))],
      "1".to_string(),
    ))
    .await;

  let feature_flag = TestFlag::register(&loader).unwrap();

  // The initial value of the watch should be 10.
  assert!(!feature_flag.watch.has_changed().unwrap());
  assert_eq!(*feature_flag.read(), 10);
}

#[test]
fn incompatible_registration() {
  int_feature_flag!(IntTestFlag, "test.path", 1);
  bool_feature_flag!(BoolTestFlag, "test.path", false);

  let sdk_directory = tempfile::TempDir::with_prefix("sdk").unwrap();
  let loader = ConfigLoader::new(sdk_directory.path());

  let _int_feature_flag = IntTestFlag::register(&loader).unwrap();
  let bool_feature_flag = BoolTestFlag::register(&loader);
  assert_eq!(
    bool_feature_flag.err().unwrap().to_string(),
    anyhow::anyhow!("Incompatible runtime subscription").to_string(),
  );
}

#[tokio::test]
async fn duration_flag() {
  duration_feature_flag!(DurationFlag, "test.duration_ms", time::Duration::seconds(5));

  let sdk_directory = tempfile::TempDir::with_prefix("sdk").unwrap();
  let loader = ConfigLoader::new(sdk_directory.path());

  let flag = DurationFlag::register(&loader).unwrap();

  assert_eq!(*flag.borrow().read(), time::Duration::seconds(5));

  loader
    .update_snapshot(&make_update(
      vec![(DurationFlag::path(), ValueKind::Int(100))],
      "1".to_string(),
    ))
    .await;

  assert_eq!(*flag.borrow().read(), time::Duration::milliseconds(100));
}

struct SetupDiskPersistence {
  directory: tempfile::TempDir,
}

impl SetupDiskPersistence {
  fn new() -> Self {
    let directory = tempfile::TempDir::with_prefix("runtime").unwrap();

    Self { directory }
  }

  fn new_loader(&self) -> Arc<ConfigLoader> {
    ConfigLoader::new(self.directory.path())
  }

  fn protobuf_file(&self) -> std::path::PathBuf {
    self.directory.path().join("runtime").join("protobuf.pb")
  }

  fn retry_file(&self) -> std::path::PathBuf {
    self.directory.path().join("runtime").join("retry_count")
  }
}

int_feature_flag!(TestFlag, "test.path", 1);

#[tokio::test]
async fn disk_persistence_happy_path() {
  let setup = SetupDiskPersistence::new();

  // Load a value into the snapshot then immediately tear down the loader.
  {
    let loader = setup.new_loader();
    loader
      .update_snapshot(&bd_test_helpers::runtime::make_update(
        vec![(TestFlag::path(), ValueKind::Int(10))],
        "1".to_string(),
      ))
      .await;
  }

  // At this point the value should be cached and we should see the previously set value on read.
  assert!(setup.protobuf_file().exists());
  assert!(setup.retry_file().exists());

  let loader = setup.new_loader();

  loader.handle_cached_config().await;

  let flag = TestFlag::register(&loader).unwrap();
  assert_eq!(*flag.read(), 10);
  assert_eq!(loader.snapshot().nonce, Some("1".to_string()));

  // With no safety marking, we should have a retry count of 1.
  assert_eq!(std::fs::read(setup.retry_file()).unwrap(), &[1]);

  // Getting a handshake without runtime being up to date should not do anything.
  loader.on_handshake_complete(0).await;
  assert_eq!(std::fs::read(setup.retry_file()).unwrap(), &[1]);

  // Getting a handshake with runtime being up to date should mark the config as safe.
  loader
    .on_handshake_complete(HANDSHAKE_FLAG_RUNTIME_UP_TO_DATE)
    .await;
  assert_eq!(std::fs::read(setup.retry_file()).unwrap(), &[0]);
}

#[tokio::test]
async fn disk_persistence_config_corruption() {
  let setup = SetupDiskPersistence::new();

  // First write some data to the cached file by setting a new snapshot.
  {
    setup
      .new_loader()
      .update_snapshot(&bd_test_helpers::runtime::make_update(
        vec![(TestFlag::path(), ValueKind::Int(10))],
        "1".to_string(),
      ))
      .await;
  }

  // Corrupt the file, verifying that we don't blow up when we read a bad file and fall back to
  // the default.
  std::fs::write(setup.protobuf_file(), [0; 10]).unwrap();

  let loader = setup.new_loader();
  loader.handle_cached_config().await;

  let flag = TestFlag::register(&loader).unwrap();
  assert_eq!(*flag.read(), 1);
  assert_eq!(loader.snapshot().nonce, None);
}

#[tokio::test]
async fn disk_persistence_retry_corruption() {
  let setup = SetupDiskPersistence::new();

  // First write some data to the cached file by setting a new snapshot.
  {
    setup
      .new_loader()
      .update_snapshot(&bd_test_helpers::runtime::make_update(
        vec![(TestFlag::path(), ValueKind::Int(10))],
        "1".to_string(),
      ))
      .await;
  }

  // Corrupt the retry file, verifying that we don't blow up when we read a bad file and fall back
  // to the default.
  std::fs::write(setup.retry_file(), []).unwrap();

  let loader = setup.new_loader();
  loader.handle_cached_config().await;

  let flag = TestFlag::register(&loader).unwrap();
  assert_eq!(*flag.read(), 1);
  assert_eq!(loader.snapshot().nonce, None);
}

#[tokio::test]
async fn disk_persistence_retry_limit() {
  let setup = SetupDiskPersistence::new();

  // First write some data to the cached file by setting a new snapshot.
  {
    setup
      .new_loader()
      .update_snapshot(&bd_test_helpers::runtime::make_update(
        vec![(TestFlag::path(), ValueKind::Int(10))],
        "1".to_string(),
      ))
      .await;
  }

  // Load the configuration 5 times without marking it as safe.
  for _ in 0 .. 6 {
    let loader = setup.new_loader();
    loader.handle_cached_config().await;
    let flag = TestFlag::register(&loader).unwrap();
    assert_eq!(*flag.read(), 10);
  }

  // On the 6th go we hit the limit and will treat it as an error, wiping all state.
  let loader = setup.new_loader();
  loader.handle_cached_config().await;
  let flag = TestFlag::register(&loader).unwrap();
  assert_eq!(*flag.read(), 1);
  assert!(!setup.protobuf_file().exists());
  assert!(!setup.retry_file().exists());
}

#[tokio::test]
async fn disk_persistence_retry_marked_safe() {
  let setup = SetupDiskPersistence::new();

  // First write some data to the cached file by setting a new snapshot.
  {
    setup
      .new_loader()
      .update_snapshot(&bd_test_helpers::runtime::make_update(
        vec![(TestFlag::path(), ValueKind::Int(10))],
        "1".to_string(),
      ))
      .await;
  }

  // Load the configuration 5 times marking it safe each time.
  for _ in 0 .. 5 {
    let loader = setup.new_loader();
    loader.handle_cached_config().await;
    let flag = TestFlag::register(&loader).unwrap();
    assert_eq!(*flag.read(), 10);

    loader.file_cache.mark_safe().await;
  }

  // On the 6th we would have hit the limit but we've been marking the uploads as safe.
  let loader = setup.new_loader();
  loader.handle_cached_config().await;
  let flag = TestFlag::register(&loader).unwrap();
  assert_eq!(*flag.read(), 10);
  assert_eq!(std::fs::read(setup.retry_file()).unwrap(), &[1]);
}

#[tokio::test]
async fn disk_persistence_missing_config_file() {
  let setup = SetupDiskPersistence::new();

  // First write some data to the cached file by setting a new snapshot.
  {
    setup
      .new_loader()
      .update_snapshot(&bd_test_helpers::runtime::make_update(
        vec![(TestFlag::path(), ValueKind::Int(10))],
        "1".to_string(),
      ))
      .await;
  }

  assert!(setup.protobuf_file().exists());
  assert!(setup.retry_file().exists());

  std::fs::remove_file(setup.protobuf_file()).unwrap();

  // If the config file is missing, we should handle this gracefully and clean up the other file,
  // falling back to the default.
  let loader = setup.new_loader();
  loader.handle_cached_config().await;
  let flag = TestFlag::register(&loader).unwrap();
  assert_eq!(*flag.read(), 1);

  assert!(!setup.retry_file().exists());
}

#[tokio::test]
async fn disk_persistence_missing_retry_file() {
  let setup = SetupDiskPersistence::new();

  // First write some data to the cached file by setting a new snapshot.
  {
    setup
      .new_loader()
      .update_snapshot(&bd_test_helpers::runtime::make_update(
        vec![(TestFlag::path(), ValueKind::Int(10))],
        "1".to_string(),
      ))
      .await;
  }

  assert!(setup.protobuf_file().exists());
  assert!(setup.retry_file().exists());

  std::fs::remove_file(setup.retry_file()).unwrap();

  // If the retry file is missing, we should handle this gracefully and clean up the other file,
  // falling back to the default.
  let loader = setup.new_loader();
  loader.handle_cached_config().await;
  let flag = TestFlag::register(&loader).unwrap();
  assert_eq!(*flag.read(), 1);

  assert!(!setup.protobuf_file().exists());
}

#[tokio::test]
async fn disk_persistence_cannot_update_retry() {
  let setup = SetupDiskPersistence::new();

  // First write some data to the cached file by setting a new snapshot.
  {
    setup
      .new_loader()
      .update_snapshot(&bd_test_helpers::runtime::make_update(
        vec![(TestFlag::path(), ValueKind::Int(10))],
        "1".to_string(),
      ))
      .await;
  }

  assert!(setup.protobuf_file().exists());
  assert!(setup.retry_file().exists());

  // Set readonly. We can read the retry value, but won't be able to update it.
  let mut perms = std::fs::metadata(setup.retry_file()).unwrap().permissions();
  perms.set_readonly(true);
  std::fs::set_permissions(setup.retry_file(), perms).unwrap();

  let loader = setup.new_loader();
  loader.handle_cached_config().await;
  let flag = TestFlag::register(&loader).unwrap();
  assert_eq!(*flag.read(), 1);

  assert!(!setup.protobuf_file().exists());
}
