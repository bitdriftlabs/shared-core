// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::loader::*;
use serde::Deserialize;
use std::fs;
use std::os::unix::fs as os_fs;
use std::path::Path;
use std::sync::Arc;

#[derive(Deserialize, Debug)]
struct TestData {
  foo: u64,
  bar: String,
}

#[tokio::test]
async fn bad_directory() {
  let helper = crate::test::Helper::new();
  assert!(
    WatchedFileLoader::new_loader(
      Path::new("bad"),
      Path::new("bad"),
      |test_data: Option<TestData>| -> ConfigPtr<TestData> { test_data.map(Arc::new) },
      helper.stats()
    )
    .is_err()
  );
}

#[tokio::test]
async fn no_file_at_start() {
  let helper = crate::test::Helper::new();
  let loader = WatchedFileLoader::new_loader(
    helper.temp_dir(),
    Path::new("bad"),
    |test_data: Option<TestData>| -> ConfigPtr<TestData> { test_data.map(Arc::new) },
    helper.stats(),
  )
  .unwrap();
  assert!(loader.snapshot_watch().borrow().is_none());
  helper.check_stats(0, 0, 1);
}

#[tokio::test]
async fn update() {
  let helper = crate::test::Helper::new();

  // Won't parse.
  helper.create_file_and_data_dir("data_dir", "test.yaml", b"");

  // Setup symlinks as we might expect in a Kubernetes ConfigMap deployment.
  // Specifically: ..data -> data_dir
  // test.yaml -> ..data/test.yaml
  os_fs::symlink(
    helper.temp_dir().path().join("data_dir"),
    helper.temp_dir().path().join("..data"),
  )
  .unwrap();
  os_fs::symlink(
    helper.temp_dir().path().join("..data").join("test.yaml"),
    helper.temp_dir().path().join("test.yaml"),
  )
  .unwrap();

  let loader = WatchedFileLoader::new_loader(
    helper.temp_dir(),
    helper.temp_dir().path().join("test.yaml"),
    |test_data: Option<TestData>| -> ConfigPtr<TestData> { test_data.map(Arc::new) },
    helper.stats(),
  )
  .unwrap();
  assert!(loader.snapshot_watch().borrow().is_none());
  helper.check_stats(0, 1, 0);

  let mut snapshot_watch = loader.snapshot_watch();

  // Will parse.
  helper.create_file_and_data_dir(
    "data_dir2",
    "test.yaml",
    b"
foo: 23
bar: hello
  ",
  );
  os_fs::symlink(
    helper.temp_dir().path().join("data_dir2"),
    helper.temp_dir().path().join("..data.new"),
  )
  .unwrap();
  fs::rename(
    helper.temp_dir().path().join("..data.new"),
    helper.temp_dir().path().join("..data"),
  )
  .unwrap();

  snapshot_watch.changed().await.unwrap();
  // The inotify backend currently correctly dedups into a single reload event. This doesn't work
  // on macos and it's not worth fixing.
  helper.check_stats(if cfg!(target_os = "linux") { 1 } else { 3 }, 1, 0);
  assert_eq!(snapshot_watch.borrow().as_ref().unwrap().foo, 23);
  assert_eq!(snapshot_watch.borrow().as_ref().unwrap().bar, "hello");

  loader.shutdown().await;
}

#[tokio::test]
async fn async_notifications() {
  let helper = crate::test::Helper::new();

  // Won't parse.
  helper.create_file_and_data_dir("data_dir", "test.yaml", b"");

  // Setup symlinks as we might expect in a Kubernetes ConfigMap deployment.
  // Specifically: ..data -> data_dir
  // test.yaml -> ..data/test.yaml
  os_fs::symlink(
    helper.temp_dir().path().join("data_dir"),
    helper.temp_dir().path().join("..data"),
  )
  .unwrap();
  os_fs::symlink(
    helper.temp_dir().path().join("..data").join("test.yaml"),
    helper.temp_dir().path().join("test.yaml"),
  )
  .unwrap();

  let loader = WatchedFileLoader::new_loader(
    helper.temp_dir(),
    helper.temp_dir().path().join("test.yaml"),
    |test_data: Option<TestData>| -> ConfigPtr<TestData> { test_data.map(Arc::new) },
    helper.stats(),
  )
  .unwrap();
  assert!(loader.snapshot_watch().borrow().is_none());

  let mut snapshot_watch = loader.snapshot_watch();

  // Will parse.
  helper.create_file_and_data_dir(
    "data_dir2",
    "test.yaml",
    b"
foo: 23
bar: hello
  ",
  );
  os_fs::symlink(
    helper.temp_dir().path().join("data_dir2"),
    helper.temp_dir().path().join("..data.new"),
  )
  .unwrap();
  fs::rename(
    helper.temp_dir().path().join("..data.new"),
    helper.temp_dir().path().join("..data"),
  )
  .unwrap();

  snapshot_watch.changed().await.unwrap();
  assert_eq!(snapshot_watch.borrow().as_ref().unwrap().foo, 23);
  assert_eq!(snapshot_watch.borrow().as_ref().unwrap().bar, "hello");

  loader.shutdown().await;
}
