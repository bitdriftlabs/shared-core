// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use std::fs;
use std::os::unix::fs as os_fs;

#[tokio::test]
async fn feature_flags() {
  let helper = crate::test::Helper::new();

  // Write an initial configuration file out.
  helper.create_file_and_data_dir(
    "data_dir",
    "test.yaml",
    b"
values:
  foo: 1
  bar: hello
  baz: true
      ",
  );

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

  let loader = crate::feature_flags::new_memory_feature_flags_loader(
    helper.temp_dir(),
    helper.temp_dir().path().join("test.yaml"),
    helper.stats(),
  )
  .unwrap();
  let mut snapshot_watch = loader.snapshot_watch();

  // Make sure the initial load is correct.
  assert!(snapshot_watch
    .borrow()
    .as_ref()
    .unwrap()
    .get_bool("baz", false));
  assert_eq!(
    snapshot_watch
      .borrow()
      .as_ref()
      .unwrap()
      .get_integer("foo", 0),
    1
  );
  assert_eq!(
    snapshot_watch
      .borrow()
      .as_ref()
      .unwrap()
      .get_string("bar", "world"),
    "hello"
  );

  // Setup a new data directory, write a new config file, and then atomically swap
  // it in as would be done during a ConfigMap deploy.
  helper.create_file_and_data_dir(
    "data_dir2",
    "test.yaml",
    b"
values:
  foo: 2
  bar: world
  baz: false
  feature_percent_always: 10000
  feature_percent_never: 0
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
  assert!(!snapshot_watch
    .borrow()
    .as_ref()
    .unwrap()
    .get_bool("baz", true));
  assert_eq!(
    snapshot_watch
      .borrow()
      .as_ref()
      .unwrap()
      .get_integer("foo", 0),
    2
  );
  assert_eq!(
    snapshot_watch
      .borrow()
      .as_ref()
      .unwrap()
      .get_string("bar", "hello"),
    "world"
  );
  assert!(snapshot_watch
    .borrow()
    .as_ref()
    .unwrap()
    .feature_enabled("feature_percent_always", false));
  assert!(!snapshot_watch
    .borrow()
    .as_ref()
    .unwrap()
    .feature_enabled("feature_percent_never", true));

  // Create a bad file which should still provide a default implementation.
  helper.create_file_and_data_dir("data_dir3", "test.yaml", b"");
  os_fs::symlink(
    helper.temp_dir().path().join("data_dir3"),
    helper.temp_dir().path().join("..data.new"),
  )
  .unwrap();
  fs::rename(
    helper.temp_dir().path().join("..data.new"),
    helper.temp_dir().path().join("..data"),
  )
  .unwrap();

  snapshot_watch.changed().await.unwrap();
  assert!(snapshot_watch
    .borrow()
    .as_ref()
    .unwrap()
    .get_bool("bad", true));
  assert!(!snapshot_watch
    .borrow()
    .as_ref()
    .unwrap()
    .get_bool("bad", false));
  assert_eq!(
    snapshot_watch
      .borrow()
      .as_ref()
      .unwrap()
      .get_integer("bad", 1),
    1
  );
  assert_eq!(
    snapshot_watch
      .borrow()
      .as_ref()
      .unwrap()
      .get_string("bad", "world"),
    "world"
  );
  assert!(!snapshot_watch
    .borrow()
    .as_ref()
    .unwrap()
    .feature_enabled("feature_percent_always", false));
  assert!(snapshot_watch
    .borrow()
    .as_ref()
    .unwrap()
    .feature_enabled("feature_percent_never", true));

  loader.shutdown().await;
}
