// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{channel, MemorySized};
use tokio_test::{assert_err, assert_ok};

#[derive(Debug)]
struct Item {
  reported_size: usize,
}

impl MemorySized for Item {
  fn size(&self) -> usize {
    self.reported_size
  }
}

#[tokio::test]
async fn test_memory_capacity_limit_is_respected() {
  let (tx, mut rx) = channel::<Item>(10, 200);

  assert_ok!(tx.try_send(Item { reported_size: 100 }));
  assert_ok!(tx.try_send(Item { reported_size: 100 }));
  assert_err!(tx.try_send(Item { reported_size: 1 }));

  assert!(rx.recv().await.is_some());
  assert!(rx.recv().await.is_some());

  assert_ok!(tx.try_send(Item { reported_size: 100 }));
  assert_ok!(tx.try_send(Item { reported_size: 100 }));
  assert_err!(tx.try_send(Item { reported_size: 1 }));
}

#[tokio::test]
async fn test_capacity_limit_is_respected() {
  let (tx, mut rx) = channel::<Item>(2, 200);

  assert_ok!(tx.try_send(Item { reported_size: 1 }));
  assert_ok!(tx.try_send(Item { reported_size: 1 }));
  assert_err!(tx.try_send(Item { reported_size: 1 }));

  assert!(rx.recv().await.is_some());
  assert!(rx.recv().await.is_some());

  assert_ok!(tx.try_send(Item { reported_size: 1 }));
  assert_ok!(tx.try_send(Item { reported_size: 1 }));
  assert_err!(tx.try_send(Item { reported_size: 1 }));
}
