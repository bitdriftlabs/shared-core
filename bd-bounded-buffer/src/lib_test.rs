// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{MemorySized, channel};
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
  let (tx, mut rx) = channel::<Item>(200);

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
async fn test_many_small_messages_within_memory_limit() {
  let (tx, mut rx) = channel::<Item>(200);

  // Can send many small messages as long as we're within memory limit
  for _ in 0 .. 100 {
    assert_ok!(tx.try_send(Item { reported_size: 1 }));
  }

  // Still within 200 byte limit (exactly at limit)
  assert_ok!(tx.try_send(Item { reported_size: 100 }));

  // Now we exceed the limit
  assert_err!(tx.try_send(Item { reported_size: 1 }));

  // Consume one message to free up space
  assert!(rx.recv().await.is_some());

  // Now we can send again
  assert_ok!(tx.try_send(Item { reported_size: 1 }));
}

#[tokio::test]
async fn test_try_recv_on_closed_channel() {
  let (tx, mut rx) = channel::<Item>(1024);

  // Drop sender to close channel
  drop(tx);

  // try_recv should return Disconnected
  let result = rx.try_recv();

  match result {
    Err(super::TryRecvError::Disconnected) => {
      // Expected behavior
    },
    Err(super::TryRecvError::Empty) => {
      panic!("try_recv returned Empty instead of Disconnected on closed channel");
    },
    Ok(_) => {
      panic!("try_recv returned Ok instead of error on closed channel");
    },
  }
}

#[tokio::test]
async fn test_recv_on_closed_channel_returns_immediately() {
  let (tx, mut rx) = channel::<Item>(1024);

  // Drop sender to close channel
  drop(tx);

  // recv should return None immediately
  let result = tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv()).await;

  assert!(matches!(result, Ok(None)));
}
