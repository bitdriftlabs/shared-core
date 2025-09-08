// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{RecvWithTimeoutError, Sender};
use std::thread;
use std::time::Duration;

#[test]
fn test_blocking_recv_with_timeout_success() {
  let (tx, rx) = Sender::<String>::new();
  let test_value = "test message".to_string();

  tx.send(test_value.clone());

  let result = rx.blocking_recv_with_timeout(Duration::from_millis(100));
  if let Ok(value) = result {
    assert_eq!(value, test_value);
  } else {
    panic!("Expected Ok result but got Err");
  }
}

#[test]
fn test_blocking_recv_with_timeout_timeout() {
  let (_tx, rx) = Sender::<String>::new();

  // Don't send anything to trigger timeout

  let result = rx.blocking_recv_with_timeout(Duration::from_millis(50));
  if let Err(e) = result {
    assert!(matches!(e, RecvWithTimeoutError::Timeout));
    assert_eq!(e.to_string(), "timeout duration reached");
  } else {
    panic!("Expected Err result but got Ok");
  }
}

#[test]
fn test_blocking_recv_with_drop_channel_closed() {
  let (tx, rx) = Sender::<String>::new();

  drop(tx);

  let result = rx.blocking_recv_with_timeout(Duration::from_millis(100));
  if let Err(e) = result {
    assert!(matches!(e, RecvWithTimeoutError::ChannelClosed));
    assert_eq!(e.to_string(), "the oneshot channel was closed");
  } else {
    panic!("Expected Err result but got Ok");
  }
}

#[test]
fn test_blocking_recv_with_timeout_delayed_send() {
  let (tx, rx) = Sender::<String>::new();
  let test_value = "delayed message".to_string();
  let timeout = Duration::from_millis(200);
  let test_value_clone = test_value.clone();

  thread::spawn(move || {
    thread::sleep(Duration::from_millis(50));
    tx.send(test_value_clone);
  });

  let result = rx.blocking_recv_with_timeout(timeout);
  if let Ok(value) = result {
    assert_eq!(value, test_value);
  } else {
    panic!("Expected Ok result but got Err");
  }
}
