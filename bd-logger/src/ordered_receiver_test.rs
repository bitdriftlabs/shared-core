// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{OrderedMessage, OrderedReceiver, SequencedMessage};
use bd_bounded_buffer::channel;
use bd_log_primitives::size::MemorySized;

#[derive(Debug, PartialEq, Eq)]
struct TestLog(String);

impl MemorySized for TestLog {
  fn size(&self) -> usize {
    self.0.len()
  }
}

#[derive(Debug, PartialEq, Eq)]
struct TestState(String);

impl MemorySized for TestState {
  fn size(&self) -> usize {
    self.0.len()
  }
}

impl MemorySized for SequencedMessage<TestLog> {
  fn size(&self) -> usize {
    self.message.size()
  }
}

impl MemorySized for SequencedMessage<TestState> {
  fn size(&self) -> usize {
    self.message.size()
  }
}

#[tokio::test]
async fn orders_interleaved_messages_by_sequence() {
  let (log_tx, log_rx) = channel(1024);
  let (state_tx, state_rx) = channel(1024);
  let mut ordered = OrderedReceiver::new(log_rx, state_rx);

  // Send messages in order: log(0), state(1), log(2), state(3)
  log_tx
    .try_send(SequencedMessage {
      sequence: 0,
      message: TestLog("log0".to_string()),
    })
    .unwrap();
  state_tx
    .try_send(SequencedMessage {
      sequence: 1,
      message: TestState("state1".to_string()),
    })
    .unwrap();
  log_tx
    .try_send(SequencedMessage {
      sequence: 2,
      message: TestLog("log2".to_string()),
    })
    .unwrap();
  state_tx
    .try_send(SequencedMessage {
      sequence: 3,
      message: TestState("state3".to_string()),
    })
    .unwrap();

  // Should receive in sequence order
  match ordered.recv().await {
    Some(OrderedMessage::Log(TestLog(msg))) => assert_eq!(msg, "log0"),
    _ => panic!("Expected log0"),
  }
  match ordered.recv().await {
    Some(OrderedMessage::State(TestState(msg))) => assert_eq!(msg, "state1"),
    _ => panic!("Expected state1"),
  }
  match ordered.recv().await {
    Some(OrderedMessage::Log(TestLog(msg))) => assert_eq!(msg, "log2"),
    _ => panic!("Expected log2"),
  }
  match ordered.recv().await {
    Some(OrderedMessage::State(TestState(msg))) => assert_eq!(msg, "state3"),
    _ => panic!("Expected state3"),
  }
}

#[tokio::test]
async fn orders_out_of_order_messages_by_sequence() {
  let (log_tx, log_rx) = channel(1024);
  let (state_tx, state_rx) = channel(1024);
  let mut ordered = OrderedReceiver::new(log_rx, state_rx);

  // Send state before log, but log has lower sequence
  state_tx
    .try_send(SequencedMessage {
      sequence: 1,
      message: TestState("state1".to_string()),
    })
    .unwrap();
  log_tx
    .try_send(SequencedMessage {
      sequence: 0,
      message: TestLog("log0".to_string()),
    })
    .unwrap();

  // Should receive log first (lower sequence)
  match ordered.recv().await {
    Some(OrderedMessage::Log(TestLog(msg))) => assert_eq!(msg, "log0"),
    _ => panic!("Expected log0 first"),
  }
  match ordered.recv().await {
    Some(OrderedMessage::State(TestState(msg))) => assert_eq!(msg, "state1"),
    _ => panic!("Expected state1 second"),
  }
}

#[tokio::test]
async fn handles_only_logs() {
  let (log_tx, log_rx) = channel(1024);
  let (_state_tx, state_rx) = channel::<SequencedMessage<TestState>>(1024);
  let mut ordered = OrderedReceiver::new(log_rx, state_rx);

  log_tx
    .try_send(SequencedMessage {
      sequence: 0,
      message: TestLog("log0".to_string()),
    })
    .unwrap();
  log_tx
    .try_send(SequencedMessage {
      sequence: 1,
      message: TestLog("log1".to_string()),
    })
    .unwrap();

  match ordered.recv().await {
    Some(OrderedMessage::Log(TestLog(msg))) => assert_eq!(msg, "log0"),
    _ => panic!("Expected log0"),
  }
  match ordered.recv().await {
    Some(OrderedMessage::Log(TestLog(msg))) => assert_eq!(msg, "log1"),
    _ => panic!("Expected log1"),
  }
}

#[tokio::test]
async fn handles_only_states() {
  let (_log_tx, log_rx) = channel::<SequencedMessage<TestLog>>(1024);
  let (state_tx, state_rx) = channel(1024);
  let mut ordered = OrderedReceiver::new(log_rx, state_rx);

  state_tx
    .try_send(SequencedMessage {
      sequence: 0,
      message: TestState("state0".to_string()),
    })
    .unwrap();
  state_tx
    .try_send(SequencedMessage {
      sequence: 1,
      message: TestState("state1".to_string()),
    })
    .unwrap();

  match ordered.recv().await {
    Some(OrderedMessage::State(TestState(msg))) => assert_eq!(msg, "state0"),
    _ => panic!("Expected state0"),
  }
  match ordered.recv().await {
    Some(OrderedMessage::State(TestState(msg))) => assert_eq!(msg, "state1"),
    _ => panic!("Expected state1"),
  }
}

#[tokio::test(flavor = "multi_thread")]
async fn returns_none_when_both_channels_closed() {
  let (log_tx, log_rx) = channel::<SequencedMessage<TestLog>>(1024);
  let (state_tx, state_rx) = channel::<SequencedMessage<TestState>>(1024);
  let mut ordered = OrderedReceiver::new(log_rx, state_rx);

  // Drop senders to close channels
  drop(log_tx);
  drop(state_tx);

  let result = tokio::time::timeout(std::time::Duration::from_secs(1), ordered.recv()).await;

  match result {
    Ok(Some(_)) => panic!("Expected None"),
    Ok(None) => {}, // Success
    Err(e) => panic!("recv() timed out - likely deadlock: {e}"),
  }
}

#[tokio::test]
async fn closed_channel_recv_returns_none() {
  let (log_tx, mut log_rx) = channel::<SequencedMessage<TestLog>>(1024);
  let (state_tx, mut state_rx) = channel::<SequencedMessage<TestState>>(1024);

  // Drop senders to close channels
  drop(log_tx);
  drop(state_tx);

  // Test that recv() on a closed channel returns None
  let log_result = log_rx.recv().await;
  let state_result = state_rx.recv().await;

  assert!(
    log_result.is_none(),
    "Closed log channel should return None"
  );
  assert!(
    state_result.is_none(),
    "Closed state channel should return None"
  );
}

#[tokio::test]
async fn drains_buffered_messages_after_channel_closes() {
  let (log_tx, log_rx) = channel(1024);
  let (state_tx, state_rx) = channel(1024);
  let mut ordered = OrderedReceiver::new(log_rx, state_rx);

  // Send some messages then close channels
  log_tx
    .try_send(SequencedMessage {
      sequence: 0,
      message: TestLog("log0".to_string()),
    })
    .unwrap();
  state_tx
    .try_send(SequencedMessage {
      sequence: 1,
      message: TestState("state1".to_string()),
    })
    .unwrap();

  drop(log_tx);
  drop(state_tx);

  // Should still receive buffered messages in order
  match ordered.recv().await {
    Some(OrderedMessage::Log(TestLog(msg))) => assert_eq!(msg, "log0"),
    _ => panic!("Expected log0"),
  }
  match ordered.recv().await {
    Some(OrderedMessage::State(TestState(msg))) => assert_eq!(msg, "state1"),
    _ => panic!("Expected state1"),
  }
  assert!(ordered.recv().await.is_none());
}

#[tokio::test]
async fn handles_equal_sequences() {
  let (log_tx, log_rx) = channel(1024);
  let (state_tx, state_rx) = channel(1024);
  let mut ordered = OrderedReceiver::new(log_rx, state_rx);

  // Send messages with same sequence number
  log_tx
    .try_send(SequencedMessage {
      sequence: 0,
      message: TestLog("log0".to_string()),
    })
    .unwrap();
  state_tx
    .try_send(SequencedMessage {
      sequence: 0,
      message: TestState("state0".to_string()),
    })
    .unwrap();

  // Log should come first (sequence <= comparison)
  match ordered.recv().await {
    Some(OrderedMessage::Log(TestLog(msg))) => assert_eq!(msg, "log0"),
    _ => panic!("Expected log0 first"),
  }
  match ordered.recv().await {
    Some(OrderedMessage::State(TestState(msg))) => assert_eq!(msg, "state0"),
    _ => panic!("Expected state0 second"),
  }
}

#[tokio::test]
async fn handles_many_logs_then_state() {
  let (log_tx, log_rx) = channel(1024);
  let (state_tx, state_rx) = channel(1024);
  let mut ordered = OrderedReceiver::new(log_rx, state_rx);

  // Send many logs, then a state update
  for i in 0 .. 10 {
    log_tx
      .try_send(SequencedMessage {
        sequence: i * 2,
        message: TestLog(format!("log{}", i * 2)),
      })
      .unwrap();
  }
  state_tx
    .try_send(SequencedMessage {
      sequence: 5,
      message: TestState("state5".to_string()),
    })
    .unwrap();

  // Should receive: log0, log2, log4, state5, log6, log8, ...
  let expected = vec![
    ("log", 0),
    ("log", 2),
    ("log", 4),
    ("state", 5),
    ("log", 6),
    ("log", 8),
    ("log", 10),
    ("log", 12),
    ("log", 14),
    ("log", 16),
    ("log", 18),
  ];

  for (msg_type, seq) in expected {
    match ordered.recv().await {
      Some(OrderedMessage::Log(TestLog(msg))) if msg_type == "log" => {
        assert_eq!(msg, format!("log{seq}"));
      },
      Some(OrderedMessage::State(TestState(msg))) if msg_type == "state" => {
        assert_eq!(msg, format!("state{seq}"));
      },
      other => panic!("Unexpected message: {other:?}"),
    }
  }
}

#[tokio::test]
async fn handles_async_arrival() {
  let (log_tx, log_rx) = channel(1024);
  let (state_tx, state_rx) = channel(1024);
  let mut ordered = OrderedReceiver::new(log_rx, state_rx);

  // Spawn task to send log after a delay
  let log_handle = tokio::spawn(async move {
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    log_tx
      .try_send(SequencedMessage {
        sequence: 1,
        message: TestLog("log1".to_string()),
      })
      .unwrap();
  });

  // Send state immediately
  state_tx
    .try_send(SequencedMessage {
      sequence: 0,
      message: TestState("state0".to_string()),
    })
    .unwrap();

  // Should receive state first (lower sequence and already available)
  match ordered.recv().await {
    Some(OrderedMessage::State(TestState(msg))) => assert_eq!(msg, "state0"),
    _ => panic!("Expected state0 first"),
  }

  // Then log arrives
  match ordered.recv().await {
    Some(OrderedMessage::Log(TestLog(msg))) => assert_eq!(msg, "log1"),
    _ => panic!("Expected log1"),
  }

  log_handle.await.unwrap();
}
