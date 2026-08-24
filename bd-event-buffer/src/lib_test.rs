use super::{
  AdmissionOutcome,
  CapturedLog,
  EventBuffer,
  EventBufferEntry,
  EventBufferLimits,
  EventBufferState,
  RetentionLane,
  StateUpdateMessage,
  retention_lane,
};
use bd_log_primitives::{AnnotatedLogFields, DataValue, LogLine, log_level};
use bd_proto::protos::logging::payload::LogType;
use tokio::sync::oneshot;

fn log(level: bd_log_primitives::LogLevel, log_type: LogType, bytes: usize) -> EventBufferEntry {
  EventBufferEntry::Log(CapturedLog::new(
    LogLine {
      log_level: level,
      log_type,
      message: "x".repeat(bytes).into(),
      fields: AnnotatedLogFields::default(),
      matching_fields: AnnotatedLogFields::default(),
      attributes_overrides: None,
      capture_session: None,
    },
    false,
    None,
  ))
}

fn blocking_log(bytes: usize) -> EventBufferEntry {
  EventBufferEntry::Log(CapturedLog::new(
    LogLine {
      log_level: log_level::TRACE,
      log_type: LogType::NORMAL,
      message: "x".repeat(bytes).into(),
      fields: AnnotatedLogFields::default(),
      matching_fields: AnnotatedLogFields::default(),
      attributes_overrides: None,
      capture_session: None,
    },
    true,
    None,
  ))
}

fn limits(bytes: usize) -> EventBufferLimits {
  EventBufferLimits {
    log_limit_bytes: bytes,
    total_limit_bytes: bytes,
  }
}

fn buffer(bytes: usize) -> EventBuffer {
  EventBuffer::new(limits(bytes))
}

fn add_field(key: &str) -> EventBufferEntry {
  EventBufferEntry::State(StateUpdateMessage::AddLogField(
    key.to_string(),
    DataValue::String("value".into()),
  ))
}

fn state_limits(log_limit_bytes: usize, total_limit_bytes: usize) -> EventBufferLimits {
  EventBufferLimits {
    log_limit_bytes,
    total_limit_bytes,
  }
}

#[test]
fn priority_mapping_covers_protected_and_evictable_logs() {
  assert_eq!(
    RetentionLane::Low,
    retention_lane(LogType::NORMAL, log_level::DEBUG, false)
  );
  assert_eq!(
    RetentionLane::High,
    retention_lane(LogType::NORMAL, log_level::INFO, false)
  );
  assert_eq!(
    RetentionLane::Protected,
    retention_lane(LogType::LIFECYCLE, log_level::TRACE, false)
  );
  assert_eq!(
    RetentionLane::Protected,
    retention_lane(LogType::NORMAL, log_level::TRACE, true)
  );
}

#[tokio::test]
async fn preserves_log_and_field_update_admission_order() {
  let buffer = buffer(10_000);
  assert_eq!(
    AdmissionOutcome::Admitted,
    buffer.admit(log(log_level::INFO, LogType::NORMAL, 1))
  );
  assert_eq!(AdmissionOutcome::Admitted, buffer.admit(add_field("field")));
  assert_eq!(
    AdmissionOutcome::Admitted,
    buffer.admit(log(log_level::DEBUG, LogType::NORMAL, 2))
  );

  let entries = buffer
    .next_batch(3)
    .await
    .into_iter()
    .map(|entry| match entry {
      EventBufferEntry::Log(log) => format!("log:{}", log.log.message),
      EventBufferEntry::State(StateUpdateMessage::AddLogField(key, _)) => {
        format!("add_field:{key}")
      },
      EventBufferEntry::State(_) => "other_state".to_string(),
    })
    .collect::<Vec<_>>();
  assert_eq!(vec!["log:x", "add_field:field", "log:xx"], entries);
}

#[tokio::test]
async fn batch_eviction_removes_tiny_lower_priority_logs() {
  const COUNT: usize = 64;
  let low_size = log(log_level::DEBUG, LogType::NORMAL, 0).size();
  let incoming_base = log(log_level::INFO, LogType::NORMAL, 0).size();
  let buffer = buffer(COUNT * low_size);
  for _ in 0 .. COUNT {
    assert_eq!(
      AdmissionOutcome::Admitted,
      buffer.admit(log(log_level::DEBUG, LogType::NORMAL, 0))
    );
  }
  assert_eq!(
    AdmissionOutcome::Admitted,
    buffer.admit(log(
      log_level::INFO,
      LogType::NORMAL,
      COUNT * low_size - incoming_base
    ))
  );

  let batch = buffer.next_batch(COUNT + 1).await;
  assert_eq!(1, batch.len());
  assert!(matches!(&batch[0], EventBufferEntry::Log(log) if log.log.log_level == log_level::INFO));
}

#[tokio::test]
async fn rejected_admission_does_not_partially_evict() {
  let low = log(log_level::DEBUG, LogType::NORMAL, 1);
  let low_size = low.size();
  let protected = blocking_log(1);
  let total = low.size() + protected.size();
  let incoming_base = log(log_level::INFO, LogType::NORMAL, 0).size();
  let buffer = EventBuffer::new(EventBufferLimits {
    log_limit_bytes: usize::MAX,
    total_limit_bytes: total,
  });
  assert_eq!(AdmissionOutcome::Admitted, buffer.admit(low));
  assert_eq!(AdmissionOutcome::Admitted, buffer.admit(protected));
  assert_eq!(
    AdmissionOutcome::RejectedFull,
    buffer.admit(log(
      log_level::INFO,
      LogType::NORMAL,
      low_size + 1 - incoming_base
    ))
  );

  assert_eq!(2, buffer.next_batch(2).await.len());
}

#[tokio::test]
async fn completion_is_sent_only_after_consumer_processing() {
  let buffer = buffer(10_000);
  let (sender, receiver) = bd_completion::Sender::new();
  let entry = EventBufferEntry::Log(CapturedLog::new(
    LogLine {
      log_level: log_level::INFO,
      log_type: LogType::NORMAL,
      message: "x".into(),
      fields: AnnotatedLogFields::default(),
      matching_fields: AnnotatedLogFields::default(),
      attributes_overrides: None,
      capture_session: None,
    },
    false,
    Some(sender),
  ));
  assert_eq!(AdmissionOutcome::Admitted, buffer.admit(entry));
  let mut batch = buffer.next_batch(1).await;
  assert_eq!(1, batch.len());
  let entry = batch.remove(0);
  let (receiver_started_tx, receiver_started_rx) = oneshot::channel();
  let completion = tokio::spawn(async move {
    let _ignored = receiver_started_tx.send(());
    receiver.recv().await
  });
  assert!(receiver_started_rx.await.is_ok());
  assert!(!completion.is_finished());

  entry.complete();
  assert!(completion.await.is_ok_and(|result| result.is_ok()));
}

#[tokio::test]
async fn close_drops_unprocessed_completion_senders() {
  let buffer = buffer(10_000);
  let (sender, receiver) = bd_completion::Sender::new();
  let entry = EventBufferEntry::Log(CapturedLog::new(
    LogLine {
      log_level: log_level::INFO,
      log_type: LogType::NORMAL,
      message: "x".into(),
      fields: AnnotatedLogFields::default(),
      matching_fields: AnnotatedLogFields::default(),
      attributes_overrides: None,
      capture_session: None,
    },
    false,
    Some(sender),
  ));
  assert_eq!(AdmissionOutcome::Admitted, buffer.admit(entry));
  buffer.close();
  assert!(receiver.recv().await.is_err());
  assert!(buffer.next_batch(1).await.is_empty());
}

#[test]
fn pending_log_limit_shrink_evicts_newest_low_then_high_entries() {
  let mut state = EventBufferState::new(state_limits(100, 100));
  for (lane, entry) in [
    (RetentionLane::Low, "low_old"),
    (RetentionLane::Low, "low_new"),
    (RetentionLane::High, "high_old"),
    (RetentionLane::High, "high_new"),
  ] {
    assert_eq!(AdmissionOutcome::Admitted, state.admit(lane, 10, entry));
  }

  // Limit updates are deferred until admission so configuration changes do not contend with the
  // producer path until there is real work to do.
  state.set_pending_limits(state_limits(15, 100));
  assert_eq!(
    AdmissionOutcome::Admitted,
    state.admit(RetentionLane::Protected, 0, "trigger")
  );

  assert_eq!(vec!["high_old", "trigger"], state.take_batch(4));
}

#[test]
fn pending_total_limit_shrink_preserves_protected_entries() {
  let mut state = EventBufferState::new(state_limits(200, 200));
  assert_eq!(
    AdmissionOutcome::Admitted,
    state.admit(RetentionLane::Protected, 100, "protected")
  );
  assert_eq!(
    AdmissionOutcome::Admitted,
    state.admit(RetentionLane::Low, 10, "low")
  );

  state.set_pending_limits(state_limits(200, 50));
  assert_eq!(
    AdmissionOutcome::RejectedFull,
    state.admit(RetentionLane::Protected, 0, "trigger")
  );

  // Protected entries are never evicted, even if they alone exceed a newly reduced total limit.
  assert_eq!(vec!["protected"], state.take_batch(3));
}

#[test]
fn latest_pending_limit_update_wins() {
  let mut state = EventBufferState::new(state_limits(100, 100));
  for entry in ["old", "middle", "new"] {
    assert_eq!(
      AdmissionOutcome::Admitted,
      state.admit(RetentionLane::Low, 10, entry)
    );
  }

  state.set_pending_limits(state_limits(25, 100));
  state.set_pending_limits(state_limits(15, 100));
  assert_eq!(
    AdmissionOutcome::Admitted,
    state.admit(RetentionLane::Protected, 0, "trigger")
  );

  assert_eq!(vec!["old", "trigger"], state.take_batch(4));
}
