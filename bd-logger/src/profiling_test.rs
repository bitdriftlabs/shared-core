// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{
  NoopProfilingTarget,
  ProfilingHandler,
  ProfilingTarget,
  StackSampleCompletion,
  StackSampleOutcome,
  StackSampleRequest,
  UNSUPPORTED_ERROR_NAME,
};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;

#[derive(Default)]
struct DeferredTarget {
  requests: Mutex<Vec<StackSampleRequest>>,
  completions: Mutex<VecDeque<StackSampleCompletion>>,
  cancelled: Mutex<Vec<u64>>,
  started_tx: Mutex<Option<mpsc::UnboundedSender<()>>>,
}

impl ProfilingTarget for DeferredTarget {
  fn capture_stack_sample(&self, request: StackSampleRequest, completion: StackSampleCompletion) {
    self.requests.lock().push(request);
    self.completions.lock().push_back(completion);
    if let Some(tx) = self.started_tx.lock().as_ref() {
      tx.send(()).unwrap();
    }
  }

  fn cancel_stack_sample(&self, request_id: u64) {
    self.cancelled.lock().push(request_id);
  }
}

#[tokio::test]
async fn default_target_fails_as_unsupported() {
  let handler = ProfilingHandler::new(Arc::new(NoopProfilingTarget));
  let error = handler
    .capture_stack_sample(time::Duration::seconds(3), 100, 2048, "bitdrift".into())
    .await
    .unwrap_err();
  assert_eq!(error.code, 0);
  assert_eq!(error.name, UNSUPPORTED_ERROR_NAME);
}

#[tokio::test]
async fn completion_resolves_the_capture_and_releases_the_slot() {
  let target = Arc::new(DeferredTarget::default());
  let handler = ProfilingHandler::new(target.clone());
  let (started_tx, mut started_rx) = mpsc::unbounded_channel();
  *target.started_tx.lock() = Some(started_tx);

  let capture = tokio::spawn({
    let handler = handler.clone();
    async move {
      handler
        .capture_stack_sample(time::Duration::seconds(3), 100, 2048, "bitdrift".into())
        .await
    }
  });
  started_rx.recv().await.unwrap();
  assert_eq!(
    *target.requests.lock(),
    vec![StackSampleRequest {
      request_id: 1,
      duration: time::Duration::seconds(3),
      frequency_hz: 100,
      buffer_size_kb: 2048,
      tag: "bitdrift".into(),
    }]
  );
  let completion = target.completions.lock().pop_front().unwrap();
  completion(Ok(StackSampleOutcome {
    path: "/tmp/trace".into(),
  }));
  assert_eq!(
    capture.await.unwrap().unwrap().path.to_str(),
    Some("/tmp/trace")
  );
  assert!(target.cancelled.lock().is_empty());

  // The slot is free again, so a second capture is accepted with a new request ID.
  let second = tokio::spawn(async move {
    handler
      .capture_stack_sample(time::Duration::seconds(1), 50, 256, "bitdrift".into())
      .await
  });
  started_rx.recv().await.unwrap();
  assert_eq!(target.requests.lock()[1].request_id, 2);
  target.completions.lock().pop_front().unwrap()(Ok(StackSampleOutcome {
    path: "/tmp/second".into(),
  }));
  second.await.unwrap().unwrap();
}

#[tokio::test]
async fn concurrent_capture_is_rejected_without_reaching_the_target() {
  let target = Arc::new(DeferredTarget::default());
  let handler = ProfilingHandler::new(target.clone());
  let (started_tx, mut started_rx) = mpsc::unbounded_channel();
  *target.started_tx.lock() = Some(started_tx);
  let first = tokio::spawn({
    let handler = handler.clone();
    async move {
      handler
        .capture_stack_sample(time::Duration::seconds(3), 100, 2048, "bitdrift".into())
        .await
    }
  });
  started_rx.recv().await.unwrap();

  let error = handler
    .capture_stack_sample(time::Duration::seconds(3), 100, 2048, "bitdrift".into())
    .await
    .unwrap_err();
  assert_eq!(error.name, "already_executing");
  assert_eq!(target.requests.lock().len(), 1);

  target.completions.lock().pop_front().unwrap()(Ok(StackSampleOutcome {
    path: "/tmp/trace".into(),
  }));
  first.await.unwrap().unwrap();
}

#[tokio::test]
async fn dropping_an_unfinished_capture_cancels_it_and_ignores_the_late_completion() {
  let target = Arc::new(DeferredTarget::default());
  let handler = ProfilingHandler::new(target.clone());
  let (started_tx, mut started_rx) = mpsc::unbounded_channel();
  *target.started_tx.lock() = Some(started_tx);
  let capture = tokio::spawn({
    let handler = handler.clone();
    async move {
      handler
        .capture_stack_sample(time::Duration::seconds(3), 100, 2048, "bitdrift".into())
        .await
    }
  });
  started_rx.recv().await.unwrap();

  capture.abort();
  assert!(capture.await.unwrap_err().is_cancelled());
  assert_eq!(*target.cancelled.lock(), vec![1]);

  // The platform still reports; nothing is waiting, and the slot is already free.
  target.completions.lock().pop_front().unwrap()(Ok(StackSampleOutcome {
    path: "/tmp/late".into(),
  }));
  let second = tokio::spawn(async move {
    handler
      .capture_stack_sample(time::Duration::seconds(3), 100, 2048, "bitdrift".into())
      .await
  });
  started_rx.recv().await.unwrap();
  assert_eq!(target.requests.lock()[1].request_id, 2);
  target.completions.lock().pop_front().unwrap()(Ok(StackSampleOutcome {
    path: "/tmp/second".into(),
  }));
  second.await.unwrap().unwrap();
}

#[test]
fn request_ids_are_unique_across_handler_clones() {
  let handler = ProfilingHandler::new(Arc::new(NoopProfilingTarget));
  let clone = handler.clone();
  let ids = [
    handler.next_request_id.fetch_add(1, Ordering::Relaxed),
    clone.next_request_id.fetch_add(1, Ordering::Relaxed),
  ];
  assert_eq!(ids, [1, 2]);
  assert_eq!(AtomicU64::new(3).load(Ordering::Relaxed), 3);
}
