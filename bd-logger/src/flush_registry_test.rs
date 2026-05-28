// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::{
  PendingTriggerUploadsStore,
  PersistedTriggerUpload,
  PersistedTriggerUploadLifecycle,
  PersistedTriggerUploadSource,
};
use bd_key_value::{Storage, Store};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Default)]
struct MockStorage {
  state: Arc<parking_lot::Mutex<HashMap<String, String>>>,
}

impl Storage for MockStorage {
  fn set_string(&self, key: &str, value: &str) -> anyhow::Result<()> {
    self
      .state
      .lock()
      .insert(key.to_string(), value.to_string());
    Ok(())
  }

  fn get_string(&self, key: &str) -> anyhow::Result<Option<String>> {
    Ok(self.state.lock().get(key).cloned())
  }

  fn delete(&self, key: &str) -> anyhow::Result<()> {
    self.state.lock().remove(key);
    Ok(())
  }
}

fn make_store() -> Arc<Store> {
  Arc::new(Store::new(Box::<MockStorage>::default()))
}

#[test]
fn upsert_replaces_existing_upload_with_same_id() {
  let store = PendingTriggerUploadsStore::new(make_store());
  store.upsert(PersistedTriggerUpload {
    id: "flush-1".to_string(),
    source: PersistedTriggerUploadSource::RemoteCommand("flush-1".to_string()),
    buffer_ids: vec!["a".to_string()],
    has_streaming: false,
    lifecycle: PersistedTriggerUploadLifecycle::ReadyToUpload,
  });
  store.upsert(PersistedTriggerUpload {
    id: "flush-1".to_string(),
    source: PersistedTriggerUploadSource::RemoteCommand("flush-1".to_string()),
    buffer_ids: vec!["b".to_string()],
    has_streaming: true,
    lifecycle: PersistedTriggerUploadLifecycle::Uploading,
  });

  assert_eq!(
    store.pending_uploads(),
    vec![PersistedTriggerUpload {
      id: "flush-1".to_string(),
      source: PersistedTriggerUploadSource::RemoteCommand("flush-1".to_string()),
      buffer_ids: vec!["b".to_string()],
      has_streaming: true,
      lifecycle: PersistedTriggerUploadLifecycle::Uploading,
    }]
  );
}

#[test]
fn remove_clears_matching_upload() {
  let store = PendingTriggerUploadsStore::new(make_store());
  store.upsert(PersistedTriggerUpload {
    id: "flush-1".to_string(),
    source: PersistedTriggerUploadSource::WorkflowAction("flush-1".to_string()),
    buffer_ids: vec!["trigger".to_string()],
    has_streaming: false,
    lifecycle: PersistedTriggerUploadLifecycle::ReadyToUpload,
  });

  store.remove("flush-1");

  assert!(store.pending_uploads().is_empty());
}

#[test]
fn mark_uploading_updates_lifecycle_without_replacing_other_fields() {
  let store = PendingTriggerUploadsStore::new(make_store());
  store.upsert(PersistedTriggerUpload {
    id: "flush-1".to_string(),
    source: PersistedTriggerUploadSource::RemoteCommand("flush-1".to_string()),
    buffer_ids: vec!["trigger".to_string()],
    has_streaming: true,
    lifecycle: PersistedTriggerUploadLifecycle::ReadyToUpload,
  });

  store.mark_uploading("flush-1");

  assert_eq!(
    store.pending_uploads(),
    vec![PersistedTriggerUpload {
      id: "flush-1".to_string(),
      source: PersistedTriggerUploadSource::RemoteCommand("flush-1".to_string()),
      buffer_ids: vec!["trigger".to_string()],
      has_streaming: true,
      lifecycle: PersistedTriggerUploadLifecycle::Uploading,
    }]
  );
}
