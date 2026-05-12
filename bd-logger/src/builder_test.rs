// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::initialize_opaque_entity_updates;
use crate::logger::PendingEntityIdUpdate;
use bd_state::test::TestStore;
use bd_state::{ENTITY_ID_KEY, Scope, StateReader, Value_type, string_value};
use pretty_assertions::assert_eq;
use tokio::sync::watch;

#[tokio::test]
async fn pending_entity_id_is_replayed_into_state_store() {
  let state_store = TestStore::new().await;
  let (opaque_entity_updates_tx, opaque_entity_updates_rx) = watch::channel(None);

  initialize_opaque_entity_updates(
    &state_store,
    &opaque_entity_updates_tx,
    Some(PendingEntityIdUpdate::Set("hashed-entity-id".to_string())),
  )
  .await;

  assert_eq!(
    Some("hashed-entity-id".to_string()),
    opaque_entity_updates_rx.borrow().clone()
  );

  let reader = state_store.read().await;
  let Some(value) = reader.get(Scope::System, ENTITY_ID_KEY) else {
    panic!("expected entity ID to be persisted")
  };
  let Some(Value_type::StringValue(entity_id)) = value.value_type.as_ref() else {
    panic!("expected string entity ID")
  };
  assert_eq!("hashed-entity-id", entity_id);
}

#[tokio::test]
async fn persisted_entity_id_seeds_watch_when_no_pending_update_exists() {
  let state_store = TestStore::new().await;
  state_store
    .insert(
      Scope::System,
      ENTITY_ID_KEY.to_string(),
      string_value("persisted-entity-id"),
    )
    .await
    .unwrap();
  let (opaque_entity_updates_tx, opaque_entity_updates_rx) = watch::channel(None);

  initialize_opaque_entity_updates(&state_store, &opaque_entity_updates_tx, None).await;

  assert_eq!(
    Some("persisted-entity-id".to_string()),
    opaque_entity_updates_rx.borrow().clone()
  );
}
