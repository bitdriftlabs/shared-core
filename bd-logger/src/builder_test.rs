// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::initialize_memory_pressure;
use super::initialize_opaque_entity_updates;
use crate::logger::PendingEntityIdUpdate;
use bd_client_stats_store::Collector;
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::MemoryPressureLevel;
use bd_resilient_kv::{Scope as KvScope, StateValue, VersionedKVStore};
use bd_state::test::TestStore;
use bd_state::{ENTITY_ID_KEY, Scope, StateReader, Value_type, string_value};
use bd_time::TestTimeProvider;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use std::sync::atomic::{AtomicI8, Ordering};
use time::macros::datetime;
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

#[tokio::test]
async fn pending_entity_id_clear_removes_persisted_value_and_clears_watch() {
  let state_store = TestStore::new().await;
  state_store
    .insert(
      Scope::System,
      ENTITY_ID_KEY.to_string(),
      string_value("persisted-entity-id"),
    )
    .await
    .unwrap();
  let (opaque_entity_updates_tx, opaque_entity_updates_rx) =
    watch::channel(Some("stale-entity-id".to_string()));

  initialize_opaque_entity_updates(
    &state_store,
    &opaque_entity_updates_tx,
    Some(PendingEntityIdUpdate::Clear),
  )
  .await;

  assert_eq!(None, opaque_entity_updates_rx.borrow().clone());

  let reader = state_store.read().await;
  assert!(reader.get(Scope::System, ENTITY_ID_KEY).is_none());
}

async fn make_previous_run_state_with_memory_pressure(
  level: MemoryPressureLevel,
) -> bd_resilient_kv::ScopedMaps {
  let mut store = VersionedKVStore::new_in_memory(
    Arc::new(TestTimeProvider::new(datetime!(2024-01-01 00:00 UTC))),
    None,
    &Collector::default().scope("test"),
    bd_runtime::runtime::IntWatch::new_for_testing(0),
  );
  store
    .insert(
      KvScope::System,
      "memory_pressure_level".to_string(),
      StateValue {
        value_type: Some(bd_resilient_kv::Value_type::StringValue(
          level.variant_name().unwrap_or("Unknown").to_string(),
        )),
        ..Default::default()
      },
    )
    .await
    .unwrap();
  store.as_hashmap().clone()
}

#[tokio::test]
async fn initialize_memory_pressure_loads_value_and_clears_store() {
  let previous_run_state =
    make_previous_run_state_with_memory_pressure(MemoryPressureLevel::Warning).await;
  let state_store = TestStore::new().await;
  state_store
    .insert(
      Scope::System,
      "memory_pressure_level".to_string(),
      string_value("Warning".to_string()),
    )
    .await
    .unwrap();

  let atomic = AtomicI8::new(0);
  initialize_memory_pressure(&previous_run_state, &state_store, &atomic).await;

  assert_eq!(MemoryPressureLevel::Warning.0, atomic.load(Ordering::Relaxed));

  let reader = state_store.read().await;
  assert!(reader.get(Scope::System, "memory_pressure_level").is_none());
}

#[tokio::test]
async fn initialize_memory_pressure_does_not_persist_to_next_session() {
  // Session 1: warning was present, initialize clears the store
  let previous_run_state =
    make_previous_run_state_with_memory_pressure(MemoryPressureLevel::Warning).await;
  let state_store = TestStore::new().await;
  state_store
    .insert(
      Scope::System,
      "memory_pressure_level".to_string(),
      string_value("Warning".to_string()),
    )
    .await
    .unwrap();

  let atomic = AtomicI8::new(0);
  initialize_memory_pressure(&previous_run_state, &state_store, &atomic).await;

  // Session 2: no memory pressure in previous run state (store was cleared)
  let empty_previous_run_state = bd_resilient_kv::ScopedMaps::default();
  let atomic2 = AtomicI8::new(0);
  initialize_memory_pressure(&empty_previous_run_state, &state_store, &atomic2).await;

  assert_eq!(0, atomic2.load(Ordering::Relaxed));
}
