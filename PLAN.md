# State Update Implementation Plan

## Goal

Implement the shared-core side of the new client state-update protocol while simplifying session
state ownership.

The main design decision is:

- `bd-session` owns durable session state directly using simple local file persistence.
- `Strategy` becomes the central orchestrator for load, mutate, persist, and deferred callback
  execution.
- `fixed` and `activity_based` stop owning persistence and instead provide backend-specific state
  transition logic.
- `bd-api` owns transport send and ack behavior for `HandshakeRequest.state_update`,
  `ApiRequest::StateUpdate`, and `StateUpdateResponse`.

## Scope

Included:

- `bd-session` persistence centralization
- durable started-session upload state
- `bd-api` request and response plumbing for the new protocol messages
- opaque entity rename and update flow on the client side
- test helper and constructor updates in shared-core

Excluded for this pass:

- loop-api server implementation
- broad replacement of `bd_key_value` outside of `bd-session`
- unrelated cleanup in other crates

## Design Summary

### Session State Ownership

`bd-session` should stop depending on `bd_key_value::Store`.

Instead:

- add a local persistence module in `bd-session`
- persist one current session state file
- persist one pending started-session batch file
- do not add durable in-flight session upload state, since duplicates are acceptable
- keep runtime-only bookkeeping, such as activity write debounce timing, in memory unless restart
  semantics require persistence

### Strategy Structure

`Strategy` in [bd-session/src/lib.rs](bd-session/src/lib.rs) should no longer be a thin enum
dispatcher.

It should become the shared orchestration point that:

1. lazily loads persisted state on first access
2. passes current state into the selected backend provider
3. persists returned state if it changed
4. updates cached in-memory state
5. runs any callbacks after releasing locks

This keeps all persistence and callback ordering rules in one place.

### Backend Structure

The backend modules should be reduced to transition providers:

- [bd-session/src/fixed.rs](bd-session/src/fixed.rs): fixed session generation and rotation rules
- [bd-session/src/activity_based.rs](bd-session/src/activity_based.rs): inactivity checks, last
  activity update, optional rotation

Each backend should operate on loaded state and return:

- updated persisted state
- whether persistence is required
- any deferred callback actions to run after persistence succeeds

### Transport Structure

`bd-api` should remain responsible for protocol transport:

- populate `HandshakeRequest.state_update`
- send `ApiRequest::StateUpdate` mid-stream when local state changes
- treat `HandshakeResponse` and `StateUpdateResponse` as atomic ack of the transmitted batch

The durable source of truth for session-related updates should remain in `bd-session`, not in
`bd-api`.

## Milestones

### Milestone 1: Centralize `bd-session` Persistence

Objective:

- move all session persistence into `bd-session`
- make `Strategy` the central owner of load, mutate, persist, and callback flow

Work:

- add a local persistence module under `bd-session/src`
- define a shared persisted session schema that can represent both backends
- refactor [bd-session/src/lib.rs](bd-session/src/lib.rs) so `Strategy` becomes a struct with an
  internal backend enum and owns cached state and persistence
- refactor [bd-session/src/fixed.rs](bd-session/src/fixed.rs) and
  [bd-session/src/activity_based.rs](bd-session/src/activity_based.rs) into transition providers
- remove `pub use bd_key_value::Store` from [bd-session/src/lib.rs](bd-session/src/lib.rs)

Review checkpoint:

- `Strategy` clearly owns persistence
- backends no longer do file or store IO directly
- lazy initialization and callback-after-unlock behavior are preserved

Exit criteria:

- `bd-session` no longer depends on `bd_key_value::Store`
- current semantics for `session_id()`, `start_new_session()`, `flush()`, and
  `previous_process_session_id()` remain intact

### Milestone 2: Add Durable Started-Session Upload State

Objective:

- keep started-session updates durable across reconnects and process restart

Work:

- add pending batch persistence for started sessions in the same `bd-session` persistence module
- define atomic removal rules for acknowledged session batches without adding durable in-flight
  state
- make both fixed and activity-based backends enqueue started-session events through the shared
  orchestration layer

Recommended semantics:

- on local enqueue: append or merge into the pending batch
- on send: read the current pending snapshot and transmit it without moving it to a second durable
  file
- on ack: atomically remove the acknowledged snapshot from the pending file, preserving any newer
  appended entries
- on restart or disconnect before ack: resend from the remaining pending file; duplicates are
  acceptable

Review checkpoint:

- all durable session-related state still has a single owner in `bd-session`
- no per-backend duplication of queue logic exists

Exit criteria:

- new sessions from startup, explicit rotation, and inactivity rollover all produce durable
  started-session state

### Milestone 3: Add `bd-api` State Update Transport

Objective:

- wire the new protocol messages into the mux client

Work:

- update [bd-api/src/api.rs](bd-api/src/api.rs) to populate `HandshakeRequest.state_update`
- add mid-stream `ApiRequest::StateUpdate` sending
- add `Response_type::StateUpdate` handling in response processing
- update [bd-test-helpers/src/test_api_server.rs](bd-test-helpers/src/test_api_server.rs) to
  understand `Request_type::StateUpdate` and emit `StateUpdateResponse`

Review checkpoint:

- `bd-api` owns transport only
- durable session state still comes from `bd-session`

Exit criteria:

- handshake and mid-stream state update messages can be exercised in tests
- atomic batch ack semantics are implemented

### Milestone 4: Rename Opaque User to Opaque Entity

Objective:

- align the client side with the new `opaque_entity_id` protocol shape

Work:

- rename `OPAQUE_USER_ID_KEY` to `OPAQUE_ENTITY_ID_KEY` in [bd-api/src/lib.rs](bd-api/src/lib.rs)
- update [bd-api/src/api.rs](bd-api/src/api.rs) to source entity changes from the new naming
- rename logger-facing APIs in [bd-logger/src/logger.rs](bd-logger/src/logger.rs), for example
  `register_opaque_entity_id`
- add a clear or unset flow so logout can be expressed
- make live entity changes feed into the new state update transport path
- leave opaque entity persistence otherwise unchanged for this pass

Review checkpoint:

- naming is consistent across shared-core
- entity updates can be sent without waiting for reconnect

Exit criteria:

- tests cover initial state, update, and clear behavior

### Milestone 5: Update Constructors and Shared Test Infrastructure

Objective:

- migrate shared-core callers to the new `bd-session` shape

Work:

- update [logger-cli/src/logger.rs](logger-cli/src/logger.rs)
- update [bd-logger/src/test/setup.rs](bd-logger/src/test/setup.rs)
- update direct session strategy constructions in logger and crash-handler tests
- update any helpers that currently assume `bd_key_value`-backed session state

Review checkpoint:

- constructor churn is contained
- no remaining in-workspace `bd-session` callers depend on the old store-based constructors

Exit criteria:

- all affected shared-core call sites compile against the new `Strategy` shape

### Milestone 6: Validation and Cleanup

Objective:

- verify the refactor end to end before server-side work begins

Work:

- add or update `bd-session` tests for:
  - lazy initialization
  - previous-process recovery
  - fixed strategy startup and explicit rotation
  - activity-based rollover and debounce behavior
  - callback-after-unlock ordering
  - persistence corruption recovery
- add or update `bd-api` tests for:
  - handshake includes state update
  - mid-stream state update request emission
  - `StateUpdateResponse` ack handling
  - resend behavior after disconnect without ack
- run validation commands listed below

Review checkpoint:

- no hidden ownership split remains between `bd-session` and `bd-api`
- the client-side design is stable enough for server work to start later

Exit criteria:

- tests and lint pass for touched crates
- plan is approved for implementation continuation

## File Targets

Primary files expected to change:

- [bd-session/src/lib.rs](bd-session/src/lib.rs)
- [bd-session/src/fixed.rs](bd-session/src/fixed.rs)
- [bd-session/src/activity_based.rs](bd-session/src/activity_based.rs)
- [bd-session/src/fixed_test.rs](bd-session/src/fixed_test.rs)
- [bd-session/src/activity_based_test.rs](bd-session/src/activity_based_test.rs)
- [bd-api/src/lib.rs](bd-api/src/lib.rs)
- [bd-api/src/api.rs](bd-api/src/api.rs)
- [bd-api/src/api_test.rs](bd-api/src/api_test.rs)
- [bd-test-helpers/src/test_api_server.rs](bd-test-helpers/src/test_api_server.rs)
- [bd-logger/src/logger.rs](bd-logger/src/logger.rs)
- [bd-logger/src/test/setup.rs](bd-logger/src/test/setup.rs)
- [logger-cli/src/logger.rs](logger-cli/src/logger.rs)

Reference patterns:

- [bd-client-common/src/safe_file_cache.rs](bd-client-common/src/safe_file_cache.rs)
- [bd-client-stats/src/file_manager.rs](bd-client-stats/src/file_manager.rs)

## Validation

Run from [shared-core](/Users/mklein/Source/shared-core):

1. `get_errors`
2. `cargo nextest run -p bd-session`
3. `cargo nextest run -p bd-api`
4. `cargo nextest run -p bd-logger`
5. `cargo nextest run -p bd-crash-handler` if session recovery behavior there changes
6. `cargo clippy -p bd-session --bins --examples --tests -- --no-deps`
7. `cargo clippy -p bd-api --bins --examples --tests -- --no-deps`
8. `cargo clippy -p bd-logger --bins --examples --tests -- --no-deps` when touched
9. `cargo +nightly fmt`
10. `get_errors`

## Resolved Decisions

1. Started-session upload durability should use a single pending durable state. Duplicates are not
  a concern, so there is no need for durable in-flight tracking. The important requirement is
  atomic removal of acknowledged entries.
2. Opaque entity state should stay as-is for now. Do the required rename and protocol plumbing,
  but do not restructure its persistence in this pass.
3. `Strategy` should become a struct with an internal backend enum, because that is the cleanest
  way to centralize orchestration without layering hacks on top of the current public enum shape.

## Recommendation

Start with Milestone 1 and Milestone 2 together. With the simplified queue model, this no longer
requires durable in-flight machinery, so it is the cleanest way to land the persistence shape once
and avoid revisiting `Strategy` orchestration immediately afterward.
