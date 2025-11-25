# bd-state - Persistent Application State Management

This document provides guidance for AI agents working with the `bd-state` crate, which manages persistent application state across process restarts.

## Purpose and Design

`bd-state` provides a high-level, synchronized wrapper around `bd-resilient-kv` for managing application state with:
- **Ephemeral scope lifecycle** - scopes are cleared on each process restart
- **Previous state snapshots** for crash reporting and debugging
- **Thread-safe async access** via RwLock for concurrent reads/writes

## Core Concepts

### Scopes
State is organized into scopes defined in `bd-resilient-kv::Scope`:
- `FeatureFlag`: Runtime feature flags and configuration
- `GlobalState`: Application-wide state that should persist

**Critical behavior**: Both scopes are **ephemeral** - they are automatically cleared when a new `Store` is created. This ensures each process starts with fresh state. Consumers must re-populate these values on startup.

### State Lifecycle
1. **Initialization**: `Store::new()` loads existing state from disk
2. **Snapshot capture**: Previous process's state is captured before clearing
3. **Scope clearing**: All ephemeral scopes are cleared for fresh start
4. **Returns**: (Store, DataLoss info, Previous snapshot)

This lifecycle enables crash reporting to access the crashed process's feature flags while ensuring the current process starts clean.

### StateReader Trait
The `StateReader` trait provides a type-generic interface for reading state:
- `get(scope, key)` - Get a single value
- `iter()` - Iterator over all entries

The `read()` method returns a guard implementing `StateReader`, allowing for flexible state access patterns.

## Performance Characteristics

### Read Performance
- **Lock contention**: Multiple concurrent readers are supported via RwLock
- **Read latency**: O(1) hash lookups via `get()`, O(n) for `iter()`
- **Snapshot cost**: O(n) where n is number of entries in the scope

### Write Performance
- **Lock contention**: Writes acquire exclusive lock, blocking all readers
- **Write latency**: Async write to underlying journal with compression
- **Batch operations**: Currently no bulk insert - consider adding if needed

### Storage Efficiency
- **Compression**: Archived journals are compressed with zlib
- **Rotation**: Automatic journal rotation when size threshold exceeded
- **Format**: Protobuf serialization via `bd-resilient-kv`

## Summary

`bd-state` provides a simple, robust abstraction for managing ephemeral application state with persistent storage and automatic fallback. Key design principles:
- **Ephemeral by default**: All state is cleared on restart
- **Snapshot preservation**: Previous state captured for debugging
- **Transparent fallback**: In-memory mode when persistence unavailable
- **Type-safe reading**: StateReader trait for flexible access patterns
- **Async-first**: All operations designed for async/await contexts

When working with bd-state, remember that it's designed for runtime state management, not long-term persistence.
