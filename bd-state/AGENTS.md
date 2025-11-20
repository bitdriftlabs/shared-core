# bd-state - Persistent Application State Management

This document provides guidance for AI agents working with the `bd-state` crate, which manages persistent application state across process restarts.

## Purpose and Design

`bd-state` provides a high-level, synchronized wrapper around `bd-resilient-kv` for managing application state with:
- **Persistent storage** with automatic fallback to in-memory mode
- **Scope-based organization** (FeatureFlags, GlobalState) for logical separation
- **Ephemeral scope lifecycle** - scopes are cleared on each process restart
- **Previous state snapshots** for crash reporting and debugging
- **Thread-safe async access** via RwLock for concurrent reads/writes

## Core Concepts

### Scopes
State is organized into scopes defined in `bd-resilient-kv::Scope`:
- `FeatureFlag`: Runtime feature flags and configuration
- `GlobalState`: Application-wide state that should persist

**Critical behavior**: Both scopes are **ephemeral** - they are automatically cleared when a new `Store` is created. This ensures each process starts with fresh state. Consumers must re-populate these values on startup.

### Store Variants
The `Store` type has two internal implementations:
1. **Persistent** (`StoreInner::Persistent`): Backed by `bd-resilient-kv::VersionedKVStore`
2. **In-Memory** (`StoreInner::InMemory`): Fallback using `AHashMap` when persistence fails

Both variants expose the same API, making the distinction transparent to consumers.

### State Lifecycle
1. **Initialization**: `Store::new()` loads existing state from disk
2. **Snapshot capture**: Previous process's state is captured before clearing
3. **Scope clearing**: All ephemeral scopes are cleared for fresh start
4. **Returns**: (Store, DataLoss info, Previous snapshot)

This lifecycle enables crash reporting to access the crashed process's feature flags while ensuring the current process starts clean.

## API Patterns

### Creating a Store

```rust
// With fallback (recommended for production)
let (store, data_loss, prev_snapshot, fallback_occurred) = 
    Store::new_or_fallback(&directory, time_provider).await;

if fallback_occurred {
    log::warn!("Using in-memory state store due to persistence failure");
}

// Without fallback (if you need to handle errors explicitly)
let (store, data_loss, prev_snapshot) = 
    Store::new(&directory, time_provider).await?;

// In-memory only (for tests)
let store = Store::new_in_memory();
```

### Writing State

```rust
// Insert a value
store.insert(Scope::FeatureFlag, "my_flag", "enabled".to_string()).await?;

// Remove a value
store.remove(Scope::GlobalState, "temp_key").await?;

// Clear an entire scope
store.clear(Scope::FeatureFlag).await?;
```

### Reading State

```rust
// Acquire a reader (holds read lock)
let reader = store.read().await;

// Get a single value
if let Some(value) = reader.get(Scope::FeatureFlag, "my_flag") {
    println!("Flag value: {}", value);
}

// Iterate all entries
for entry in reader.iter() {
    println!("{:?}: {} = {}", entry.scope, entry.key, entry.value);
}

// Create a snapshot of a specific scope
let feature_flags = reader.to_scoped_snapshot(Scope::FeatureFlag);

// Create a snapshot of all scopes (more efficient than multiple calls)
let snapshot = reader.to_snapshot();
```

### StateReader Trait
The `StateReader` trait provides a type-generic interface for reading state:
- `get(scope, key)` - Get a single value
- `iter()` - Iterator over all entries
- `to_scoped_snapshot(scope)` - Owned snapshot of one scope
- `to_snapshot()` - Owned snapshot of all scopes

The `read()` method returns a guard implementing `StateReader`, allowing for flexible state access patterns.

## Integration Points

### bd-logger
- Uses bd-state to track feature flags and global state
- Passes state_store to async_log_buffer for log processing
- Runtime flag `global_state.use_persistent_storage` controls persistence

### bd-crash-handler
- Receives previous process snapshot during initialization
- Includes previous state (especially feature flags) in crash reports
- Helps diagnose crashes related to specific feature flag combinations

## Testing Patterns

### Test Helpers
The `bd_state::test` module provides utilities for testing:

```rust
use bd_state::test::TestStore;

// Create a simple test store
let store = TestStore::new();

// Insert test data
store.insert(Scope::FeatureFlag, "test_flag", "value").await.unwrap();

// Read and verify
let reader = store.read().await;
assert_eq!(reader.get(Scope::FeatureFlag, "test_flag"), Some("value"));
```

### Testing Persistence
```rust
use tempfile::TempDir;

let temp_dir = TempDir::new()?;
let time_provider = Arc::new(bd_time::SystemTimeProvider);

// First process
{
    let (store, _, _) = Store::new(temp_dir.path(), time_provider.clone()).await?;
    store.insert(Scope::GlobalState, "key", "value".to_string()).await?;
}

// Second process (simulated restart)
{
    let (store, _, prev_snapshot) = Store::new(temp_dir.path(), time_provider).await?;
    
    // Current state is empty (scopes are ephemeral)
    let reader = store.read().await;
    assert_eq!(reader.get(Scope::GlobalState, "key"), None);
    
    // But previous state is captured in snapshot
    assert_eq!(prev_snapshot.global_state.get("key"), Some(&("value".to_string(), timestamp)));
}
```

### Testing Fallback Behavior
```rust
// Test fallback by providing invalid directory
let invalid_path = Path::new("/nonexistent/path");
let (store, data_loss, snapshot, fallback_occurred) = 
    Store::new_or_fallback(invalid_path, time_provider).await;

assert!(fallback_occurred);
assert!(data_loss.is_none());
assert!(snapshot.feature_flags.is_empty());

// Store still works, just in-memory
store.insert(Scope::FeatureFlag, "test", "value".to_string()).await.unwrap();
```

## Common Pitfalls

### 1. Expecting State to Persist Across Restarts
**Wrong assumption**: "State written to the store will be available after restart"
**Reality**: All scopes are ephemeral - they're cleared on each `Store::new()` call

**Why**: This design ensures processes start with a clean slate, preventing stale state from affecting behavior. The previous snapshot allows debugging while keeping runtime state fresh.

### 2. Not Handling Fallback
**Wrong pattern**:
```rust
let (store, _, _) = Store::new(&directory, time_provider).await?;
// What if persistence fails? App crashes.
```

**Right pattern**:
```rust
let (store, _, _, fallback) = Store::new_or_fallback(&directory, time_provider).await;
if fallback {
    metrics.increment("state_store_fallback");
    log::warn!("Using in-memory state store");
}
```

### 3. Holding Read Locks Too Long
**Wrong pattern**:
```rust
let reader = store.read().await;
// ... do expensive computation ...
// ... make network calls ...
let value = reader.get(scope, key); // Still holding lock!
```

**Right pattern**:
```rust
let value = {
    let reader = store.read().await;
    reader.get(scope, key).map(|s| s.to_string())
}; // Lock released
// Now do expensive work with value
```

### 4. Multiple Snapshot Calls
**Inefficient**:
```rust
let feature_flags = reader.to_scoped_snapshot(Scope::FeatureFlag);
let global_state = reader.to_scoped_snapshot(Scope::GlobalState);
// Iterates the store twice
```

**Efficient**:
```rust
let snapshot = reader.to_snapshot();
// Iterates once, populates both maps
let feature_flags = &snapshot.feature_flags;
let global_state = &snapshot.global_state;
```

## Runtime Configuration

The `global_state.use_persistent_storage` runtime flag (in `bd-runtime`) controls storage backend selection:
- `true` (default): Use persistent storage, fallback to in-memory if needed
- `false`: Always use in-memory storage

This flag is checked during logger initialization in `bd-logger/src/builder.rs`.

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

## Future Enhancements

Potential improvements to consider:
1. **Bulk operations**: Add `insert_multiple()` for efficient batch writes
2. **TTL support**: Automatic expiration for time-limited state
3. **Change notifications**: Watchers for specific keys or scopes
4. **Non-ephemeral scopes**: Add persistent scopes for truly durable state
5. **Metrics**: Track read/write rates, snapshot sizes, fallback frequency

## Dependencies

- `bd-resilient-kv`: Underlying versioned KV store (see bd-resilient-kv/AGENTS.md)
- `bd-proto`: Protobuf definitions for StateValue serialization
- `bd-time`: TimeProvider for timestamp generation
- `tokio`: Async runtime for RwLock and async operations

## Summary

`bd-state` provides a simple, robust abstraction for managing ephemeral application state with persistent storage and automatic fallback. Key design principles:
- **Ephemeral by default**: All state is cleared on restart
- **Snapshot preservation**: Previous state captured for debugging
- **Transparent fallback**: In-memory mode when persistence unavailable
- **Type-safe reading**: StateReader trait for flexible access patterns
- **Async-first**: All operations designed for async/await contexts

When working with bd-state, remember that it's designed for runtime state management, not long-term persistence. For durable storage, use bd-resilient-kv directly or another appropriate mechanism.
