# Batch Operations and Scope Clearing - Design Discussion

## Overview

This document discusses implementation options for batch operations in `VersionedKVStore`, particularly:
1. Batch insert operations
2. Scope clearing operations (especially important for process startup)

## Current State

The `VersionedKVStore` currently has:
- Individual `insert()` and `remove()` operations (both async)
- `ScopedMaps` structure that separates `feature_flags` and `global_state` `AHashMap`s
- Individual writes to the memory-mapped journal via `MemMappedVersionedJournal`

Note: The older `KVStore` system already has `set_multiple()` batch operations (see AGENTS.md), but `VersionedKVStore` does not.

## Implementation Options

### 1. Batch Insert Operation

#### Option A: Transactional Batch (Recommended)
```rust
pub async fn insert_multiple(
  &mut self,
  entries: impl IntoIterator<Item = (Scope, String, StateValue)>,
) -> Result<Vec<(String, u64)>, UpdateError>
```

**Pros:**
- Single timestamp check/allocation for the entire batch (more efficient)
- One rotation check at the end instead of per-operation
- Better performance: reduces repeated `is_high_water_mark_triggered()` checks
- Can optimize journal writes (fewer small writes, better cache locality)

**Cons:**
- All-or-nothing semantics might not be desired (though partial failures are complex)
- Need to handle buffer overflow mid-batch (may need to rotate and retry)

**Implementation approach:**
- Collect entries into a Vec
- Loop through and write to journal without checking high water mark each time
- Batch update the `cached_map` 
- Single high water mark check and rotation at the end
- Return Vec of (key, timestamp) pairs

#### Option B: Optimized Individual Inserts
```rust
pub async fn insert_multiple(
  &mut self,
  entries: impl IntoIterator<Item = (Scope, String, StateValue)>,
) -> Result<Vec<Result<u64, UpdateError>>, UpdateError>
```

**Pros:**
- Simpler to implement (reuse existing logic)
- Each insert can succeed/fail independently
- Easier error handling

**Cons:**
- Less efficient (repeated rotation checks)
- Multiple potential rotations mid-batch
- Doesn't leverage batch nature for optimization

### 2. Scope Clearing Operation

This is the most important operation since it happens on every process start. Several approaches:

#### Option A: Clear by Scope with Tombstone Writes (Explicit)
```rust
pub async fn clear_scope(&mut self, scope: Scope) -> Result<Vec<u64>, UpdateError>
```

**Implementation:**
- Iterate through all keys in the scope's hashmap
- Write tombstone (empty `StateValue`) for each key to journal
- Clear the scope's hashmap
- Return timestamps of all deletions

**Pros:**
- Explicit audit trail in journal (you can see when scope was cleared)
- Maintains versioned history (useful for recovery/debugging)
- Consistent with individual deletes

**Cons:**
- Potentially many journal writes on startup
- If scope has 1000 entries, writes 1000 tombstones
- Can trigger rotation during startup (not ideal)

#### Option B: Batch Tombstone Write (Optimized Explicit)
```rust
pub async fn clear_scope(&mut self, scope: Scope) -> Result<u64, UpdateError>
```

**Implementation:**
- Get all keys from scope's hashmap
- Write a batch of tombstones in one operation (similar to batch insert)
- Clear scope's hashmap
- Return single timestamp for the batch operation

**Pros:**
- Better performance than Option A
- Still maintains audit trail
- Single timestamp simplifies reasoning

**Cons:**
- Still writes many entries to journal
- May require journal format changes to support batch tombstones efficiently
- Can still trigger rotation on startup

#### Option C: In-Memory Clear with Rotation Marker (Recommended for startup)
```rust
pub async fn clear_scope(&mut self, scope: Scope) -> Result<(), UpdateError>
```

**Implementation:**
- Simply clear the scope's hashmap in memory
- Write a special "scope clear" marker to journal OR just rely on rotation
- Trigger rotation immediately after clearing
- The rotation creates a new journal with only the remaining (non-cleared) scope

**Pros:**
- **Extremely efficient for startup**: O(1) journal writes instead of O(n)
- Rotation naturally compacts away the cleared entries
- No per-key tombstones needed
- Perfect for "start fresh" semantics

**Cons:**
- Forces rotation (but this might be desirable on startup anyway)
- Less granular audit trail (you know scope was cleared but not each key)
- Requires rotation to persist the clear operation

#### Option D: Hybrid Approach (Context-Dependent)
Provide both clearing mechanisms:

```rust
// For startup/bulk clearing - efficient
pub async fn clear_scope_fast(&mut self, scope: Scope) -> Result<(), UpdateError>

// For audited clearing - explicit tombstones
pub async fn clear_scope_audited(&mut self, scope: Scope) -> Result<Vec<u64>, UpdateError>
```

### 3. Journal Format Considerations

You might want to add a new frame type for batch operations:

```rust
enum FrameType {
  SingleEntry,
  BatchStart,    // Marker for batch begin
  BatchEnd,      // Marker for batch end  
  ScopeClear,    // Special marker for scope clearing
}
```

This would allow:
- Atomic batch semantics in recovery
- Efficient scope clearing without individual tombstones
- Better recovery logic (can skip cleared scopes efficiently)

### 4. Specific Recommendations

Given that **scope clearing happens on every process start**, I'd recommend:

**For Scope Clearing:**
- **Use Option C (In-Memory Clear with Immediate Rotation)** for the common startup case
- Possibly offer Option B (Batch Tombstone) if you need audit trails
- The startup path should be: clear scope(s) → rotate once → done

**For Batch Insert:**
- **Use Option A (Transactional Batch)** with retry logic similar to `KVStore::set_multiple()`
- Handle mid-batch rotation by retrying on the new journal
- Return Vec of (key, timestamp) tuples

**Implementation Priority:**
1. `clear_scope()` with rotation (highest value for startup performance)
2. `insert_multiple()` for bulk operations
3. Optional: `clear_scope_audited()` if audit trail is needed

### 5. Example Implementation Sketch

```rust
impl VersionedKVStore {
  /// Clear all entries in a scope efficiently.
  /// This triggers an immediate rotation to compact away the cleared entries.
  pub async fn clear_scope(&mut self, scope: Scope) -> Result<(), UpdateError> {
    match &mut self.backend {
      StoreBackend::Persistent(store) => {
        // Clear in-memory state
        match scope {
          Scope::FeatureFlag => store.cached_map.feature_flags.clear(),
          Scope::GlobalState => store.cached_map.global_state.clear(),
        }
        
        // Rotate to persist the cleared state
        // The new journal will only contain the non-cleared scope
        store.rotate_journal().await?;
        
        Ok(())
      },
      StoreBackend::InMemory(store) => {
        match scope {
          Scope::FeatureFlag => store.cached_map.feature_flags.clear(),
          Scope::GlobalState => store.cached_map.global_state.clear(),
        }
        Ok(())
      },
    }
  }

  /// Insert multiple entries efficiently in a single batch.
  pub async fn insert_multiple(
    &mut self,
    entries: Vec<(Scope, String, StateValue)>,
  ) -> Result<Vec<u64>, UpdateError> {
    match &mut self.backend {
      StoreBackend::Persistent(store) => {
        let mut timestamps = Vec::with_capacity(entries.len());
        
        for (scope, key, value) in entries {
          let timestamp = store.journal.insert_entry(scope, &key, value.clone())?;
          
          if value.value_type.is_none() {
            store.cached_map.remove(scope, &key);
          } else {
            store.cached_map.insert(
              scope,
              key,
              TimestampedValue { value, timestamp },
            );
          }
          
          timestamps.push(timestamp);
        }
        
        // Check rotation once at the end
        if store.journal.is_high_water_mark_triggered() {
          store.rotate_journal().await?;
        }
        
        Ok(timestamps)
      },
      StoreBackend::InMemory(store) => {
        let mut timestamps = Vec::with_capacity(entries.len());
        
        for (scope, key, value) in entries {
          let timestamp = store.insert(scope, &key, &value)?;
          timestamps.push(timestamp);
        }
        
        Ok(timestamps)
      },
    }
  }
}
```

### 6. Performance Considerations

**Scope Clearing Performance:**
- Option A (individual tombstones): O(n) journal writes where n = number of keys in scope
- Option B (batch tombstones): O(n) entries but potentially fewer disk operations
- Option C (rotation): O(1) journal operations, O(m) where m = keys in remaining scope(s)

For startup with 1000 feature flags to clear:
- Option A: 1000 journal writes + potential rotation
- Option C: 1 rotation (writes only GlobalState entries if any)

**Batch Insert Performance:**
- Single high water mark check vs. per-insert checks
- Better cache locality when writing consecutive entries
- Reduced syscall overhead for persistent stores

### 7. Error Handling Considerations

**Batch Insert Errors:**
- Mid-batch rotation failure: Need to decide on rollback vs. partial success
- Buffer overflow mid-batch: Rotate and retry remaining entries
- Individual entry serialization failure: Skip entry or abort batch?

**Scope Clear Errors:**
- Rotation failure after clear: In-memory state is cleared but not persisted
  - Could restore from old journal
  - Or treat as acceptable (will be re-cleared on next startup)
- Should clearing an already-empty scope be an error or no-op?

### 8. Testing Strategy

**Key Test Cases:**
1. **Batch Insert:**
   - Large batches that trigger rotation mid-operation
   - Mixed scopes in single batch
   - Batch with some delete operations (null values)
   - Recovery after partial batch write

2. **Scope Clearing:**
   - Clear empty scope
   - Clear scope with 1000s of entries
   - Clear scope then immediately write to it
   - Recovery after clear but before rotation
   - Clear both scopes in sequence

3. **Integration:**
   - Clear scope + batch insert in same session
   - Multiple batch operations with rotations
   - Concurrent operations (if applicable)

### 9. Alternative: Deferred Rotation

Instead of immediate rotation after scope clear, could defer it:

```rust
impl PersistentStore {
  clear_requested: bool,
  
  pub async fn clear_scope(&mut self, scope: Scope) -> Result<(), UpdateError> {
    // Clear in memory
    match scope {
      Scope::FeatureFlag => self.cached_map.feature_flags.clear(),
      Scope::GlobalState => self.cached_map.global_state.clear(),
    }
    
    // Mark for rotation but don't rotate yet
    self.clear_requested = true;
    Ok(())
  }
  
  async fn insert(&mut self, ...) -> Result<u64, UpdateError> {
    // ... normal insert logic ...
    
    // Rotate if clear was requested OR high water mark triggered
    if self.clear_requested || self.journal.is_high_water_mark_triggered() {
      self.rotate_journal().await?;
      self.clear_requested = false;
    }
    
    Ok(timestamp)
  }
}
```

This defers the rotation cost until the next write operation, but adds state tracking complexity.

### 10. Summary

The rotation-based clearing approach (Option C) is particularly elegant because it:
- Leverages existing rotation machinery
- Naturally compacts the journal
- Provides excellent startup performance
- Maintains data integrity through the journal format

Combined with transactional batch inserts, this provides efficient bulk operations while maintaining the versioned semantics and recovery capabilities of the system.
