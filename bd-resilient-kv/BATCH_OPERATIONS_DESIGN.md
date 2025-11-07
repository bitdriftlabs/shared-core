# Batch Operations Design for VersionedKVStore

## Context

The versioned KV store needs efficient batch operations for use cases like:
- Clearing all state on app startup (while preserving journal history)
- Bulk insertions/deletions (e.g., clearing keys with specific prefix)
- Reducing overhead from repeated timestamp syscalls and rotation checks

## Key Insight: Version Transparency

**Users don't need to worry about internal version splitting.** If a batch operation spans multiple versions internally due to capacity constraints, the returned version still accurately represents the final state after all operations complete.

### Example:
```rust
// User calls:
let v = store.insert_batch(100_entries).await?;

// Internally might use:
// v-2: entries 1-50
// v-1: entries 51-80  
// v: entries 81-100

// User receives v, and at version v, all 100 entries are present ✅
```

This works because:
1. Single-threaded context (no concurrent observers)
2. Version number represents "state identifier" not "operation identifier"
3. By the time function returns, all entries are in `cached_map`

## Critical Problem: Mid-Batch Rotation

### The Issue

When rotation happens mid-batch:
- Rotation compacts state from `cached_map` to new journal
- New journal is NOT empty - it contains all current state
- Available space after rotation = buffer_size - compacted_state_size
- **Pre-flight rotation check doesn't guarantee batch will fit**

### Example Scenario:
```
cached_map has 500 keys (50KB when serialized)
buffer_size = 64KB
Batch of 200 keys (20KB estimated)

After rotation:
- New journal has 500 keys = 50KB
- Available space = 64KB - 50KB = 14KB
- Batch needs 20KB
- ❌ Still doesn't fit!
```

## Solution: Transparent Chunking

Let batch operations automatically split when needed, but return single version representing final state.

### API Design

```rust
impl VersionedKVStore {
    /// Insert multiple key-value pairs efficiently.
    /// 
    /// Returns the final version after all entries have been written.
    /// Entries are written in a batch to minimize overhead (shared timestamp,
    /// deferred rotation checks). If the batch is too large to fit in available
    /// journal space, it will be automatically split across multiple versions.
    /// 
    /// The returned version represents the state after ALL entries have been
    /// applied, regardless of whether they were split internally.
    pub async fn insert_batch(
        &mut self, 
        entries: Vec<(String, Value)>
    ) -> anyhow::Result<u64> {
        if entries.is_empty() {
            return Ok(self.current_version());
        }
        
        let timestamp = current_timestamp()?;
        
        for (key, value) in entries {
            // Check if we have space for this entry
            let estimated_size = self.estimate_entry_size(&key, &value);
            
            if self.journal.remaining_capacity() < estimated_size {
                // Rotate to make space
                self.rotate_journal().await?;
                
                // Verify single entry fits after rotation
                if self.journal.remaining_capacity() < estimated_size {
                    anyhow::bail!(
                        "Single entry too large: key='{}', size={} bytes, available={} bytes",
                        key, estimated_size, self.journal.remaining_capacity()
                    );
                }
            }
            
            // Write entry
            self.current_version += 1;
            let version = self.current_version;
            self.journal.write_versioned_entry_with_timestamp(
                version, &key, &value, timestamp
            )?;
            self.cached_map.insert(key, TimestampedValue { value, timestamp });
        }
        
        // Check if we should rotate after batch
        if self.journal.is_high_water_mark_triggered() {
            self.rotate_journal().await?;
        }
        
        Ok(self.current_version())
    }
    
    /// Delete multiple keys efficiently.
    pub async fn delete_batch(&mut self, keys: Vec<String>) -> anyhow::Result<u64> {
        if keys.is_empty() {
            return Ok(self.current_version());
        }
        
        let timestamp = current_timestamp()?;
        
        for key in keys {
            // Deletion entries are small (null value), less likely to overflow
            if self.journal.remaining_capacity() < 100 {  // Conservative
                self.rotate_journal().await?;
            }
            
            self.current_version += 1;
            let version = self.current_version;
            self.journal.write_versioned_entry_with_timestamp(
                version, &key, &Value::Null, timestamp
            )?;
            self.cached_map.remove(&key);
        }
        
        if self.journal.is_high_water_mark_triggered() {
            self.rotate_journal().await?;
        }
        
        Ok(self.current_version())
    }
    
    /// Clear all keys in the store.
    pub async fn clear_all(&mut self) -> anyhow::Result<u64> {
        let keys: Vec<String> = self.cached_map.keys().cloned().collect();
        self.delete_batch(keys).await
    }
    
    /// Clear all keys with the given prefix.
    pub async fn clear_prefix(&mut self, prefix: &str) -> anyhow::Result<u64> {
        let keys: Vec<String> = self.cached_map
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        self.delete_batch(keys).await
    }
}
```

## Implementation Requirements

### 1. Add Helper Methods to VersionedKVJournal

```rust
impl VersionedKVJournal<'_> {
    /// Write a versioned entry with a pre-computed timestamp.
    /// This enables batch operations to share a single timestamp.
    pub fn write_versioned_entry_with_timestamp(
        &mut self,
        version: u64,
        key: &str,
        value: &Value,
        timestamp: u64,
    ) -> anyhow::Result<()> {
        // Similar to write_versioned_entry but uses provided timestamp
    }
    
    /// Get remaining capacity in bytes.
    pub fn remaining_capacity(&self) -> usize {
        self.buffer.len() - self.position
    }
}
```

### 2. Add Estimation Helpers to VersionedKVStore

```rust
impl VersionedKVStore {
    /// Estimate bytes needed for a single entry
    fn estimate_entry_size(&self, key: &str, value: &Value) -> usize {
        // Conservative estimate:
        // - Version: 10 bytes
        // - Timestamp: 10 bytes  
        // - Key: key.len() + 10 bytes overhead
        // - Value: estimated_value_size(v) + 10 bytes overhead
        // - Object overhead: ~20 bytes
        60 + key.len() + self.estimate_value_size(value)
    }
    
    /// Estimate bytes needed for value
    fn estimate_value_size(&self, value: &Value) -> usize {
        match value {
            Value::Null => 1,
            Value::Bool(_) => 1,
            Value::Signed(_) | Value::Unsigned(_) => 9,
            Value::Float(_) => 9,
            Value::String(s) => s.len() + 5,
            Value::Array(arr) => arr.iter().map(|v| self.estimate_value_size(v)).sum::<usize>() + 5,
            Value::Object(obj) => obj.iter().map(|(k, v)| k.len() + self.estimate_value_size(v)).sum::<usize>() + 5,
            Value::Binary(b) => b.len() + 5,
        }
    }
}
```

## Benefits

1. **Simple API**: Returns single version representing final state
2. **Transparent**: Automatic capacity handling, no user intervention needed
3. **Efficient**: Shared timestamp within natural chunks
4. **Always succeeds**: No "batch too large" errors (except if single entry > buffer)
5. **Correct semantics**: Final version accurately represents state
6. **Single-threaded safe**: No intermediate state observation issues

## Trade-offs

### What We Give Up:
- Guarantee that batch uses single version number internally
- Ability to say "these N operations happened atomically at version V"

### What We Keep:
- ✅ Final state correctness
- ✅ Simple API (returns single version)
- ✅ Efficient batch processing (50-80% overhead reduction from original analysis)
- ✅ Automatic capacity management
- ✅ Full audit trail (can still see all operations in journal)

## Use Cases

### Startup State Reset
```rust
// On app startup
let mut store = VersionedKVStore::open_existing(path, name, size, ratio)?;

// Log what state existed before (for recovery/debugging)
let previous_version = store.current_version();
let previous_keys = store.len();
log::info!("Recovered state from version {previous_version} with {previous_keys} keys");

// Clear all state to start fresh session
let clear_version = store.clear_all().await?;
log::info!("Started new session at version {clear_version}");
```

### Prefix-Based Operations
```rust
// Clear all metrics
store.clear_prefix("metrics:").await?;

// Clear all temporary data
store.clear_prefix("temp:").await?;

// Clear user-specific data
store.clear_prefix("user:123:").await?;
```

### Bulk Updates
```rust
// Bulk insert metrics
let metrics = vec![
    ("metrics:cpu".into(), Value::from(50)),
    ("metrics:mem".into(), Value::from(80)),
    ("metrics:disk".into(), Value::from(60)),
];
let version = store.insert_batch(metrics).await?;
```

## Testing Requirements

1. **Basic batch operations**
   - Empty batch
   - Small batch (fits in one chunk)
   - Large batch (requires splitting)

2. **Capacity edge cases**
   - Batch that exactly fills buffer
   - Batch that triggers rotation mid-way
   - Batch that requires multiple rotations
   - Single entry too large (should error)

3. **State correctness**
   - Verify all entries present after batch completes
   - Verify version monotonicity
   - Verify timestamp consistency within chunks
   - Verify rotation doesn't lose data

4. **Prefix operations**
   - Clear prefix with no matches
   - Clear prefix with some matches
   - Clear prefix with all keys matching
   - Overlapping prefixes

5. **Recovery scenarios**
   - Replay journal with split batch operations
   - Verify final state matches expected

## Open Questions

None - design is ready for implementation.

## References

- Original optimization analysis: 50-80% overhead reduction from batch operations
- Current single-operation overhead: timestamp syscall + HashMap allocation + encoding + rotation check per operation
- Target: Amortize overhead across batch
