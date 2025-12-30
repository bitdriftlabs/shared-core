# KV Journal System - Understanding and Insights

This document provides insights and understanding about the `bd-resilient-kv` journal system to help AI agents work effectively with this codebase.

## Core Architecture

The `bd-resilient-kv` library provides a versioned storage model:

**VersionedKVStore**: Version-tracked store with point-in-time recovery and automatic rotation
- Best for: Audit logs, state history, remote backup
- Architecture: Single journal with archived versions
- Rotation: Creates new journal with compacted state
- Timestamp tracking: Every write returns a timestamp
- Format: Protobuf-based entries (VERSION 1)

### Key Implementations

1. **VersionedKVJournal**: Versioned journal with entry-level version tracking
2. **MemMappedVersionedKVJournal**: Memory-mapped wrapper for versioned journals
3. **VersionedKVStore**: High-level API for versioned key-value storage with automatic rotation

### Versioned Storage Architecture

The `VersionedKVStore` provides a higher-level API built on top of `VersionedKVJournal`:

**Key Components**:
- **VersionedKVJournal**: Low-level journal that tracks timestamps for each entry
- **MemMappedVersionedKVJournal**: Memory-mapped persistence layer
- **VersionedKVStore**: High-level HashMap-like API with automatic rotation and async write operations

**Async API**:
- Write operations (`insert()`, `remove()`, `rotate_journal()`) are async and require a Tokio runtime
- Compression of archived journals is performed asynchronously using streaming I/O
- Read operations remain synchronous and operate on the in-memory cache
- The async API enables efficient background compression without blocking the main thread

**Version Tracking**:
- Every write operation (`insert`, `remove`) returns a monotonically non-decreasing timestamp (microseconds since UNIX epoch)
- Timestamps serve as both version identifiers and logical clocks
- If the system clock goes backward, timestamps are clamped to the last timestamp to maintain monotonicity
- Entries with `Value::Null` are treated as deletions but still timestamped
- During rotation, snapshot entries preserve their original timestamps

**Timestamp Tracking**:
- Each entry records a timestamp (microseconds since UNIX epoch) when the write occurred
- Timestamps are monotonically non-decreasing, not strictly increasing
- Multiple entries may share the same timestamp if the system clock doesn't advance between writes
- This is expected behavior, particularly during rapid writes or in test environments
- Recovery at timestamp T includes ALL entries with timestamp ≤ T, applied in version order
- Timestamps are preserved during rotation, maintaining temporal accuracy for audit purposes
- Test coverage: `test_timestamp_collision_across_rotation` in `versioned_recovery_test.rs`

**Rotation Strategy**:
- Automatic rotation when journal size exceeds high water mark (triggered during async write operations)
- Current state is compacted into a new journal as versioned entries
- Old journal is archived with `.t{timestamp}.zz` suffix
- Archived journals are automatically compressed using zlib (RFC 1950, level 5) asynchronously

**Compression**:
- All archived journals are automatically compressed during rotation using async I/O
- Active journals remain uncompressed for write performance
- Compression uses zlib format (RFC 1950) with level 5 for balanced speed/ratio
- Streaming compression avoids loading entire journals into memory
- Typical compression achieves >50% size reduction for text-based data
- File extension `.zz` indicates compressed archives
- Recovery transparently decompresses archived journals when needed

**Point-in-Time Recovery**:
The `VersionedRecovery` utility provides point-in-time recovery by replaying journal entries up to a target timestamp. It works with raw uncompressed journal bytes and can reconstruct state at any historical timestamp across rotation boundaries. Recovery is optimized: `recover_current()` only reads the last journal (since rotation writes complete compacted state), while `recover_at_timestamp()` intelligently selects and replays only necessary journals. The `new()` constructor accepts a vector of `(&[u8], u64)` tuples (byte slice and snapshot timestamp). Callers must decompress archived journals before passing them to the constructor.

## Critical Design Insights

### 1. Versioned Storage Model

**VersionedKVStore (Single Journal with Rotation)**:
- Best for: Audit logs, state history, remote backup
- Architecture: Single journal with archived versions
- Rotation: Creates new journal with compacted state
- Timestamp tracking: Every write returns a timestamp
- Format: Protobuf-based entries (VERSION 1)

### 2. Compaction Efficiency
**Key Insight**: Compaction via `reinit_from()` is already maximally efficient. It writes data in the most compact possible serialized form (hashmap → bytes). If even this compact representation exceeds high water marks, then the data volume itself is the limiting factor, not inefficient storage.

**Implication**: Never assume compaction can always solve high water mark issues. Sometimes both buffers are legitimately full.

### 3. Journal Rotation Strategy

**When Rotation Occurs**:
- Triggered during `insert()` or `remove()` when journal size exceeds high water mark
- Can be manually triggered via `rotate_journal()`
- Automatic and transparent to the caller

**Rotation Process**:
- Archives old journal with `.t{timestamp}` suffix
- Creates new journal with compacted current state
- Asynchronously compresses archived journal to `.t{timestamp}.zz`
- Preserves complete history for point-in-time recovery

### 5. Simplified High Water Mark Detection

The system uses a straightforward approach to high water mark detection (internal to journal implementation):

```rust
// Check if high water mark is triggered
if journal.is_high_water_mark_triggered() {
    // React to high water mark - typically involves compaction or rotation
}
```

**Benefits**:
- Simple, clear API
- No callback complexity or thread safety concerns
- Direct control over when to check status

## Testing Strategies

### Effective Test Patterns

1. **Repeated Key Updates**: Create journal bloat by updating same keys multiple times
   ```rust
   for round in 0..20 {
       for key in ["key1", "key2", "key3"] {
           store.insert(scope, key, value).await?;
       }
   }
   // Journal has 60 entries but only 3 unique keys
   // Rotation reduces to 3 final entries
   ```

2. **Point-in-Time Recovery**: Test timestamp-based recovery
   ```rust
   let ts1 = store.insert(scope, "key1", value1).await?;
   let ts2 = store.insert(scope, "key2", value2).await?;
   let ts3 = store.remove(scope, "key1").await?;

   // Recover at ts2 should have both keys
   let state_at_ts2 = recovery.recover_at_timestamp(ts2)?;
   assert!(state_at_ts2.contains("key1"));
   ```

3. **Rotation Scenarios**: Test automatic rotation on high water mark
   ```rust
   let config = PersistentStoreConfig {
       buffer_size: 1024,  // Small buffer
       high_water_mark_ratio: 0.8,
   };

   // Write until rotation occurs
   for i in 0..100 {
       store.insert(scope, &format!("key_{}", i), large_value).await?;
   }

   // Verify archived journals exist
   assert!(archived_journals_exist());
   ```

4. **Compression Verification**: Verify archived journals are compressed
   ```rust
   // Trigger rotation
   store.rotate_journal().await?;

  // Check that archived journal has .zz extension
   // and is smaller than original
   ```

### Test Expectations

- **Successful Rotation**: Archived journals created with proper timestamps
- **Compression**: Archived journals have `.zz` extension and are compressed
- **Point-in-Time Recovery**: Can reconstruct state at any historical timestamp
- **Async Operations**: Write operations complete asynchronously
- **Error Scenarios**: Actual errors should be propagated, not masked

## Failure Modes: What to Handle vs What's Impossible

### Failure Modes Applications Must Handle

1. **Buffer Full During Normal Writes**
   - **When**: Writing to journal when buffer is at capacity
   - **Result**: Write operations return error
   - **Action**: Automatic rotation will occur on next write attempt
   - **Note**: VersionedKVStore handles rotation automatically during write operations

2. **I/O Errors During Persistence**
   - **When**: File operations fail (disk full, permissions, etc.)
   - **Result**: I/O errors propagated from memory-mapped operations
   - **Action**: Handle as standard I/O errors

3. **Compression/Archive Errors**
   - **When**: Asynchronous compression of archived journal fails
   - **Result**: Error during rotation's async compression phase
   - **Action**: Retry compression, handle cleanup appropriately

4. **Recovery Errors**
   - **When**: Archived journals are corrupted or missing
   - **Result**: Recovery operations fail or return incomplete state
   - **Action**: Handle missing data gracefully, implement backup strategies

### Impossible Failure Modes (Architectural Guarantees)

1. **Timestamp Overflow**
   - **Why Practically Impossible**: Uses u64 for microsecond timestamps, would require 584,000+ years to overflow (u64::MAX microseconds ≈ year 586,524 CE)
   - **Implication**: No overflow handling needed in practice

## Common Pitfalls

### 1. Assuming Rotation Always Solves Space Issues
**Wrong Assumption**: "If we trigger rotation, the buffer will have enough space"
**Reality**: Rotation reduces size but unique data still needs to fit in buffer

### 2. Forgetting Async Context
**Wrong**: Calling write operations without `.await` or outside async context
**Right**: All write operations require async context and must be awaited

### 3. Error Handling
**Wrong**: Ignoring errors from write operations
**Right**: Properly handle and propagate errors, especially I/O and rotation failures

### 4. Point-in-Time Recovery Assumptions
**Wrong**: Assuming timestamps are strictly increasing
**Right**: Timestamps are monotonically non-decreasing (multiple entries can have same timestamp)

## Key Methods and Their Purposes

### `insert(scope, key, value)` (VersionedKVStore)
- **Purpose**: Insert or update a key-value pair in the specified scope
- **Behavior**: Async operation that writes to journal and updates cache
- **Returns**: Timestamp of the write operation
- **Side Effects**: May trigger automatic rotation if high water mark exceeded

### `remove(scope, key)` (VersionedKVStore)
- **Purpose**: Remove a key from the specified scope
- **Behavior**: Async operation that writes deletion entry to journal
- **Returns**: Timestamp of the deletion
- **Note**: Deletion is recorded with timestamp for point-in-time recovery

### `rotate_journal()` (VersionedKVStore)
- **Purpose**: Manually trigger journal rotation
- **Behavior**: Async operation that creates new journal and archives old one
- **Side Effects**: Compresses archived journal asynchronously
- **Use Case**: Proactive rotation before expected high write volume

### `recover_at_timestamp(timestamp)` (VersionedRecovery)
- **Purpose**: Reconstruct state at a specific point in time
- **Behavior**: Replays journal entries up to the target timestamp
- **Returns**: Map of scoped state at that timestamp
- **Use Case**: Audit trails, rollback scenarios, historical queries

## Architecture Evolution

The system has evolved to focus on versioned storage:
1. **VersionedKVStore**: Current primary implementation with timestamp tracking
2. **Async Architecture**: Write operations are async for efficient compression
3. **Automatic Management**: Rotation and compression handled automatically

## Working with the Code

### When Adding New Features
1. **Use async/await**: All write operations are async
2. **Handle timestamps**: Every write returns a timestamp for tracking
3. **Consider rotation**: New features should work correctly across rotation boundaries
4. **Test recovery**: Ensure new features work with point-in-time recovery

### When Debugging
1. **Check timestamps**: Verify timestamp ordering and monotonicity
2. **Examine archived journals**: Look for `.zz` compressed archives
3. **Test async operations**: Ensure proper async context and awaiting
4. **Verify compression**: Check that archived journals are actually compressed

## Future Considerations

### Potential Improvements
1. **Metrics**: Track rotation frequency, compression ratios, and recovery times
2. **Adaptive Buffer Sizes**: Automatically adjust buffer size based on usage patterns
3. **Parallel Compression**: Compress multiple archived journals concurrently
4. **Incremental Recovery**: Optimize recovery for large history sets
5. **Retention Policies**: Enhanced automatic cleanup of old archived journals

### API Stability
The current versioned store provides a robust foundation for persistent state management with full audit trail capabilities. The async architecture enables efficient background operations without blocking application threads.

## Summary

The bd-resilient-kv system provides versioned persistent key-value storage with point-in-time recovery capabilities. The key insight is that every write operation returns a timestamp, enabling precise state tracking and historical queries. The async architecture enables efficient background compression of archived journals without blocking application threads.

**Current State**: The system focuses on VersionedKVStore as the primary storage implementation. Key features include:

- **Versioned Storage**: Every write tracked with timestamps
- **Point-in-Time Recovery**: Reconstruct state at any historical timestamp
- **Automatic Rotation**: Journal rotation triggered by high water mark
- **Async Compression**: Archived journals compressed asynchronously
- **Cross-Rotation Recovery**: Recovery works seamlessly across rotation boundaries
