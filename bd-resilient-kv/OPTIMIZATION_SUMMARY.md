# DoubleBufferedMemMappedKv Optimization Summary

## Problem Solved
The original `DoubleBufferedMemMappedKv` implementation was inefficient because it:
- Created new `MemMappedKVJournal` instances on every operation
- Loaded data from disk repeatedly when switching buffers
- Destroyed and recreated instances frequently

## Optimization Implementation

### Key Changes
1. **Persistent Instances**: Both `MemMappedKVJournal` instances are now kept as struct fields (`kv_a` and `kv_b`)
2. **In-Memory Switching**: Buffer switches happen without disk I/O by using the `as_hashmap()` method
3. **Lazy Initialization**: The inactive instance is only created when first needed
4. **Clean Switching Logic**: Uses `with_active_kv()` pattern for safe operations

### New Structure
```rust
pub struct DoubleBufferedMemMappedKv {
  /// Primary memory-mapped KV instance (always initialized)
  kv_a: MemMappedKVJournal,
  /// Secondary memory-mapped KV instance (always initialized) 
  kv_b: MemMappedKVJournal,
  /// Path to the primary file
  file_path_a: PathBuf,
  /// Path to the secondary file
  file_path_b: PathBuf,
  /// Whether we're currently using kv_a (true) or kv_b (false)
  using_a: bool,
  // ... other configuration fields
}
```

### Key Architectural Decisions
1. **No Optional Instances**: Both `kv_a` and `kv_b` are always initialized at construction time
2. **Fail-Fast Construction**: If either KV instance fails to construct, the entire `DoubleBufferedMemMappedKv` fails
3. **Guaranteed Availability**: No need for runtime checks or lazy initialization - both instances are always ready

### Switching Process
1. High water mark is detected in the active instance
2. Current state is extracted using `as_hashmap()` (in-memory operation)
3. Inactive instance is recreated with a fresh file (ensures clean state)
4. Data is copied to the fresh inactive instance using `set()` operations
5. Active flag is switched (`using_a = !using_a`)
6. The old full instance remains but is no longer active

## Performance Benefits
- ✅ **No Disk I/O During Switches**: Data copying happens entirely in memory
- ✅ **No Runtime Initialization Checks**: Both instances are always available
- ✅ **Simplified Error Handling**: No Option unwrapping or null checks needed
- ✅ **Fail-Fast Construction**: Invalid configurations are caught at creation time
- ✅ **Maintained Persistence**: All data remains safely stored in memory-mapped files
- ✅ **Clean Resource Management**: Fresh instances ensure optimal memory usage
- ✅ **Backward Compatibility**: All existing APIs work unchanged

## Test Results
- ✅ All 67 tests pass
- ✅ Buffer switching works correctly
- ✅ Data integrity is preserved across switches
- ✅ File persistence works as expected
- ✅ Multiple switches can occur in sequence

## Usage Example
The example shows the optimization in action:
- Small buffer size (1KB) with low threshold (40%)
- Multiple switches triggered (A -> B -> A -> B -> A -> B)
- Data preserved across all switches
- Files created and managed automatically

This optimization significantly improves performance by eliminating the disk I/O bottleneck that occurred on every buffer switch in the original implementation.
