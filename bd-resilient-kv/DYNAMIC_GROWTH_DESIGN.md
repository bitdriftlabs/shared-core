# Dynamic Growth Design for Persistent Store

**Date**: 2025-11-22  
**Status**: Design Discussion  
**Related Files**: 
- `bd-resilient-kv/src/versioned_kv_journal/store.rs`
- `bd-resilient-kv/src/versioned_kv_journal/memmapped_journal.rs`
- `bd-resilient-kv/src/versioned_kv_journal/journal.rs`

## Problem Statement

Currently, the `PersistentStore` in `VersionedKVStore` uses a fixed `buffer_size` for its memory-mapped journal file. The goal is to enable the persistence store to:

1. **Start small**: Initialize with a minimal file size (e.g., 64KB or 256KB)
2. **Grow dynamically**: Expand the file as needed when data grows
3. **Respect capacity limits**: Cap growth at a configurable maximum (similar to in-memory store's `max_bytes`)
4. **Optimize disk usage**: Avoid pre-allocating large files when they're not needed

### Current State

**In-Memory Store** (`InMemoryStore`):
- Has `max_bytes: Option<usize>` capacity limiting
- Grows dynamically as entries are added
- Tracks size with `estimate_entry_size()`
- Returns `UpdateError::CapacityExceeded` when limit reached

**Persistent Store** (`PersistentStore`):
- Uses fixed `buffer_size: usize`
- File is created/resized to full size on initialization
- No dynamic growth capability
- Rotation creates new journal with same fixed size

## Proposed Solution

### 1. Configuration Changes

Add dynamic growth parameters to `PersistentStore`:

```rust
struct PersistentStore {
  journal: MemMappedVersionedJournal<StateValue>,
  dir_path: PathBuf,
  journal_name: String,
  
  // Growth configuration
  min_buffer_size: usize,              // Starting/minimum buffer size (e.g., 256KB)
  max_capacity_bytes: Option<usize>,   // Total capacity limit (e.g., 10MB)
  current_buffer_size: usize,          // Current actual buffer size
  growth_factor: f32,                   // Growth multiplier (e.g., 1.5 or 2.0)
  
  high_water_mark_ratio: Option<f32>,
  current_generation: u64,
  cached_map: ScopedMaps,
  retention_registry: Arc<RetentionRegistry>,
}
```

### 2. Growth Strategy: Geometric Growth at Rotation Time

**Why Rotation Time?**
- Natural compaction point - we know exact space requirements
- Already has error handling infrastructure
- Minimizes complexity - growth is predictable and controlled
- Aligns with existing architecture

**Growth Algorithm**:

```rust
impl PersistentStore {
  async fn rotate_journal(&mut self) -> anyhow::Result<Rotation> {
    // Step 1: Calculate space needed for compacted state
    let compacted_size = self.estimate_compacted_size();
    
    // Step 2: Determine new buffer size
    let new_buffer_size = self.calculate_next_buffer_size(compacted_size);
    
    // Step 3: Create new journal with calculated size
    let new_journal = MemMappedVersionedJournal::new(
      &new_journal_path,
      new_buffer_size,  // Dynamic size, not fixed
      self.high_water_mark_ratio,
      time_provider,
      compacted_entries,
    )?;
    
    // Step 4: Update current buffer size
    self.current_buffer_size = new_buffer_size;
    self.journal = new_journal;
    
    // ... rest of rotation logic (archival, cleanup, etc.)
  }
  
  fn calculate_next_buffer_size(&self, required_size: usize) -> usize {
    // Add headroom for future writes (e.g., 20% buffer)
    let target_size = (required_size as f32 * 1.2) as usize;
    
    // No growth needed if current buffer is sufficient
    if target_size <= self.current_buffer_size {
      return self.current_buffer_size;
    }
    
    // Calculate geometric growth
    let grown_size = (self.current_buffer_size as f32 * self.growth_factor) as usize;
    
    // Use larger of: geometric growth or target size
    let new_size = grown_size.max(target_size);
    
    // Apply capacity cap
    if let Some(max) = self.max_capacity_bytes {
      new_size.min(max)
    } else {
      new_size
    }
  }
  
  fn estimate_compacted_size(&self) -> usize {
    self.cached_map.iter()
      .map(|(_, key, tv)| {
        // Estimate: key + value + timestamp + frame overhead
        key.len() 
          + protobuf::MessageDyn::compute_size_dyn(&tv.value)
            .try_into()
            .unwrap_or(0)
          + 8  // u64 timestamp
          + 50 // Frame header, CRC, varint overhead
      })
      .sum()
  }
}
```

### 3. API Design

Update constructors to accept growth parameters:

```rust
pub async fn new<P: AsRef<Path>>(
  dir_path: P,
  name: &str,
  initial_buffer_size: usize,           // Renamed from buffer_size
  max_capacity_bytes: Option<usize>,    // NEW: Total capacity limit
  high_water_mark_ratio: Option<f32>,
  time_provider: Arc<dyn TimeProvider>,
  retention_registry: Arc<RetentionRegistry>,
) -> anyhow::Result<(Self, DataLoss)>
```

**Parameters**:
- `initial_buffer_size`: Starting size (e.g., 256KB)
- `max_capacity_bytes`: Optional hard limit (e.g., 10MB)
- Growth factor hardcoded to 1.5x or 2.0x (can be made configurable later)

### 4. Handling Existing Files: Remote Config Problem

**Challenge**: Process restarts with new remote config values that differ from file's actual size.

**Key Questions**:
1. Can we reliably use file size to determine current buffer size?
2. What if remote config changes `initial_buffer_size` or `max_capacity_bytes`?
3. How do we handle corruption that affects file size?

#### File Size Reliability Analysis

**Good News**: The journal format has built-in validation!

From `journal.rs:92-100`:
```rust
fn read_position(buffer: &[u8]) -> anyhow::Result<usize> {
  let position = u64::from_le_bytes(position_bytes);
  let buffer_len = buffer.len();
  if position >= buffer_len {
    anyhow::bail!("Invalid position: {position}, buffer size: {buffer_len}");
  }
  // ...
}
```

The `position` field in the header tracks how much data is written. If file size is corrupted smaller than the data, this check catches it immediately.

#### Corruption Scenarios

**Scenario 1: File Size Too Small** ✓ Detected
```
Actual data: 800KB written (position = 800KB in header)
File size corrupted to: 500KB
→ read_position() fails: position >= buffer_len
→ Falls back to DataLoss::Total, creates fresh journal with config size
```

**Scenario 2: File Size Too Large** ✓ Mostly OK
```
Actual data: 500KB written (position = 500KB)
File size corrupted to: 2MB
→ Opens fine, just wastes disk space
→ Next rotation compacts to actual data size
```

**Scenario 3: File Size Slightly Wrong** ⚠️ Edge Case
```
Should be: 1,048,576 bytes (1MB)
Actually is: 1,048,500 bytes (76 bytes short)
→ Might work if last frame fits
→ Could fail during write operations
→ Would trigger rotation earlier than expected
```

#### Recommended Approach: File Size with Validation

Use file size to determine current buffer size, with defensive checks:

```rust
async fn determine_buffer_size(
  journal_path: &Path,
  config_initial_size: usize,
  config_max_size: Option<usize>,
) -> anyhow::Result<usize> {
  if let Ok(metadata) = tokio::fs::metadata(journal_path).await {
    let file_size = metadata.len() as usize;
    
    // Sanity check: minimum size
    const MIN_BUFFER_SIZE: usize = 17 + 4; // HEADER_SIZE + min frame
    if file_size < MIN_BUFFER_SIZE {
      log::warn!(
        "Journal file {} too small ({} bytes), treating as new",
        journal_path.display(),
        file_size
      );
      return Ok(config_initial_size);
    }
    
    // Sanity check: maximum reasonable size
    const MAX_REASONABLE_SIZE: usize = 1024 * 1024 * 1024; // 1GB
    if file_size > MAX_REASONABLE_SIZE {
      log::warn!(
        "Journal file {} suspiciously large ({} bytes), treating as new",
        journal_path.display(),
        file_size
      );
      return Ok(config_initial_size);
    }
    
    // Apply new config's max capacity as a cap
    let size = if let Some(max) = config_max_size {
      file_size.min(max)
    } else {
      file_size
    };
    
    // Consider growing to new baseline if config increased
    if size < config_initial_size 
       && config_max_size.map_or(true, |max| config_initial_size <= max) {
      return Ok(config_initial_size);
    }
    
    Ok(size)
  } else {
    // New file - use config
    Ok(config_initial_size)
  }
}
```

#### Reconciliation Policy for Remote Config

When config changes between process restarts:

| Scenario | File Size | New Initial | New Max | Action |
|----------|-----------|-------------|---------|--------|
| Normal growth | 2MB | 256KB | 10MB | Keep 2MB (grown naturally) |
| Config increased max | 2MB | 256KB | 20MB | Keep 2MB (allow further growth) |
| Config decreased max | 2MB | 256KB | 1MB | **Shrink to 1MB** at next rotation |
| Config increased initial | 100KB | 512KB | 10MB | **Grow to 512KB** immediately |
| Corrupted small | 100KB (corrupted) | 256KB | 10MB | Fail validation → fresh 256KB |
| Corrupted large | 5GB (corrupted) | 256KB | 10MB | Fail validation → fresh 256KB |

**Key Principles**:
1. **File size is "memory" of natural growth**
2. **New max capacity takes effect immediately** (shrink if needed)
3. **New initial size applies if file is below it** (grow if needed)
4. **Validation failures → fresh start** with config values

### 5. Advantages of This Design

**Efficiency**:
- Start small, grow only as needed
- Optimal disk usage
- Natural growth pattern (geometric)

**Safety**:
- Built-in validation catches corruption
- Max capacity prevents unbounded growth
- Graceful degradation (DataLoss::Total on corruption)

**Simplicity**:
- Growth only at rotation (controlled point)
- Existing error handling works
- No complex resize-during-operation logic

**Compatibility**:
- Works with existing mmap infrastructure
- Natural extension of rotation mechanism
- Minimal code changes needed

### 6. Alternative Considered: Store Size in Filename

Encode buffer size in journal filename:
```
// Instead of: store.jrn.5
// Use: store.jrn.5.sz1048576
```

**Pros**:
- Size visible at a glance
- Easy debugging
- Validates file size matches expectation

**Cons**:
- More complex file management
- Harder to parse filenames
- Not necessary given built-in validation

**Decision**: Not recommended. The `position` field in the journal header provides sufficient validation.

### 7. Edge Cases and Error Handling

#### Growth Failure Scenarios

**Disk Full**:
```rust
// During rotation, if file.set_len() fails due to disk full
Err(e) if e.kind() == io::ErrorKind::OutOfSpace => {
  log::error!("Cannot grow journal: disk full");
  // Option 1: Keep current size, fail rotation
  // Option 2: Try shrinking by dropping old data (if retention allows)
  return Err(e.into());
}
```

**Cannot Fit Compacted State**:
```rust
// If compacted data > max_capacity_bytes
if compacted_size > max_capacity_bytes {
  log::error!(
    "Compacted state ({} bytes) exceeds max capacity ({} bytes)",
    compacted_size, max_capacity_bytes
  );
  // This is a configuration error - data volume exceeds configured limit
  // Options:
  // 1. Return error (current data cannot be preserved)
  // 2. Implement LRU eviction (drop oldest entries)
  // 3. Allow temporary overflow (breach the limit)
  return Err(anyhow!("Data volume exceeds configured capacity"));
}
```

**Growth Hits Max Capacity**:
```rust
// Normal case - we've hit the limit
if calculated_size >= max_capacity_bytes {
  log::info!("Journal reached max capacity: {} bytes", max_capacity_bytes);
  // Continue with max_capacity_bytes as the size
  // Future rotations will maintain this size
  // If data grows beyond this, operations will eventually fail
  return max_capacity_bytes;
}
```

### 8. Implementation Checklist

- [ ] Add `min_buffer_size`, `max_capacity_bytes`, `current_buffer_size`, `growth_factor` fields to `PersistentStore`
- [ ] Implement `calculate_next_buffer_size()` with geometric growth
- [ ] Implement `estimate_compacted_size()` for accurate space calculation
- [ ] Implement `determine_buffer_size()` for existing file handling
- [ ] Update `rotate_journal()` to use dynamic sizing
- [ ] Update `new()` and `open_existing()` constructors with new parameters
- [ ] Add validation checks for file size sanity
- [ ] Add logging for growth events and decisions
- [ ] Update tests to cover:
  - [ ] Normal growth pattern
  - [ ] Max capacity limiting
  - [ ] Config changes on restart
  - [ ] Corruption detection
  - [ ] Growth failure handling
- [ ] Update documentation and AGENTS.md

### 9. Testing Strategy

**Test Cases**:

1. **Normal Growth Pattern**
   ```rust
   // Start with small buffer, insert data, trigger multiple rotations
   // Verify file size grows geometrically
   initial: 256KB → 384KB → 576KB → 864KB → 1.3MB → ...
   ```

2. **Max Capacity Limiting**
   ```rust
   // Set max_capacity = 1MB
   // Grow until hitting limit
   // Verify size caps at 1MB
   ```

3. **Config Changes on Restart**
   ```rust
   // Scenario A: Increase max capacity
   // Scenario B: Decrease max capacity (should shrink at next rotation)
   // Scenario C: Increase initial size (should grow immediately)
   ```

4. **Corruption Detection**
   ```rust
   // Manually corrupt file size
   // Verify validation catches it
   // Verify fallback to fresh journal
   ```

5. **Growth at Rotation**
   ```rust
   // Fill journal to trigger rotation
   // Verify new journal is larger if needed
   // Verify compaction works correctly
   ```

6. **Edge Case: Compacted State Too Large**
   ```rust
   // Create scenario where compacted data > max_capacity
   // Verify error handling
   ```

### 10. Performance Considerations

**File System Operations**:
- `file.set_len()` is generally fast (metadata update)
- May be slow on some filesystems (especially if actually allocating blocks)
- Consider using `fallocate()` on Linux for better performance

**Memory Mapping**:
- Remapping is relatively cheap (kernel operation)
- No data copying needed
- Works seamlessly with existing mmap infrastructure

**Growth Overhead**:
- Only occurs at rotation (infrequent)
- Geometric growth minimizes number of resizes
- Typical growth: 5-10 resizes to reach max capacity

### 11. Future Enhancements

**Adaptive Growth Factor**:
```rust
// Adjust growth rate based on usage patterns
if recent_growth_rate_high {
  growth_factor = 2.0; // Aggressive growth
} else {
  growth_factor = 1.5; // Conservative growth
}
```

**Shrinking**:
```rust
// If compacted size is much smaller than buffer, consider shrinking
if compacted_size < current_buffer_size / 3 {
  new_size = (compacted_size as f32 * 2.0) as usize; // 2x headroom
}
```

**Multiple File Approach**:
- Instead of growing single file, use multiple fixed-size segments
- More complex but avoids large file resizing
- Better for very large datasets

## Summary

The recommended approach is to implement **geometric growth at rotation time** using **file size with validation** to handle restarts and config changes. This provides:

- ✅ Simple implementation (growth at natural compaction point)
- ✅ Safe (built-in validation via position field)
- ✅ Flexible (respects remote config changes)
- ✅ Efficient (geometric growth, optimal disk usage)
- ✅ Compatible (works with existing infrastructure)

The key insight is that the journal's `position` field provides built-in validation against file size corruption, making it safe to trust file size as the source of truth for the current buffer size while still respecting new configuration values.

## References

- Main implementation: `bd-resilient-kv/src/versioned_kv_journal/store.rs`
- Journal format: `bd-resilient-kv/src/versioned_kv_journal/journal.rs`
- Mmap layer: `bd-resilient-kv/src/versioned_kv_journal/memmapped_journal.rs`
- In-memory store (reference): `store.rs:482-589` (`InMemoryStore` implementation)
